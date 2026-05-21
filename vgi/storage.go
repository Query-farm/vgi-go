// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// ---------------------------------------------------------------------------
// ExecutionStorage: per-execution view onto a shared FunctionStorage
//
// Thin binding wrapper around a FunctionStorage backend (selected once at
// worker startup — SQLite locally, Cloudflare Durable Object for HTTP
// deployments, etc.). The methods on this type are scoped to one
// execution_id so user code in Process/NewState/OnInit doesn't have to
// thread the execution_id through every call.
//
// Existing examples that talk to params.Storage continue to work unchanged.
// ---------------------------------------------------------------------------

var errStorageNotInitialized = errors.New("storage: execution ID not set")

// ExecutionStorage binds a FunctionStorage to one execution_id.
type ExecutionStorage struct {
	mu          sync.Mutex
	back        FunctionStorage
	executionID []byte
}

// NewExecutionStorage creates a new unbound ExecutionStorage. SetBackend and
// SetExecutionID must be called before use; the Worker does this for you.
func NewExecutionStorage() *ExecutionStorage {
	return &ExecutionStorage{}
}

// SetBackend wires a FunctionStorage into this binding wrapper. Called once
// by the framework before SetExecutionID.
func (s *ExecutionStorage) SetBackend(back FunctionStorage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.back = back
}

// SetExecutionID binds this wrapper to one execution_id.
func (s *ExecutionStorage) SetExecutionID(execID []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executionID = execID
	return nil
}

// ExecutionID returns the bound execution_id, or nil if unset.
func (s *ExecutionStorage) ExecutionID() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executionID
}

func (s *ExecutionStorage) resolve() (FunctionStorage, []byte, error) {
	s.mu.Lock()
	back, exec := s.back, s.executionID
	s.mu.Unlock()
	if back == nil {
		return nil, nil, errors.New("storage: backend not set (Worker should call SetBackend)")
	}
	if exec == nil {
		return nil, nil, errStorageNotInitialized
	}
	return back, exec, nil
}

// ---------------------------------------------------------------------------
// Work queue (bound to execution_id)
// ---------------------------------------------------------------------------

// QueuePush appends items to the per-execution work queue.
func (s *ExecutionStorage) QueuePush(items [][]byte) error {
	back, exec, err := s.resolve()
	if err != nil {
		return err
	}
	_, err = back.QueuePush(exec, items)
	return err
}

// QueuePop atomically claims one item. Returns (nil, nil) when the queue is
// registered but empty. ErrUnknownInvocation is masked as (nil, nil) for
// backwards compatibility with consumers that poll without registering.
func (s *ExecutionStorage) QueuePop() ([]byte, error) {
	back, exec, err := s.resolve()
	if err != nil {
		return nil, err
	}
	item, err := back.QueuePop(exec)
	if errors.Is(err, ErrUnknownInvocation) {
		return nil, nil
	}
	return item, err
}

// QueuePushBatches serializes record batches and appends them.
func (s *ExecutionStorage) QueuePushBatches(batches []arrow.RecordBatch) error {
	items := make([][]byte, 0, len(batches))
	for i, batch := range batches {
		data, err := SerializeRecordBatch(batch)
		if err != nil {
			return fmt.Errorf("storage: serializing batch %d: %w", i, err)
		}
		items = append(items, data)
	}
	return s.QueuePush(items)
}

// QueuePopBatch claims and deserializes the next batch, or (nil, nil) if empty.
func (s *ExecutionStorage) QueuePopBatch() (arrow.RecordBatch, error) {
	data, err := s.QueuePop()
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	batch, err := DeserializeRecordBatch(data)
	if err != nil {
		return nil, fmt.Errorf("storage: deserializing batch: %w", err)
	}
	return batch, nil
}

// ---------------------------------------------------------------------------
// State log (bound to execution_id, keyed append-only log with a cursor)
// ---------------------------------------------------------------------------

// errStateLogUnsupported is returned when the configured backend doesn't
// implement StateLogStorage.
var errStateLogUnsupported = errors.New("storage: backend does not support the state log (required by table-buffering functions)")

func (s *ExecutionStorage) stateLog() (StateLogStorage, []byte, error) {
	back, exec, err := s.resolve()
	if err != nil {
		return nil, nil, err
	}
	sl, ok := back.(StateLogStorage)
	if !ok {
		return nil, nil, errStateLogUnsupported
	}
	return sl, exec, nil
}

// StateAppend appends value under key in the execution-scoped log, returning
// the new monotonic log id.
func (s *ExecutionStorage) StateAppend(key, value []byte) (int64, error) {
	sl, exec, err := s.stateLog()
	if err != nil {
		return 0, err
	}
	return sl.StateAppend(exec, key, value)
}

// StateLogScan returns entries under key with id > afterID (use -1 from the
// start), ordered by id. limit <= 0 means no limit.
func (s *ExecutionStorage) StateLogScan(key []byte, afterID int64, limit int) ([]StateLogEntry, error) {
	sl, exec, err := s.stateLog()
	if err != nil {
		return nil, err
	}
	return sl.StateLogScan(exec, key, afterID, limit)
}

// StateLogClear removes all state-log rows for this execution.
func (s *ExecutionStorage) StateLogClear() error {
	sl, exec, err := s.stateLog()
	if err != nil {
		return err
	}
	return sl.StateLogClear(exec)
}

// ---------------------------------------------------------------------------
// Worker state (bound to execution_id, keyed by os.Getpid())
// ---------------------------------------------------------------------------

// Put stores a value keyed by the current worker PID. Upsert semantics.
func (s *ExecutionStorage) Put(data []byte) error {
	back, exec, err := s.resolve()
	if err != nil {
		return err
	}
	return back.WorkerPut(exec, int64(os.Getpid()), data)
}

// Snapshot returns all stored worker values without removing them.
func (s *ExecutionStorage) Snapshot() ([][]byte, error) {
	back, exec, err := s.resolve()
	if err != nil {
		return nil, err
	}
	entries, err := back.WorkerScan(exec)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(entries))
	for i, e := range entries {
		out[i] = e.State
	}
	return out, nil
}

// Collect returns all stored worker values and removes them.
func (s *ExecutionStorage) Collect() ([][]byte, error) {
	back, exec, err := s.resolve()
	if err != nil {
		return nil, err
	}
	return back.WorkerCollect(exec)
}

// Cleanup drops every record under this execution_id (work queue + worker
// state). The underlying FunctionStorage is owned by the Worker and is
// shared across executions; it is NOT closed here.
func (s *ExecutionStorage) Cleanup() {
	back, exec, err := s.resolve()
	if err != nil {
		return
	}
	_, _ = back.QueueClear(exec)
	_, _ = back.WorkerCollect(exec)
}
