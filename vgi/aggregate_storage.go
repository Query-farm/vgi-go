// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"fmt"
	"sync"
)

// ---------------------------------------------------------------------------
// aggregateStorage: cross-process state for aggregate functions
//
// DuckDB spawns multiple worker subprocesses per parallel-aggregate query and
// expects state to flow across them (worker_A.update → worker_B.combine), so
// in-memory state per process loses data. We delegate to the worker-shared
// FunctionStorage backend (today: SQLite at ~/.local/state/vgi/storage.db;
// tomorrow: Cloudflare DO / Azure SQL for HTTP deployments).
//
// The stateBucket type pre-binds a (function_name, execution_id) view so
// call sites in aggregate_protocol.go stay terse. It maps onto the
// FunctionStorage interface's aggregate methods directly.
// ---------------------------------------------------------------------------

// aggregateStorage is a thin shim that lazily resolves a FunctionStorage
// backend from a setter and exposes the bucket(funcName, execID) pattern
// the protocol layer uses.
type aggregateStorage struct {
	mu      sync.Mutex
	back    FunctionStorage
	resolve func() (FunctionStorage, error) // set by the Worker
}

func newAggregateStorage() *aggregateStorage {
	return &aggregateStorage{}
}

// setResolver wires a lazy backend resolver. Called once by the Worker so
// the FunctionStorage is shared with ExecutionStorage and any future
// backends (Cloudflare DO, etc.).
func (s *aggregateStorage) setResolver(r func() (FunctionStorage, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolve = r
}

// ensureOpen resolves and caches the backend on first use.
func (s *aggregateStorage) ensureOpen() (FunctionStorage, error) {
	s.mu.Lock()
	if s.back != nil {
		back := s.back
		s.mu.Unlock()
		return back, nil
	}
	resolver := s.resolve
	s.mu.Unlock()
	if resolver == nil {
		return nil, fmt.Errorf("aggregateStorage: backend resolver not set (Worker should call setResolver)")
	}
	back, err := resolver()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.back == nil {
		s.back = back
	} else {
		back = s.back
	}
	s.mu.Unlock()
	return back, nil
}

// stateBucket binds aggregate operations to one (function_name, execution_id).
// Mirrors the original surface so aggregate_protocol.go and aggregate_helpers.go
// don't have to change.
type stateBucket struct {
	storage      *aggregateStorage
	functionName string
	executionID  []byte
	// shardKey routes per logical ATTACH (att-<hex uuid>) for the CfDo backend;
	// "" for non-sharding backends. Derived by the handler from the request's
	// unwrapped attach UUID.
	shardKey string
}

func (s *aggregateStorage) bucket(funcName string, execID []byte, shardKey string) *stateBucket {
	return &stateBucket{storage: s, functionName: funcName, executionID: execID, shardKey: shardKey}
}

// backend resolves the shared backend and, for a remote-sharding backend
// (CfDo), pins it to this bucket's shard key — so aggregate state routes to
// the same Durable Object as the execution's other storage.
func (b *stateBucket) backend() (FunctionStorage, error) {
	back, err := b.storage.ensureOpen()
	if err != nil {
		return nil, err
	}
	if b.shardKey != "" {
		if sb, ok := back.(ShardedBackend); ok {
			return sb.ForShard(b.shardKey), nil
		}
	}
	return back, nil
}

// loadStates fetches all states for the given group_ids. Returns a map keyed
// by group_id; gids absent from the result are not yet stored. Matches the
// pre-shared-backend surface so callers don't change.
func (b *stateBucket) loadStates(gids []int64) (map[int64][]byte, error) {
	back, err := b.backend()
	if err != nil {
		return nil, err
	}
	if len(gids) == 0 {
		return map[int64][]byte{}, nil
	}
	entries, err := back.AggregateStateGet(b.executionID, gids)
	if err != nil {
		return nil, err
	}
	out := make(map[int64][]byte, len(entries))
	for _, e := range entries {
		if e.State == nil {
			continue
		}
		out[e.GroupID] = e.State
	}
	return out, nil
}

// saveStates writes (gid, bytes) pairs.
func (b *stateBucket) saveStates(states map[int64][]byte) error {
	back, err := b.backend()
	if err != nil {
		return err
	}
	if len(states) == 0 {
		return nil
	}
	entries := make([]AggregateStateEntry, 0, len(states))
	for gid, data := range states {
		entries = append(entries, AggregateStateEntry{GroupID: gid, State: data})
	}
	return back.AggregateStatePut(b.executionID, entries)
}

// clear drops aggregate state and window partition rows for this
// execution_id. Const args are intentionally left behind: they're small,
// keyed by (execution_id, function_name), and reaped by the FunctionStorage's
// TTL sweep. Matches vgi-python which also has no per-call const-args clear.
func (b *stateBucket) clear() error {
	back, err := b.backend()
	if err != nil {
		return err
	}
	if err := back.AggregateStateClear(b.executionID); err != nil {
		return err
	}
	return back.AggregateWindowPartitionClear(b.executionID)
}

func (b *stateBucket) putConstArgs(args []byte) error {
	back, err := b.backend()
	if err != nil {
		return err
	}
	return back.AggregateConstArgsPut(b.executionID, b.functionName, args)
}

func (b *stateBucket) getConstArgs() ([]byte, error) {
	back, err := b.backend()
	if err != nil {
		return nil, err
	}
	return back.AggregateConstArgsGet(b.executionID, b.functionName)
}

func (b *stateBucket) putWindowPartition(partitionID int64, payload []byte) error {
	back, err := b.backend()
	if err != nil {
		return err
	}
	return back.AggregateWindowPartitionPut(b.executionID, partitionID, payload)
}

func (b *stateBucket) getWindowPartition(partitionID int64) ([]byte, error) {
	back, err := b.backend()
	if err != nil {
		return nil, err
	}
	return back.AggregateWindowPartitionGet(b.executionID, partitionID)
}

func (b *stateBucket) deleteWindowPartition(partitionID int64) error {
	back, err := b.backend()
	if err != nil {
		return err
	}
	return back.AggregateWindowPartitionDelete(b.executionID, partitionID)
}
