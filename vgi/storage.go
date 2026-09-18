// Copyright 2025, 2026 Query Farm LLC - https://query.farm

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

// ShardedBackend is implemented by FunctionStorage backends that route remotely
// on a per-attach shard key (the Cloudflare Durable Object backend). ForShard
// returns a view of the backend pinned to one shard key; backends that ignore
// sharding (SQLite) don't implement it. Lets ExecutionStorage attach the shard
// key without threading it through all ~22 FunctionStorage methods.
type ShardedBackend interface {
	// ForShard returns a FunctionStorage view of the backend pinned to shardKey.
	ForShard(shardKey string) FunctionStorage
}

// ExecutionStorage binds a FunctionStorage to one execution_id.
type ExecutionStorage struct {
	mu          sync.Mutex
	back        FunctionStorage
	executionID []byte
	// shardKey routes per logical ATTACH for the CfDo backend (att-<hex uuid>);
	// "" for non-attach / non-sharding paths. The framework sets it from the
	// unwrapped attach UUID when the execution's storage is created.
	shardKey string
	// substreamID is the client-minted InitRequest.substream_id of the one
	// stream this view serves, and the key Put stores that stream's worker
	// state under. nil on the execution-wide instance the worker caches, and
	// for a stream whose client sent none; Put then keys by process id.
	substreamID []byte
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

// SetShardKey pins the per-attach routing key (att-<hex uuid>). A no-op for
// backends that ignore sharding; used by the CfDo backend to route to the
// right Durable Object. Empty means "no attach" (CfDo would reject it).
func (s *ExecutionStorage) SetShardKey(shardKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shardKey = shardKey
}

// forSubstream returns the view of s one stream's ProcessParams.Storage should
// carry: the same backend, execution and shard, with Put keyed by that
// stream's substream id. s itself when the client sent no substream id, so
// such a stream behaves exactly as it did before the key existed.
//
// A view rather than a field set on s, because s is shared: the worker caches
// one ExecutionStorage per execution, and every stream of that execution a
// process serves -- many, under the launcher, TCP or HTTP -- holds it.
func (s *ExecutionStorage) forSubstream(substreamID []byte) *ExecutionStorage {
	if len(substreamID) == 0 {
		return s
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return &ExecutionStorage{
		back:        s.back,
		executionID: s.executionID,
		shardKey:    s.shardKey,
		substreamID: append([]byte(nil), substreamID...),
	}
}

// workerKey is the slot Put stores this stream's state under: its substream
// id, or -- only when the client sent none -- this process's id.
func (s *ExecutionStorage) workerKey() []byte {
	s.mu.Lock()
	id := s.substreamID
	s.mu.Unlock()
	if len(id) > 0 {
		return id
	}
	return int64Key(int64(os.Getpid()))
}

// ExecutionID returns the bound execution_id, or nil if unset.
func (s *ExecutionStorage) ExecutionID() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executionID
}

func (s *ExecutionStorage) resolve() (FunctionStorage, []byte, error) {
	s.mu.Lock()
	back, exec, shardKey := s.back, s.executionID, s.shardKey
	s.mu.Unlock()
	if back == nil {
		return nil, nil, errors.New("storage: backend not set (Worker should call SetBackend)")
	}
	if exec == nil {
		return nil, nil, errStorageNotInitialized
	}
	// Pin a sharded backend to this attach's shard key, if it shards remotely.
	if shardKey != "" {
		if sb, ok := back.(ShardedBackend); ok {
			back = sb.ForShard(shardKey)
		}
	}
	return back, exec, nil
}

// ---------------------------------------------------------------------------
// Attach state (bound to an ATTACH scope; persists across queries)
// ---------------------------------------------------------------------------

// errAttachStateUnsupported is returned when the configured backend doesn't
// implement AttachStateStorage.
var errAttachStateUnsupported = errors.New("storage: backend does not support attach state (required by attach-scoped functions)")

// AttachStore is a namespaced, ordered key/value view bound to one ATTACH
// scope. Unlike ExecutionStorage's execution-scoped state, it persists for the
// life of the shared backend, so it survives the fresh worker process a
// subprocess-transport query spawns. Used by attach-scoped fixtures such as the
// accumulate example.
type AttachStore struct {
	back  AttachStateStorage
	scope []byte
}

// newAttachStore binds an AttachStateStorage-capable backend to one scope.
func newAttachStore(back FunctionStorage, scope []byte) (*AttachStore, error) {
	if back == nil {
		return nil, errors.New("storage: backend not set (Worker should call SetBackend)")
	}
	as, ok := back.(AttachStateStorage)
	if !ok {
		return nil, errAttachStateUnsupported
	}
	return &AttachStore{back: as, scope: scope}, nil
}

// ScanOption configures an AttachStore.Scan. See WithRange / WithStart /
// WithEnd / WithReverse / WithLimit.
type ScanOption func(*AttachScanOptions)

// WithStart bounds a scan to keys >= start (inclusive).
func WithStart(start []byte) ScanOption { return func(o *AttachScanOptions) { o.Start = start } }

// WithEnd bounds a scan to keys < end (exclusive).
func WithEnd(end []byte) ScanOption { return func(o *AttachScanOptions) { o.End = end } }

// WithRange bounds a scan to the half-open key range [start, end) (a nil bound
// is open on that side).
func WithRange(start, end []byte) ScanOption {
	return func(o *AttachScanOptions) { o.Start, o.End = start, end }
}

// WithReverse returns the scan in descending key order.
func WithReverse() ScanOption { return func(o *AttachScanOptions) { o.Reverse = true } }

// WithLimit caps the scan at n rows (n <= 0 means no limit).
func WithLimit(n int) ScanOption { return func(o *AttachScanOptions) { o.Limit = n } }

// Put stores or replaces value under (ns, key) in this attach scope.
func (a *AttachStore) Put(ns, key, value []byte) error {
	return a.back.AttachStatePut(a.scope, ns, key, value)
}

// Get returns the value under (ns, key), or (nil, nil) if absent.
func (a *AttachStore) Get(ns, key []byte) ([]byte, error) {
	return a.back.AttachStateGet(a.scope, ns, key)
}

// Scan returns the (key, value) pairs under ns ordered by key. With no options
// it returns the whole namespace ascending; pass WithRange/WithReverse/WithLimit
// to bound it.
func (a *AttachStore) Scan(ns []byte, opts ...ScanOption) ([]AttachStateKV, error) {
	var o AttachScanOptions
	for _, opt := range opts {
		opt(&o)
	}
	return a.back.AttachStateScan(a.scope, ns, o)
}

// DeleteKey removes one key under ns. No-op if absent.
func (a *AttachStore) DeleteKey(ns, key []byte) error {
	return a.back.AttachStateDeleteKey(a.scope, ns, key)
}

// DeleteNS removes every key under ns.
func (a *AttachStore) DeleteNS(ns []byte) error {
	return a.back.AttachStateDeleteNS(a.scope, ns)
}

// DeleteRange removes every key in the half-open range [start, end) under ns
// (a nil bound is open on that side) and returns the number removed.
func (a *AttachStore) DeleteRange(ns, start, end []byte) (int, error) {
	return a.back.AttachStateDeleteRange(a.scope, ns, start, end)
}

// Drain atomically reads and removes every (key, value) under ns, returning
// them ordered by key.
func (a *AttachStore) Drain(ns []byte) ([]AttachStateKV, error) {
	return a.back.AttachStateDrain(a.scope, ns)
}

// CounterGet returns the int64 counter under (ns, key), or 0 if absent.
func (a *AttachStore) CounterGet(ns, key []byte) (int64, error) {
	return a.back.AttachCounterGet(a.scope, ns, key)
}

// CounterAdd atomically adds delta to the counter under (ns, key) and returns
// the new value.
func (a *AttachStore) CounterAdd(ns, key []byte, delta int64) (int64, error) {
	return a.back.AttachCounterAdd(a.scope, ns, key, delta)
}

// CounterSet overwrites the counter under (ns, key) with value.
func (a *AttachStore) CounterSet(ns, key []byte, value int64) error {
	return a.back.AttachCounterSet(a.scope, ns, key, value)
}

// CounterDelete removes the counter under (ns, key). No-op if absent.
func (a *AttachStore) CounterDelete(ns, key []byte) error {
	return a.back.AttachCounterDelete(a.scope, ns, key)
}

// AttachStore returns an attach-scoped key/value store bound to scope, using
// this execution's underlying backend. The scope is typically
// ProcessParams.AttachScope. Errors if the backend lacks AttachStateStorage.
func (s *ExecutionStorage) AttachStore(scope []byte) (*AttachStore, error) {
	s.mu.Lock()
	back := s.back
	s.mu.Unlock()
	return newAttachStore(back, scope)
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
// empty or the execution_id was never pushed (no registration).
func (s *ExecutionStorage) QueuePop() ([]byte, error) {
	back, exec, err := s.resolve()
	if err != nil {
		return nil, err
	}
	return back.QueuePop(exec)
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
// Worker state (bound to execution_id, keyed per substream)
// ---------------------------------------------------------------------------

// Put stores this stream's state, replacing whatever it stored before (upsert
// semantics): one row per substream of the execution.
//
// The row is keyed by the stream's InitRequest.substream_id -- the random id
// the client mints per substream and sends on its init, every tick and its
// finalize -- not by the process id. Storage is already scoped to the
// execution, which every connection of a fanned-out scan shares, so the key
// must tell those connections apart; a pid only does when each connection is
// its own process. Under the launcher, TCP or a threaded HTTP server one
// process serves many of them, and a per-process key let them overwrite each
// other so Collect/Snapshot (and so a finalize) undercounted. The pid remains
// only for a client that sends no substream_id, which is keyed exactly as
// before.
func (s *ExecutionStorage) Put(data []byte) error {
	back, exec, err := s.resolve()
	if err != nil {
		return err
	}
	return back.WorkerPut(exec, s.workerKey(), data)
}

// Snapshot returns every stored worker value of the execution -- one per
// substream that called Put (per process, for a client that sends no
// substream_id) -- without removing them.
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

// Collect returns every stored worker value of the execution -- one per
// substream that called Put (per process, for a client that sends no
// substream_id) -- and removes them.
func (s *ExecutionStorage) Collect() ([][]byte, error) {
	back, exec, err := s.resolve()
	if err != nil {
		return nil, err
	}
	return back.WorkerCollect(exec)
}

// Cleanup drops every record under this execution_id: all scope-keyed state
// (worker, scan-worker, aggregate, attach, log, counters — via ExecutionClear)
// plus the separately-keyed work queue. The underlying FunctionStorage is owned
// by the Worker and is shared across executions; it is NOT closed here.
func (s *ExecutionStorage) Cleanup() {
	back, exec, err := s.resolve()
	if err != nil {
		return
	}
	_, _ = back.ExecutionClear(exec)
	_, _ = back.QueueClear(exec)
}
