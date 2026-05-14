// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"errors"
	"time"
)

// ---------------------------------------------------------------------------
// FunctionStorage: pluggable shared storage for VGI distributed execution
//
// Mirrors the vgi-python FunctionStorage Protocol so a single interface can
// be implemented by multiple backends:
//
//   - SQLite      (default, local subprocess transport)
//   - Cloudflare  (Durable Object over HTTP — for HTTP/edge deployments)
//
// (vgi-python additionally has an Azure SQL backend; vgi-go can adopt it
// later if there's a user. The interface shape is the same.)
//
// The interface is "unbound": every method takes execution_id or
// transaction_opaque_data explicitly. For in-function ergonomics, BoundStorage
// pre-binds an execution_id and provides the slimmer per-function method
// set that table-function code typically uses through params.Storage.
//
// Implementation contract:
//   - All methods are safe for concurrent use from multiple goroutines and
//     (for the SQLite backend) multiple processes sharing the same DB file.
//   - "INSERT OR REPLACE" semantics for *Put methods: re-putting the same
//     key overwrites without error.
//   - Methods that take a list/slice may receive an empty slice; result
//     ordering is parallel to the input.
//   - QueuePop returns (nil, ErrUnknownInvocation) if the execution_id was
//     never registered via QueuePush or has been cleared via QueueClear.
//     Returning (nil, nil) means the queue is registered but empty.
//   - Implementations should periodically (or opportunistically) call
//     CleanupOldEntries to bound storage growth.
// ---------------------------------------------------------------------------

// ErrUnknownInvocation indicates QueuePop was called for an execution_id
// that was never registered via QueuePush (or has been cleared). Callers
// should treat this as "stop polling for work."
var ErrUnknownInvocation = errors.New("vgi: unknown invocation (queue_pop without queue_push)")

// WorkerStateEntry is one (worker_id, state) pair returned by WorkerScan.
type WorkerStateEntry struct {
	WorkerID int64
	State    []byte
}

// ScanWorkerStateEntry is one (stream_id, state) pair returned by ScanWorkerScan.
type ScanWorkerStateEntry struct {
	StreamID []byte
	State    []byte
}

// AggregateStateEntry is one (group_id, state) pair.
type AggregateStateEntry struct {
	GroupID int64
	State   []byte
}

// TransactionStateItem is one (key, value) pair for transaction-scoped K/V.
type TransactionStateItem struct {
	Key   []byte
	Value []byte
}

// AggregateConstArgs is a (function_name, args) pair stashed at bind time
// for an aggregate, recovered at finalize time. Unlike the Python protocol
// (where const args ride the wire on every phase), Go's aggregate runtime
// stashes them in storage so all worker processes see the same bind-time
// arguments. Backends can implement this against any (execution_id,
// function_name) → bytes K/V table.
type AggregateConstArgs struct {
	FunctionName string
	Args         []byte
}

// FunctionStorage is the cross-process shared-state interface backing
// distributed VGI execution. One implementation per backend (SQLite,
// Cloudflare Durable Object, ...); selected at worker startup.
type FunctionStorage interface {

	// --- Worker state (one slot per pid, keyed by execution_id) ---

	// WorkerPut stores or replaces the state for one worker process under
	// the given execution_id.
	WorkerPut(executionID []byte, workerID int64, state []byte) error

	// WorkerCollect atomically reads and deletes all worker states for an
	// execution_id. Typically called by the primary worker at finalize time.
	WorkerCollect(executionID []byte) ([][]byte, error)

	// WorkerScan reads all worker states without deleting them. Order is
	// implementation-defined. Used by best-effort end-of-stream consumers
	// like dynamic_to_string where multiple readers see the same state.
	WorkerScan(executionID []byte) ([]WorkerStateEntry, error)

	// --- Scan-worker state (one slot per stream_id, distinct from pid) ---
	//
	// Under HTTP transport multiple scan workers can share one process; pid
	// alone collides. The framework's per-stream UUID disambiguates.

	// ScanWorkerPut stores or replaces state for one scan worker.
	ScanWorkerPut(executionID, streamID, state []byte) error

	// ScanWorkerScan reads all per-stream-worker states without deleting.
	ScanWorkerScan(executionID []byte) ([]ScanWorkerStateEntry, error)

	// --- Work queue (FIFO over per-invocation queues) ---

	// QueuePush appends items to the queue for the given execution_id. Also
	// registers the invocation: subsequent QueuePop calls will succeed for
	// this execution_id (returning items or nil-on-empty) instead of
	// returning ErrUnknownInvocation. Empty items still registers.
	QueuePush(executionID []byte, items [][]byte) (int, error)

	// QueuePop atomically claims one item from the queue. Returns:
	//   - (item, nil) when an item was claimed.
	//   - (nil, nil) when the invocation is registered but the queue is empty.
	//   - (nil, ErrUnknownInvocation) when the invocation was never
	//     registered or has been cleared.
	QueuePop(executionID []byte) ([]byte, error)

	// QueueClear removes all remaining items for an execution_id and
	// unregisters the invocation. Returns the number of items dropped.
	QueueClear(executionID []byte) (int, error)

	// --- Aggregate state (per group_id, keyed by execution_id) ---

	// AggregateStateGet loads states for the given group_ids. Returns a
	// list parallel to group_ids: each entry is the state for that group
	// or nil if no state has been stored. DuckDB's thread-local hash tables
	// guarantee no two callers race on the same group_id during update.
	AggregateStateGet(executionID []byte, groupIDs []int64) ([]AggregateStateEntry, error)

	// AggregateStatePut writes states for the given group_ids using
	// INSERT OR REPLACE semantics.
	AggregateStatePut(executionID []byte, entries []AggregateStateEntry) error

	// AggregateStateClear drops all aggregate state for an execution_id.
	AggregateStateClear(executionID []byte) error

	// --- Aggregate const args (Go-specific; not in Python protocol) ---
	//
	// Go stashes bind-time args in storage so finalize-phase workers in
	// other processes can reload them. Python re-supplies args on each
	// phase; Go does not. Backends can implement this as a (execution_id,
	// function_name) → bytes K/V table.

	// AggregateConstArgsPut stashes serialized bind-time arguments for an
	// aggregate execution.
	AggregateConstArgsPut(executionID []byte, functionName string, args []byte) error

	// AggregateConstArgsGet loads previously stashed arguments. Returns
	// (nil, nil) if no args have been stashed (the aggregate had no const args).
	AggregateConstArgsGet(executionID []byte, functionName string) ([]byte, error)

	// --- Aggregate window partition (per partition_id, keyed by execution_id) ---

	// AggregateWindowPartitionPut writes the cached payload for a single
	// window-aggregate partition. INSERT OR REPLACE.
	AggregateWindowPartitionPut(executionID []byte, partitionID int64, data []byte) error

	// AggregateWindowPartitionGet loads the cached payload for a window
	// partition, or (nil, nil) if absent.
	AggregateWindowPartitionGet(executionID []byte, partitionID int64) ([]byte, error)

	// AggregateWindowPartitionDelete removes one partition. No-op if absent.
	AggregateWindowPartitionDelete(executionID []byte, partitionID int64) error

	// AggregateWindowPartitionClear drops every cached partition for an
	// execution_id (safety sweep for dropped destructor RPCs).
	AggregateWindowPartitionClear(executionID []byte) error

	// --- Transaction state (per transaction_opaque_data K/V store) ---
	//
	// Distinct from worker / aggregate state because the key is a
	// transaction_opaque_data, not an execution_id. The intended use is "snapshot
	// data the user expects to stay stable for the lifetime of a
	// transaction" (e.g. Kafka topic watermarks).

	// TransactionStateGet loads values for the given keys under one
	// transaction_opaque_data. Returns a list parallel to keys: nil entries for
	// keys with no stored value.
	TransactionStateGet(transactionOpaqueData []byte, keys [][]byte) ([][]byte, error)

	// TransactionStatePut writes (key, value) pairs for a transaction_opaque_data
	// using INSERT OR REPLACE semantics.
	TransactionStatePut(transactionOpaqueData []byte, items []TransactionStateItem) error

	// TransactionStateClear removes all keys for a transaction_opaque_data. Called
	// when the catalog observes commit/rollback; implementations should
	// also TTL-sweep to handle leaked transaction_opaque_data values.
	TransactionStateClear(transactionOpaqueData []byte) error

	// --- Maintenance ---

	// CleanupOldEntries removes entries older than maxAge from every table.
	// Returns the total number of rows deleted. Implementations may amortize
	// this work across regular calls (1% sample) so users rarely need to
	// invoke it directly.
	CleanupOldEntries(maxAge time.Duration) (int, error)

	// Close releases any underlying resources (DB handles, HTTP clients).
	// Safe to call multiple times.
	Close() error
}
