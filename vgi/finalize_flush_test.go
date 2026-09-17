// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A table-in-out FINALIZE flush is drained one batch per turn, and over HTTP
// every turn rebuilds the producer state from a continuation token. Carrying
// the flush itself in that token therefore costs all N batches on each of the
// N turns — O(N^2) bytes on the wire and O(N^2) IPC decodes on rehydrate. It
// stayed invisible for as long as no fixture flushed more than one batch;
// once one did, a single 5000-row query took 161s over HTTP against 1s over
// the byte-stream transports.
//
// These tests pin the two properties that keep it linear. Both are cheap: the
// growth is quadratic, so a few hundred batches already separates a linear
// implementation from a quadratic one by two orders of magnitude, and there is
// no reason to make the suite wait for five thousand.

// finalizeFlushIPC builds n single-row int64 batches and their IPC encodings.
func finalizeFlushIPC(t *testing.T, n int) ([]arrow.RecordBatch, [][]byte) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	batches := make([]arrow.RecordBatch, 0, n)
	ipc := make([][]byte, 0, n)
	for i := range n {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.Append(int64(i))
		arr := b.NewArray()
		batch := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
		data, err := SerializeRecordBatch(batch)
		if err != nil {
			t.Fatalf("serializing batch %d: %v", i, err)
		}
		batches = append(batches, batch)
		ipc = append(ipc, data)
		arr.Release()
		b.Release()
	}
	return batches, ipc
}

// finalizeFlushStorage returns an ExecutionStorage over an in-memory SQLite
// backend, shaped the way getOrCreateStorage shapes one.
func finalizeFlushStorage(t *testing.T) *ExecutionStorage {
	t.Helper()
	back, err := NewSQLiteStorage(SQLiteStorageOptions{Path: ":memory:"})
	if err != nil {
		t.Fatalf("opening sqlite backend: %v", err)
	}
	t.Cleanup(func() { _ = back.Close() })
	s := NewExecutionStorage()
	s.SetBackend(back)
	if err := s.SetExecutionID([]byte("finalize-flush-test")); err != nil {
		t.Fatalf("binding execution id: %v", err)
	}
	return s
}

func finalizeFlushState(t *testing.T, n int) *FinalizeProducerState {
	t.Helper()
	batches, ipc := finalizeFlushIPC(t, n)
	return &FinalizeProducerState{
		BatchIPC: ipc,
		batches:  batches,
		storage:  finalizeFlushStorage(t),
	}
}

// TestFinalizeFlushTokenDoesNotGrowWithFlushSize is the regression guard. The
// continuation token must describe *where the drain is*, not *what is left to
// drain* — so a hundredfold larger flush must not make the token meaningfully
// larger. Before the flush was offloaded to the state log this failed by
// roughly the ratio of the two flush sizes.
func TestFinalizeFlushTokenDoesNotGrowWithFlushSize(t *testing.T) {
	small, err := finalizeFlushState(t, 4).GobEncode()
	if err != nil {
		t.Fatalf("encoding 4-batch token: %v", err)
	}
	large, err := finalizeFlushState(t, 400).GobEncode()
	if err != nil {
		t.Fatalf("encoding 400-batch token: %v", err)
	}
	// The two tokens differ only in a log key and a cursor, so allow a small
	// absolute slack rather than a ratio; a payload-carrying token blows past
	// this by orders of magnitude (400 single-row batches are ~200KB of IPC).
	if len(large) > len(small)+64 {
		t.Fatalf("continuation token grows with flush size: 4 batches -> %d bytes, "+
			"400 batches -> %d bytes. The flush must be offloaded to the state log "+
			"and referenced by cursor, not carried in the token (that is O(N^2) over "+
			"an N-turn HTTP drain).", len(small), len(large))
	}
}

// TestFinalizeFlushRehydrateDecodesNothing pins the other half of the cost:
// even a token that stayed small would be quadratic if resuming it decoded the
// whole flush. After a round trip there must be nothing left in the token for
// rehydrateFinalize to decode — produceFromLog reads the single batch it needs.
func TestFinalizeFlushRehydrateDecodesNothing(t *testing.T) {
	token, err := finalizeFlushState(t, 32).GobEncode()
	if err != nil {
		t.Fatalf("encoding token: %v", err)
	}
	var resumed FinalizeProducerState
	if err := resumed.GobDecode(token); err != nil {
		t.Fatalf("decoding token: %v", err)
	}
	if len(resumed.LogKey) == 0 {
		t.Fatal("resumed state has no log key: the flush was not offloaded")
	}
	if len(resumed.BatchIPC) != 0 {
		t.Fatalf("resumed state still carries %d inline batches; rehydrateFinalize "+
			"would decode all of them on every turn", len(resumed.BatchIPC))
	}
	if resumed.LogCursor != -1 {
		t.Fatalf("resumed cursor = %d, want -1 (before-first)", resumed.LogCursor)
	}
}

// TestFinalizeFlushOffloadsOnlyUndrainedBatches checks the boundary an
// "offload what is left" change is most likely to get wrong: batches already
// emitted must not be replayed, and the rest must keep their order.
func TestFinalizeFlushOffloadsOnlyUndrainedBatches(t *testing.T) {
	const total, emitted = 6, 2
	batches, ipc := finalizeFlushIPC(t, total)
	s := &FinalizeProducerState{
		BatchIPC: ipc,
		batches:  batches,
		BatchIdx: emitted,
		storage:  finalizeFlushStorage(t),
	}
	if _, err := s.GobEncode(); err != nil {
		t.Fatalf("encoding token: %v", err)
	}
	entries, err := s.storage.StateLogScan(s.LogKey, -1, 0)
	if err != nil {
		t.Fatalf("scanning offloaded flush: %v", err)
	}
	if len(entries) != total-emitted {
		t.Fatalf("offloaded %d batches, want the %d not yet emitted", len(entries), total-emitted)
	}
	for i, e := range entries {
		if !bytes.Equal(e.Value, ipc[emitted+i]) {
			t.Fatalf("offloaded batch %d is not input batch %d: the drain would emit "+
				"the flush out of order or replay an already-emitted batch", i, emitted+i)
		}
	}
}

// TestFinalizeFlushKeysAreStreamUnique guards the sharing hazard the offload
// introduces: a parallel query opens several finalize substreams under one
// execution_id, and a shared log key would let them drain each other's
// batches (a wrong COUNT(*), which is exactly what multi_batch_finalize.test
// watches for).
func TestFinalizeFlushKeysAreStreamUnique(t *testing.T) {
	storage := finalizeFlushStorage(t)
	keys := make(map[string]bool, 4)
	for range 4 {
		_, ipc := finalizeFlushIPC(t, 3)
		s := &FinalizeProducerState{BatchIPC: ipc, storage: storage}
		if _, err := s.GobEncode(); err != nil {
			t.Fatalf("encoding token: %v", err)
		}
		if keys[string(s.LogKey)] {
			t.Fatal("two finalize streams sharing one execution_id got the same log key")
		}
		keys[string(s.LogKey)] = true
		entries, err := storage.StateLogScan(s.LogKey, -1, 0)
		if err != nil {
			t.Fatalf("scanning offloaded flush: %v", err)
		}
		if len(entries) != 3 {
			t.Fatalf("stream drained %d batches, want its own 3", len(entries))
		}
	}
}

// TestFinalizeFlushResumesOnAColdWorker is the constraint the offload must not
// break. HTTP continuations are stateless by design: a continuation may arrive
// at a worker process that has never seen this stream, and it has to work from
// the token alone. It does because the flush goes to the shared, durable
// FunctionStorage backend rather than to process memory — so here a *second*,
// independently opened handle on the same database, holding none of the first
// one's state, drains the flush the first one offloaded.
func TestFinalizeFlushResumesOnAColdWorker(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.db")
	const flush = 16

	// Worker A: runs the finalize and writes the continuation token.
	backA, err := NewSQLiteStorage(SQLiteStorageOptions{Path: dbPath})
	if err != nil {
		t.Fatalf("opening backend A: %v", err)
	}
	storageA := NewExecutionStorage()
	storageA.SetBackend(backA)
	if err := storageA.SetExecutionID([]byte("cold-resume")); err != nil {
		t.Fatalf("binding execution id: %v", err)
	}
	_, ipc := finalizeFlushIPC(t, flush)
	token, err := (&FinalizeProducerState{BatchIPC: ipc, storage: storageA}).GobEncode()
	if err != nil {
		t.Fatalf("encoding token: %v", err)
	}
	_ = backA.Close() // worker A is gone

	// Worker B: has only the token and the shared backend.
	backB, err := NewSQLiteStorage(SQLiteStorageOptions{Path: dbPath})
	if err != nil {
		t.Fatalf("opening backend B: %v", err)
	}
	t.Cleanup(func() { _ = backB.Close() })
	var resumed FinalizeProducerState
	if err := resumed.GobDecode(token); err != nil {
		t.Fatalf("decoding token: %v", err)
	}
	storageB := NewExecutionStorage()
	storageB.SetBackend(backB)
	// rehydrateFinalize takes the execution id off the token's recipe; this
	// state was built directly, so bind the id worker A used.
	if err := storageB.SetExecutionID([]byte("cold-resume")); err != nil {
		t.Fatalf("binding execution id: %v", err)
	}
	resumed.storage = storageB

	// Drain it the way produceFromLog does: one row per turn, cursor only.
	var drained int
	for {
		entries, err := resumed.storage.StateLogScan(resumed.LogKey, resumed.LogCursor, 1)
		if err != nil {
			t.Fatalf("scanning on the cold worker: %v", err)
		}
		if len(entries) == 0 {
			break
		}
		if !bytes.Equal(entries[0].Value, ipc[drained]) {
			t.Fatalf("cold worker drained batch %d out of order", drained)
		}
		resumed.LogCursor = entries[0].ID
		drained++
	}
	if drained != flush {
		t.Fatalf("cold worker drained %d of %d batches: a continuation arriving at a "+
			"worker that never saw the stream must still serve the whole flush", drained, flush)
	}
}

// TestFinalizeFlushWithoutStorageStaysInline pins the degradation path: a
// backend that cannot serve a state log keeps the old inline carry, which is
// slow but correct, rather than failing the stream.
func TestFinalizeFlushWithoutStorageStaysInline(t *testing.T) {
	batches, ipc := finalizeFlushIPC(t, 8)
	s := &FinalizeProducerState{BatchIPC: ipc, batches: batches}
	if _, err := s.GobEncode(); err != nil {
		t.Fatalf("encoding token without storage: %v", err)
	}
	if len(s.LogKey) != 0 {
		t.Fatal("offloaded a flush with no storage handle")
	}
	if len(s.BatchIPC) != 8 {
		t.Fatalf("inline carry lost batches: %d of 8 left", len(s.BatchIPC))
	}
}
