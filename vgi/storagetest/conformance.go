// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package storagetest provides a behavioral conformance suite for any
// implementation of vgi.FunctionStorage. Each backend (SQLite, Cloudflare
// Durable Object, ...) plugs into RunConformance to verify it satisfies
// the FunctionStorage contract identically.
package storagetest

import (
	"bytes"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
)

// Local aliases so subtest code stays terse.
type (
	FunctionStorage       = vgi.FunctionStorage
	WorkerStateEntry      = vgi.WorkerStateEntry
	ScanWorkerStateEntry  = vgi.ScanWorkerStateEntry
	AggregateStateEntry   = vgi.AggregateStateEntry
	TransactionStateItem  = vgi.TransactionStateItem
)

var ErrUnknownInvocation = vgi.ErrUnknownInvocation

// RunFunctionStorageConformance runs the full FunctionStorage behavioral
// contract against the provided factory. Each subtest gets a freshly
// constructed storage instance (so backends needn't worry about test
// isolation between subtests). The factory must return a storage that is
// empty and ready to use; the framework calls its Close() when the subtest
// ends.
//
// Backends register their conformance entry point in <backend>_test.go via:
//
//   func TestSQLiteStorage_Conformance(t *testing.T) {
//       storagetest.RunConformance(t, func(t *testing.T) vgi.FunctionStorage {
//           s, err := vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: ":memory:"})
//           if err != nil {
//               t.Fatal(err)
//           }
//           return s
//       })
//   }
//
// When a new backend (Cloudflare DO, Azure SQL, ...) lands, plug it into
// the same conformance suite to guarantee behavior parity.
func RunConformance(t *testing.T, factory func(t *testing.T) FunctionStorage) {
	RunConformanceFiltered(t, factory)
}

// SkipSet is the set of conformance subtests a backend opts out of.
type SkipSet int

const (
	// SkipAggregate skips aggregate state, aggregate const args, and
	// aggregate window partition subtests. Use for backends that don't
	// support aggregate functions (e.g. the Cloudflare DO client, which
	// matches vgi-python in returning NotImplemented for those methods).
	SkipAggregate SkipSet = 1 << iota

	// SkipCleanup skips the cleanup-purges subtest. Use for backends
	// whose CleanupOldEntries is a no-op (e.g. cloud services that handle
	// TTL on the server side).
	SkipCleanup
)

// RunConformanceFiltered runs the conformance suite with optional skips.
// Backends that don't implement a subset of FunctionStorage methods opt out
// rather than failing the whole suite.
func RunConformanceFiltered(t *testing.T, factory func(t *testing.T) FunctionStorage, skips ...SkipSet) {
	t.Helper()

	var skip SkipSet
	for _, s := range skips {
		skip |= s
	}
	skipAgg := skip&SkipAggregate != 0
	skipCleanup := skip&SkipCleanup != 0

	type subtest struct {
		name      string
		run       func(t *testing.T, s FunctionStorage)
		aggregate bool
		cleanup   bool
	}

	subtests := []subtest{
		{name: "WorkerPut_then_WorkerScan", run: testWorkerPutThenScan},
		{name: "WorkerPut_replaces_existing", run: testWorkerPutReplaces},
		{name: "WorkerCollect_drains_and_returns_in_order", run: testWorkerCollectDrains},
		{name: "WorkerScan_isolates_by_executionID", run: testWorkerScanIsolation},
		{name: "ScanWorkerPut_then_ScanWorkerScan", run: testScanWorkerRoundtrip},
		{name: "QueuePush_then_QueuePop_FIFO", run: testQueueFIFO},
		{name: "QueuePop_unknown_invocation_errors", run: testQueuePopUnknown},
		{name: "QueuePop_empty_registered_queue_returns_nil_nil", run: testQueuePopEmpty},
		{name: "QueuePush_empty_items_registers", run: testQueuePushEmptyRegisters},
		{name: "QueueClear_drops_and_unregisters", run: testQueueClearUnregisters},
		{name: "AggregateState_put_get_clear", run: testAggregateStateLifecycle, aggregate: true},
		{name: "AggregateState_get_with_missing_groups", run: testAggregateStateMissing, aggregate: true},
		{name: "AggregateConstArgs_put_get", run: testAggregateConstArgs, aggregate: true},
		{name: "AggregateWindowPartition_put_get_delete_clear", run: testAggregateWindowPartition, aggregate: true},
		{name: "TransactionState_put_get_clear", run: testTransactionStateLifecycle},
		{name: "CleanupOldEntries_zero_window_purges_everything", run: testCleanupPurges, cleanup: true},
		{name: "Close_idempotent", run: testCloseIdempotent},
	}

	for _, st := range subtests {
		st := st
		if skipAgg && st.aggregate {
			continue
		}
		if skipCleanup && st.cleanup {
			continue
		}
		t.Run(st.name, func(t *testing.T) {
			s := factory(t)
			t.Cleanup(func() { _ = s.Close() })
			st.run(t, s)
		})
	}
}

// ---------------------------------------------------------------------------
// Subtests
// ---------------------------------------------------------------------------

func testWorkerPutThenScan(t *testing.T, s FunctionStorage) {
	exec := []byte("exec-1")
	if err := s.WorkerPut(exec, 1, []byte("state-1")); err != nil {
		t.Fatal(err)
	}
	if err := s.WorkerPut(exec, 2, []byte("state-2")); err != nil {
		t.Fatal(err)
	}
	got, err := s.WorkerScan(exec)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].WorkerID < got[j].WorkerID })
	want := []WorkerStateEntry{
		{WorkerID: 1, State: []byte("state-1")},
		{WorkerID: 2, State: []byte("state-2")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func testWorkerPutReplaces(t *testing.T, s FunctionStorage) {
	exec := []byte("exec-2")
	_ = s.WorkerPut(exec, 1, []byte("v1"))
	_ = s.WorkerPut(exec, 1, []byte("v2"))
	got, _ := s.WorkerScan(exec)
	if len(got) != 1 || !bytes.Equal(got[0].State, []byte("v2")) {
		t.Errorf("expected one row with state v2, got %+v", got)
	}
}

func testWorkerCollectDrains(t *testing.T, s FunctionStorage) {
	exec := []byte("exec-3")
	_ = s.WorkerPut(exec, 1, []byte("a"))
	_ = s.WorkerPut(exec, 2, []byte("b"))
	_ = s.WorkerPut(exec, 3, []byte("c"))

	got, err := s.WorkerCollect(exec)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 states, got %d (%v)", len(got), got)
	}
	// After collect the scan should be empty.
	after, _ := s.WorkerScan(exec)
	if len(after) != 0 {
		t.Errorf("WorkerCollect should drain; still see %d states", len(after))
	}
}

func testWorkerScanIsolation(t *testing.T, s FunctionStorage) {
	_ = s.WorkerPut([]byte("A"), 1, []byte("for-a"))
	_ = s.WorkerPut([]byte("B"), 1, []byte("for-b"))
	a, _ := s.WorkerScan([]byte("A"))
	b, _ := s.WorkerScan([]byte("B"))
	if len(a) != 1 || !bytes.Equal(a[0].State, []byte("for-a")) {
		t.Errorf("isolation A: %+v", a)
	}
	if len(b) != 1 || !bytes.Equal(b[0].State, []byte("for-b")) {
		t.Errorf("isolation B: %+v", b)
	}
}

func testScanWorkerRoundtrip(t *testing.T, s FunctionStorage) {
	exec := []byte("exec-sw")
	_ = s.ScanWorkerPut(exec, []byte("stream-1"), []byte("s1"))
	_ = s.ScanWorkerPut(exec, []byte("stream-2"), []byte("s2"))
	// Replace stream-1
	_ = s.ScanWorkerPut(exec, []byte("stream-1"), []byte("s1b"))

	got, err := s.ScanWorkerScan(exec)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	m := map[string]string{}
	for _, e := range got {
		m[string(e.StreamID)] = string(e.State)
	}
	if m["stream-1"] != "s1b" || m["stream-2"] != "s2" {
		t.Errorf("scan-worker map: %v", m)
	}
}

func testQueueFIFO(t *testing.T, s FunctionStorage) {
	exec := []byte("q-fifo")
	if n, err := s.QueuePush(exec, [][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil || n != 3 {
		t.Fatalf("QueuePush: n=%d err=%v", n, err)
	}
	for _, want := range []string{"a", "b", "c"} {
		got, err := s.QueuePop(exec)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("FIFO: got %q want %q", got, want)
		}
	}
	// Now empty but registered → (nil, nil)
	got, err := s.QueuePop(exec)
	if got != nil || err != nil {
		t.Errorf("empty registered: got %v err %v", got, err)
	}
}

func testQueuePopUnknown(t *testing.T, s FunctionStorage) {
	_, err := s.QueuePop([]byte("never-pushed"))
	if !errors.Is(err, ErrUnknownInvocation) {
		t.Errorf("expected ErrUnknownInvocation, got %v", err)
	}
}

func testQueuePopEmpty(t *testing.T, s FunctionStorage) {
	exec := []byte("q-empty")
	_, _ = s.QueuePush(exec, nil)
	got, err := s.QueuePop(exec)
	if got != nil || err != nil {
		t.Errorf("registered empty queue: got %v err %v", got, err)
	}
}

func testQueuePushEmptyRegisters(t *testing.T, s FunctionStorage) {
	exec := []byte("q-empty-push")
	if _, err := s.QueuePush(exec, nil); err != nil {
		t.Fatal(err)
	}
	// Should now be registered — QueuePop returns (nil, nil), not ErrUnknownInvocation.
	got, err := s.QueuePop(exec)
	if err != nil {
		t.Errorf("after empty push, QueuePop unexpectedly errored: %v", err)
	}
	if got != nil {
		t.Errorf("after empty push, expected (nil, nil), got %v", got)
	}
}

func testQueueClearUnregisters(t *testing.T, s FunctionStorage) {
	exec := []byte("q-clear")
	_, _ = s.QueuePush(exec, [][]byte{[]byte("x"), []byte("y")})
	n, err := s.QueueClear(exec)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("QueueClear returned %d, want 2", n)
	}
	// Subsequent pop should be ErrUnknownInvocation.
	_, err = s.QueuePop(exec)
	if !errors.Is(err, ErrUnknownInvocation) {
		t.Errorf("after clear, expected ErrUnknownInvocation, got %v", err)
	}
}

func testAggregateStateLifecycle(t *testing.T, s FunctionStorage) {
	exec := []byte("agg-1")
	if err := s.AggregateStatePut(exec, []AggregateStateEntry{
		{GroupID: 10, State: []byte("a")},
		{GroupID: 20, State: []byte("b")},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.AggregateStateGet(exec, []int64{10, 20, 30})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries parallel to input, got %d", len(got))
	}
	if !bytes.Equal(got[0].State, []byte("a")) || got[0].GroupID != 10 {
		t.Errorf("got[0] = %+v", got[0])
	}
	if !bytes.Equal(got[1].State, []byte("b")) || got[1].GroupID != 20 {
		t.Errorf("got[1] = %+v", got[1])
	}
	if got[2].State != nil {
		t.Errorf("got[2] (missing group) should be zero-value, got %+v", got[2])
	}

	// Clear and verify gone.
	if err := s.AggregateStateClear(exec); err != nil {
		t.Fatal(err)
	}
	again, _ := s.AggregateStateGet(exec, []int64{10})
	if again[0].State != nil {
		t.Errorf("after clear, still see state: %+v", again[0])
	}
}

func testAggregateStateMissing(t *testing.T, s FunctionStorage) {
	got, err := s.AggregateStateGet([]byte("none"), []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 zero entries, got %d", len(got))
	}
	for i, e := range got {
		if e.State != nil {
			t.Errorf("entry %d should be zero, got %+v", i, e)
		}
	}
}

func testAggregateConstArgs(t *testing.T, s FunctionStorage) {
	exec := []byte("agg-args")
	if err := s.AggregateConstArgsPut(exec, "vgi_percentile", []byte("ipc-bytes")); err != nil {
		t.Fatal(err)
	}
	got, err := s.AggregateConstArgsGet(exec, "vgi_percentile")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("ipc-bytes")) {
		t.Errorf("got %v", got)
	}
	// Unknown function returns nil.
	got, err = s.AggregateConstArgsGet(exec, "no_such_fn")
	if err != nil || got != nil {
		t.Errorf("missing args: got %v err %v", got, err)
	}
}

func testAggregateWindowPartition(t *testing.T, s FunctionStorage) {
	exec := []byte("wp-1")
	if err := s.AggregateWindowPartitionPut(exec, 0, []byte("p0")); err != nil {
		t.Fatal(err)
	}
	if err := s.AggregateWindowPartitionPut(exec, 1, []byte("p1")); err != nil {
		t.Fatal(err)
	}
	got, _ := s.AggregateWindowPartitionGet(exec, 0)
	if !bytes.Equal(got, []byte("p0")) {
		t.Errorf("get(0) = %v", got)
	}
	// Delete one, the other remains.
	if err := s.AggregateWindowPartitionDelete(exec, 0); err != nil {
		t.Fatal(err)
	}
	if g, _ := s.AggregateWindowPartitionGet(exec, 0); g != nil {
		t.Errorf("after delete, get(0) = %v", g)
	}
	if g, _ := s.AggregateWindowPartitionGet(exec, 1); !bytes.Equal(g, []byte("p1")) {
		t.Errorf("get(1) after delete-of-0 = %v", g)
	}
	// Clear all.
	if err := s.AggregateWindowPartitionClear(exec); err != nil {
		t.Fatal(err)
	}
	if g, _ := s.AggregateWindowPartitionGet(exec, 1); g != nil {
		t.Errorf("after clear, get(1) = %v", g)
	}
}

func testTransactionStateLifecycle(t *testing.T, s FunctionStorage) {
	txn := []byte("txn-1")
	if err := s.TransactionStatePut(txn, []TransactionStateItem{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.TransactionStateGet(txn, [][]byte{[]byte("k1"), []byte("k2"), []byte("missing")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	if !bytes.Equal(got[0], []byte("v1")) || !bytes.Equal(got[1], []byte("v2")) || got[2] != nil {
		t.Errorf("get: %v", got)
	}
	if err := s.TransactionStateClear(txn); err != nil {
		t.Fatal(err)
	}
	again, _ := s.TransactionStateGet(txn, [][]byte{[]byte("k1")})
	if again[0] != nil {
		t.Errorf("after clear: %v", again)
	}
}

func testCleanupPurges(t *testing.T, s FunctionStorage) {
	exec := []byte("cleanup")
	_ = s.WorkerPut(exec, 1, []byte("a"))
	_, _ = s.QueuePush(exec, [][]byte{[]byte("x")})
	_ = s.AggregateStatePut(exec, []AggregateStateEntry{{GroupID: 0, State: []byte("g")}})

	// Negative duration → "older than future now" → everything is too old.
	deleted, err := s.CleanupOldEntries(-1 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if deleted < 3 {
		t.Errorf("expected at least 3 rows deleted, got %d", deleted)
	}

	// Worker state should be gone.
	got, _ := s.WorkerScan(exec)
	if len(got) != 0 {
		t.Errorf("worker_state not purged: %v", got)
	}
}

func testCloseIdempotent(t *testing.T, s FunctionStorage) {
	if err := s.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
