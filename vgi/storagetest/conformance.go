// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package storagetest provides a behavioral conformance suite for any
// implementation of vgi.FunctionStorage. Each backend (SQLite, Cloudflare
// Durable Object, ...) plugs into RunConformance to verify it satisfies
// the FunctionStorage contract identically.
package storagetest

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
)

// Local aliases so subtest code stays terse.
type (
	// FunctionStorage is a local alias for vgi.FunctionStorage so subtest code stays terse.
	FunctionStorage = vgi.FunctionStorage
	// WorkerStateEntry is a local alias for vgi.WorkerStateEntry so subtest code stays terse.
	WorkerStateEntry = vgi.WorkerStateEntry
	// ScanWorkerStateEntry is a local alias for vgi.ScanWorkerStateEntry so subtest code stays terse.
	ScanWorkerStateEntry = vgi.ScanWorkerStateEntry
	// AggregateStateEntry is a local alias for vgi.AggregateStateEntry so subtest code stays terse.
	AggregateStateEntry = vgi.AggregateStateEntry
	// TransactionStateItem is a local alias for vgi.TransactionStateItem so subtest code stays terse.
	TransactionStateItem = vgi.TransactionStateItem
)

// RunConformance runs the full FunctionStorage behavioral
// contract against the provided factory. Each subtest gets a freshly
// constructed storage instance (so backends needn't worry about test
// isolation between subtests). The factory must return a storage that is
// empty and ready to use; the framework calls its Close() when the subtest
// ends.
//
// Backends register their conformance entry point in <backend>_test.go via:
//
//	func TestSQLiteStorage_Conformance(t *testing.T) {
//	    storagetest.RunConformance(t, func(t *testing.T) vgi.FunctionStorage {
//	        s, err := vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: ":memory:"})
//	        if err != nil {
//	            t.Fatal(err)
//	        }
//	        return s
//	    })
//	}
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
	// support aggregate functions.
	SkipAggregate SkipSet = 1 << iota
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

	type subtest struct {
		name      string
		run       func(t *testing.T, s FunctionStorage)
		aggregate bool
	}

	subtests := []subtest{
		{name: "WorkerPut_then_WorkerScan", run: testWorkerPutThenScan},
		{name: "WorkerPut_replaces_existing", run: testWorkerPutReplaces},
		{name: "WorkerCollect_drains_and_returns_in_order", run: testWorkerCollectDrains},
		{name: "WorkerScan_isolates_by_executionID", run: testWorkerScanIsolation},
		{name: "ScanWorkerPut_then_ScanWorkerScan", run: testScanWorkerRoundtrip},
		{name: "QueuePush_then_QueuePop_FIFO", run: testQueueFIFO},
		{name: "QueuePop_empty_or_unknown_returns_nil_nil", run: testQueuePopEmpty},
		{name: "QueuePush_empty_items_noop", run: testQueuePushEmpty},
		{name: "QueueClear_drops_items", run: testQueueClear},
		{name: "AggregateState_put_get_clear", run: testAggregateStateLifecycle, aggregate: true},
		{name: "AggregateState_get_with_missing_groups", run: testAggregateStateMissing, aggregate: true},
		{name: "AggregateConstArgs_put_get", run: testAggregateConstArgs, aggregate: true},
		{name: "AggregateWindowPartition_put_get_delete_clear", run: testAggregateWindowPartition, aggregate: true},
		{name: "TransactionState_put_get_clear", run: testTransactionStateLifecycle},
		{name: "StateLog_append_scan_clear", run: testStateLogLifecycle},
		{name: "Close_idempotent", run: testCloseIdempotent},
	}

	for _, st := range subtests {
		st := st
		if skipAgg && st.aggregate {
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

func testQueuePopEmpty(t *testing.T, s FunctionStorage) {
	// No registration: both an empty queue and a never-pushed id return (nil, nil).
	got, err := s.QueuePop([]byte("never-pushed"))
	if got != nil || err != nil {
		t.Errorf("never-pushed queue: got %v err %v", got, err)
	}
	exec := []byte("q-empty")
	_, _ = s.QueuePush(exec, nil)
	got, err = s.QueuePop(exec)
	if got != nil || err != nil {
		t.Errorf("empty queue: got %v err %v", got, err)
	}
}

func testQueuePushEmpty(t *testing.T, s FunctionStorage) {
	exec := []byte("q-empty-push")
	if _, err := s.QueuePush(exec, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.QueuePop(exec)
	if err != nil || got != nil {
		t.Errorf("after empty push, expected (nil, nil), got %v err %v", got, err)
	}
}

func testQueueClear(t *testing.T, s FunctionStorage) {
	exec := []byte("q-clear")
	_, _ = s.QueuePush(exec, [][]byte{[]byte("x"), []byte("y")})
	n, err := s.QueueClear(exec)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("QueueClear returned %d, want 2", n)
	}
	got, err := s.QueuePop(exec)
	if got != nil || err != nil {
		t.Errorf("after clear, expected (nil, nil), got %v err %v", got, err)
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

// testStateLogLifecycle exercises the optional StateLogStorage capability
// (StateAppend / StateLogScan / StateLogClear) used by table-buffering: a
// per-(execution,key) append-only log with a monotonic cursor. Backends that
// don't implement it (none currently) skip.
func testStateLogLifecycle(t *testing.T, s FunctionStorage) {
	ls, ok := s.(vgi.StateLogStorage)
	if !ok {
		t.Skip("backend does not implement StateLogStorage")
	}
	exec := []byte("exec-statelog")
	key := []byte("log-key")

	id1, err := ls.StateAppend(exec, key, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := ls.StateAppend(exec, key, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	id3, err := ls.StateAppend(exec, key, []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !(id1 < id2 && id2 < id3) {
		t.Fatalf("ordinals not monotonic: %d, %d, %d", id1, id2, id3)
	}

	// Scan from the start returns all rows in append order, with their ids.
	rows, err := ls.StateLogScan(exec, key, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("scan from start: expected 3 rows, got %d", len(rows))
	}
	if rows[0].ID != id1 || !bytes.Equal(rows[0].Value, []byte("a")) ||
		rows[1].ID != id2 || !bytes.Equal(rows[1].Value, []byte("b")) ||
		rows[2].ID != id3 || !bytes.Equal(rows[2].Value, []byte("c")) {
		t.Errorf("scan rows mismatch: %+v", rows)
	}

	// after_id cursor: rows with id > id1.
	tail, err := ls.StateLogScan(exec, key, id1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 2 || !bytes.Equal(tail[0].Value, []byte("b")) || !bytes.Equal(tail[1].Value, []byte("c")) {
		t.Errorf("after_id cursor: %+v", tail)
	}

	// limit caps the page.
	limited, err := ls.StateLogScan(exec, key, -1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Errorf("limit=2: expected 2 rows, got %d", len(limited))
	}

	// A different key under the same execution is isolated.
	other, err := ls.StateLogScan(exec, []byte("other-key"), -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Errorf("distinct key should be empty, got %d rows", len(other))
	}

	// Clear drops the log rows for the execution.
	if err := ls.StateLogClear(exec); err != nil {
		t.Fatal(err)
	}
	cleared, err := ls.StateLogScan(exec, key, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared) != 0 {
		t.Errorf("after clear: expected 0 rows, got %d", len(cleared))
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
