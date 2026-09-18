// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/base64"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The history a stream keeps is bounded by its predicate IDs, not its ticks,
// and replaying it rebuilds exactly the state the stream had.

func historyDelta(t *testing.T, updates string, values ...int64) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	var fields []arrow.Field
	var columns []arrow.Array
	for i, value := range values {
		builder := array.NewInt64Builder(mem)
		builder.Append(value)
		columns = append(columns, builder.NewArray())
		builder.Release()
		fields = append(fields, arrow.Field{Name: "value_" + strconv.Itoa(i), Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	}
	batch := filterV2Batch(t, `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[`+updates+`]}`, fields, columns)
	defer batch.Release()
	data, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func historyUpsert(id string, revision int, op string, valueRef int) string {
	return `{"operation":"upsert","id":"` + id + `","revision":` + strconv.Itoa(revision) + `,"mode":"advisory","source":"top_n","expression":{"node":"comparison","op":"` + op + `","left":{"node":"column_ref","column_index":0,"column_name":"n"},"right":{"node":"literal","value_ref":` + strconv.Itoa(valueRef) + `}}}`
}

func historyRemove(id string, revision int) string {
	return `{"operation":"remove","id":"` + id + `","revision":` + strconv.Itoa(revision) + `}`
}

// historyStream is a stream's filter bookkeeping outside any transport.
type historyStream struct {
	t      *testing.T
	params *ProcessParams
	deltas [][]byte
	order  []string
}

func newHistoryStream(t *testing.T) *historyStream {
	t.Helper()
	snapshot := filterV2Batch(t, `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[]}`, nil, nil)
	return &historyStream{t: t, params: &ProcessParams{PushdownFilters: snapshot, BindOutputSchema: int64OutputSchema()}}
}

func (s *historyStream) tick(delta []byte) {
	s.t.Helper()
	meta := arrow.NewMetadata([]string{"vgi_pushdown_filters"}, []string{base64.StdEncoding.EncodeToString(delta)})
	h := streamFilterHistory{deltas: &s.deltas, order: &s.order}
	if applied, err := h.applyTick(s.params, meta); err != nil || !applied {
		s.t.Fatalf("applying a delta: applied=%t err=%v", applied, err)
	}
}

// rebuilt is the filter state an HTTP turn rebuilds from the history.
func (s *historyStream) rebuilt() *PushdownFilters {
	s.t.Helper()
	params := &ProcessParams{PushdownFilters: s.params.PushdownFilters, BindOutputSchema: s.params.BindOutputSchema}
	pf, err := deserializeFiltersV2(params.PushdownFilters, nil, nil, false, params.BindOutputSchema)
	if err != nil {
		s.t.Fatal(err)
	}
	params.CurrentPushdownFilters = pf
	if err := replayFilterDeltas(params, s.deltas, s.order); err != nil {
		s.t.Fatal(err)
	}
	return params.CurrentPushdownFilters
}

func (s *historyStream) requireRebuilt() {
	s.t.Helper()
	live, rebuilt := s.params.CurrentPushdownFilters, s.rebuilt()
	if live.Repr() != rebuilt.Repr() || !slices.Equal(live.predicateOrder(), rebuilt.predicateOrder()) {
		s.t.Fatalf("rebuilt %s, the stream had %s", rebuilt.Repr(), live.Repr())
	}
	if len(live.v2Revisions) != len(rebuilt.v2Revisions) {
		s.t.Fatalf("rebuilt revisions %v, the stream had %v", rebuilt.v2Revisions, live.v2Revisions)
	}
	for id, revision := range live.v2Revisions {
		if rebuilt.v2Revisions[id] != revision {
			s.t.Fatalf("rebuilt revisions %v, the stream had %v", rebuilt.v2Revisions, live.v2Revisions)
		}
	}
}

func TestFilterHistoryKeepsOneDeltaPerLivePredicate(t *testing.T) {
	s := newHistoryStream(t)
	for revision := 1; revision <= 200; revision++ {
		s.tick(historyDelta(t, historyUpsert("top_n:0", revision, "lt", 0), int64(10000-revision)))
		if len(s.deltas) != 1 {
			t.Fatalf("after %d deltas the history holds %d", revision, len(s.deltas))
		}
	}
	s.requireRebuilt()
	if got := s.rebuilt().Repr(); got != "PushdownFilters([ConstantFilter(n < 9800)])" {
		t.Fatalf("rebuilt %s", got)
	}
	// A stale re-send is dropped: it installed nothing.
	s.tick(historyDelta(t, historyUpsert("top_n:0", 200, "lt", 0), 1))
	if len(s.deltas) != 1 {
		t.Fatalf("a stale re-send grew the history to %d", len(s.deltas))
	}
	s.requireRebuilt()
}

func TestFilterHistoryRebuildsEveryShape(t *testing.T) {
	s := newHistoryStream(t)
	s.tick(historyDelta(t, historyUpsert("a", 1, "lt", 0)+","+historyUpsert("b", 1, "gt", 1), 100000, 5))
	s.requireRebuilt()
	s.tick(historyDelta(t, historyRemove("a", 2)+","+historyUpsert("b", 1, "gt", 0), 7))
	s.requireRebuilt()
	s.tick(historyDelta(t, historyUpsert("a", 3, "lt", 0)+","+historyUpsert("c", 1, "ne", 1), 99999, 3))
	s.requireRebuilt()
	s.tick(historyDelta(t, historyUpsert("b", 2, "gt", 0), 6))
	s.requireRebuilt()
	s.tick(historyDelta(t, historyRemove("c", 2)))
	s.requireRebuilt()
	// a:3 (third delta), b:2 (fourth), c's tombstone (fifth); the first two
	// installed nothing that is still live.
	if len(s.deltas) != 3 {
		t.Fatalf("the history holds %d deltas, want 3", len(s.deltas))
	}
	// The tombstone outlives compaction: a stale re-add stays out.
	s.tick(historyDelta(t, historyUpsert("c", 1, "ne", 0), 3))
	if got := s.params.CurrentPushdownFilters.Repr(); got != "PushdownFilters([ConstantFilter(n < 99999), ConstantFilter(n > 6)])" {
		t.Fatalf("after a stale re-add: %s", got)
	}
	s.requireRebuilt()
}

func TestFilterHistoryRestoresRecordedOrder(t *testing.T) {
	s := newHistoryStream(t)
	s.tick(historyDelta(t, historyUpsert("a", 1, "lt", 0)+","+historyUpsert("b", 1, "gt", 1), 100000, 5))
	// An order the replay alone would not produce is restored exactly.
	s.order = []string{"b", "a"}
	if got := s.rebuilt().predicateOrder(); !slices.Equal(got, []string{"b", "a"}) {
		t.Fatalf("rebuilt order %v, want [b a]", got)
	}
	// An order naming other predicates fails closed.
	s.order = []string{"a", "x"}
	params := &ProcessParams{PushdownFilters: s.params.PushdownFilters, BindOutputSchema: s.params.BindOutputSchema}
	pf, err := deserializeFiltersV2(params.PushdownFilters, nil, nil, false, params.BindOutputSchema)
	if err != nil {
		t.Fatal(err)
	}
	params.CurrentPushdownFilters = pf
	if err := replayFilterDeltas(params, s.deltas, s.order); err == nil {
		t.Fatal("replayed a history whose recorded order names other predicates")
	}
}
