// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Dynamic-filter state across HTTP turns.
//
// DuckDB tightens a running scan's filters (a Top-N bound, a join's runtime
// filter) by sending a delta document on a tick's custom metadata under
// vgi_pushdown_filters, and it sends each change exactly once per stream. A
// byte-stream transport (stdio, unix, tcp) keeps the stream's state in memory,
// so an applied delta stays applied. An HTTP worker keeps nothing: every turn
// rebuilds the stream from its tokens -- the init snapshot from the recipe and
// everything since from the cursor -- so the cursor has to carry the deltas.
//
// Carrying every delta is the quadratic bug vgi-python had: turn k replays k
// deltas. So the history is compacted: for each (id, revision) of the live
// state, tombstones included, only the first delta that carried it is kept.
// Replaying those reproduces the same predicates, values and revisions -- an
// ID's earlier updates are overwritten by its current revision, and its later
// ones were stale and stay stale -- and the history is bounded by the number of
// predicate IDs instead of growing with the tick count. The live predicate
// order is recorded alongside and restored after the replay, so a rebuilt
// state never depends on a shorter replay arranging the IDs the same way (it
// does under this package's upsert-moves-to-end rule; vgi-python, whose upsert
// keeps an ID's position, needs the restore for a removed-and-re-added ID).

// streamFilterHistory points at a table stream's dynamic-filter bookkeeping:
// the compacted deltas it has applied and the live predicate order they
// rebuild. Both are carried in the stream's HTTP cursor.
type streamFilterHistory struct {
	deltas *[][]byte
	order  *[]string
}

// applyTick applies the dynamic-filter delta a tick's metadata carries, if
// any, and folds it into the history. It reports whether a delta was present.
// Malformed or unsupported updates fail closed without mutating the previous
// state.
func (h streamFilterHistory) applyTick(params *ProcessParams, meta arrow.Metadata) (bool, error) {
	delta, carried, err := applyTickFilters(params, meta)
	if err != nil || delta == nil {
		return false, err
	}
	deltas, err := recordFilterDelta(params.CurrentPushdownFilters, *h.deltas, delta, carried)
	if err != nil {
		return false, err
	}
	*h.deltas = deltas
	*h.order = params.CurrentPushdownFilters.predicateOrder()
	return true, nil
}

// recordFilterDelta folds one applied delta into a stream's compacted history.
//
// current is the stream's filters with delta already applied, carried the
// (id, revision) pairs delta carried, and history the deltas kept so far. It
// returns the new history: for each (id, revision) of current's live state,
// tombstones included, the first delta that carried it, in arrival order. The
// history is bounded by the number of predicate IDs, so re-reading it here is
// bounded too.
func recordFilterDelta(current *PushdownFilters, history [][]byte, delta []byte, carried []predicateRevision) ([][]byte, error) {
	wanted := make(map[string]uint64, len(current.v2Revisions))
	for id, revision := range current.v2Revisions {
		wanted[id] = revision
	}
	var kept [][]byte
	keep := func(delta []byte, pairs []predicateRevision) {
		needed := false
		for _, pair := range pairs {
			if revision, ok := wanted[pair.ID]; ok && revision == pair.Revision {
				delete(wanted, pair.ID)
				needed = true
			}
		}
		if needed {
			kept = append(kept, delta)
		}
	}
	for _, earlier := range history {
		pairs, err := filterDeltaRevisions(earlier)
		if err != nil {
			return nil, err
		}
		keep(earlier, pairs)
	}
	keep(delta, carried)
	return kept, nil
}

// replayFilterDeltas rebuilds a stream's current filters on an HTTP turn:
// params.CurrentPushdownFilters holds the init snapshot, the compacted history
// recorded by recordFilterDelta is applied on top of it, and the recorded
// predicate order is restored. Every document here was validated when it first
// arrived, and the cursor carrying them is sealed by the worker.
func replayFilterDeltas(params *ProcessParams, deltas [][]byte, order []string) error {
	if len(deltas) == 0 {
		return nil
	}
	if params.CurrentPushdownFilters == nil {
		return fmt.Errorf("filter delta history requires an initial snapshot")
	}
	for _, encoded := range deltas {
		// Not released: an applied predicate's literals are columns of this
		// batch (see applyTickFilters).
		batch, err := DeserializeRecordBatch(encoded)
		if err != nil {
			return err
		}
		if err := params.CurrentPushdownFilters.ApplyDelta(batch, params.JoinKeys); err != nil {
			return err
		}
	}
	return params.CurrentPushdownFilters.restorePredicateOrder(order)
}

// filterDeltaRevisions reads the (id, revision) of every update a delta
// carries, without applying it: bookkeeping over a delta that was already
// applied, and so already validated.
func filterDeltaRevisions(delta []byte) ([]predicateRevision, error) {
	batch, err := DeserializeRecordBatch(delta)
	if err != nil {
		return nil, err
	}
	defer batch.Release()
	_, raw, err := validateFilterV2Batch(batch)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Updates []predicateRevision `json:"updates"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("reading a recorded filter delta: %w", err)
	}
	return doc.Updates, nil
}
