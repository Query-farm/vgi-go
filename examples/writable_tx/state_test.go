// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package writable_tx

import (
	"strings"
	"testing"
)

func ids(items []Item) []int32 {
	out := make([]int32, len(items))
	for i, it := range items {
		out[i] = it.ID
	}
	return out
}

func sameIDs(t *testing.T, what string, got []Item, want ...int32) {
	t.Helper()
	g := ids(got)
	if len(g) != len(want) {
		t.Fatalf("%s: ids %v, want %v", what, g, want)
	}
	for i := range g {
		if g[i] != want[i] {
			t.Fatalf("%s: ids %v, want %v", what, g, want)
		}
	}
}

// Staged inserts and deletes are visible to their transaction only, in id
// order; deleting a staged insert drops it, updating one keeps it an insert.
func TestStagedInsertDeleteReadYourWrites(t *testing.T) {
	s := initialState()
	tx := []byte{0xab, 0x01}
	s.stage(tx, Item{ID: 0, Name: "zero", Qty: i64(0)})
	s.stage(tx, Item{ID: 7, Name: "seven", Qty: i64(7)})
	s.stageDelete(tx, 2)
	sameIDs(t, "tx view", s.visibleItems(tx), 0, 1, 3, 7)
	sameIDs(t, "committed view", s.visibleItems(nil), 1, 2, 3)

	// Updating a staged insert edits it and keeps it an insert.
	s.stage(tx, Item{ID: 7, Name: "seven", Qty: i64(70)})
	if st := s.Pending[txHex(tx)][7]; !st.Inserted || *st.Qty != 70 {
		t.Fatalf("staged insert after update: %+v", st)
	}
	// Deleting a staged insert drops it from the staged changes.
	s.stageDelete(tx, 0)
	if _, ok := s.Pending[txHex(tx)][0]; ok {
		t.Fatal("deleted staged insert still staged")
	}
	sameIDs(t, "tx view after delete", s.visibleItems(tx), 1, 3, 7)

	// Re-inserting a committed id this transaction deleted is a replacement,
	// not an insert (so it does not collide at commit).
	s.stage(tx, Item{ID: 2, Name: "b2"})
	if st := s.Pending[txHex(tx)][2]; st.Inserted || st.Deleted {
		t.Fatalf("re-insert of a deleted committed row: %+v", st)
	}

	if why := s.commitViolation(s.Pending[txHex(tx)]); why != "" {
		t.Fatalf("clean transaction refused: %s", why)
	}
	s.applyStaged(s.Pending[txHex(tx)])
	sameIDs(t, "after commit", s.visibleItems(nil), 1, 2, 3, 7)
	if s.Items[1].Name != "b2" || s.Items[1].Qty != nil {
		t.Fatalf("replaced row %+v", s.Items[1])
	}
}

// A commit refuses a staged negative qty and an inserted id another attach
// committed meanwhile; staged deletes are not checked.
func TestCommitViolation(t *testing.T) {
	s := initialState()
	tx := []byte{1}
	s.stage(tx, Item{ID: 9, Name: "n", Qty: i64(-1)})
	if why := s.commitViolation(s.Pending[txHex(tx)]); !strings.Contains(why, "qty -1 for id 9") {
		t.Fatalf("negative qty: %q", why)
	}
	s.Pending = map[string]map[int32]Staged{}
	s.stage(tx, Item{ID: 9, Name: "n"})
	s.putItems(map[int32]Item{9: {ID: 9, Name: "other"}})
	if why := s.commitViolation(s.Pending[txHex(tx)]); !strings.Contains(why, "duplicate id 9") {
		t.Fatalf("racing insert: %q", why)
	}
	s.Pending = map[string]map[int32]Staged{}
	s.stageDelete(tx, 1)
	if why := s.commitViolation(s.Pending[txHex(tx)]); why != "" {
		t.Fatalf("delete refused: %q", why)
	}
}

// One writes() entry per call, keyed by its execution id: batches add rows,
// finalize (which shares only the execution id with them) marks it, and a call
// that never applied a batch gets its entry at finalize.
func TestRecordWrite(t *testing.T) {
	s := initialState()
	call, empty := []byte{1, 2}, []byte{3, 4}
	seq := s.recordWrite(call, "insert", nil, 2048, false)
	if again := s.recordWrite(call, "insert", nil, 952, false); again != seq {
		t.Fatalf("second batch opened entry %d, want %d", again, seq)
	}
	if fin := s.recordWrite(call, "insert", nil, 0, true); fin != seq {
		t.Fatalf("finalize opened entry %d, want %d", fin, seq)
	}
	other := s.recordWrite(empty, "delete", []byte{0xff}, 0, true)
	if len(s.Writes) != 2 || other == seq {
		t.Fatalf("writes %+v", s.Writes)
	}
	if w := s.Writes[0]; w.Rows != 3000 || !w.Finalized || w.Op != "insert" || w.TxID != "" {
		t.Fatalf("first call %+v", w)
	}
	if w := s.Writes[1]; w.Rows != 0 || !w.Finalized || w.TxID != "ff" {
		t.Fatalf("second call %+v", w)
	}
}

// putItems upserts by id and keeps id order; deleteItems ignores missing ids.
func TestPutDeleteItems(t *testing.T) {
	s := initialState()
	s.putItems(map[int32]Item{0: {ID: 0, Name: "z"}, 2: {ID: 2, Name: "B"}})
	sameIDs(t, "put", s.Items, 0, 1, 2, 3)
	if s.Items[2].Name != "B" {
		t.Fatalf("replace: %+v", s.Items[2])
	}
	s.deleteItems(map[int32]bool{1: true, 42: true})
	sameIDs(t, "delete", s.Items, 0, 2, 3)
}
