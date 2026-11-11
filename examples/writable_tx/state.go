// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package writable_tx

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// The fixture's state is shared by every worker process of a run (the stdio,
// launch-pool and http lanes may each use several), so it lives in one JSON
// file guarded by a file lock. It is partitioned by the ATTACH's token (a
// NULL token is its own partition): each partition holds the committed rows
// of both tables, staged changes per transaction id, the event log and the
// write-call log. The partition a function or hook works on proves the
// secret token reached it.

// Item is one row of main.items.
type Item struct {
	ID   int32  `json:"id"`
	Name string `json:"name"`
	Qty  *int64 `json:"qty"`
}

// Staged is a transaction's pending change to one item id: the row's new
// image, or Deleted. Inserted marks an id the transaction added that was not
// committed when it was staged (committing it then must not collide with a
// row another attach committed meanwhile).
type Staged struct {
	Item
	Deleted  bool `json:"deleted,omitempty"`
	Inserted bool `json:"inserted,omitempty"`
}

// Label is one row of main.labels.
type Label struct {
	ID    int32   `json:"id"`
	Label *string `json:"label"`
}

// Event is one entry of the event log.
type Event struct {
	Seq  int64  `json:"seq"`
	Kind string `json:"kind"`
	TxID string `json:"tx_id"`
	OK   bool   `json:"ok"`
}

// Write is one entry of the write-call log (writes()): one per call of a
// write function, created by its first successful batch (or by its finalize
// when it received no batch). ExecID is the call's execution id, the one
// thing its process and finalize phases share: the SDK gives Finalize a fresh
// NewState (and a call can move between worker processes), so the entry is
// found by the execution id, not by anything carried in the call's state.
type Write struct {
	Seq       int64  `json:"seq"`
	Op        string `json:"op"`
	TxID      string `json:"tx_id"`
	Rows      int64  `json:"rows"`
	Finalized bool   `json:"finalized"`
	ExecID    string `json:"exec_id"`
}

// partitionKey is the state partition of an ATTACH's token.
func partitionKey(token *string) string {
	if token == nil {
		return "null"
	}
	return "token:" + *token
}

// fileState is the whole persisted fixture state: one partition per token.
type fileState struct {
	Partitions map[string]*state `json:"partitions"`
}

// state is one token's partition.
type state struct {
	Items  []Item  `json:"items"`
	Labels []Label `json:"labels"`
	// Pending maps a transaction id (lowercase hex) to the staged changes
	// of that transaction, keyed by item id.
	Pending      map[string]map[int32]Staged `json:"pending"`
	Events       []Event                     `json:"events"`
	NextSeq      int64                       `json:"next_seq"`
	Writes       []Write                     `json:"writes"`
	NextWriteSeq int64                       `json:"next_write_seq"`
}

func i64(v int64) *int64     { return &v }
func str(v string) *string   { return &v }
func txHex(tx []byte) string { return hex.EncodeToString(tx) }

func initialState() *state {
	return &state{
		Items: []Item{
			{ID: 1, Name: "a", Qty: i64(10)},
			{ID: 2, Name: "b", Qty: i64(20)},
			{ID: 3, Name: "c", Qty: i64(30)},
		},
		Labels:       []Label{{ID: 1, Label: str("x")}, {ID: 2, Label: str("y")}},
		Pending:      map[string]map[int32]Staged{},
		Events:       []Event{},
		NextSeq:      1,
		Writes:       []Write{},
		NextWriteSeq: 1,
	}
}

// stateDir is $VGI_TXFIX_STATE_DIR, default <system temp dir>/vgi-txfix.
func stateDir() string {
	if d := os.Getenv("VGI_TXFIX_STATE_DIR"); d != "" {
		return d
	}
	return filepath.Join(os.TempDir(), "vgi-txfix")
}

// withState runs fn on the token's partition under the file lock and, when
// write is set, writes the state back atomically. fn's error is returned
// after the state is saved (so a refused commit still records its event):
// fn must therefore validate before it mutates anything it would not keep.
func withState(token *string, write bool, fn func(s *state) error) error {
	dir := stateDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("txfix: state dir: %w", err)
	}
	unlock, err := lockFile(filepath.Join(dir, "state.lock"))
	if err != nil {
		return fmt.Errorf("txfix: lock: %w", err)
	}
	defer unlock()
	path := filepath.Join(dir, "state.json")
	fs := &fileState{}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(data, fs); err != nil {
			return fmt.Errorf("txfix: state file: %w", err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("txfix: state file: %w", err)
	}
	if fs.Partitions == nil {
		fs.Partitions = map[string]*state{}
	}
	key := partitionKey(token)
	s := fs.Partitions[key]
	if s == nil {
		s = initialState()
		fs.Partitions[key] = s
	}
	if s.Pending == nil {
		s.Pending = map[string]map[int32]Staged{}
	}
	if s.NextWriteSeq == 0 {
		s.NextWriteSeq = 1
	}
	fnErr := fn(s)
	if !write {
		return fnErr
	}
	out, err := json.Marshal(fs)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return fnErr
}

// record appends an event; ok is false only for a refused commit.
func (s *state) record(kind string, tx []byte, ok bool) {
	s.Events = append(s.Events, Event{Seq: s.NextSeq, Kind: kind, TxID: txHex(tx), OK: ok})
	s.NextSeq++
}

// recordWrite adds rows to the write-call log entry of the call execID
// (creating the entry on the call's first record) and returns the entry's
// seq. finalized marks the call's finalize.
func (s *state) recordWrite(execID []byte, op string, tx []byte, rows int64, finalized bool) int64 {
	key := hex.EncodeToString(execID)
	for i := range s.Writes {
		if s.Writes[i].ExecID == key && s.Writes[i].Op == op {
			s.Writes[i].Rows += rows
			s.Writes[i].Finalized = s.Writes[i].Finalized || finalized
			return s.Writes[i].Seq
		}
	}
	seq := s.NextWriteSeq
	s.NextWriteSeq++
	s.Writes = append(s.Writes, Write{Seq: seq, Op: op, TxID: txHex(tx), Rows: rows, Finalized: finalized, ExecID: key})
	return seq
}

// visibleItems is the committed rows overlaid with tx's staged changes
// (read-your-writes), ordered by id; committed rows only when tx is empty.
func (s *state) visibleItems(tx []byte) []Item {
	byID := map[int32]Item{}
	for _, it := range s.Items {
		byID[it.ID] = it
	}
	if len(tx) > 0 {
		for id, st := range s.Pending[txHex(tx)] {
			if st.Deleted {
				delete(byID, id)
			} else {
				byID[id] = st.Item
			}
		}
	}
	out := make([]Item, 0, len(byID))
	for _, it := range byID {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// committedIDs is the set of committed item ids.
func (s *state) committedIDs() map[int32]bool {
	ids := make(map[int32]bool, len(s.Items))
	for _, it := range s.Items {
		ids[it.ID] = true
	}
	return ids
}

// putItems inserts or replaces committed rows by id, keeping id order.
func (s *state) putItems(images map[int32]Item) {
	for i := range s.Items {
		if img, ok := images[s.Items[i].ID]; ok {
			s.Items[i] = img
			delete(images, s.Items[i].ID)
		}
	}
	for _, img := range images {
		s.Items = append(s.Items, img)
	}
	sort.Slice(s.Items, func(i, j int) bool { return s.Items[i].ID < s.Items[j].ID })
}

// deleteItems removes the committed rows with the given ids.
func (s *state) deleteItems(ids map[int32]bool) {
	kept := s.Items[:0]
	for _, it := range s.Items {
		if !ids[it.ID] {
			kept = append(kept, it)
		}
	}
	s.Items = kept
}

// stage records tx's new image of an item. An id the transaction inserted
// keeps its Inserted mark; an id neither committed nor staged becomes
// Inserted; anything else (a committed row, or one this transaction deleted
// and now re-inserts) is a plain replacement.
func (s *state) stage(tx []byte, img Item) {
	key := txHex(tx)
	if s.Pending[key] == nil {
		s.Pending[key] = map[int32]Staged{}
	}
	prev, had := s.Pending[key][img.ID]
	inserted := (had && prev.Inserted && !prev.Deleted) || (!had && !s.committedIDs()[img.ID])
	s.Pending[key][img.ID] = Staged{Item: img, Inserted: inserted}
}

// stageDelete records tx's delete of an item: a row this transaction inserted
// is simply dropped from the staged changes.
func (s *state) stageDelete(tx []byte, id int32) {
	key := txHex(tx)
	if s.Pending[key] == nil {
		s.Pending[key] = map[int32]Staged{}
	}
	if prev, ok := s.Pending[key][id]; ok && prev.Inserted {
		delete(s.Pending[key], id)
		return
	}
	s.Pending[key][id] = Staged{Item: Item{ID: id}, Deleted: true}
}

// commitViolation is why tx's staged changes cannot commit ("" when they
// can): the first staged image (by id) with a negative qty, or an inserted id
// another attach committed meanwhile.
func (s *state) commitViolation(staged map[int32]Staged) string {
	ids := make([]int, 0, len(staged))
	for id := range staged {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	committed := s.committedIDs()
	for _, id := range ids {
		st := staged[int32(id)]
		if st.Deleted {
			continue
		}
		if st.Qty != nil && *st.Qty < 0 {
			return fmt.Sprintf("txfix: commit refused: qty %d for id %d", *st.Qty, st.ID)
		}
		if st.Inserted && committed[st.ID] {
			return fmt.Sprintf("txfix: commit refused: duplicate id %d", st.ID)
		}
	}
	return ""
}

// applyStaged commits staged changes into the committed rows.
func (s *state) applyStaged(staged map[int32]Staged) {
	images := map[int32]Item{}
	deleted := map[int32]bool{}
	for id, st := range staged {
		if st.Deleted {
			deleted[id] = true
		} else {
			images[id] = st.Item
		}
	}
	s.deleteItems(deleted)
	s.putItems(images)
}
