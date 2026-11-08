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
// of both tables, staged changes per transaction id, and the event log. The
// partition a function or hook works on proves the secret token reached it.

// Item is one row of main.items.
type Item struct {
	ID   int32  `json:"id"`
	Name string `json:"name"`
	Qty  *int64 `json:"qty"`
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
	// Pending maps a transaction id (lowercase hex) to the staged item
	// images of that transaction, keyed by item id.
	Pending map[string]map[int32]Item `json:"pending"`
	Events  []Event                   `json:"events"`
	NextSeq int64                     `json:"next_seq"`
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
		Labels:  []Label{{ID: 1, Label: str("x")}, {ID: 2, Label: str("y")}},
		Pending: map[string]map[int32]Item{},
		Events:  []Event{},
		NextSeq: 1,
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
// after the state is saved (so a refused commit still records its event).
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
		s.Pending = map[string]map[int32]Item{}
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

// visibleItems is the committed rows overlaid with tx's staged images
// (read-your-writes); committed rows only when tx is empty.
func (s *state) visibleItems(tx []byte) []Item {
	out := make([]Item, len(s.Items))
	copy(out, s.Items)
	if len(tx) == 0 {
		return out
	}
	staged := s.Pending[txHex(tx)]
	for i := range out {
		if img, ok := staged[out[i].ID]; ok {
			out[i] = img
		}
	}
	return out
}

// applyItems writes images into the committed rows.
func (s *state) applyItems(images map[int32]Item) {
	for i := range s.Items {
		if img, ok := images[s.Items[i].ID]; ok {
			s.Items[i] = img
		}
	}
}

// stagedViolation returns the first staged image (by id) with a negative qty.
func stagedViolation(images map[int32]Item) *Item {
	ids := make([]int, 0, len(images))
	for id := range images {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, id := range ids {
		img := images[int32(id)]
		if img.Qty != nil && *img.Qty < 0 {
			return &img
		}
	}
	return nil
}
