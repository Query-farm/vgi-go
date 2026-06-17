// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package simple_writable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/Query-farm/vgi-go/vgi"
)

// Per-attach row storage over vgi.AttachStore.
//
// Each pre-defined table gets its own namespace ("sw:<table>"); rows live there
// keyed by an 8-byte big-endian rowid (assigned monotonically from a per-table
// atomic counter), with the value a gob-encoded column→Go-value map. The
// AttachStore is scoped to the random per-ATTACH id minted at catalog_attach, so
// two ATTACH sessions never share rows — matching vgi-python's per-attach SQLite
// file. The gob map (rather than a serialized Arrow batch) makes UPDATE a trivial
// key overwrite.

func init() {
	// gob needs the concrete types behind the map's interface values.
	gob.Register(int64(0))
	gob.Register("")
}

// rowMap is one stored row: user column name → value (int64 or string; nil = NULL).
type rowMap = map[string]any

func tableNS(table string) []byte { return []byte("sw:" + table) }

// rowidSeqKey names the per-table monotonic rowid counter. It lives in the
// counter store, distinct from the row data (which is keyed by 8-byte rowid),
// so it never collides with a row.
var rowidSeqKey = []byte("rowid_seq")

func ridKey(rid int64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(rid))
	return k
}

func ridFromKey(k []byte) int64 { return int64(binary.BigEndian.Uint64(k)) }

func encodeRow(r rowMap) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRow(data []byte) (rowMap, error) {
	var r rowMap
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&r); err != nil {
		return nil, err
	}
	return r, nil
}

// allocRowids atomically reserves n consecutive rowids for table and returns the
// first. The per-table counter is monotonic and race-free across concurrent
// writers (unlike a scan-for-max read-modify-write) and never reuses a freed id.
func allocRowids(st *vgi.AttachStore, table string, n int64) (int64, error) {
	if n <= 0 {
		return 0, nil
	}
	last, err := st.CounterAdd(tableNS(table), rowidSeqKey, n)
	if err != nil {
		return 0, err
	}
	return last - n + 1, nil
}

// storedRow is a rowid + decoded values, returned by scanRows in rowid order.
type storedRow struct {
	rid  int64
	cols rowMap
}

// scanRows returns every row for table in ascending rowid order (AttachStore.Scan
// returns key-sorted, and big-endian keys sort numerically).
func scanRows(st *vgi.AttachStore, table string) ([]storedRow, error) {
	kvs, err := st.Scan(tableNS(table))
	if err != nil {
		return nil, err
	}
	rows := make([]storedRow, 0, len(kvs))
	for _, kv := range kvs {
		cols, err := decodeRow(kv.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, storedRow{rid: ridFromKey(kv.Key), cols: cols})
	}
	return rows, nil
}

func getRow(st *vgi.AttachStore, table string, rid int64) (rowMap, error) {
	data, err := st.Get(tableNS(table), ridKey(rid))
	if err != nil || data == nil {
		return nil, err
	}
	return decodeRow(data)
}

func putRow(st *vgi.AttachStore, table string, rid int64, r rowMap) error {
	data, err := encodeRow(r)
	if err != nil {
		return err
	}
	return st.Put(tableNS(table), ridKey(rid), data)
}

func deleteRow(st *vgi.AttachStore, table string, rid int64) error {
	return st.DeleteKey(tableNS(table), ridKey(rid))
}
