// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package accumulate

import (
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
)

// Persistent, attach-scoped collection store over vgi.AttachStore.
//
// A collection's rows live as append-only *segments* under a per-collection
// namespace, keyed by ingest time so a scan returns them oldest-first. The
// pinned output schema lives under a shared meta namespace keyed by name. This
// mirrors vgi-python's accumulate fixture, minus the separate row counter (row
// counts are derived by scanning the few per-call segments — cheap for the
// sizes these functions see).
var (
	metaNS      = []byte("acc.meta")
	segNSPrefix = []byte("acc.seg:")
)

// tsKeyBytes is the width of the big-endian ingest-time prefix on a segment
// key, so segment keys sort by time (memcmp == numeric for fixed-width
// big-endian unsigned).
const tsKeyBytes = 8

func segNS(name string) []byte {
	out := make([]byte, 0, len(segNSPrefix)+len(name))
	out = append(out, segNSPrefix...)
	out = append(out, name...)
	return out
}

// segKey is the big-endian ingest time followed by a random uuid, so keys sort
// by time and never collide within a call.
func segKey(callTsMicros int64) []byte {
	k := make([]byte, tsKeyBytes+16)
	binary.BigEndian.PutUint64(k[:tsKeyBytes], uint64(callTsMicros))
	u := uuid.New()
	copy(k[tsKeyBytes:], u[:])
	return k
}

func segKeyTimeMicros(key []byte) int64 {
	if len(key) < tsKeyBytes {
		return 0
	}
	return int64(binary.BigEndian.Uint64(key[:tsKeyBytes]))
}

// getSchema returns the pinned output schema for name, or (nil, nil) if absent.
func getSchema(st *vgi.AttachStore, name string) (*arrow.Schema, error) {
	blob, err := st.Get(metaNS, []byte(name))
	if err != nil || blob == nil {
		return nil, err
	}
	return vgi.DeserializeSchema(blob)
}

// putSchema pins the output schema for name.
func putSchema(st *vgi.AttachStore, name string, schema *arrow.Schema) error {
	b, err := vgi.SerializeSchema(schema)
	if err != nil {
		return err
	}
	return st.Put(metaNS, []byte(name), b)
}

// appendSegment stores one time-keyed output segment for name.
func appendSegment(st *vgi.AttachStore, name string, batch arrow.RecordBatch, callTsMicros int64) error {
	ipc, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return err
	}
	return st.Put(segNS(name), segKey(callTsMicros), ipc)
}

// readCollection returns every stored segment for name, oldest-first.
func readCollection(st *vgi.AttachStore, name string) ([]arrow.RecordBatch, error) {
	kvs, err := st.Scan(segNS(name))
	if err != nil {
		return nil, err
	}
	out := make([]arrow.RecordBatch, 0, len(kvs))
	for _, kv := range kvs {
		b, err := vgi.DeserializeRecordBatch(kv.Value)
		if err != nil {
			for _, prev := range out {
				prev.Release()
			}
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// countRows returns the current row count for name (summed across segments).
func countRows(st *vgi.AttachStore, name string) (int64, error) {
	kvs, err := st.Scan(segNS(name))
	if err != nil {
		return 0, err
	}
	var n int64
	for _, kv := range kvs {
		b, err := vgi.DeserializeRecordBatch(kv.Value)
		if err != nil {
			return 0, err
		}
		n += b.NumRows()
		b.Release()
	}
	return n, nil
}

// evictTTL drops every segment whose ingest time is before cutoffMicros. A
// segment carries a single call timestamp, so the time-keyed range below the
// cutoff is exactly the expired rows.
func evictTTL(st *vgi.AttachStore, name string, cutoffMicros int64) error {
	if cutoffMicros <= 0 {
		return nil
	}
	kvs, err := st.Scan(segNS(name))
	if err != nil {
		return err
	}
	ns := segNS(name)
	for _, kv := range kvs {
		if segKeyTimeMicros(kv.Key) < cutoffMicros {
			if err := st.DeleteKey(ns, kv.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// evictMaxRows drops the oldest rows until at most maxRows remain, deleting
// whole segments and trimming only the one segment that straddles the cap.
func evictMaxRows(st *vgi.AttachStore, name string, maxRows int64) error {
	if maxRows <= 0 {
		return nil
	}
	kvs, err := st.Scan(segNS(name)) // oldest-first
	if err != nil {
		return err
	}
	type seg struct {
		key   []byte
		batch arrow.RecordBatch
		rows  int64
	}
	segs := make([]seg, 0, len(kvs))
	var total int64
	for _, kv := range kvs {
		b, err := vgi.DeserializeRecordBatch(kv.Value)
		if err != nil {
			for _, s := range segs {
				s.batch.Release()
			}
			return err
		}
		segs = append(segs, seg{key: kv.Key, batch: b, rows: b.NumRows()})
		total += b.NumRows()
	}
	defer func() {
		for _, s := range segs {
			s.batch.Release()
		}
	}()
	if total <= maxRows {
		return nil
	}
	ns := segNS(name)
	overflow := total - maxRows
	var removed int64
	for _, s := range segs {
		if removed+s.rows <= overflow {
			if err := st.DeleteKey(ns, s.key); err != nil {
				return err
			}
			removed += s.rows
			if removed == overflow {
				break
			}
			continue
		}
		// Boundary segment: keep its newest rows, drop the oldest `offset`.
		offset := overflow - removed
		trimmed := s.batch.NewSlice(offset, s.rows)
		ipc, serErr := vgi.SerializeRecordBatch(trimmed)
		trimmed.Release()
		if serErr != nil {
			return serErr
		}
		if err := st.Put(ns, s.key, ipc); err != nil {
			return err
		}
		break
	}
	return nil
}

// clearCollection drops a collection (segments + pinned schema) and returns the
// number of rows removed.
func clearCollection(st *vgi.AttachStore, name string) (int64, error) {
	total, err := countRows(st, name)
	if err != nil {
		return 0, err
	}
	if err := st.DeleteNS(segNS(name)); err != nil {
		return 0, err
	}
	if err := st.DeleteKey(metaNS, []byte(name)); err != nil {
		return 0, err
	}
	return total, nil
}
