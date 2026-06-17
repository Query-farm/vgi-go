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
// pinned output schema lives under a shared meta namespace keyed by name, and an
// atomic per-collection counter tracks the live row count so reads, TTL and
// max-rows eviction don't have to scan-and-deserialize every segment. Mirrors
// vgi-python's accumulate fixture.
var (
	metaNS      = []byte("acc.meta")
	countNS     = []byte("acc.count")
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

// appendSegment stores one time-keyed output segment for name and bumps the
// collection's live row counter.
func appendSegment(st *vgi.AttachStore, name string, batch arrow.RecordBatch, callTsMicros int64) error {
	ipc, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return err
	}
	if err := st.Put(segNS(name), segKey(callTsMicros), ipc); err != nil {
		return err
	}
	_, err = st.CounterAdd(countNS, []byte(name), batch.NumRows())
	return err
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

// countRows returns the current row count for name, read in O(1) from the
// collection's counter.
func countRows(st *vgi.AttachStore, name string) (int64, error) {
	return st.CounterGet(countNS, []byte(name))
}

// ttlEndKey is the 8-byte big-endian time bound for a cutoff. Segment keys are
// the time prefix followed by a uuid, so a key at exactly cutoffMicros sorts
// after this bound (it has the bound as a strict prefix) — making [.., end)
// exactly the segments whose ingest time is strictly before the cutoff.
func ttlEndKey(cutoffMicros int64) []byte {
	end := make([]byte, tsKeyBytes)
	binary.BigEndian.PutUint64(end, uint64(cutoffMicros))
	return end
}

// evictTTL drops every segment whose ingest time is before cutoffMicros via a
// single range delete over the time-keyed segments, decrementing the row
// counter by the rows removed.
func evictTTL(st *vgi.AttachStore, name string, cutoffMicros int64) error {
	if cutoffMicros <= 0 {
		return nil
	}
	ns := segNS(name)
	end := ttlEndKey(cutoffMicros)
	expired, err := st.Scan(ns, vgi.WithEnd(end))
	if err != nil {
		return err
	}
	if len(expired) == 0 {
		return nil
	}
	var rows int64
	for _, kv := range expired {
		b, err := vgi.DeserializeRecordBatch(kv.Value)
		if err != nil {
			return err
		}
		rows += b.NumRows()
		b.Release()
	}
	if _, err := st.DeleteRange(ns, nil, end); err != nil {
		return err
	}
	_, err = st.CounterAdd(countNS, []byte(name), -rows)
	return err
}

// evictMaxRows drops the oldest rows until at most maxRows remain, deleting
// whole segments and trimming only the one segment that straddles the cap. The
// live total comes from the counter, and only the segments actually evicted are
// deserialized (oldest-first).
func evictMaxRows(st *vgi.AttachStore, name string, maxRows int64) error {
	if maxRows <= 0 {
		return nil
	}
	total, err := countRows(st, name)
	if err != nil {
		return err
	}
	if total <= maxRows {
		return nil
	}
	overflow := total - maxRows
	ns := segNS(name)
	kvs, err := st.Scan(ns) // oldest-first
	if err != nil {
		return err
	}
	var removed int64
	for _, kv := range kvs {
		b, err := vgi.DeserializeRecordBatch(kv.Value)
		if err != nil {
			return err
		}
		rows := b.NumRows()
		if removed+rows <= overflow {
			b.Release()
			if err := st.DeleteKey(ns, kv.Key); err != nil {
				return err
			}
			removed += rows
			if removed == overflow {
				break
			}
			continue
		}
		// Boundary segment: keep its newest rows, drop the oldest `offset`.
		offset := overflow - removed
		trimmed := b.NewSlice(offset, rows)
		ipc, serErr := vgi.SerializeRecordBatch(trimmed)
		trimmed.Release()
		b.Release()
		if serErr != nil {
			return serErr
		}
		if err := st.Put(ns, kv.Key, ipc); err != nil {
			return err
		}
		removed = overflow
		break
	}
	_, err = st.CounterAdd(countNS, []byte(name), -removed)
	return err
}

// clearCollection drops a collection (segments + pinned schema + counter) and
// returns the number of rows removed.
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
	if err := st.CounterDelete(countNS, []byte(name)); err != nil {
		return 0, err
	}
	return total, nil
}
