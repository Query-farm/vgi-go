// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Per-batch output annotation metadata keys. These mirror vgi-python and are
// read by the C++ extension off the emitted Arrow batch's schema metadata.
const (
	metaBatchIndex      = "vgi_batch_index"
	metaPartitionValues = "vgi_partition_values#b64"
	// metaParentRow carries per-output-row provenance for the batched
	// correlated LATERAL operator: a base64-encoded raw little-endian int32[]
	// (NOT Arrow IPC) with one input-row index per emitted output row. Mirrors
	// vgi-python's vgi_rpc.parent_row#b64 carrier.
	metaParentRow = "vgi_rpc.parent_row#b64"
	// partitionColumnKey marks a schema field as a partition column.
	partitionColumnKey = "vgi.partition_column"
)

// PartitionField builds an Arrow field annotated as a partition column (schema
// metadata vgi.partition_column=true). Mirrors vgi-python's partition_field.
func PartitionField(name string, typ arrow.DataType, nullable bool) arrow.Field {
	md := arrow.NewMetadata([]string{partitionColumnKey}, []string{"true"})
	return arrow.Field{Name: name, Type: typ, Nullable: nullable, Metadata: md}
}

// EmitOption contributes annotation metadata to an emitted batch. Options are
// applied in order, so a later option overwrites an earlier one's keys.
type EmitOption func(map[string]string) error

// WithCacheControl advertises a cacheable result. The cache-control keys are
// read once per result, off the FIRST data batch of the stream — passing this
// on a later batch has no effect. Nil is a no-op, so a caller can write
//
//	var cc *vgi.CacheControl
//	if firstBatch { cc = &vgi.CacheControl{Ttl: vgi.Seconds(300)} }
//	vgi.Emit(out, batch, vgi.WithCacheControl(cc))
func WithCacheControl(cc *CacheControl) EmitOption {
	return func(md map[string]string) error {
		if cc == nil {
			return nil
		}
		if err := cc.Validate(); err != nil {
			return err
		}
		for k, v := range cc.Metadata() {
			md[k] = v
		}
		return nil
	}
}

// WithMetadata sets one arbitrary annotation key on the emitted batch.
func WithMetadata(key, value string) EmitOption {
	return func(md map[string]string) error {
		md[key] = value
		return nil
	}
}

// applyEmitOptions folds opts into a fresh metadata map, seeded with base.
// Returns nil when nothing was contributed, so the caller can emit unannotated.
func applyEmitOptions(base map[string]string, opts []EmitOption) (map[string]string, error) {
	if len(base) == 0 && len(opts) == 0 {
		// Common case (plain Emit with no options): nothing to fold, so skip the
		// throwaway map allocation entirely.
		return nil, nil
	}
	md := make(map[string]string, len(base)+len(opts))
	for k, v := range base {
		md[k] = v
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(md); err != nil {
			return nil, err
		}
	}
	if len(md) == 0 {
		return nil, nil
	}
	return md, nil
}

// Emit emits a batch with the annotation metadata contributed by opts. With no
// options it is equivalent to out.Emit(batch).
func Emit(out *vgirpc.OutputCollector, batch arrow.RecordBatch, opts ...EmitOption) error {
	md, err := applyEmitOptions(nil, opts)
	if err != nil {
		return err
	}
	if md == nil {
		return out.Emit(batch)
	}
	return out.EmitWithMetadata(batch, md)
}

// EmitBatchIndex emits a batch tagged with vgi_batch_index. Use it from a
// table function that declares SupportsBatchIndex. The C++ extension enforces
// monotonicity and the per-pipeline cap.
func EmitBatchIndex(out *vgirpc.OutputCollector, batch arrow.RecordBatch, batchIndex int64, opts ...EmitOption) error {
	md, err := applyEmitOptions(map[string]string{
		metaBatchIndex: strconv.FormatInt(batchIndex, 10),
	}, opts)
	if err != nil {
		return err
	}
	return out.EmitWithMetadata(batch, md)
}

// EmitParentRows emits a batch declaring, per output row, which input row
// produced it — the provenance map for the **batched correlated LATERAL**
// operator (a blended function under FROM t, f(t.x) / LATERAL f(t.x)): the C++
// extension ships a whole input chunk to the worker in ONE exchange and reads
// ONE output batch, then maps each output row back to the input row that
// produced it via this array — so a 1->N fan-out or 1->0 filter can be batched
// instead of driven row-by-row.
//
// parentRows[i] is the 0-based index (into the input batch) of the row that
// produced output row i. Encoded as a raw little-endian int32 array (NOT Arrow
// IPC), base64-encoded, under vgi_rpc.parent_row#b64 — the C++ side
// reinterprets the bytes directly. Absent metadata means an identity 1->1 map
// (the common case: the extension assumes it, and requires output rows ==
// input rows).
//
// Contract: len(parentRows) MUST equal batch.NumRows() (a mismatch is a worker
// bug that would corrupt the stamping). Values are range-checked against the
// input width on the C++ side (which knows it authoritatively). Mirrors
// vgi-python's out.emit(..., parent_rows=...).
func EmitParentRows(out *vgirpc.OutputCollector, batch arrow.RecordBatch, parentRows []int32, opts ...EmitOption) error {
	if int64(len(parentRows)) != batch.NumRows() {
		return fmt.Errorf(
			"EmitParentRows length %d != batch.NumRows %d; parentRows must carry exactly one input-row index per emitted output row",
			len(parentRows), batch.NumRows())
	}
	if batch.NumRows() == 0 {
		// Nothing to map; skip the base64+pack for an empty emit.
		return Emit(out, batch, opts...)
	}
	raw := make([]byte, 4*len(parentRows))
	for i, v := range parentRows {
		binary.LittleEndian.PutUint32(raw[i*4:], uint32(v))
	}
	md, err := applyEmitOptions(map[string]string{
		metaParentRow: base64.StdEncoding.EncodeToString(raw),
	}, opts)
	if err != nil {
		return err
	}
	return out.EmitWithMetadata(batch, md)
}

// PartitionValue is the (min, max) pair for one partition column. Values are Go
// scalars compatible with the column's Arrow type (see appendValue).
type PartitionValue struct {
	Min any
	Max any
}

// EmitPartitioned emits a batch annotated with vgi_partition_values for a
// partition-aware table function. partitionFields are the declared partition
// columns (build them with PartitionField). For each, the (min, max) pair comes
// from explicit when present, else is auto-extracted from the same-named column
// in the emitted batch. Mirrors vgi-python's _merge_partition_values contract:
// the SINGLE_VALUE distinct-value check, the "requires partition-annotated
// fields" guard, and the annotated-but-absent error.
func EmitPartitioned(out *vgirpc.OutputCollector, batch arrow.RecordBatch, partitionFields []arrow.Field, kind PartitionKind, explicit map[string]PartitionValue, opts ...EmitOption) error {
	if len(partitionFields) == 0 {
		if len(explicit) > 0 {
			return fmt.Errorf("EmitPartitioned requires partition-annotated fields, but none were declared")
		}
		return Emit(out, batch, opts...)
	}
	// Empty batches carry no partition metadata.
	if batch.NumRows() == 0 {
		return Emit(out, batch, opts...)
	}

	mem := defaultAllocator
	builders := make([]array.Builder, len(partitionFields))
	releaseBuilders := func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}
	for i, f := range partitionFields {
		b := array.NewBuilder(mem, f.Type)
		builders[i] = b
		var minV, maxV any
		if pv, ok := explicit[f.Name]; ok {
			minV, maxV = pv.Min, pv.Max
		} else {
			idx := batch.Schema().FieldIndices(f.Name)
			if len(idx) == 0 {
				releaseBuilders()
				return fmt.Errorf("partition column %q is partition-annotated but absent from emitted batch; pass partition_values", f.Name)
			}
			mn, mx, moreThanOne, err := columnMinMax(batch.Column(idx[0]))
			if err != nil {
				releaseBuilders()
				return fmt.Errorf("partition column %q: %w", f.Name, err)
			}
			if kind == PartitionKindSingleValuePartitions && moreThanOne {
				releaseBuilders()
				return fmt.Errorf("SINGLE_VALUE_PARTITIONS function emitted a chunk with more than one distinct value in partition column %q (expected exactly 1)", f.Name)
			}
			minV, maxV = mn, mx
		}
		appendValue(b, minV)
		appendValue(b, maxV)
	}

	cols := make([]arrow.Array, len(builders))
	for i, b := range builders {
		cols[i] = b.NewArray()
		b.Release()
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	pvSchema := arrow.NewSchema(partitionFields, nil)
	pvBatch := array.NewRecordBatch(pvSchema, cols, 2)
	defer pvBatch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(pvSchema))
	if err := w.Write(pvBatch); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	b64 := base64.StdEncoding.EncodeToString(buf.Bytes())
	md, err := applyEmitOptions(map[string]string{metaPartitionValues: b64}, opts)
	if err != nil {
		return err
	}
	return out.EmitWithMetadata(batch, md)
}

// columnMinMax returns the min and max of a non-null Arrow column, plus whether
// the column carries more than one distinct value. Supports the scalar types
// used by partition columns. moreThanOne is exactly (min != max) over the
// non-null values, so no per-row set is needed to detect a SINGLE_VALUE
// violation.
func columnMinMax(col arrow.Array) (min, max any, moreThanOne bool, err error) {
	n := col.Len()
	switch c := col.(type) {
	case *array.Int64:
		var lo, hi int64
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max, moreThanOne = lo, hi, lo != hi
	case *array.Int32:
		var lo, hi int32
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max, moreThanOne = lo, hi, lo != hi
	case *array.Float64:
		var lo, hi float64
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max, moreThanOne = lo, hi, lo != hi
	case *array.String:
		var lo, hi string
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max, moreThanOne = lo, hi, lo != hi
	default:
		return nil, nil, false, fmt.Errorf("unsupported partition column type %T", col)
	}
	return min, max, moreThanOne, nil
}
