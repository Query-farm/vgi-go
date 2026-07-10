// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Per-batch output annotation metadata keys. These mirror vgi-python and are
// read by the C++ extension off the emitted Arrow batch's schema metadata.
const (
	metaBatchIndex      = "vgi_batch_index"
	metaPartitionValues = "vgi_partition_values#b64"
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

	mem := memory.NewGoAllocator()
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
			mn, mx, distinct, err := columnMinMax(batch.Column(idx[0]))
			if err != nil {
				releaseBuilders()
				return fmt.Errorf("partition column %q: %w", f.Name, err)
			}
			if kind == PartitionKindSingleValuePartitions && distinct > 1 {
				releaseBuilders()
				return fmt.Errorf("SINGLE_VALUE_PARTITIONS function emitted a chunk with %d distinct values in partition column %q (expected exactly 1)", distinct, f.Name)
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

// columnMinMax returns the min, max, and distinct-value count of a non-null
// Arrow column. Supports the scalar types used by partition columns.
func columnMinMax(col arrow.Array) (min, max any, distinct int, err error) {
	n := col.Len()
	seen := map[any]struct{}{}
	switch c := col.(type) {
	case *array.Int64:
		var lo, hi int64
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			seen[v] = struct{}{}
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max = lo, hi
	case *array.Int32:
		var lo, hi int32
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			seen[v] = struct{}{}
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max = lo, hi
	case *array.Float64:
		var lo, hi float64
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			seen[v] = struct{}{}
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max = lo, hi
	case *array.String:
		var lo, hi string
		first := true
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				continue
			}
			v := c.Value(i)
			seen[v] = struct{}{}
			if first || v < lo {
				lo = v
			}
			if first || v > hi {
				hi = v
			}
			first = false
		}
		min, max = lo, hi
	default:
		return nil, nil, 0, fmt.Errorf("unsupported partition column type %T", col)
	}
	return min, max, len(seen), nil
}
