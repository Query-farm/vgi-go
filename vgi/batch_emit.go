// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-rpc/vgirpc"
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

// isPartitionField reports whether a field carries the partition-column marker.
func isPartitionField(f arrow.Field) bool {
	if !f.HasMetadata() {
		return false
	}
	if i := f.Metadata.FindKey(partitionColumnKey); i >= 0 {
		return f.Metadata.Values()[i] == "true"
	}
	return false
}

// EmitBatchIndex emits a batch tagged with vgi_batch_index. Use it from a
// table function that declares SupportsBatchIndex. The C++ extension enforces
// monotonicity and the per-pipeline cap.
func EmitBatchIndex(out *vgirpc.OutputCollector, batch arrow.RecordBatch, batchIndex int64) error {
	return out.EmitWithMetadata(batch, map[string]string{
		metaBatchIndex: strconv.FormatInt(batchIndex, 10),
	})
}

// PartitionValue is the (min, max) pair for one partition column. Values are Go
// scalars compatible with the column's Arrow type (see appendValue).
type PartitionValue struct {
	Min any
	Max any
}

// EmitPartitioned emits a batch annotated with vgi_partition_values for a
// partition-aware table function. Partition columns are the batch fields
// annotated via PartitionField. When explicit is nil, min/max are auto-extracted
// from each partition column in the batch; otherwise explicit overrides are
// used. Mirrors vgi-python's _merge_partition_values contract, including the
// SINGLE_VALUE distinct-value check and the annotated-but-absent error.
func EmitPartitioned(out *vgirpc.OutputCollector, batch arrow.RecordBatch, kind PartitionKind, explicit map[string]PartitionValue) error {
	schema := batch.Schema()
	partFields := make([]arrow.Field, 0)
	partIdx := make([]int, 0)
	for i, f := range schema.Fields() {
		if isPartitionField(f) {
			partFields = append(partFields, f)
			partIdx = append(partIdx, i)
		}
	}
	if len(explicit) > 0 && len(partFields) == 0 {
		return fmt.Errorf("partition_values supplied but the output schema has no partition-annotated fields")
	}
	if len(partFields) == 0 {
		// Nothing to annotate; emit plain.
		return out.Emit(batch)
	}
	// Empty batches carry no partition metadata.
	if batch.NumRows() == 0 {
		return out.Emit(batch)
	}

	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(partFields))
	for i, f := range partFields {
		b := array.NewBuilder(mem, f.Type)
		builders[i] = b
		var minV, maxV any
		if explicit != nil {
			pv, ok := explicit[f.Name]
			if !ok {
				for _, bb := range builders {
					bb.Release()
				}
				return fmt.Errorf("partition column %q is partition-annotated but absent from partition_values", f.Name)
			}
			minV, maxV = pv.Min, pv.Max
		} else {
			col := batch.Column(partIdx[i])
			mn, mx, distinct, err := columnMinMax(col)
			if err != nil {
				for _, bb := range builders {
					bb.Release()
				}
				return fmt.Errorf("partition column %q: %w", f.Name, err)
			}
			if kind == PartitionKindSingleValuePartitions && distinct > 1 {
				for _, bb := range builders {
					bb.Release()
				}
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
	pvSchema := arrow.NewSchema(partFields, nil)
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
	return out.EmitWithMetadata(batch, map[string]string{metaPartitionValues: b64})
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
