// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// defaultAllocator is the shared Go memory allocator used by all Map*/Generate* helpers.
var defaultAllocator = memory.NewGoAllocator()

// ArrayBuilder is a constraint for Arrow typed builders used by the Map* functions.
type ArrayBuilder[T any] interface {
	// Append adds a non-null value of type T to the end of the array being built.
	Append(T)
	// AppendNull adds a null entry to the end of the array being built.
	AppendNull()
	// Reserve pre-allocates capacity for at least n additional elements.
	Reserve(int)
	// NewArray finalizes the builder and returns the constructed Arrow array.
	NewArray() arrow.Array
	// Release frees the buffers held by the builder.
	Release()
}

// MapColumn maps a single input column to an output column.
// Null inputs are propagated as null outputs (DEFAULT null handling).
func MapColumn[T any, B ArrayBuilder[T]](
	params *ProcessParams,
	batch arrow.RecordBatch,
	colIndex int,
	newBuilder func(memory.Allocator) B,
	transform func(col arrow.Array, i int) T,
) (arrow.RecordBatch, error) {
	mem := defaultAllocator
	col := batch.Column(colIndex)
	n := int(batch.NumRows())

	builder := newBuilder(mem)
	defer builder.Release()
	builder.Reserve(n)

	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(transform(col, i))
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

// MapColumnCustomNulls maps a single input column to an output column
// without automatic null propagation. The transform is called for every row
// including nulls, giving the author full control over null handling.
func MapColumnCustomNulls[T any, B ArrayBuilder[T]](
	params *ProcessParams,
	batch arrow.RecordBatch,
	colIndex int,
	newBuilder func(memory.Allocator) B,
	transform func(col arrow.Array, i int) T,
) (arrow.RecordBatch, error) {
	mem := defaultAllocator
	col := batch.Column(colIndex)
	n := int(batch.NumRows())

	builder := newBuilder(mem)
	defer builder.Release()
	builder.Reserve(n)

	for i := 0; i < n; i++ {
		builder.Append(transform(col, i))
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

// MapColumns maps multiple input columns to a single output column.
// If any input column is null for a given row, the output is null.
func MapColumns[T any, B ArrayBuilder[T]](
	params *ProcessParams,
	batch arrow.RecordBatch,
	colIndices []int,
	newBuilder func(memory.Allocator) B,
	transform func(cols []arrow.Array, i int) T,
) (arrow.RecordBatch, error) {
	mem := defaultAllocator
	n := int(batch.NumRows())

	cols := make([]arrow.Array, len(colIndices))
	for ci, idx := range colIndices {
		cols[ci] = batch.Column(idx)
	}

	builder := newBuilder(mem)
	defer builder.Release()
	builder.Reserve(n)

	for i := 0; i < n; i++ {
		isNull := false
		for _, c := range cols {
			if c.IsNull(i) {
				isNull = true
				break
			}
		}
		if isNull {
			builder.AppendNull()
		} else {
			builder.Append(transform(cols, i))
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

// MapAllColumns maps all input columns to a single output column.
// If any column is null for a given row, the output is null.
func MapAllColumns[T any, B ArrayBuilder[T]](
	params *ProcessParams,
	batch arrow.RecordBatch,
	newBuilder func(memory.Allocator) B,
	transform func(cols []arrow.Array, i int) T,
) (arrow.RecordBatch, error) {
	indices := make([]int, int(batch.NumCols()))
	for i := range indices {
		indices[i] = i
	}
	return MapColumns(params, batch, indices, newBuilder, transform)
}

// GenerateColumn creates an output column by calling generateFn for each row.
// No input columns are read; batch is used only for its row count.
// This is for generators and constant-per-row functions.
func GenerateColumn[T any, B ArrayBuilder[T]](
	params *ProcessParams,
	batch arrow.RecordBatch,
	newBuilder func(memory.Allocator) B,
	generateFn func(i int) T,
) (arrow.RecordBatch, error) {
	mem := defaultAllocator
	n := int(batch.NumRows())

	builder := newBuilder(mem)
	defer builder.Release()
	builder.Reserve(n)

	for i := 0; i < n; i++ {
		builder.Append(generateFn(i))
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

// BuildResultBatch wraps a single result array into a RecordBatch using the
// output schema from ProcessParams. This is a low-level building block for
// Process implementations that need manual builder control (e.g., complex
// binary construction) but still want to avoid the batch-wrapping boilerplate.
func BuildResultBatch(params *ProcessParams, resultArr arrow.Array, numRows int64) arrow.RecordBatch {
	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, numRows)
}
