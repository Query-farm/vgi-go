// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BatchState tracks the remaining/batchSize/index bookkeeping for table functions
// that generate a fixed number of rows in batches. Embed this in your state struct
// to use with GenerateBatch.
//
// Index is a public field so callbacks can read the current row offset.
// All fields are managed by GenerateBatch — do not modify them directly.
type BatchState struct {
	Remaining int64
	BatchSize int64
	Index     int64
}

// NewBatchState creates a BatchState with the given total count and batch size.
func NewBatchState(count, batchSize int64) BatchState {
	return BatchState{
		Remaining: count,
		BatchSize: batchSize,
	}
}

// Three paths for batch construction:
//
//   - GenerateBatch: Fixed schema, known column order, no projection pushdown.
//     Callback returns []arrow.Array in schema order.
//
//   - GenerateBatchMap: Dynamic or projected schema, name-keyed columns.
//     Callback returns map[string]arrow.Array; columns are reordered to match
//     the schema automatically.
//
//   - BatchFromMap: Standalone batch construction from name-keyed columns,
//     for non-BatchState use cases like Finalize or one-off batch creation.

// GenerateBatch handles one batch of a batch-splitting table function.
// Call this from Process — it handles the complete remaining-work pattern:
//
//  1. If Remaining <= 0, calls out.Finish() and returns
//  2. Computes size = min(Remaining, BatchSize)
//  3. Calls generateFn(size) to produce arrays
//  4. Emits the arrays via out.EmitArrays
//  5. Releases all returned arrays
//  6. Updates Remaining and Index
//
// VGI's Process is called once per batch by the framework. This function
// processes exactly one batch per call — it does NOT loop.
//
// The generateFn closure should capture bs.Index at the start to know the
// current row offset before GenerateBatch advances it.
func GenerateBatch(bs *BatchState, out *vgirpc.OutputCollector, generateFn func(size int64) ([]arrow.Array, error)) error {
	if bs.Remaining <= 0 {
		return out.Finish()
	}

	size := bs.BatchSize
	if bs.Remaining < size {
		size = bs.Remaining
	}

	arrays, err := generateFn(size)
	if err != nil {
		return err
	}

	bs.Remaining -= size
	bs.Index += size

	err = out.EmitArrays(arrays, size)

	for _, a := range arrays {
		a.Release()
	}

	return err
}

// BatchFromMap reorders columns from a name-keyed map to match schema field order
// and creates a RecordBatch. It consumes all arrays in the map — every array
// (both schema-matched and extra) is released on success and on error, matching
// GenerateBatch's ownership model. Returns an error if a schema field is missing
// from the map. Extra map keys are silently ignored but their arrays are still released.
func BatchFromMap(schema *arrow.Schema, columns map[string]arrow.Array, numRows int64) (arrow.RecordBatch, error) {
	cols := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		a, ok := columns[field.Name]
		if !ok {
			for _, arr := range columns {
				arr.Release()
			}
			return nil, fmt.Errorf("BatchFromMap: schema field %q not found in column map", field.Name)
		}
		cols[i] = a
	}

	batch := array.NewRecordBatch(schema, cols, numRows)
	for _, arr := range columns {
		arr.Release()
	}
	return batch, nil
}

// GenerateBatchMap handles one batch of a batch-splitting table function using
// name-keyed columns. Same lifecycle as GenerateBatch (check remaining, compute
// size, call callback, update state) but the callback returns map[string]arrow.Array.
// Uses BatchFromMap internally and emits via out.Emit.
func GenerateBatchMap(bs *BatchState, out *vgirpc.OutputCollector, schema *arrow.Schema, generateFn func(size int64) (map[string]arrow.Array, error)) error {
	if bs.Remaining <= 0 {
		return out.Finish()
	}

	size := bs.BatchSize
	if bs.Remaining < size {
		size = bs.Remaining
	}

	colMap, err := generateFn(size)
	if err != nil {
		return err
	}

	batch, err := BatchFromMap(schema, colMap, size)
	if err != nil {
		return err
	}

	bs.Remaining -= size
	bs.Index += size

	return out.Emit(batch)
}

// BuildInt64Array creates an int64 array by calling fn for each row index.
// The caller is responsible for releasing the returned array, unless using
// GenerateBatch which handles release automatically.
func BuildInt64Array(n int64, fn func(i int64) int64) arrow.Array {
	builder := array.NewInt64Builder(defaultAllocator)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildFloat64Array creates a float64 array by calling fn for each row index.
func BuildFloat64Array(n int64, fn func(i int64) float64) arrow.Array {
	builder := array.NewFloat64Builder(defaultAllocator)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildUint64Array creates a uint64 array by calling fn for each row index.
func BuildUint64Array(n int64, fn func(i int64) uint64) arrow.Array {
	builder := array.NewUint64Builder(defaultAllocator)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildStringArray creates a string array by calling fn for each row index.
func BuildStringArray(n int64, fn func(i int64) string) arrow.Array {
	builder := array.NewStringBuilder(defaultAllocator)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildBooleanArray creates a boolean array by calling fn for each row index.
func BuildBooleanArray(n int64, fn func(i int64) bool) arrow.Array {
	builder := array.NewBooleanBuilder(defaultAllocator)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildBinaryArray creates a binary array by calling fn for each row index.
func BuildBinaryArray(n int64, fn func(i int64) []byte) arrow.Array {
	builder := array.NewBinaryBuilder(defaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildInt32Array creates an int32 array by calling fn for each row index.
func BuildInt32Array(n int64, fn func(i int64) int32) arrow.Array {
	builder := array.NewInt32Builder(defaultAllocator)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildAllNullArray creates an n-row all-NULL array of the given type. The
// type is created via NewBuilder so any Arrow primitive / nested type works.
func BuildAllNullArray(dt arrow.DataType, n int64) arrow.Array {
	builder := array.NewBuilder(defaultAllocator, dt)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.AppendNull()
	}
	return builder.NewArray()
}

// BuildArray creates an array of any type using the ArrayBuilder generic constraint.
// Use this for types not covered by BuildInt64Array, BuildFloat64Array, etc.
func BuildArray[T any, B ArrayBuilder[T]](n int64, newBuilder func(memory.Allocator) B, fn func(i int64) T) arrow.Array {
	builder := newBuilder(defaultAllocator)
	defer builder.Release()
	builder.Reserve(int(n))
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}
