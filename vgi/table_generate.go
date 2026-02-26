// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"github.com/Query-farm/vgi-rpc/vgirpc"
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

// BuildInt64Array creates an int64 array by calling fn for each row index.
// The caller is responsible for releasing the returned array, unless using
// GenerateBatch which handles release automatically.
func BuildInt64Array(n int64, fn func(i int64) int64) arrow.Array {
	builder := array.NewInt64Builder(defaultAllocator)
	defer builder.Release()
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildFloat64Array creates a float64 array by calling fn for each row index.
func BuildFloat64Array(n int64, fn func(i int64) float64) arrow.Array {
	builder := array.NewFloat64Builder(defaultAllocator)
	defer builder.Release()
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildStringArray creates a string array by calling fn for each row index.
func BuildStringArray(n int64, fn func(i int64) string) arrow.Array {
	builder := array.NewStringBuilder(defaultAllocator)
	defer builder.Release()
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildBooleanArray creates a boolean array by calling fn for each row index.
func BuildBooleanArray(n int64, fn func(i int64) bool) arrow.Array {
	builder := array.NewBooleanBuilder(defaultAllocator)
	defer builder.Release()
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}

// BuildArray creates an array of any type using the ArrayBuilder generic constraint.
// Use this for types not covered by BuildInt64Array, BuildFloat64Array, etc.
func BuildArray[T any, B ArrayBuilder[T]](n int64, newBuilder func(memory.Allocator) B, fn func(i int64) T) arrow.Array {
	builder := newBuilder(defaultAllocator)
	defer builder.Release()
	for i := int64(0); i < n; i++ {
		builder.Append(fn(i))
	}
	return builder.NewArray()
}
