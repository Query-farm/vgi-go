// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Tests for the combined tutorial worker (a scalar and a table function).
package main

import (
	"context"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The argument type is the thing a reader copies first, and getting it wrong
// fails far from the tag — see the type= note in the SDK. Assert it directly.
func TestRegisteredSignatures(t *testing.T) {
	scalar := NewDouble()
	if scalar.Name() != "double" {
		t.Errorf("scalar name = %q", scalar.Name())
	}
	if specs := scalar.ArgumentSpecs(); len(specs) != 1 || specs[0].ArrowType != "int64" {
		t.Errorf("double should take one int64 argument, got %+v", specs)
	}

	table := NewSeries()
	if table.Name() != "series" {
		t.Errorf("table name = %q", table.Name())
	}
	specs := table.ArgumentSpecs()
	if len(specs) != 1 || specs[0].ArrowType != "int64" {
		t.Fatalf("series should take one int64 argument, got %+v", specs)
	}
	// ge=0 is what turns series(-1) into a bind error instead of a hang or an
	// empty result.
	if specs[0].Ge == nil {
		t.Error("series(count) should carry its ge=0 constraint into the spec")
	}
}

func TestDoubleDoublesAndPreservesNulls(t *testing.T) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	b.AppendValues([]int64{1, 2, 0}, []bool{true, true, false})
	col := b.NewArray()
	defer col.Release()

	in := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil),
		[]arrow.Array{col}, 3)
	defer in.Release()

	params := &vgi.ProcessParams{OutputSchema: arrow.NewSchema(
		[]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)}
	out, err := (&DoubleFn{}).ProcessTyped(context.Background(), &doubleArgs{}, params, in)
	if err != nil {
		t.Fatalf("ProcessTyped: %v", err)
	}
	defer out.Release()

	if out.NumRows() != in.NumRows() {
		t.Fatalf("a scalar must not change the row count: %d != %d", out.NumRows(), in.NumRows())
	}
	got := out.Column(0).(*array.Int64)
	if got.Value(0) != 2 || got.Value(1) != 4 {
		t.Errorf("values = %d, %d; want 2, 4", got.Value(0), got.Value(1))
	}
	if !got.IsNull(2) {
		t.Error("NULL must stay NULL")
	}
}

// series(count) must chunk: BatchState is created with a 1024 batch size, so a
// larger count has to come back over several Process calls. Getting this wrong
// is how a generator silently truncates.
func TestSeriesChunksAcrossBatches(t *testing.T) {
	const count = 2500
	state := &seriesState{BatchState: vgi.NewBatchState(count, 1024)}

	var produced int64
	var last int64 = -1
	for state.Remaining > 0 {
		size := state.BatchSize
		if state.Remaining < size {
			size = state.Remaining
		}
		start := state.Index
		arr := vgi.BuildInt64Array(size, func(i int64) int64 { return start + i }).(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			v := arr.Value(i)
			if v != last+1 {
				t.Fatalf("sequence broke at %d: got %d after %d", produced, v, last)
			}
			last = v
		}
		arr.Release()
		produced += size
		state.Index += size
		state.Remaining -= size
	}
	if produced != count {
		t.Fatalf("produced %d rows, want %d", produced, count)
	}
	if last != count-1 {
		t.Fatalf("last value = %d, want %d", last, count-1)
	}
}
