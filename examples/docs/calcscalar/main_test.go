// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Tests for the documentation example worker.
//
// The vgi-go docs on query.farm embed these files verbatim, so a bug here ships
// as a copy-pasteable bug. These tests are what stop that: they assert the
// registered SQL signature (the thing a reader copies first) and the per-batch
// behaviour, without needing the C++ extension or a DuckDB build.
//
// The signature assertion is not incidental. `type=bigint` in a vgi tag looks
// right, is not a recognised Arrow type name, and silently degrades the argument
// to VARCHAR — the function then fails to bind against an integer literal with a
// "No function matches" error that points at the call site rather than the tag.
// Asserting the derived spec catches that at `go test` time.
package main

import (
	"context"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDoubleRegistersABigintArgument(t *testing.T) {
	fn := NewDouble()
	if got := fn.Name(); got != "double" {
		t.Fatalf("name = %q, want %q", got, "double")
	}
	specs := fn.ArgumentSpecs()
	if len(specs) != 1 {
		t.Fatalf("want 1 argument spec, got %d", len(specs))
	}
	if specs[0].ArrowType != "int64" {
		t.Fatalf("argument type = %q, want %q — the function will not bind against "+
			"an integer literal otherwise", specs[0].ArrowType, "int64")
	}
	if specs[0].IsConst {
		t.Fatal("argument should be columnar, not const")
	}
}

func TestDoubleDoublesEveryRowAndPreservesNulls(t *testing.T) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	b.AppendValues([]int64{1, 2, 3, 0}, []bool{true, true, true, false})
	col := b.NewArray()
	defer col.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{col}, 4)
	defer batch.Release()

	// MapColumn builds its result against params.OutputSchema, which is what
	// bind resolved — here a single int64 column, the same thing OnBindTyped
	// returns.
	params := &vgi.ProcessParams{
		OutputSchema: arrow.NewSchema(
			[]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil),
	}
	out, err := (&DoubleFn{}).ProcessTyped(context.Background(), &doubleArgs{}, params, batch)
	if err != nil {
		t.Fatalf("ProcessTyped: %v", err)
	}
	defer out.Release()

	// A scalar function must return exactly as many rows as it received.
	if out.NumRows() != batch.NumRows() {
		t.Fatalf("row count = %d, want %d", out.NumRows(), batch.NumRows())
	}
	got := out.Column(0).(*array.Int64)
	for i, want := range []int64{2, 4, 6} {
		if got.Value(i) != want {
			t.Errorf("row %d = %d, want %d", i, got.Value(i), want)
		}
	}
	if !got.IsNull(3) {
		t.Error("a NULL input must stay NULL on output")
	}
}
