// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Tests for the table-in-out documentation example.
package main

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A TABLE argument cannot be declared with a struct tag — the struct-tag path
// binds scalar values, and a relation is not a value. Assert the explicit spec,
// since getting this wrong makes the function un-callable rather than wrong.
func TestFilterDeclaresATableArgument(t *testing.T) {
	specs := (&FilterPositiveFn{}).ArgumentSpecs()
	if len(specs) != 1 {
		t.Fatalf("want 1 spec, got %d", len(specs))
	}
	if specs[0].ArrowType != "table" {
		t.Errorf("argument type = %q, want %q", specs[0].ArrowType, "table")
	}
}

// collect runs the predicate over one batch of values.
func collect(t *testing.T, values []int64, valid []bool) []int64 {
	t.Helper()
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	b.AppendValues(values, valid)
	col := b.NewArray()
	defer col.Release()
	return keepPositive(col, len(values))
}

func TestFilterKeepsOnlyPositiveValues(t *testing.T) {
	got := collect(t, []int64{-2, 5, 0, 9, -1}, nil)
	want := []int64{5, 9}
	if len(got) != len(want) {
		t.Fatalf("kept %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("kept %v, want %v", got, want)
		}
	}
}

// A NULL is not positive. Treating it as one is the classic off-by-null here,
// and it only shows up on data that has NULLs.
func TestFilterDropsNulls(t *testing.T) {
	got := collect(t, []int64{3, 0, -4}, []bool{true, false, true})
	if len(got) != 1 || got[0] != 3 {
		t.Fatalf("kept %v, want [3]", got)
	}
}

// Emitting a zero-row batch is legitimate: this batch simply had nothing to
// keep. It must not be an error, or a filter that rejects everything breaks.
func TestFilterEmitsAnEmptyBatchWhenNothingMatches(t *testing.T) {
	if got := collect(t, []int64{-1, -2}, nil); len(got) != 0 {
		t.Fatalf("kept %v, want nothing", got)
	}
}
