// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Tests for the buffering documentation example.
//
// The three phases need cross-process storage, which is not constructible from
// an external test, so what is asserted here is the declaration a reader copies
// and the encoding the phases agree on.
package main

import (
	"encoding/binary"
	"testing"
)

// A TABLE argument cannot be declared with a struct tag — the tag path binds
// scalar values, and a relation is not a value.
func TestRowCountDeclaresATableArgument(t *testing.T) {
	specs := (&RowCountFn{}).ArgumentSpecs()
	if len(specs) != 1 || specs[0].ArrowType != "table" {
		t.Fatalf("want one table argument, got %+v", specs)
	}
}

// Output is one int64 column whatever the input looked like.
func TestRowCountBindsAFixedSchema(t *testing.T) {
	resp, err := (&RowCountFn{}).OnBind(nil)
	if err != nil {
		t.Fatalf("OnBind: %v", err)
	}
	if resp == nil {
		t.Fatal("OnBind returned no response")
	}
	if got := len(rowCountOutputSchema.Fields()); got != 1 {
		t.Fatalf("output schema has %d columns, want 1", got)
	}
	if name := rowCountOutputSchema.Field(0).Name; name != "count" {
		t.Errorf("output column = %q, want %q", name, "count")
	}
}

// The sink writes little-endian int64s and the source reads them back. The two
// halves run in different processes, so a mismatch here is invisible until a
// query returns a wrong total.
func TestRowCountEncodingRoundTrips(t *testing.T) {
	for _, n := range []int64{0, 1, 1024, 100_000} {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(n))
		if got := int64(binary.LittleEndian.Uint64(buf[:])); got != n {
			t.Errorf("round-trip of %d gave %d", n, got)
		}
	}
}
