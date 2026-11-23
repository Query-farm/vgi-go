// Copyright 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// testNested wraps a bind call the way it arrives inside an embedding request:
// serialized once, held as bytes.
func testNested(t testing.TB, b BindRequestWire) generated.Nested[generated.BindRequest] {
	t.Helper()
	n, err := generated.NestedOf(generated.BindRequest(b))
	if err != nil {
		t.Fatalf("NestedOf: %v", err)
	}
	return n
}

// testNoArgs is an inline, empty arguments stream: a bind call must set
// exactly one of arguments and arguments_location, so "no arguments" is an
// empty stream, never a null.
func testNoArgs(t testing.TB) *[]byte {
	t.Helper()
	data, err := SerializeRecordBatch(emptyArgumentsBatch())
	if err != nil {
		t.Fatalf("serializing empty arguments: %v", err)
	}
	return &data
}
