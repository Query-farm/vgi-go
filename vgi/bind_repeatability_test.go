// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestBindRepeatabilityDefaultAndCodec(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32}}, nil)
	for _, repeatability := range []BindRepeatability{"", BindRepeatabilityUnknown, BindRepeatabilityStableWithinQuery} {
		wire, err := wireProjectionBind(&BindResponse{OutputSchema: schema, Repeatability: repeatability})
		if err != nil {
			t.Fatal(err)
		}
		expected := string(repeatability)
		if expected == "" {
			expected = "UNKNOWN"
		}
		if wire.Repeatability != expected {
			t.Fatalf("provider %q encoded as %q", repeatability, wire.Repeatability)
		}
		raw, err := encodeProjectionBind(wire)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeProjectionBind(raw)
		if err != nil || decoded.Repeatability != expected {
			t.Fatalf("repeatability round trip: got %q, err=%v", decoded.Repeatability, err)
		}
	}
	for _, invalid := range []BindRepeatability{"stable_within_query", "FUTURE"} {
		if _, err := wireProjectionBind(&BindResponse{OutputSchema: schema, Repeatability: invalid}); err == nil {
			t.Fatalf("invalid provider repeatability %q accepted", invalid)
		}
	}
}

func TestBindRepeatabilityRejectsMalformedWire(t *testing.T) {
	output, err := SerializeSchema(arrow.NewSchema(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	pointer := func(s string) *string { return &s }
	for _, value := range []*string{nil, pointer(""), pointer("FUTURE"), pointer("stable_within_query")} {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, generated.BindResultSchema)
		builder.Field(0).(*array.BinaryBuilder).Append(output)
		builder.Field(1).AppendNull()
		for i := 2; i < 6; i++ {
			builder.Field(i).(*array.ListBuilder).Append(true)
		}
		if value == nil {
			builder.Field(6).AppendNull()
		} else if err := builder.Field(6).(*array.BinaryDictionaryBuilder).AppendString(*value); err != nil {
			t.Fatal(err)
		}
		row := builder.NewRecordBatch()
		builder.Release()
		raw, err := SerializeRecordBatch(row)
		row.Release()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeProjectionBind(raw); err == nil {
			t.Fatal("malformed repeatability accepted")
		}
	}
}

func TestBindRepeatabilityRejectsHiddenDictionaryValueNull(t *testing.T) {
	bound, err := wireProjectionBind(&BindResponse{OutputSchema: arrow.NewSchema(nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeProjectionBind(bound)
	if err != nil {
		t.Fatal(err)
	}
	row, err := DeserializeRecordBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer row.Release()
	original := row.Column(6).(*array.Dictionary)
	values := original.Dictionary().(*array.String)
	bitmap := memory.NewBufferBytes([]byte{0})
	defer bitmap.Release()
	data := array.NewData(arrow.BinaryTypes.String, 1, []*memory.Buffer{bitmap, values.Data().Buffers()[1], values.Data().Buffers()[2]}, nil, 1, 0)
	defer data.Release()
	hidden := array.NewStringData(data)
	defer hidden.Release()
	if !hidden.IsNull(0) || hidden.Value(0) != "UNKNOWN" {
		t.Fatal("test must retain valid enum bytes beneath the NULL bitmap")
	}
	dictionary := array.NewDictionaryArray(original.DataType(), original.Indices(), hidden)
	defer dictionary.Release()
	columns := append([]arrow.Array(nil), row.Columns()...)
	columns[6] = dictionary
	invalid := array.NewRecordBatch(row.Schema(), columns, 1)
	defer invalid.Release()
	encoded, err := SerializeRecordBatch(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeProjectionBind(encoded); err == nil {
		t.Fatal("hidden NULL dictionary value was accepted as UNKNOWN")
	}
}
