// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestBatchBuilder_AllPrimitives(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "b", Type: &arrow.BooleanType{}, Nullable: false},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	b := NewBatchBuilder(schema)

	if err := b.AppendRow(map[string]any{
		"s":   "hello",
		"i64": int64(42),
		"i32": int32(7),
		"f64": 3.14,
		"b":   true,
		"bin": []byte("blob"),
	}); err != nil {
		t.Fatal(err)
	}
	// Row with nulls for nullable columns.
	if err := b.AppendRow(map[string]any{
		"s":   "world",
		"i64": int64(0),
		"i32": nil,
		"f64": 0.0,
		"b":   false,
		"bin": nil,
	}); err != nil {
		t.Fatal(err)
	}

	batch, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	if batch.NumRows() != 2 {
		t.Fatalf("rows: got %d", batch.NumRows())
	}
	if batch.Column(0).(*array.String).Value(0) != "hello" {
		t.Errorf("s[0]: %v", batch.Column(0))
	}
	if batch.Column(1).(*array.Int64).Value(0) != 42 {
		t.Errorf("i64[0]: %v", batch.Column(1))
	}
	if !batch.Column(2).IsNull(1) {
		t.Errorf("i32[1] should be null")
	}
	if !batch.Column(5).IsNull(1) {
		t.Errorf("bin[1] should be null")
	}
}

func TestBatchBuilder_MissingKeyIsNull(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	b := NewBatchBuilder(schema)
	// Row entirely missing "b" — should produce null.
	if err := b.AppendRow(map[string]any{"a": int64(1)}); err != nil {
		t.Fatal(err)
	}
	batch, _ := b.Build()
	defer batch.Release()

	if !batch.Column(1).IsNull(0) {
		t.Errorf("missing key should produce null")
	}
}

func TestBatchBuilder_PointerDeref(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	s := "hi"
	var nilStr *string
	n := int32(5)
	var nilInt *int32

	b := NewBatchBuilder(schema)
	if err := b.AppendRow(map[string]any{"s": &s, "n": &n}); err != nil {
		t.Fatal(err)
	}
	if err := b.AppendRow(map[string]any{"s": nilStr, "n": nilInt}); err != nil {
		t.Fatal(err)
	}
	batch, _ := b.Build()
	defer batch.Release()

	if batch.Column(0).(*array.String).Value(0) != "hi" {
		t.Errorf("ptr deref: got %v", batch.Column(0))
	}
	if !batch.Column(0).IsNull(1) {
		t.Errorf("nil pointer should be null")
	}
	if batch.Column(1).(*array.Int32).Value(0) != 5 {
		t.Errorf("int32 ptr: got %v", batch.Column(1))
	}
	if !batch.Column(1).IsNull(1) {
		t.Errorf("nil int32 pointer should be null")
	}
}

func TestBatchBuilder_Timestamp(t *testing.T) {
	ts := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "t", Type: ts, Nullable: true},
	}, nil)

	now := time.Date(2024, 1, 15, 12, 34, 56, 0, time.UTC)
	b := NewBatchBuilder(schema)
	_ = b.AppendRow(map[string]any{"t": now})
	_ = b.AppendRow(map[string]any{"t": nil})
	batch, _ := b.Build()
	defer batch.Release()

	col := batch.Column(0).(*array.Timestamp)
	if col.IsNull(0) {
		t.Errorf("row 0 should have a timestamp")
	}
	if !col.IsNull(1) {
		t.Errorf("row 1 should be null")
	}
}

func TestBatchBuilder_ListOfString(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	}, nil)

	b := NewBatchBuilder(schema)
	_ = b.AppendRow(map[string]any{"tags": []string{"a", "b", "c"}})
	_ = b.AppendRow(map[string]any{"tags": nil}) // null list
	_ = b.AppendRow(map[string]any{"tags": []string{}})

	batch, _ := b.Build()
	defer batch.Release()

	list := batch.Column(0).(*array.List)
	if list.IsNull(1) != true {
		t.Errorf("list[1] should be null")
	}
	if list.IsNull(0) {
		t.Errorf("list[0] should be valid")
	}
	if list.IsNull(2) {
		t.Errorf("list[2] should be valid (empty list)")
	}
	values := list.ListValues().(*array.String)
	if values.Len() != 3 {
		t.Errorf("expected 3 total string values, got %d", values.Len())
	}
	if values.Value(0) != "a" || values.Value(2) != "c" {
		t.Errorf("values: %v / %v / %v", values.Value(0), values.Value(1), values.Value(2))
	}
}

func TestBatchBuilder_ListNullElements(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "xs", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)

	b := NewBatchBuilder(schema)
	// Use []any so we can mix nil elements.
	_ = b.AppendRow(map[string]any{"xs": []any{int64(1), nil, int64(3)}})

	batch, _ := b.Build()
	defer batch.Release()

	list := batch.Column(0).(*array.List)
	values := list.ListValues().(*array.Int64)
	if values.Len() != 3 {
		t.Errorf("expected 3 elements, got %d", values.Len())
	}
	if values.IsNull(0) || !values.IsNull(1) || values.IsNull(2) {
		t.Errorf("null mask wrong: %v %v %v", values.IsNull(0), values.IsNull(1), values.IsNull(2))
	}
	if values.Value(0) != 1 || values.Value(2) != 3 {
		t.Errorf("values: %v %v", values.Value(0), values.Value(2))
	}
}

func TestBatchBuilder_NumericCoercion(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := NewBatchBuilder(schema)
	// Accept int, int32, int64, float, etc.
	for _, v := range []any{int(1), int32(2), int64(3), uint(4), float64(5)} {
		if err := b.AppendRow(map[string]any{"n": v}); err != nil {
			t.Fatal(err)
		}
	}
	batch, _ := b.Build()
	defer batch.Release()

	col := batch.Column(0).(*array.Int64)
	if col.Len() != 5 {
		t.Fatalf("rows: %d", col.Len())
	}
	for i, want := range []int64{1, 2, 3, 4, 5} {
		if col.Value(i) != want {
			t.Errorf("row %d: got %d", i, col.Value(i))
		}
	}
}

func TestBatchBuilder_TypeErrorSurface(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := NewBatchBuilder(schema)
	defer b.Release()
	if err := b.AppendRow(map[string]any{"n": "not a number"}); err == nil {
		t.Error("expected type error")
	}
}

func TestBatchBuilder_AppendNullRow(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := NewBatchBuilder(schema)
	b.AppendNullRow()
	batch, _ := b.Build()
	defer batch.Release()
	if !batch.Column(0).IsNull(0) || !batch.Column(1).IsNull(0) {
		t.Errorf("AppendNullRow should null all columns")
	}
}
