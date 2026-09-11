// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package simple_writable

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestChangeImagesPreserveOldNewAndNullSides(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	rowType := arrow.StructOf(schema.Fields()...)
	oldBuilder := array.NewStructBuilder(memory.DefaultAllocator, rowType)
	defer oldBuilder.Release()
	newBuilder := array.NewStructBuilder(memory.DefaultAllocator, rowType)
	defer newBuilder.Release()

	if err := appendStructRow(oldBuilder, schema, rowMap{"id": int64(1), "name": "before"}); err != nil {
		t.Fatal(err)
	}
	if err := appendStructRow(newBuilder, schema, rowMap{"id": int64(1), "name": "after"}); err != nil {
		t.Fatal(err)
	}
	if err := appendStructRow(oldBuilder, schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := appendStructRow(newBuilder, schema, rowMap{"id": int64(2), "name": "inserted"}); err != nil {
		t.Fatal(err)
	}

	oldRows := oldBuilder.NewStructArray()
	defer oldRows.Release()
	newRows := newBuilder.NewStructArray()
	defer newRows.Release()
	if oldRows.IsNull(0) || newRows.IsNull(0) {
		t.Fatal("update must carry both OLD and NEW")
	}
	if !oldRows.IsNull(1) || newRows.IsNull(1) {
		t.Fatal("insert must null OLD and populate NEW")
	}
	if got := oldRows.Field(1).(*array.String).Value(0); got != "before" {
		t.Fatalf("OLD name = %q, want before", got)
	}
	if got := newRows.Field(1).(*array.String).Value(0); got != "after" {
		t.Fatalf("NEW name = %q, want after", got)
	}
}
