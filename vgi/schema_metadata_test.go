// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/endian"
)

func TestRestoreSchemaMetadataPreservesReaderEndianness(t *testing.T) {
	schema := arrow.NewSchemaWithEndian([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32}}, nil, endian.BigEndian)
	raw, err := SerializeSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := restoreSchemaFieldMetadata(raw, schema)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Endianness() != schema.Endianness() {
		t.Fatal("metadata restoration changed reader endianness")
	}
}
