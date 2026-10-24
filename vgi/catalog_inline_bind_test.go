// Copyright 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestCatalogInlineBindUsesCompleteCurrentRecord(t *testing.T) {
	metadata := arrow.NewMetadata([]string{"source"}, []string{"static"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: metadata}}, &metadata)
	worker := NewWorker()
	info, err := worker.catalogTableInfo([]string{"main"}, &CatalogTable{Name: "cached", Columns: schema, InlineBind: true})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := decodeProjectionBind(info.BindResult)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DeserializeSchema(bound.OutputSchema)
	if err != nil || !got.Equal(schema) || !got.Metadata().Equal(metadata) {
		t.Fatalf("inline bind changed schema: %v", err)
	}
	if bound.Repeatability != string(BindRepeatabilityUnknown) || len(bound.ProjectionExpressionProfiles) != 0 || bound.OpaqueData != nil {
		t.Fatal("static inline bind invented repeatability, projection support or opaque state")
	}
}
