// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// A kind whose estimated_object_count is exactly 0 is a hard guarantee, so
// catalog_contents does not compute it — but still sends it, as an empty list,
// because every kind in a SchemaContents is complete.
func TestSchemaContentsSkipsKindsCountedZero(t *testing.T) {
	w := NewWorker(WithCatalogName("example"))
	cols := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	w.RegisterCatalogTable("data", CatalogTable{Name: "t", Columns: cols})
	w.RegisterCatalogView("data", CatalogView{Name: "v", Definition: "SELECT 1"})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	info := *w.catalog.schemas["data"].info

	full, err := w.schemaContentsOf([]byte("example"), &info)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Tables) != 1 || len(full.Views) != 1 {
		t.Fatalf("counted schema: %d tables, %d views; want 1 and 1", len(full.Tables), len(full.Views))
	}

	// The same schema, claiming it has no tables: the worker trusts the claim.
	info.EstimatedObjectCount = map[string]int64{"table": 0}
	skipped, err := w.schemaContentsOf([]byte("example"), &info)
	if err != nil {
		t.Fatal(err)
	}
	if skipped.Tables == nil || len(skipped.Tables) != 0 {
		t.Fatalf("a kind counted 0 must be an empty (not nil, not computed) list, got %v", skipped.Tables)
	}
	if len(skipped.Views) != 1 {
		t.Fatalf("a kind with no count must still be computed, got %d views", len(skipped.Views))
	}
	for name, items := range map[string][][]byte{
		"scalar_functions": skipped.ScalarFunctions, "aggregate_functions": skipped.AggregateFunctions,
		"table_functions": skipped.TableFunctions, "scalar_macros": skipped.ScalarMacros,
		"table_macros": skipped.TableMacros, "indexes": skipped.Indexes,
	} {
		if items == nil {
			t.Errorf("%s is nil; every kind must be present", name)
		}
	}
}
