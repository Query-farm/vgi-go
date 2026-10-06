// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi/generated"
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

// countingTable is a catalog table whose columns come from OnBind, counting
// how often the catalog listing resolves them.
type countingTable struct {
	TableFunction // only the methods below are called
	binds         *int
}

func (c *countingTable) Name() string               { return "counting" }
func (c *countingTable) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (c *countingTable) ArgumentSpecs() []ArgSpec   { return nil }

func (c *countingTable) OnBind(*BindParams) (*BindResponse, error) {
	*c.binds++
	return &BindResponse{OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)}, nil
}

// The default catalog is static and version-frozen, so catalog_contents is
// built once per (catalog, name, version) and every later call reuses it —
// revalidation included. A schema contents handler (which sees the whole
// attach) or a catalog_contents handler turns the cache off.
func TestCatalogContentsCache(t *testing.T) {
	build := func(opts ...WorkerOption) (*Worker, *int) {
		binds := 0
		w := NewWorker(append([]WorkerOption{WithCatalogName("example"), WithCatalogAliases("other"),
			WithCatalogContentsEtag(CatalogContentsEtagContentHash)}, opts...)...)
		w.RegisterCatalogTable("data", CatalogTable{Name: "t", Function: &countingTable{binds: &binds}})
		w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
		return w, &binds
	}
	call := func(w *Worker, attach string, inm *string) generated.CatalogContentsResponse {
		t.Helper()
		resp, err := w.catalogContents(CatalogContentsRequestWire{AttachOpaqueData: []byte(attach), IfNoneMatch: inm}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	w, binds := build()
	first := call(w, "example", nil)
	if *binds != 1 || first.Etag == nil {
		t.Fatalf("first call: %d binds, etag %v", *binds, first.Etag)
	}
	again := call(w, "example", nil)
	nm := call(w, "example", first.Etag)
	if *binds != 1 {
		t.Fatalf("cached catalog rebuilt: %d binds after three calls", *binds)
	}
	if *again.Etag != *first.Etag || len(again.Schemas) != len(first.Schemas) {
		t.Fatal("a cache hit answered differently")
	}
	if !nm.NotModified || len(nm.Schemas) != 0 || *nm.Etag != *first.Etag {
		t.Fatalf("a cache hit with a matching if_none_match: not_modified=%v schemas=%d", nm.NotModified, len(nm.Schemas))
	}
	// Another catalog name served by the same worker is its own entry.
	call(w, "other", nil)
	if *binds != 2 {
		t.Fatalf("alias catalog shared the primary's cache entry (%d binds)", *binds)
	}
	// A rebuilt catalog (a new server) is a new entry.
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	call(w, "example", nil)
	if *binds != 3 {
		t.Fatalf("a rebuilt catalog served the old instance's snapshot (%d binds)", *binds)
	}

	for name, opt := range map[string]WorkerOption{
		"schema contents handler": WithSchemaContentsHandler(func([]byte, SchemaPath) ([]SerializedSchemaItem, bool) { return nil, false }),
		"catalog contents handler": WithCatalogContentsHandler(func(c *CatalogContentsCall) (CatalogContentsResult, error) {
			s, err := c.Contents()
			return CatalogContentsResult{Schemas: s}, err
		}),
	} {
		w, binds := build(opt)
		call(w, "example", nil)
		call(w, "example", nil)
		if *binds != 2 {
			t.Errorf("with a %s: %d binds over two calls, want 2 (no cache)", name, *binds)
		}
	}
}
