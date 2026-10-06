// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/examples/aggregate"
	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/examples/table"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Routed catalogs (RegisterSubCatalog / RegisterMemoryCatalog): one worker
// serving several independent catalogs, each answering its own catalog_* RPCs.

// exec runs a catalog RPC whose result is not inspected (a void DDL method)
// and returns its error.
func (c *contentsClient) exec(method string, params *arrow.Schema, row map[string]any) error {
	c.t.Helper()
	batch := jsonRow(c.t, params, row)
	defer batch.Release()
	result, err := c.client.CallUnary(context.Background(), method, batch, nil)
	if err == nil {
		result.Release()
	}
	return err
}

func (c *contentsClient) version(attach []byte) int64 {
	c.t.Helper()
	rec := c.call("catalog_version", generated.CatalogVersionParamsSchema, map[string]any{"attach_opaque_data": attach})
	defer rec.Release()
	return column[*array.Int64](c.t, rec, "version").Value(0)
}

func (c *contentsClient) schemaNames(attach []byte) []string {
	c.t.Helper()
	var names []string
	for _, item := range c.items("catalog_schemas", generated.CatalogSchemasParamsSchema, map[string]any{"attach_opaque_data": attach}) {
		names = append(names, strings.Join(schemaPathOf(c.t, item), "."))
	}
	return names
}

func (c *contentsClient) createView(attach []byte, name string) error {
	c.t.Helper()
	return c.exec("catalog_view_create", generated.CatalogViewCreateParamsSchema, map[string]any{
		"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": name,
		"definition": "SELECT 1 AS x", "on_conflict": "ERROR",
	})
}

func (c *contentsClient) views(attach []byte) [][]byte {
	c.t.Helper()
	return c.items("catalog_schema_contents_views", generated.CatalogSchemaContentsViewsParamsSchema,
		map[string]any{"attach_opaque_data": attach, "path": []string{"main"}})
}

func (c *contentsClient) functions(attach []byte, functionType string) [][]byte {
	c.t.Helper()
	return c.items("catalog_schema_contents_functions", generated.CatalogSchemaContentsFunctionsParamsSchema,
		map[string]any{"attach_opaque_data": attach, "path": []string{"main"}, "type": functionType})
}

// staticChild is a small static catalog: two schemas, a function-backed table
// in each, a view, a macro, and three functions.
func staticChild(name string, opts ...vgi.WorkerOption) *vgi.Worker {
	c := vgi.NewWorker(append([]vgi.WorkerOption{vgi.WithCatalogName(name)}, opts...)...)
	seq := table.NewSequenceFunction()
	c.RegisterScalar(scalar.NewDouble())
	c.RegisterAggregate(&aggregate.SumFunction{})
	c.RegisterTable(seq)
	c.RegisterCatalogTable("main", vgi.CatalogTable{Name: "ten", Function: seq,
		FuncArgs: []vgi.CatalogTableArg{{Position: 0, Value: int64(10), Type: arrow.PrimitiveTypes.Int64}}})
	c.RegisterCatalogTable("extra", vgi.CatalogTable{Name: "five", Function: seq,
		FuncArgs: []vgi.CatalogTableArg{{Position: 0, Value: int64(5), Type: arrow.PrimitiveTypes.Int64}}})
	c.RegisterCatalogView("main", vgi.CatalogView{Name: "answer", Definition: "SELECT 42 AS answer"})
	c.RegisterCatalogMacro("main", vgi.CatalogMacro{Name: "triple", MacroType: vgi.MacroTypeScalar,
		Parameters: []string{"x"}, Definition: "x * 3"})
	return c
}

// A sub-catalog answers as the worker it is: its own schemas and objects,
// its own catalog_contents, listed by catalog_catalogs — and the parent's own
// catalog does not see its functions.
func TestSubCatalogServesItsOwnCatalog(t *testing.T) {
	parent := vgi.NewWorker(vgi.WithCatalogName("parent"))
	parent.RegisterSubCatalog(staticChild("kid"))
	c := newContentsClient(t, parent)

	rec := c.call("catalog_catalogs", generated.CatalogCatalogsParamsSchema, map[string]any{})
	items := binaryList(t, rec, "items")
	rec.Release()
	var names []string
	for _, item := range items {
		info, err := vgi.DeserializeRecordBatch(item)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, column[*array.String](t, info, "name").Value(0))
		info.Release()
	}
	if !slices.Equal(names, []string{"parent", "kid"}) {
		t.Fatalf("catalog_catalogs lists %v, want [parent kid]", names)
	}

	kid := c.attach("kid")
	if !kid.SupportsCatalogContents {
		t.Fatal("the sub-catalog does not advertise catalog_contents")
	}
	if got := c.schemaNames(kid.AttachOpaqueData); !slices.Equal(got, []string{"extra", "main"}) {
		t.Fatalf("sub-catalog schemas %v, want [extra main]", got)
	}
	if n := len(c.views(kid.AttachOpaqueData)); n != 1 {
		t.Fatalf("sub-catalog main has %d views, want 1", n)
	}
	if n := len(c.functions(kid.AttachOpaqueData, "SCALAR_FUNCTION")); n != 1 {
		t.Fatalf("sub-catalog main lists %d scalar functions, want 1 (double)", n)
	}
	resp := c.contents(kid.AttachOpaqueData, nil)
	var paths []string
	for _, s := range resp.Schemas {
		paths = append(paths, strings.Join(s.Path, "."))
	}
	if !slices.Equal(paths, []string{"extra", "main"}) || resp.Etag != nil {
		t.Fatalf("sub-catalog catalog_contents: schemas %v etag %v", paths, resp.Etag)
	}

	// The parent's own catalog never sees the sub-catalog's functions or
	// schemas.
	own := c.attach("parent")
	if got := c.schemaNames(own.AttachOpaqueData); !slices.Equal(got, []string{"main"}) {
		t.Fatalf("parent schemas %v, want [main]", got)
	}
	if n := len(c.functions(own.AttachOpaqueData, "SCALAR_FUNCTION")); n != 0 {
		t.Fatalf("parent lists %d scalar functions; the sub-catalog's leaked into it", n)
	}

	// A function-backed table resolves through the sub-catalog.
	rec = c.call("catalog_table_scan_function_get", generated.CatalogTableScanFunctionGetParamsSchema,
		map[string]any{"attach_opaque_data": kid.AttachOpaqueData, "schema_path": []string{"extra"}, "name": "five"})
	defer rec.Release()
	if fn := column[*array.String](t, rec, "function_name").Value(0); fn != "sequence" {
		t.Fatalf("five scans %q, want sequence", fn)
	}
}

// A sub-catalog keeps its own catalog_contents options: a handler, and the
// advertise switch.
func TestSubCatalogContentsOptions(t *testing.T) {
	parent := vgi.NewWorker(vgi.WithCatalogName("parent"))
	parent.RegisterSubCatalog(staticChild("broken", vgi.WithCatalogContentsHandler(
		func(*vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
			return vgi.CatalogContentsResult{}, errors.New("deliberately fails")
		})))
	parent.RegisterSubCatalog(staticChild("legacy", vgi.WithCatalogContents(false)))
	c := newContentsClient(t, parent)

	broken := c.attach("broken")
	if !broken.SupportsCatalogContents {
		t.Fatal("broken does not advertise catalog_contents")
	}
	if err := c.contentsErr(broken.AttachOpaqueData, nil); err == nil || !strings.Contains(err.Error(), "deliberately fails") {
		t.Fatalf("broken catalog_contents error = %v", err)
	}
	// ... and the per-schema RPCs still serve it.
	if got := c.schemaNames(broken.AttachOpaqueData); !slices.Equal(got, []string{"extra", "main"}) {
		t.Fatalf("broken schemas %v", got)
	}
	if c.attach("legacy").SupportsCatalogContents {
		t.Fatal("legacy advertises catalog_contents")
	}
	if !c.attach("parent").SupportsCatalogContents {
		t.Fatal("the parent stopped advertising catalog_contents")
	}
}

func TestRoutedCatalogNameCollisionsPanic(t *testing.T) {
	for _, tc := range []struct {
		name string
		reg  func(w *vgi.Worker)
	}{
		{"own name", func(w *vgi.Worker) { w.RegisterMemoryCatalog(vgi.NewMemoryCatalog("parent")) }},
		{"twice", func(w *vgi.Worker) {
			w.RegisterMemoryCatalog(vgi.NewMemoryCatalog("m"))
			w.RegisterSubCatalog(vgi.NewWorker(vgi.WithCatalogName("m")))
		}},
		{"alias", func(w *vgi.Worker) { w.RegisterMemoryCatalog(vgi.NewMemoryCatalog("alias")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := vgi.NewWorker(vgi.WithCatalogName("parent"), vgi.WithCatalogAliases("alias"))
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			tc.reg(w)
		})
	}
}

// Every ATTACH of a MemoryCatalog is private; DDL changes only that attach and
// bumps its version; DETACH drops it.
func TestMemoryCatalogAttachesArePrivate(t *testing.T) {
	parent := vgi.NewWorker(vgi.WithCatalogName("parent"))
	parent.RegisterMemoryCatalog(vgi.NewMemoryCatalog("mem"))
	c := newContentsClient(t, parent)

	a, b := c.attach("mem"), c.attach("mem")
	if !a.SupportsCatalogContents || a.CatalogVersion != 1 {
		t.Fatalf("attach: supports_catalog_contents=%v version=%d", a.SupportsCatalogContents, a.CatalogVersion)
	}
	if err := c.createView(a.AttachOpaqueData, "v"); err != nil {
		t.Fatal(err)
	}
	if err := c.createView(a.AttachOpaqueData, "v"); err == nil {
		t.Fatal("a second CREATE VIEW v (on_conflict ERROR) succeeded")
	}
	if n := len(c.views(a.AttachOpaqueData)); n != 1 {
		t.Fatalf("attach a has %d views, want 1", n)
	}
	if n := len(c.views(b.AttachOpaqueData)); n != 0 {
		t.Fatalf("attach b sees %d views of attach a", n)
	}
	if v := c.version(a.AttachOpaqueData); v != 2 {
		t.Fatalf("version after DDL = %d, want 2", v)
	}
	if v := c.version(b.AttachOpaqueData); v != 1 {
		t.Fatalf("untouched attach version = %d, want 1", v)
	}

	// Schemas: CREATE SCHEMA adds one (parents first in catalog_contents).
	if err := c.exec("catalog_schema_create", generated.CatalogSchemaCreateParamsSchema, map[string]any{
		"attach_opaque_data": a.AttachOpaqueData, "path": []string{"s2"}, "on_conflict": "ERROR",
	}); err != nil {
		t.Fatal(err)
	}
	if got := c.schemaNames(a.AttachOpaqueData); !slices.Equal(got, []string{"main", "s2"}) {
		t.Fatalf("schemas %v, want [main s2]", got)
	}
	resp := c.contents(a.AttachOpaqueData, nil)
	if resp.CatalogVersion != 3 || len(resp.Schemas) != 2 || len(resp.Schemas[0].Views) != 1 {
		t.Fatalf("catalog_contents: version %d, %d schemas", resp.CatalogVersion, len(resp.Schemas))
	}
	if resp.Etag != nil {
		t.Fatal("a MemoryCatalog with no etag option returned an etag")
	}

	if err := c.exec("catalog_view_drop", generated.CatalogViewDropParamsSchema, map[string]any{
		"attach_opaque_data": a.AttachOpaqueData, "schema_path": []string{"main"}, "name": "V",
		"ignore_not_found": false, "cascade": false,
	}); err != nil {
		t.Fatalf("DROP VIEW (case-insensitive): %v", err)
	}
	if n := len(c.views(a.AttachOpaqueData)); n != 0 {
		t.Fatalf("%d views after DROP", n)
	}

	if err := c.exec("catalog_detach", generated.CatalogDetachParamsSchema, map[string]any{"attach_opaque_data": a.AttachOpaqueData}); err != nil {
		t.Fatal(err)
	}
	if err := c.contentsErr(a.AttachOpaqueData, nil); err == nil || !strings.Contains(err.Error(), "not attached") {
		t.Fatalf("catalog_contents after DETACH: %v", err)
	}
	// A method the catalog does not implement is refused, not misrouted to
	// the parent.
	err := c.exec("catalog_table_scan_function_get", generated.CatalogTableScanFunctionGetParamsSchema,
		map[string]any{"attach_opaque_data": b.AttachOpaqueData, "schema_path": []string{"main"}, "name": "t"})
	if err == nil || !strings.Contains(err.Error(), "does not support catalog_table_scan_function_get") {
		t.Fatalf("unsupported method: %v", err)
	}
}

func TestMemoryCatalogUnversioned(t *testing.T) {
	parent := vgi.NewWorker(vgi.WithCatalogName("parent"))
	parent.RegisterMemoryCatalog(vgi.NewMemoryCatalog("mem", vgi.WithMemoryCatalogUnversioned()))
	c := newContentsClient(t, parent)
	a := c.attach("mem")
	if a.CatalogVersion != 0 {
		t.Fatalf("attach version %d, want 0", a.CatalogVersion)
	}
	if err := c.createView(a.AttachOpaqueData, "v"); err != nil {
		t.Fatal(err)
	}
	if v := c.version(a.AttachOpaqueData); v != 0 {
		t.Fatalf("version after DDL %d, want 0", v)
	}
	if resp := c.contents(a.AttachOpaqueData, nil); resp.CatalogVersion != 0 || resp.Etag != nil {
		t.Fatalf("catalog_contents version %d etag %v", resp.CatalogVersion, resp.Etag)
	}
}

// A cheap validator: the handler answers not_modified without building, and a
// DDL changes the etag.
func TestMemoryCatalogContentsHandler(t *testing.T) {
	builds := 0
	parent := vgi.NewWorker(vgi.WithCatalogName("parent"))
	parent.RegisterMemoryCatalog(vgi.NewMemoryCatalog("mem", vgi.WithMemoryCatalogContentsHandler(
		func(call *vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
			etag := "gen-" + strings.Repeat("I", int(call.CatalogVersion))
			if call.IfNoneMatch != nil && *call.IfNoneMatch == etag {
				return vgi.CatalogContentsResult{Etag: &etag, NotModified: true}, nil
			}
			builds++
			schemas, err := call.Contents()
			return vgi.CatalogContentsResult{Schemas: schemas, Etag: &etag}, err
		})))
	c := newContentsClient(t, parent)
	a := c.attach("mem").AttachOpaqueData
	full := c.contents(a, nil)
	if full.Etag == nil || *full.Etag != "gen-I" || builds != 1 {
		t.Fatalf("etag %v builds %d", full.Etag, builds)
	}
	if nm := c.contents(a, full.Etag); !nm.NotModified || len(nm.Schemas) != 0 || builds != 1 {
		t.Fatalf("not_modified=%v schemas=%d builds=%d", nm.NotModified, len(nm.Schemas), builds)
	}
	if err := c.createView(a, "v"); err != nil {
		t.Fatal(err)
	}
	if again := c.contents(a, full.Etag); again.NotModified || *again.Etag != "gen-II" || len(again.Schemas[0].Views) != 1 {
		t.Fatalf("after DDL: not_modified=%v etag=%v", again.NotModified, again.Etag)
	}
}

// The framework content hash on a MemoryCatalog: stable while the contents are
// unchanged, different after a DDL.
func TestMemoryCatalogContentHashEtag(t *testing.T) {
	parent := vgi.NewWorker(vgi.WithCatalogName("parent"))
	parent.RegisterMemoryCatalog(vgi.NewMemoryCatalog("mem",
		vgi.WithMemoryCatalogContentsEtag(vgi.CatalogContentsEtagContentHash)))
	c := newContentsClient(t, parent)
	a := c.attach("mem").AttachOpaqueData
	if err := c.createView(a, "v"); err != nil {
		t.Fatal(err)
	}
	full := c.contents(a, nil)
	if full.Etag == nil || len(*full.Etag) != 64 || *full.Etag != vgi.CatalogContentsDigest(full.Schemas) {
		t.Fatalf("etag %v is not the snapshot's digest", full.Etag)
	}
	if nm := c.contents(a, full.Etag); !nm.NotModified {
		t.Fatal("unchanged contents were not not_modified")
	}
	if err := c.createView(a, "w"); err != nil {
		t.Fatal(err)
	}
	if after := c.contents(a, full.Etag); after.NotModified || *after.Etag == *full.Etag {
		t.Fatalf("after DDL: not_modified=%v etag unchanged=%v", after.NotModified, *after.Etag == *full.Etag)
	}
}
