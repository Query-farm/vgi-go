// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/examples/all"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Function listings are built once.
//
// catalog_schema_contents_functions is answered from the static registrations,
// yet every request rebuilt the listing and Arrow-encoded every listed
// FunctionInfo again -- ~66 ms of worker time per listing of the example
// worker's main schema, paid on every DuckDB function-set load. These serve the
// shipped example worker over its real HTTP handler, attach the way DuckDB
// does, and list; the requests are hand-built, every response is the worker's.

type listingClient struct {
	t      *testing.T
	client *vgirpc.HttpClient
}

func newListingClient(t *testing.T) *listingClient {
	t.Helper()
	w := vgi.NewWorker(vgi.WithCatalogName("example"), vgi.WithCatalogAliases("projection_repro"))
	all.RegisterAll(w)
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatalf("building the HTTP server: %v", err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	client, err := vgirpc.NewHttpClient(ts.URL,
		vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatalf("building the HTTP client: %v", err)
	}
	t.Cleanup(client.Close)
	return &listingClient{t: t, client: client}
}

// attach runs catalog_attach for name and returns the attach_opaque_data the
// worker answered with.
func (c *listingClient) attach(name string) []byte {
	t := c.t
	t.Helper()
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, map[string]any{"name": name}))
	params := jsonRow(t, generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer params.Release()
	result, err := c.client.CallUnary(context.Background(), "catalog_attach", params, nil)
	if err != nil {
		t.Fatalf("attaching %q: %v", name, err)
	}
	defer result.Release()
	attach := resultStruct(t, result)
	defer attach.Release()
	column := attach.Column(attach.Schema().FieldIndices("attach_opaque_data")[0]).(*array.Binary)
	return bytes.Clone(column.Value(0))
}

// resultStruct decodes a unary result: one binary "result" column holding the
// IPC-encoded result struct.
func resultStruct(t *testing.T, result *vgirpc.ClientBatch) arrow.RecordBatch {
	t.Helper()
	column := result.Batch.Column(result.Batch.Schema().FieldIndices("result")[0]).(*array.Binary)
	batch, err := vgi.DeserializeRecordBatch(column.Value(0))
	if err != nil {
		t.Fatalf("decoding a unary result: %v", err)
	}
	return batch
}

// list runs catalog_schema_contents_functions and returns its items.
func (c *listingClient) list(attach []byte, schema, functionType string) [][]byte {
	t := c.t
	t.Helper()
	params := jsonRow(t, generated.CatalogSchemaContentsFunctionsParamsSchema, map[string]any{
		"attach_opaque_data": attach,
		"path":               []string{schema},
		"type":               functionType,
	})
	defer params.Release()
	result, err := c.client.CallUnary(context.Background(), "catalog_schema_contents_functions", params, nil)
	if err != nil {
		t.Fatalf("listing %s %q: %v", schema, functionType, err)
	}
	defer result.Release()
	listing := resultStruct(t, result)
	defer listing.Release()
	list := listing.Column(listing.Schema().FieldIndices("items")[0]).(*array.List)
	values := list.ListValues().(*array.Binary)
	start, end := list.ValueOffsets(0)
	items := make([][]byte, 0, end-start)
	for i := start; i < end; i++ {
		items = append(items, bytes.Clone(values.Value(int(i))))
	}
	return items
}

// functionName decodes the name a listed FunctionInfo carries.
func functionName(t *testing.T, item []byte) string {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(item), ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		t.Fatalf("decoding a listed item: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("a listed item carries no batch")
	}
	batch := reader.RecordBatch()
	return batch.Column(batch.Schema().FieldIndices("name")[0]).(*array.String).Value(0)
}

func functionNames(t *testing.T, items [][]byte) []string {
	names := make([]string, len(items))
	for i, item := range items {
		names[i] = functionName(t, item)
	}
	slices.Sort(names)
	return names
}

// A repeated listing does no per-function encoding: its allocations do not
// scale with the functions it lists the way encoding each one did.
func TestFunctionListingIsNotReEncodedPerCall(t *testing.T) {
	c := newListingClient(t)
	attach := c.attach("example")
	items := c.list(attach, "main", "")
	if len(items) < 100 {
		t.Fatalf("the main schema listed %d functions; the example worker registers well over 100", len(items))
	}
	started := time.Now()
	for i := 0; i < 10; i++ {
		c.list(attach, "main", "")
	}
	perListing := time.Since(started) / 10
	allocs := testing.AllocsPerRun(5, func() { c.list(attach, "main", "") })
	t.Logf("main schema: %d functions, %.0f allocations and %v per repeated listing (client included)", len(items), allocs, perListing)
	// Encoding one FunctionInfo alone allocates far more than this; the
	// remaining work (the RPC round trip and copying each item into the
	// response) is a handful of allocations per item.
	if limit := float64(12 * len(items)); allocs > limit {
		t.Fatalf("a repeated listing of %d functions made %.0f allocations (limit %.0f): every function is re-encoded per call",
			len(items), allocs, limit)
	}
}

// The cached listings answer exactly what a fresh listing does: every filter
// and catalog gets its own entry, and repeated calls return the same bytes.
func TestFunctionListingsKeepTheirFilters(t *testing.T) {
	c := newListingClient(t)
	example := c.attach("example")
	repro := c.attach("projection_repro")

	everything := c.list(example, "main", "")
	var parts [][]byte
	for _, functionType := range []string{"TABLE_FUNCTION", "SCALAR_FUNCTION", "AGGREGATE_FUNCTION"} {
		first := c.list(example, "main", functionType)
		if again := c.list(example, "main", functionType); !slices.EqualFunc(first, again, bytes.Equal) {
			t.Fatalf("two %s listings differ", functionType)
		}
		parts = append(parts, first...)
	}
	if got, want := functionNames(t, parts), functionNames(t, everything); !slices.Equal(got, want) {
		t.Fatalf("the typed listings together name %d functions, the unfiltered listing %d", len(got), len(want))
	}
	// An unrecognized type filters nothing.
	if got := c.list(example, "main", "NOT_A_TYPE"); !slices.EqualFunc(got, everything, bytes.Equal) {
		t.Fatalf("an unrecognized type listed %d functions, want all %d", len(got), len(everything))
	}

	// Each catalog lists only what it owns, cached separately.
	exampleTables := functionNames(t, c.list(example, "main", "TABLE_FUNCTION"))
	reproTables := functionNames(t, c.list(repro, "main", "TABLE_FUNCTION"))
	if !slices.Contains(reproTables, "proj_repro_strict") || slices.Contains(exampleTables, "proj_repro_strict") {
		t.Fatalf("proj_repro_strict listed under example=%t projection_repro=%t, want only projection_repro",
			slices.Contains(exampleTables, "proj_repro_strict"), slices.Contains(reproTables, "proj_repro_strict"))
	}
	if again := functionNames(t, c.list(example, "main", "TABLE_FUNCTION")); !slices.Equal(again, exampleTables) {
		t.Fatal("listing another catalog changed the example catalog's cached listing")
	}
	if got := c.list(example, "no_such_schema", ""); len(got) != 0 {
		t.Fatalf("an unknown schema listed %d functions", len(got))
	}
}
