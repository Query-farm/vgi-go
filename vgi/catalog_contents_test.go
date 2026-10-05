// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/examples/all"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// catalog_contents serves the whole catalog in one call. Its contract is that
// it is exactly the per-schema RPCs, batched: one entry per catalog_schemas
// schema, parents first, and every item byte-for-byte what the matching
// catalog_schema_contents_* call returns. These drive the real HTTP handler
// with vgi-rpc-go's client, the same way DuckDB does, and compare.

type contentsClient struct {
	t      *testing.T
	client *vgirpc.HttpClient
}

func newContentsClient(t *testing.T, w *vgi.Worker) *contentsClient {
	t.Helper()
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
	return &contentsClient{t: t, client: client}
}

// call runs a unary catalog RPC and returns its decoded result record.
func (c *contentsClient) call(method string, params *arrow.Schema, row map[string]any) arrow.RecordBatch {
	t := c.t
	t.Helper()
	batch := jsonRow(t, params, row)
	defer batch.Release()
	result, err := c.client.CallUnary(context.Background(), method, batch, nil)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	defer result.Release()
	return resultStruct(t, result)
}

func (c *contentsClient) attach(name string) generated.CatalogAttachResult {
	t := c.t
	t.Helper()
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, map[string]any{"name": name}))
	rec := c.call("catalog_attach", generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer rec.Release()
	return generated.CatalogAttachResult{
		AttachOpaqueData:        bytes.Clone(column[*array.Binary](t, rec, "attach_opaque_data").Value(0)),
		SupportsTransactions:    column[*array.Boolean](t, rec, "supports_transactions").Value(0),
		CatalogVersion:          column[*array.Int64](t, rec, "catalog_version").Value(0),
		SupportsCatalogContents: column[*array.Boolean](t, rec, "supports_catalog_contents").Value(0),
	}
}

func (c *contentsClient) items(method string, params *arrow.Schema, row map[string]any) [][]byte {
	c.t.Helper()
	rec := c.call(method, params, row)
	defer rec.Release()
	return binaryList(c.t, rec, "items")
}

func column[A arrow.Array](t *testing.T, rec arrow.RecordBatch, name string) A {
	t.Helper()
	idx := rec.Schema().FieldIndices(name)
	if len(idx) != 1 {
		t.Fatalf("result has no column %q (schema %v)", name, rec.Schema())
	}
	col, ok := rec.Column(idx[0]).(A)
	if !ok {
		t.Fatalf("column %q is %T", name, rec.Column(idx[0]))
	}
	return col
}

func binaryList(t *testing.T, rec arrow.RecordBatch, name string) [][]byte {
	t.Helper()
	list := column[*array.List](t, rec, name)
	values := list.ListValues().(*array.Binary)
	start, end := list.ValueOffsets(0)
	out := make([][]byte, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, bytes.Clone(values.Value(int(i))))
	}
	return out
}

// schemaPathOf decodes the path a SchemaInfo item carries.
func schemaPathOf(t *testing.T, item []byte) []string {
	t.Helper()
	rec, err := vgi.DeserializeRecordBatch(item)
	if err != nil {
		t.Fatalf("decoding a SchemaInfo item: %v", err)
	}
	defer rec.Release()
	list := column[*array.List](t, rec, "path")
	values := list.ListValues().(*array.String)
	start, end := list.ValueOffsets(0)
	out := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, values.Value(int(i)))
	}
	return out
}

// contentsWorker is the shipped example functions plus catalog tables, views
// and macros, spread over the default schema, "data", and a nested
// "data"."nested" schema so the parents-first ordering has something to order.
func contentsWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	w := vgi.NewWorker(append([]vgi.WorkerOption{vgi.WithCatalogName("example")}, opts...)...)
	all.RegisterAll(w)
	cols := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	w.RegisterCatalogTable("data", vgi.CatalogTable{Name: "t1", Columns: cols, Comment: "first"})
	w.RegisterCatalogTable("data", vgi.CatalogTable{Name: "t2", Columns: cols,
		ColumnComments: map[string]string{"id": "the id"}})
	w.RegisterCatalogSchemaPath(vgi.SchemaPath{"data", "nested"}, "a nested schema")
	w.RegisterCatalogViewPath(vgi.SchemaPath{"data", "nested"}, vgi.CatalogView{
		Name: "v", Definition: "SELECT 1 AS one", Tags: map[string]string{"b": "2", "a": "1", "c": "3"},
	})
	w.RegisterCatalogView("main", vgi.CatalogView{Name: "small", Definition: "SELECT 2 AS two"})
	w.RegisterCatalogMacro("main", vgi.CatalogMacro{Name: "m_scalar", MacroType: vgi.MacroTypeScalar,
		Parameters: []string{"x"}, Definition: "x + 1"})
	w.RegisterCatalogMacro("main", vgi.CatalogMacro{Name: "m_table", MacroType: vgi.MacroTypeTable,
		Parameters: []string{"n"}, Definition: "SELECT * FROM range(n)"})
	return w
}

func TestCatalogContentsMatchesPerSchemaRPCs(t *testing.T) {
	c := newContentsClient(t, contentsWorker())
	attached := c.attach("example")
	if !attached.SupportsCatalogContents {
		t.Fatal("the default read-only catalog must advertise supports_catalog_contents")
	}
	attach := attached.AttachOpaqueData

	rec := c.call("catalog_contents", generated.CatalogContentsParamsSchema, map[string]any{"attach_opaque_data": attach})
	defer rec.Release()
	if got := column[*array.Int64](t, rec, "catalog_version").Value(0); got != attached.CatalogVersion {
		t.Errorf("catalog_version = %d, attach said %d", got, attached.CatalogVersion)
	}
	entries := binaryList(t, rec, "schemas")

	// One entry per catalog_schemas schema, byte-identical SchemaInfo items.
	listed := c.items("catalog_schemas", generated.CatalogSchemasParamsSchema, map[string]any{"attach_opaque_data": attach})
	byPath := map[string][]byte{}
	for _, item := range listed {
		byPath[strings.Join(schemaPathOf(t, item), ".")] = item
	}
	if len(entries) != len(listed) {
		t.Fatalf("catalog_contents has %d schemas, catalog_schemas %d", len(entries), len(listed))
	}

	seen := map[string]bool{}
	var kinds = map[string]int{}
	for i, entry := range entries {
		sc, err := vgi.DeserializeSchemaContents(entry)
		if err != nil {
			t.Fatalf("schema entry %d: %v", i, err)
		}
		path := schemaPathOf(t, sc.Schema)
		key := strings.Join(path, ".")
		if !bytes.Equal(sc.Schema, byPath[key]) {
			t.Errorf("schema %s: SchemaInfo item differs from catalog_schemas'", key)
		}
		// Parents first: a schema's parent was emitted before it.
		if len(path) > 1 && !seen[strings.Join(path[:len(path)-1], ".")] {
			t.Errorf("schema %s precedes its parent", key)
		}
		if i > 0 {
			prev, _ := vgi.DeserializeSchemaContents(entries[i-1])
			if len(schemaPathOf(t, prev.Schema)) > len(path) {
				t.Errorf("schema %s follows a deeper schema", key)
			}
		}
		seen[key] = true

		scoped := map[string]any{"attach_opaque_data": attach, "path": path}
		typed := func(kind string) map[string]any {
			return map[string]any{"attach_opaque_data": attach, "path": path, "type": kind}
		}
		for _, cmp := range []struct {
			kind string
			got  [][]byte
			want [][]byte
		}{
			{"tables", sc.Tables, c.items("catalog_schema_contents_tables", generated.CatalogSchemaContentsTablesParamsSchema, scoped)},
			{"views", sc.Views, c.items("catalog_schema_contents_views", generated.CatalogSchemaContentsViewsParamsSchema, scoped)},
			{"scalar_functions", sc.ScalarFunctions, c.items("catalog_schema_contents_functions", generated.CatalogSchemaContentsFunctionsParamsSchema, typed("SCALAR_FUNCTION"))},
			{"aggregate_functions", sc.AggregateFunctions, c.items("catalog_schema_contents_functions", generated.CatalogSchemaContentsFunctionsParamsSchema, typed("AGGREGATE_FUNCTION"))},
			{"table_functions", sc.TableFunctions, c.items("catalog_schema_contents_functions", generated.CatalogSchemaContentsFunctionsParamsSchema, typed("TABLE_FUNCTION"))},
			{"scalar_macros", sc.ScalarMacros, c.items("catalog_schema_contents_macros", generated.CatalogSchemaContentsMacrosParamsSchema, typed("SCALAR_MACRO"))},
			{"table_macros", sc.TableMacros, c.items("catalog_schema_contents_macros", generated.CatalogSchemaContentsMacrosParamsSchema, typed("TABLE_MACRO"))},
		} {
			if !slices.EqualFunc(cmp.got, cmp.want, bytes.Equal) {
				t.Errorf("schema %s %s: catalog_contents has %d items, the per-schema RPC %d, or their bytes differ",
					key, cmp.kind, len(cmp.got), len(cmp.want))
			}
			kinds[cmp.kind] += len(cmp.got)
		}
		if len(sc.Indexes) != 0 {
			t.Errorf("schema %s: %d indexes from an SDK with no index API", key, len(sc.Indexes))
		}
	}
	for path := range byPath {
		if !seen[path] {
			t.Errorf("catalog_schemas lists %s; catalog_contents does not", path)
		}
	}
	// The fixture populates every kind, so a comparison of empty lists cannot
	// pass vacuously.
	for _, kind := range []string{"tables", "views", "scalar_functions", "aggregate_functions", "table_functions", "scalar_macros", "table_macros"} {
		if kinds[kind] == 0 {
			t.Errorf("fixture lists no %s; the comparison above proves nothing for that kind", kind)
		}
	}
	if !seen["data.nested"] {
		t.Error("the nested schema is missing")
	}
}

func TestCatalogContentsCanBeTurnedOff(t *testing.T) {
	c := newContentsClient(t, contentsWorker(vgi.WithCatalogContents(false)))
	if c.attach("example").SupportsCatalogContents {
		t.Fatal("WithCatalogContents(false) still advertises supports_catalog_contents")
	}
}

func TestWritableCatalogDoesNotAdvertiseCatalogContents(t *testing.T) {
	w := contentsWorker()
	w.RegisterWritableCatalog(vgi.NewWritableCatalog("scratch"))
	c := newContentsClient(t, w)
	if c.attach("scratch").SupportsCatalogContents {
		t.Fatal("a writable (transactional) catalog advertises supports_catalog_contents")
	}
	if !c.attach("example").SupportsCatalogContents {
		t.Fatal("the read-only catalog beside it stopped advertising supports_catalog_contents")
	}
}

func TestSchemaContentsRoundTrip(t *testing.T) {
	in := generated.SchemaContents{
		Schema:             []byte("schema"),
		Tables:             [][]byte{[]byte("t1"), []byte("t2")},
		Views:              [][]byte{},
		ScalarFunctions:    [][]byte{[]byte("f")},
		AggregateFunctions: [][]byte{},
		TableFunctions:     [][]byte{[]byte("tf")},
		ScalarMacros:       [][]byte{},
		TableMacros:        [][]byte{[]byte("tm")},
		Indexes:            [][]byte{},
	}
	data, err := vgi.SerializeSchemaContents(&in)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := vgi.DeserializeRecordBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if !rec.Schema().Equal(generated.SchemaContentsSchema) {
		t.Fatalf("encoded schema %v, protocol says %v", rec.Schema(), generated.SchemaContentsSchema)
	}
	out, err := vgi.DeserializeSchemaContents(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Schema, in.Schema) || !slices.EqualFunc(out.Tables, in.Tables, bytes.Equal) ||
		!slices.EqualFunc(out.TableFunctions, in.TableFunctions, bytes.Equal) ||
		!slices.EqualFunc(out.TableMacros, in.TableMacros, bytes.Equal) || len(out.Views) != 0 {
		t.Fatalf("round trip changed the record: %+v", out)
	}
}
