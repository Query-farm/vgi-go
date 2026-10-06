// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
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

// contents calls catalog_contents (with if_none_match when non-nil) and
// decodes the response, list<struct<SchemaContents>> included.
func (c *contentsClient) contents(attach []byte, ifNoneMatch *string) generated.CatalogContentsResponse {
	t := c.t
	t.Helper()
	row := map[string]any{"attach_opaque_data": attach}
	if ifNoneMatch != nil {
		row["if_none_match"] = *ifNoneMatch
	}
	rec := c.call("catalog_contents", generated.CatalogContentsParamsSchema, row)
	defer rec.Release()
	return decodeContentsResponse(t, rec)
}

// contentsErr calls catalog_contents and returns the RPC error (nil on
// success).
func (c *contentsClient) contentsErr(attach []byte, ifNoneMatch *string) error {
	t := c.t
	t.Helper()
	row := map[string]any{"attach_opaque_data": attach}
	if ifNoneMatch != nil {
		row["if_none_match"] = *ifNoneMatch
	}
	batch := jsonRow(t, generated.CatalogContentsParamsSchema, row)
	defer batch.Release()
	result, err := c.client.CallUnary(context.Background(), "catalog_contents", batch, nil)
	if err == nil {
		result.Release()
	}
	return err
}

// decodeContentsResponse decodes a catalog_contents result record, checking
// its schema is the protocol's.
func decodeContentsResponse(t *testing.T, rec arrow.RecordBatch) generated.CatalogContentsResponse {
	t.Helper()
	if !rec.Schema().Equal(generated.CatalogContentsResultSchema) {
		t.Fatalf("catalog_contents result schema\n%v\nprotocol says\n%v", rec.Schema(), generated.CatalogContentsResultSchema)
	}
	out := generated.CatalogContentsResponse{
		CatalogVersion: column[*array.Int64](t, rec, "catalog_version").Value(0),
		NotModified:    column[*array.Boolean](t, rec, "not_modified").Value(0),
	}
	if etag := column[*array.String](t, rec, "etag"); etag.IsValid(0) {
		v := etag.Value(0)
		out.Etag = &v
	}
	list := column[*array.List](t, rec, "schemas")
	rows := list.ListValues().(*array.Struct)
	field := func(name string) arrow.Array {
		idx, ok := rows.DataType().(*arrow.StructType).FieldIdx(name)
		if !ok {
			t.Fatalf("SchemaContents has no field %q", name)
		}
		return rows.Field(idx)
	}
	strs := func(arr arrow.Array, i int) []string {
		l := arr.(*array.List)
		values := l.ListValues().(*array.String)
		start, end := l.ValueOffsets(i)
		out := make([]string, 0, end-start)
		for j := start; j < end; j++ {
			out = append(out, values.Value(int(j)))
		}
		return out
	}
	bins := func(arr arrow.Array, i int) [][]byte {
		l := arr.(*array.List)
		values := l.ListValues().(*array.Binary)
		start, end := l.ValueOffsets(i)
		out := make([][]byte, 0, end-start)
		for j := start; j < end; j++ {
			out = append(out, bytes.Clone(values.Value(int(j))))
		}
		return out
	}
	start, end := list.ValueOffsets(0)
	out.Schemas = []generated.SchemaContents{}
	for i := int(start); i < int(end); i++ {
		out.Schemas = append(out.Schemas, generated.SchemaContents{
			Path:               strs(field("path"), i),
			Schema:             bytes.Clone(field("schema").(*array.Binary).Value(i)),
			Tables:             bins(field("tables"), i),
			Views:              bins(field("views"), i),
			ScalarFunctions:    bins(field("scalar_functions"), i),
			AggregateFunctions: bins(field("aggregate_functions"), i),
			TableFunctions:     bins(field("table_functions"), i),
			ScalarMacros:       bins(field("scalar_macros"), i),
			TableMacros:        bins(field("table_macros"), i),
			Indexes:            bins(field("indexes"), i),
		})
	}
	return out
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

	resp := c.contents(attach, nil)
	if resp.CatalogVersion != attached.CatalogVersion {
		t.Errorf("catalog_version = %d, attach said %d", resp.CatalogVersion, attached.CatalogVersion)
	}
	if resp.NotModified || resp.Etag != nil {
		t.Errorf("the default catalog answered not_modified=%v etag=%v; it does not revalidate", resp.NotModified, resp.Etag)
	}
	entries := resp.Schemas

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
	for i, sc := range entries {
		path := schemaPathOf(t, sc.Schema)
		key := strings.Join(path, ".")
		if !slices.Equal(sc.Path, path) {
			t.Errorf("schema %s: SchemaContents.path is %v", key, sc.Path)
		}
		if !bytes.Equal(sc.Schema, byPath[key]) {
			t.Errorf("schema %s: SchemaInfo item differs from catalog_schemas'", key)
		}
		// Parents first: a schema's parent was emitted before it.
		if len(path) > 1 && !seen[strings.Join(path[:len(path)-1], ".")] {
			t.Errorf("schema %s precedes its parent", key)
		}
		if i > 0 {
			if len(entries[i-1].Path) > len(path) {
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

func ptr(s string) *string { return &s }

// The request wire type derives exactly the protocol's params schema
// (if_none_match: nullable utf8).
func TestCatalogContentsRequestWireMatchesParams(t *testing.T) {
	got, err := vgirpc.SchemaForStruct(reflect.TypeOf(vgi.CatalogContentsRequestWire{}))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(generated.CatalogContentsParamsSchema) {
		t.Fatalf("CatalogContentsRequestWire derives\n%v\nthe protocol declares\n%v", got, generated.CatalogContentsParamsSchema)
	}
}

// A catalog with no etag does not revalidate: it ignores if_none_match and
// always answers in full.
func TestCatalogContentsWithoutEtagIgnoresIfNoneMatch(t *testing.T) {
	c := newContentsClient(t, contentsWorker())
	attach := c.attach("example").AttachOpaqueData
	full := c.contents(attach, nil)
	for _, inm := range []string{"", "anything"} {
		got := c.contents(attach, ptr(inm))
		if got.NotModified || got.Etag != nil || len(got.Schemas) != len(full.Schemas) {
			t.Errorf("if_none_match=%q: not_modified=%v etag=%v schemas=%d, want a full answer with no etag",
				inm, got.NotModified, got.Etag, len(got.Schemas))
		}
	}
}

// The framework content hash: the etag is CatalogContentsDigest of the
// snapshot, a match is not_modified (no schemas, same etag), and anything else
// gets the full contents with that etag.
func TestCatalogContentsContentHashEtag(t *testing.T) {
	c := newContentsClient(t, contentsWorker(vgi.WithCatalogContentsEtag(vgi.CatalogContentsEtagContentHash)))
	attach := c.attach("example").AttachOpaqueData
	full := c.contents(attach, nil)
	if full.Etag == nil || full.NotModified {
		t.Fatalf("content-hash mode answered etag=%v not_modified=%v", full.Etag, full.NotModified)
	}
	if want := vgi.CatalogContentsDigest(full.Schemas); *full.Etag != want {
		t.Fatalf("etag %s is not the digest of the snapshot it came with (%s)", *full.Etag, want)
	}
	if len(*full.Etag) != 64 {
		t.Fatalf("etag %q is not a hex SHA-256", *full.Etag)
	}
	nm := c.contents(attach, full.Etag)
	if !nm.NotModified || len(nm.Schemas) != 0 || nm.Etag == nil || *nm.Etag != *full.Etag || nm.CatalogVersion != full.CatalogVersion {
		t.Fatalf("matching if_none_match: not_modified=%v schemas=%d etag=%v version=%d", nm.NotModified, len(nm.Schemas), nm.Etag, nm.CatalogVersion)
	}
	other := c.contents(attach, ptr("not-the-etag"))
	if other.NotModified || len(other.Schemas) != len(full.Schemas) || other.Etag == nil || *other.Etag != *full.Etag {
		t.Fatalf("stale if_none_match: not_modified=%v schemas=%d etag=%v", other.NotModified, len(other.Schemas), other.Etag)
	}
}

// The content hash depends only on the catalog: separately built workers of
// the same catalog (with Go's randomized map iteration over tags, column
// comments and object counts) hash alike, every time.
func TestCatalogContentsContentHashIsDeterministic(t *testing.T) {
	var first string
	for i := range 8 {
		c := newContentsClient(t, contentsWorker(vgi.WithCatalogContentsEtag(vgi.CatalogContentsEtagContentHash),
			vgi.WithSchemaTags(map[string]map[string]string{
				"main": {"k1": "1", "k2": "2", "k3": "3", "k4": "4", "k5": "5", "k6": "6"},
			})))
		got := c.contents(c.attach("example").AttachOpaqueData, nil)
		if i == 0 {
			first = *got.Etag
		} else if *got.Etag != first {
			t.Fatalf("build %d hashed %s, build 0 %s: the snapshot encoding is not deterministic", i, *got.Etag, first)
		}
	}
}

// CatalogContentsDigest is vgi-python's catalog_contents_digest: the same
// snapshot gives the same etag in every SDK. The expected values were computed
// with vgi-python (vgi.worker.catalog_contents_digest) for the same input.
func TestCatalogContentsDigestMatchesPython(t *testing.T) {
	b := func(ss ...string) [][]byte {
		out := [][]byte{}
		for _, s := range ss {
			out = append(out, []byte(s))
		}
		return out
	}
	snapshot := []generated.SchemaContents{
		{Path: []string{"main"}, Schema: []byte("schema-main"), Tables: b("t1", "t2"), Views: b(),
			ScalarFunctions: b("f"), AggregateFunctions: b(), TableFunctions: b("tf"), ScalarMacros: b(),
			TableMacros: b("tm"), Indexes: b()},
		{Path: []string{"main", "nested"}, Schema: []byte("schema-nested"), Tables: b(), Views: b("v"),
			ScalarFunctions: b(), AggregateFunctions: b("agg"), TableFunctions: b(), ScalarMacros: b("sm"),
			TableMacros: b(), Indexes: b("ix")},
	}
	if got, want := vgi.CatalogContentsDigest(snapshot), "be09c2413dd2dc4eb5996641aacc8525c5d507057ffb97ba15adf872d278acac"; got != want {
		t.Errorf("digest %s, vgi-python %s", got, want)
	}
	if got, want := vgi.CatalogContentsDigest(nil), "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc"; got != want {
		t.Errorf("empty digest %s, vgi-python %s", got, want)
	}
}

// A catalog with a cheap validator answers not_modified before building
// anything; on a miss it builds and returns its etag.
func TestCatalogContentsHandlerShortCircuits(t *testing.T) {
	builds := 0
	handler := func(call *vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
		etag := fmt.Sprintf("gen-%d", call.CatalogVersion)
		if call.IfNoneMatch != nil && *call.IfNoneMatch == etag {
			return vgi.CatalogContentsResult{Etag: &etag, NotModified: true}, nil
		}
		builds++
		schemas, err := call.Contents()
		return vgi.CatalogContentsResult{Schemas: schemas, Etag: &etag}, err
	}
	c := newContentsClient(t, contentsWorker(vgi.WithCatalogContentsHandler(handler)))
	attach := c.attach("example").AttachOpaqueData
	full := c.contents(attach, nil)
	if full.Etag == nil || *full.Etag != fmt.Sprintf("gen-%d", full.CatalogVersion) || len(full.Schemas) == 0 || builds != 1 {
		t.Fatalf("full answer: etag=%v schemas=%d builds=%d", full.Etag, len(full.Schemas), builds)
	}
	// The handler's contents are the default composition.
	plain := newContentsClient(t, contentsWorker())
	if want := plain.contents(plain.attach("example").AttachOpaqueData, nil); vgi.CatalogContentsDigest(want.Schemas) != vgi.CatalogContentsDigest(full.Schemas) {
		t.Error("call.Contents() differs from the default catalog_contents")
	}
	nm := c.contents(attach, full.Etag)
	if !nm.NotModified || len(nm.Schemas) != 0 || *nm.Etag != *full.Etag || builds != 1 {
		t.Fatalf("revalidation: not_modified=%v schemas=%d builds=%d (want no build)", nm.NotModified, len(nm.Schemas), builds)
	}
	if again := c.contents(attach, ptr("gen-old")); again.NotModified || builds != 2 {
		t.Fatalf("stale etag: not_modified=%v builds=%d", again.NotModified, builds)
	}
}

// The worker enforces the protocol's rules on what a handler returns.
func TestCatalogContentsHandlerRules(t *testing.T) {
	sample := newContentsClient(t, contentsWorker())
	snapshot := sample.contents(sample.attach("example").AttachOpaqueData, nil).Schemas
	byPath := func(path ...string) generated.SchemaContents {
		for _, s := range snapshot {
			if slices.Equal(s.Path, path) {
				return s
			}
		}
		t.Fatalf("fixture has no schema %v", path)
		return generated.SchemaContents{}
	}
	nested := byPath("data", "nested")
	data := byPath("data")
	main := byPath("main")
	mislabeled := main
	mislabeled.Path = []string{"other"}

	for _, tc := range []struct {
		name        string
		ifNoneMatch *string
		result      vgi.CatalogContentsResult
		wantErr     string
	}{
		{"not_modified without etag", ptr("e"), vgi.CatalogContentsResult{NotModified: true}, "not_modified"},
		{"not_modified without if_none_match", nil, vgi.CatalogContentsResult{NotModified: true, Etag: ptr("e")}, "not_modified"},
		{"not_modified with another etag", ptr("e"), vgi.CatalogContentsResult{NotModified: true, Etag: ptr("f")}, "not_modified"},
		{"not_modified with schemas", ptr("e"), vgi.CatalogContentsResult{NotModified: true, Etag: ptr("e"),
			Schemas: []generated.SchemaContents{main}}, "with schemas"},
		{"duplicate path", nil, vgi.CatalogContentsResult{Schemas: []generated.SchemaContents{main, main}}, "duplicate"},
		{"missing parent", nil, vgi.CatalogContentsResult{Schemas: []generated.SchemaContents{main, nested}}, "without its parent"},
		{"path differs from SchemaInfo", nil, vgi.CatalogContentsResult{Schemas: []generated.SchemaContents{mislabeled}}, "differs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.result
			c := newContentsClient(t, contentsWorker(vgi.WithCatalogContentsHandler(
				func(*vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) { return result, nil })))
			err := c.contentsErr(c.attach("example").AttachOpaqueData, tc.ifNoneMatch)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got error %v, want one mentioning %q", err, tc.wantErr)
			}
		})
	}

	// A full answer whose etag equals if_none_match becomes not_modified;
	// children listed before parents are reordered parents first.
	c := newContentsClient(t, contentsWorker(vgi.WithCatalogContentsHandler(
		func(*vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
			return vgi.CatalogContentsResult{Schemas: []generated.SchemaContents{nested, main, data}, Etag: ptr("e")}, nil
		})))
	attach := c.attach("example").AttachOpaqueData
	if got := c.contents(attach, ptr("e")); !got.NotModified || len(got.Schemas) != 0 || *got.Etag != "e" {
		t.Fatalf("a full answer with etag == if_none_match: not_modified=%v schemas=%d", got.NotModified, len(got.Schemas))
	}
	got := c.contents(attach, nil)
	var order []string
	for _, s := range got.Schemas {
		order = append(order, strings.Join(s.Path, "."))
	}
	if !slices.Equal(order, []string{"main", "data", "data.nested"}) {
		t.Fatalf("schemas served in order %v, want parents first (stable)", order)
	}

	// A handler with no etag under content-hash mode gets the digest.
	h := newContentsClient(t, contentsWorker(vgi.WithCatalogContentsEtag(vgi.CatalogContentsEtagContentHash),
		vgi.WithCatalogContentsHandler(func(call *vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
			schemas, err := call.Contents()
			return vgi.CatalogContentsResult{Schemas: schemas}, err
		})))
	if got := h.contents(h.attach("example").AttachOpaqueData, nil); got.Etag == nil || *got.Etag != vgi.CatalogContentsDigest(got.Schemas) {
		t.Fatalf("content-hash mode over a handler with no etag: etag=%v", got.Etag)
	}
}
