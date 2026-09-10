// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// schema_path (protocol 2.0.0) is the one field on ScanFunctionResult and
// ScanBranch whose ABSENCE is meaningful: nil tells the client to fall back to
// its pre-1.5.0 table-schema/default-schema guess, which is the permanently
// correct answer for a branch that delegates to a native DuckDB function. So
// both states have to survive the round trip distinguishably — a serializer
// that wrote "" for nil would silently claim a schema named "".

// readScanSchemaPath reads back the trailing schema_path column of a one-row
// IPC record, returning nil when the cell is Arrow-null.
func readScanSchemaPath(t *testing.T, origin string, data []byte) *SchemaPath {
	t.Helper()

	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("%s: reading back the IPC stream failed: %v", origin, err)
	}
	defer reader.Release()

	idx := reader.Schema().FieldIndices("schema_path")
	if len(idx) != 1 {
		t.Fatalf("%s: expected exactly one schema_path column, got %d", origin, len(idx))
	}
	if !reader.Next() {
		t.Fatalf("%s: the IPC stream carried no record batch", origin)
	}
	col := reader.RecordBatch().Column(idx[0])
	if col.IsNull(0) {
		return nil
	}
	list, ok := col.(*array.List)
	if !ok {
		t.Fatalf("%s: schema_path decoded as %T, want *array.List", origin, col)
	}
	start, end := list.ValueOffsets(0)
	values := list.ListValues().(*array.String)
	path := make(SchemaPath, 0, end-start)
	for i := start; i < end; i++ {
		path = append(path, values.Value(int(i)))
	}
	return &path
}

func TestSerializeScanFunctionResultSchemaPath(t *testing.T) {
	b, err := SerializeScanFunctionResult(&ScanFunctionResult{
		FunctionName: "test_same_name_table_scan",
		SchemaPath:   pathPtr("analytics", "data"),
	})
	if err != nil {
		t.Fatalf("SerializeScanFunctionResult: %v", err)
	}
	got := readScanSchemaPath(t, "ScanFunctionResult", b)
	if got == nil || !slices.Equal(*got, SchemaPath{"analytics", "data"}) {
		t.Errorf("schema_path round-tripped as %v, want analytics.data", got)
	}
}

func TestSerializeScanFunctionResultSchemaPathAbsent(t *testing.T) {
	// A worker that only names a native DuckDB function has no VGI-side schema
	// to report — the null must stay null rather than becoming "".
	b, err := SerializeScanFunctionResult(&ScanFunctionResult{
		FunctionName:       "read_parquet",
		RequiredExtensions: []string{"parquet"},
	})
	if err != nil {
		t.Fatalf("SerializeScanFunctionResult: %v", err)
	}
	if got := readScanSchemaPath(t, "ScanFunctionResult", b); got != nil {
		t.Errorf("schema_path round-tripped as %q, want an Arrow null", *got)
	}
}

func TestSerializeScanBranchSchemaPath(t *testing.T) {
	b, err := SerializeScanBranch(&ScanBranch{
		FunctionName: "test_same_name_table_scan",
		SchemaPath:   pathPtr("analytics", "data"),
	})
	if err != nil {
		t.Fatalf("SerializeScanBranch: %v", err)
	}
	got := readScanSchemaPath(t, "ScanBranch", b)
	if got == nil || !slices.Equal(*got, SchemaPath{"analytics", "data"}) {
		t.Errorf("schema_path round-tripped as %v, want analytics.data", got)
	}
}

func TestSerializeScanBranchSchemaPathAbsent(t *testing.T) {
	// A format branch names no function at all, so it has no function schema —
	// and source_schema_path, which it may well carry, is a different field.
	src := SchemaPath{"catalog", "main"}
	b, err := SerializeScanBranch(&ScanBranch{
		FormatName:       strPtr("parquet"),
		FormatLocations:  []string{"s3://bucket/a.parquet"},
		SourceSchemaPath: &src,
	})
	if err != nil {
		t.Fatalf("SerializeScanBranch: %v", err)
	}
	if got := readScanSchemaPath(t, "ScanBranch", b); got != nil {
		t.Errorf("schema_path round-tripped as %q, want an Arrow null", *got)
	}
}

// TestResolveFunctionSchema covers the registry lookup that populates
// schema_path: which schema a table's backing function ACTUALLY lives in, which
// is not necessarily the schema the table is declared in.
func TestResolveFunctionSchema(t *testing.T) {
	w := NewWorker(WithCatalogName("example"))
	// One name homed in both schemas — the schema-disambiguation case.
	w.RegisterTable(AsTableFunction[struct{}](&namedTable{name: "test_resolve_probe"}))
	w.RegisterTableInSchema("data", AsTableFunction[struct{}](&namedTable{name: "test_resolve_probe"}))
	w.RegisterTableInSchemaPath(SchemaPath{"analytics", "data"}, AsTableFunction[struct{}](&namedTable{name: "nested_probe"}))
	// One name homed only in main, backing a table declared in data.
	w.RegisterTable(AsTableFunction[struct{}](&namedTable{name: "lonely_scan"}))

	cases := []struct {
		name        string
		function    string
		tableSchema SchemaPath
		want        *SchemaPath
	}{
		{"same name, main table", "test_resolve_probe", SchemaPath{"main"}, pathPtr("main")},
		{"same name, data table", "test_resolve_probe", SchemaPath{"data"}, pathPtr("data")},
		{"main-homed function backing a data table", "lonely_scan", SchemaPath{"data"}, pathPtr("main")},
		{"schema name case-insensitive", "test_resolve_probe", SchemaPath{"DATA"}, pathPtr("data")},
		{"arbitrarily nested schema", "nested_probe", SchemaPath{"analytics", "data"}, pathPtr("analytics", "data")},
		// Nothing registered: the worker only NAMES this function (read_parquet
		// and friends), so it has no schema of its own to report — ever.
		{"native delegation", "read_parquet", SchemaPath{"data"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := w.resolveFunctionSchema(kindTable, tc.function, tc.tableSchema)
			switch {
			case tc.want == nil && got != nil:
				t.Errorf("resolved %q, want no schema at all", *got)
			case tc.want != nil && got == nil:
				t.Errorf("resolved no schema, want %q", *tc.want)
			case tc.want != nil && got != nil && !slices.Equal(*got, *tc.want):
				t.Errorf("resolved %q, want %q", *got, *tc.want)
			}
		})
	}
}

// namedTable is a minimal table function whose Name() is supplied at
// construction, so two instances can deliberately collide on one name.
type namedTable struct{ name string }

func (f *namedTable) Name() string { return f.name }

func (f *namedTable) Metadata() FunctionMetadata { return FunctionMetadata{} }

func (f *namedTable) ArgumentSpecs() []ArgSpec { return nil }

func (f *namedTable) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{{Name: "tag", Type: arrow.BinaryTypes.String}}, nil))
}

func (f *namedTable) NewState(*ProcessParams) (*struct{}, error) { return &struct{}{}, nil }

func (f *namedTable) Process(context.Context, *ProcessParams, *struct{}, *vgirpc.OutputCollector) error {
	return nil
}

// sameNameProbeWorker builds a worker whose `main` and `data` schemas each
// declare test_same_name_table over their OWN test_same_name_table_scan — the
// shape table/same_name_schemas.test drives end to end.
func sameNameProbeWorker(t *testing.T) *Worker {
	t.Helper()
	w := NewWorker(WithCatalogName("example"))
	w.RegisterTable(AsTableFunction[struct{}](&namedTable{name: "test_same_name_table_scan"}))
	w.RegisterTableInSchema("data", AsTableFunction[struct{}](&namedTable{name: "test_same_name_table_scan"}))
	for _, schema := range []string{"main", "data"} {
		w.RegisterCatalogTable(schema, CatalogTable{
			Name:     "test_same_name_table",
			Function: AsTableFunction[struct{}](&namedTable{name: "test_same_name_table_scan"}),
		})
	}
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	return w
}

// The lazy RPC path: catalog_table_scan_function_get must name the schema whose
// implementation actually backs the table it was asked about.
func TestResolveScanFunctionCarriesSchemaPath(t *testing.T) {
	w := sameNameProbeWorker(t)
	for _, schema := range []string{"main", "data"} {
		result, err := w.resolveScanFunction(TableScanFunctionGetRequestWire{
			SchemaPath: SchemaPath{schema},
			Name:       "test_same_name_table",
		})
		if err != nil {
			t.Fatalf("resolveScanFunction(%s): %v", schema, err)
		}
		if result.SchemaPath == nil || !slices.Equal(*result.SchemaPath, SchemaPath{schema}) {
			t.Errorf("schema %s: resolved schema_path %v, want %q", schema, result.SchemaPath, schema)
		}
	}
}

// The inline path: the ScanFunctionResult embedded in TableInfo at attach time
// carries the same answer, since the extension uses it instead of the RPC.
func TestInlineScanFunctionCarriesSchemaPath(t *testing.T) {
	w := sameNameProbeWorker(t)
	for _, schema := range []string{"main", "data"} {
		ct := &w.catalogTables[schema][0]
		infoBytes, err := w.serializeCatalogTable(SchemaPath{schema}, ct)
		if err != nil {
			t.Fatalf("serializeCatalogTable(%s): %v", schema, err)
		}
		got := readScanSchemaPath(t, "TableInfo.scan_function", inlineScanFunctionBytes(t, infoBytes))
		if got == nil || !slices.Equal(*got, SchemaPath{schema}) {
			t.Errorf("schema %s: inlined schema_path %v, want %q", schema, got, schema)
		}
	}
}

// inlineScanFunctionBytes pulls the embedded ScanFunctionResult IPC blob out of
// a serialized TableInfo record.
func inlineScanFunctionBytes(t *testing.T, tableInfo []byte) []byte {
	t.Helper()

	reader, err := ipc.NewReader(bytes.NewReader(tableInfo))
	if err != nil {
		t.Fatalf("reading back the TableInfo stream failed: %v", err)
	}
	defer reader.Release()

	idx := reader.Schema().FieldIndices("scan_function")
	if len(idx) != 1 {
		t.Fatalf("expected exactly one scan_function column, got %d", len(idx))
	}
	if !reader.Next() {
		t.Fatal("the TableInfo stream carried no record batch")
	}
	col, ok := reader.RecordBatch().Column(idx[0]).(*array.Binary)
	if !ok {
		t.Fatalf("scan_function decoded as %T, want *array.Binary", reader.RecordBatch().Column(idx[0]))
	}
	if col.IsNull(0) {
		t.Fatal("scan_function is null — the table was not inlined at all")
	}
	return col.Value(0)
}
