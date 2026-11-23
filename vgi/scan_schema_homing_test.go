// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// taggedScan is a table function whose name is shared on purpose and whose
// tag tells the instances apart: OnBind names its one column after the tag,
// so a bind dispatched to the wrong instance returns the wrong column.
type taggedScan struct {
	name string
	tag  string
}

func (f *taggedScan) Name() string               { return f.name }
func (f *taggedScan) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (f *taggedScan) ArgumentSpecs() []ArgSpec   { return nil }

func (f *taggedScan) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{{Name: f.tag, Type: arrow.BinaryTypes.String}}, nil))
}

func (f *taggedScan) NewState(*ProcessParams) (*struct{}, error) { return &struct{}{}, nil }

func (f *taggedScan) Process(context.Context, *ProcessParams, *struct{}, *vgirpc.OutputCollector) error {
	return nil
}

func taggedScanFn(name, tag string) TableFunction {
	return AsTableFunction[struct{}](&taggedScan{name: name, tag: tag})
}

// scanColumnFor drives a table's scan the way the extension does: ask
// catalog_table_scan_branches_get (resolveScanFunction) which function and
// schema back the table, then bind that function naming that schema. Returns
// the bound output's single column name -- the backing instance's tag.
func scanColumnFor(t *testing.T, w *Worker, schema SchemaPath, table string) string {
	t.Helper()
	result, err := w.resolveScanFunction(scanFunctionLookup{SchemaPath: schema, Name: table})
	if err != nil {
		t.Fatalf("resolveScanFunction(%v.%s): %v", schema, table, err)
	}
	if result.SchemaPath == nil || !slices.Equal(*result.SchemaPath, schema) {
		t.Fatalf("%v.%s: scan homed in %v, want the table's own schema", schema, table, result.SchemaPath)
	}
	resp, err := w.handleBind(context.Background(), shapeTestCallCtx(), BindRequestWire{
		Arguments:    testNoArgs(t),
		FunctionName: result.FunctionName,
		FunctionType: string(FunctionTypeTable),
		SchemaPath:   result.SchemaPath,
	})
	if err != nil {
		t.Fatalf("bind %s in %v: %v", result.FunctionName, *result.SchemaPath, err)
	}
	out, err := DeserializeSchema(resp.OutputSchema)
	if err != nil {
		t.Fatalf("decoding bind output schema: %v", err)
	}
	if out.NumFields() != 1 {
		t.Fatalf("bind output has %d columns, want 1", out.NumFields())
	}
	return out.Field(0).Name
}

// Two tables in different schemas whose scan functions share a name but are
// different functions: each table scans with its own function. The flat
// name-keyed registry used to keep only the first and silently scan the second
// table with it.
func TestSameNamedScansInDifferentSchemasDispatchPerSchema(t *testing.T) {
	w := NewWorker(WithCatalogName("homing"))
	w.RegisterCatalogTable("alpha", CatalogTable{Name: "t", Function: taggedScanFn("shared_scan", "alpha_rows")})
	w.RegisterCatalogTable("beta", CatalogTable{Name: "t", Function: taggedScanFn("shared_scan", "beta_rows")})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)

	if got := scanColumnFor(t, w, SchemaPath{"alpha"}, "t"); got != "alpha_rows" {
		t.Errorf("alpha.t scanned with the %q function, want alpha_rows", got)
	}
	if got := scanColumnFor(t, w, SchemaPath{"beta"}, "t"); got != "beta_rows" {
		t.Errorf("beta.t scanned with the %q function, want beta_rows", got)
	}
	// Each is homed (and so listed) in its table's schema and nowhere else.
	for _, schema := range []string{"alpha", "beta"} {
		if !slices.Contains(listedFunctionNames(t, w, schema), "shared_scan") {
			t.Errorf("shared_scan not listed in schema %s", schema)
		}
	}
	if slices.Contains(listedFunctionNames(t, w, "main"), "shared_scan") {
		t.Error("shared_scan is listed in main; a table's scan belongs in the table's schema")
	}
}

// A table's scan is homed in the table's own schema, not the catalog default.
func TestTableScanHomedInTablesSchema(t *testing.T) {
	w := NewWorker(WithCatalogName("homing"))
	w.RegisterCatalogTablePath(SchemaPath{"Side"}, CatalogTable{Name: "t", Function: taggedScanFn("range", "side_rows")})
	origins := w.funcOrigins[funcKey{kind: kindTable, name: "range"}]
	if len(origins) != 1 || origins[0].schema != "side" || !origins[0].tableOnly {
		t.Fatalf("range homed at %+v, want one table_only registration in schema side", origins)
	}
}

// The same function backing several tables -- in one schema or in several --
// keeps working: one registration per schema, every table scans with it.
func TestSameScanFunctionBacksSeveralTables(t *testing.T) {
	w := NewWorker(WithCatalogName("homing"))
	shared := taggedScanFn("shared_scan", "shared_rows")
	w.RegisterCatalogTable("main", CatalogTable{Name: "a", Function: shared})
	w.RegisterCatalogTable("main", CatalogTable{Name: "b", Function: shared})
	w.RegisterCatalogTable("other", CatalogTable{Name: "c", Function: shared})
	// A freshly constructed, identical instance is the same function too.
	w.RegisterCatalogTable("main", CatalogTable{Name: "d", Function: taggedScanFn("shared_scan", "shared_rows")})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)

	if n := len(w.tables["shared_scan"]); n != 2 {
		t.Fatalf("%d registrations of shared_scan, want 2 (one per schema)", n)
	}
	for _, tc := range []struct {
		schema string
		table  string
	}{{"main", "a"}, {"main", "b"}, {"other", "c"}, {"main", "d"}} {
		if got := scanColumnFor(t, w, SchemaPath{tc.schema}, tc.table); got != "shared_rows" {
			t.Errorf("%s.%s scanned with %q, want shared_rows", tc.schema, tc.table, got)
		}
	}
}

// Within one schema, a different function reusing a scan name is a developer
// error: it panics at registration, whichever registers first -- another table
// or the author.
func TestDifferentScanWithDuplicateNameInOneSchemaPanics(t *testing.T) {
	cases := []struct {
		name  string
		first func(w *Worker)
	}{
		{"after another table", func(w *Worker) {
			w.RegisterCatalogTable("data", CatalogTable{Name: "a", Function: taggedScanFn("dup_scan", "a_rows")})
		}},
		{"after the author's registration", func(w *Worker) {
			w.RegisterTableInSchema("data", taggedScanFn("dup_scan", "a_rows"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWorker(WithCatalogName("homing"))
			tc.first(w)
			msg := mustPanic(t, func() {
				w.RegisterCatalogTable("data", CatalogTable{Name: "b", Function: taggedScanFn("dup_scan", "b_rows")})
			})
			for _, want := range []string{"dup_scan", `"data"`, "different function"} {
				if !strings.Contains(msg, want) {
					t.Errorf("panic %q does not mention %s", msg, want)
				}
			}
		})
	}

	// The author registering a different same-named function into a schema
	// where a table already registered its scan panics the same way.
	w := NewWorker(WithCatalogName("homing"))
	w.RegisterCatalogTable("data", CatalogTable{Name: "a", Function: taggedScanFn("dup_scan", "a_rows")})
	msg := mustPanic(t, func() { w.RegisterTableInSchema("data", taggedScanFn("dup_scan", "other_rows")) })
	if !strings.Contains(msg, "dup_scan") || !strings.Contains(msg, "different function") {
		t.Errorf("panic %q does not explain the duplicate", msg)
	}
}

func mustPanic(t *testing.T, f func()) (msg string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic, got none")
		}
		if err, ok := r.(error); ok {
			msg = err.Error()
		} else {
			msg, _ = r.(string)
		}
	}()
	f()
	return ""
}

// sameFunction looks through adapters and compares by identity or deep value.
func TestSameFunction(t *testing.T) {
	a := &taggedScan{name: "n", tag: "a"}
	cases := []struct {
		name string
		x, y any
		want bool
	}{
		{"one pointer", a, a, true},
		{"one pointer, wrapped twice", AsTableFunction[struct{}](a), AsTableFunction[struct{}](a), true},
		{"equal fresh instances", &taggedScan{name: "n", tag: "a"}, &taggedScan{name: "n", tag: "a"}, true},
		{"same name, different tag", &taggedScan{name: "n", tag: "a"}, &taggedScan{name: "n", tag: "b"}, false},
		{"same name, different type", &taggedScan{name: "shape_test_table"}, shapeTestTableFunction{}, false},
	}
	for _, tc := range cases {
		if got := sameFunction(tc.x, tc.y); got != tc.want {
			t.Errorf("%s: sameFunction = %t, want %t", tc.name, got, tc.want)
		}
	}
}

// listedFunctionNames lists schema of w's catalog the way the extension does.
func listedFunctionNames(t *testing.T, w *Worker, schema string) []string {
	t.Helper()
	items, ok, err := w.catalog.functionListing(schema, "", w.catalogName)
	if err != nil || !ok {
		t.Fatalf("functionListing(%s): ok=%t err=%v", schema, ok, err)
	}
	var names []string
	for _, item := range items {
		name, _ := decodeVisibility(t, item)
		names = append(names, name)
	}
	return names
}
