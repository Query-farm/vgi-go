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

// taggedWrite is a write (table in-out) function whose name is shared on
// purpose and whose tag tells the instances apart: OnBind names its one
// output column after the tag, so a bind dispatched to the wrong instance
// returns the wrong column.
type taggedWrite struct {
	name string
	tag  string
}

func (f *taggedWrite) Name() string               { return f.name }
func (f *taggedWrite) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (f *taggedWrite) ArgumentSpecs() []ArgSpec   { return nil }

func (f *taggedWrite) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{{Name: f.tag, Type: arrow.PrimitiveTypes.Int64}}, nil))
}

func (f *taggedWrite) NewState(*ProcessParams) (*struct{}, error) { return &struct{}{}, nil }

func (f *taggedWrite) Process(context.Context, *ProcessParams, *struct{}, arrow.RecordBatch, *vgirpc.OutputCollector) error {
	return nil
}

func (f *taggedWrite) Finalize(context.Context, *ProcessParams, *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func taggedWriteFn(name, tag string) TableInOutFunction {
	return AsTableInOutFunction[struct{}](&taggedWrite{name: name, tag: tag})
}

// insertColumnFor drives a table's INSERT the way the extension does: ask
// catalog_table_insert_function_get (declarativeWriteFunctionGet) which
// function and schema back the write, then bind that function naming that
// schema. Returns the bound output's single column name -- the backing
// instance's tag.
func insertColumnFor(t *testing.T, w *Worker, schema SchemaPath, table string) string {
	t.Helper()
	ct := w.findCatalogTable(schema, table)
	if ct == nil {
		t.Fatalf("no table %v.%s", schema, table)
	}
	result := w.writeFunctionResult(schema, ct, WriteOpInsert)
	if result == nil {
		t.Fatalf("%v.%s has no insert function", schema, table)
	}
	if result.SchemaPath == nil || !slices.Equal(*result.SchemaPath, schema) {
		t.Fatalf("%v.%s: insert function homed in %v, want the table's own schema", schema, table, result.SchemaPath)
	}
	input := shapeTestInputSchema(t)
	resp, err := w.handleBind(context.Background(), shapeTestCallCtx(), BindRequestWire{
		FunctionName: result.FunctionName,
		FunctionType: string(FunctionTypeTable),
		SchemaPath:   result.SchemaPath,
		InputSchema:  &input,
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

// Two tables in different schemas whose write functions share a name but are
// different functions: each table writes with its own. The name-keyed dedupe
// used to keep only the first and silently write the second table with it.
func TestSameNamedWritersInDifferentSchemasDispatchPerSchema(t *testing.T) {
	w := NewWorker(WithCatalogName("homing"))
	w.RegisterCatalogTable("alpha", CatalogTable{Name: "t", Columns: visColumns, InsertFunction: taggedWriteFn("shared_insert", "alpha_rows")})
	w.RegisterCatalogTable("beta", CatalogTable{Name: "t", Columns: visColumns, InsertFunction: taggedWriteFn("shared_insert", "beta_rows")})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)

	if got := insertColumnFor(t, w, SchemaPath{"alpha"}, "t"); got != "alpha_rows" {
		t.Errorf("alpha.t inserted with the %q function, want alpha_rows", got)
	}
	if got := insertColumnFor(t, w, SchemaPath{"beta"}, "t"); got != "beta_rows" {
		t.Errorf("beta.t inserted with the %q function, want beta_rows", got)
	}
	for _, schema := range []string{"alpha", "beta"} {
		if !slices.Contains(listedFunctionNames(t, w, schema), "shared_insert") {
			t.Errorf("shared_insert not listed in schema %s", schema)
		}
	}
}

// One write function backing several tables -- and several operations of one
// table -- in one schema or several: one table_only registration per schema.
func TestSameWriteFunctionBacksSeveralTables(t *testing.T) {
	w := NewWorker(WithCatalogName("homing"))
	shared := taggedWriteFn("shared_write", "shared_rows")
	w.RegisterCatalogTable("main", CatalogTable{Name: "a", Columns: visColumns, InsertFunction: shared})
	w.RegisterCatalogTable("main", CatalogTable{Name: "b", Columns: visColumns, InsertFunction: shared})
	w.RegisterCatalogTable("other", CatalogTable{Name: "c", Columns: visColumns, InsertFunction: shared})
	// A freshly constructed, identical instance is the same function too.
	w.RegisterCatalogTable("main", CatalogTable{Name: "d", Columns: visColumns, InsertFunction: taggedWriteFn("shared_write", "shared_rows")})
	// And the author registering it as a table in-out function adds nothing.
	w.RegisterTableInOut(shared)
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)

	if n := len(w.tableInOuts["shared_write"]); n != 2 {
		t.Fatalf("%d registrations of shared_write, want 2 (one per schema)", n)
	}
	for _, tc := range []struct {
		schema string
		table  string
	}{{"main", "a"}, {"main", "b"}, {"other", "c"}, {"main", "d"}} {
		if got := insertColumnFor(t, w, SchemaPath{tc.schema}, tc.table); got != "shared_rows" {
			t.Errorf("%s.%s inserted with %q, want shared_rows", tc.schema, tc.table, got)
		}
	}
	expectVisibility(t, listedVisibility(t, w), map[string]string{"shared_write": "table_only"})
}

// A write function name is table_only only in the schema a table registered
// it in: an author's unrelated table in-out function of that name in another
// schema stays public.
func TestWriteFunctionTableOnlyIsPerSchema(t *testing.T) {
	w := NewWorker(WithCatalogName("homing"))
	w.RegisterCatalogTable("data", CatalogTable{Name: "t", Columns: visColumns, InsertFunction: taggedWriteFn("w_ins", "data_rows")})
	w.RegisterTableInOut(taggedWriteFn("w_ins", "main_rows"))
	expectVisibility(t, listedVisibility(t, w), map[string]string{"w_ins": ""})
}

// Within one schema, a different function reusing a write function's name is
// a developer error: it panics at registration in every order -- after
// another table's write function, or before or after the author's own table
// in-out function of that name.
func TestDifferentWriterWithDuplicateNameInOneSchemaPanics(t *testing.T) {
	cases := []struct {
		name   string
		first  func(w *Worker)
		second func(w *Worker)
	}{
		{"table then table", func(w *Worker) {
			w.RegisterCatalogTable("data", CatalogTable{Name: "a", Columns: visColumns, InsertFunction: taggedWriteFn("dup_write", "a_rows")})
		}, func(w *Worker) {
			w.RegisterCatalogTable("data", CatalogTable{Name: "b", Columns: visColumns, InsertFunction: taggedWriteFn("dup_write", "b_rows")})
		}},
		{"author then table", func(w *Worker) {
			w.RegisterTableInOutInSchema("data", taggedWriteFn("dup_write", "author_rows"))
		}, func(w *Worker) {
			w.RegisterCatalogTable("data", CatalogTable{Name: "a", Columns: visColumns, InsertFunction: taggedWriteFn("dup_write", "a_rows")})
		}},
		{"table then author", func(w *Worker) {
			w.RegisterCatalogTable("data", CatalogTable{Name: "a", Columns: visColumns, InsertFunction: taggedWriteFn("dup_write", "a_rows")})
		}, func(w *Worker) {
			w.RegisterTableInOutInSchema("data", taggedWriteFn("dup_write", "author_rows"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWorker(WithCatalogName("homing"))
			tc.first(w)
			msg := mustPanic(t, func() { tc.second(w) })
			for _, want := range []string{"dup_write", `"data"`, "different function"} {
				if !strings.Contains(msg, want) {
					t.Errorf("panic %q does not mention %s", msg, want)
				}
			}
		})
	}
}
