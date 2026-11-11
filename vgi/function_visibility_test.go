// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Protocol 3.3: every function a catalog table names is listed, with
// FunctionInfo.visibility telling the extension whether it is also callable.
// Write functions are always table_only; a table's scan is table_only unless
// the table sets ExposeScan or the author registers it as an ordinary
// function; everything else is public (null on the wire).

// visScan is a table function named by the test's tables.
type visScan struct{ name string }

func (s visScan) Name() string { return s.name }
func (visScan) Metadata() FunctionMetadata {
	return FunctionMetadata{ProjectionPushdown: true}
}
func (visScan) ArgumentSpecs() []ArgSpec { return nil }
func (visScan) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (visScan) NewState(*ProcessParams) (*struct{}, error) { return &struct{}{}, nil }
func (visScan) Process(context.Context, *ProcessParams, *struct{}, *vgirpc.OutputCollector) error {
	return nil
}

// visWrite is a table in-out function used as a write function.
type visWrite struct{ name string }

func (s visWrite) Name() string { return s.name }
func (visWrite) Metadata() FunctionMetadata {
	return FunctionMetadata{HasFinalize: true}
}
func (visWrite) ArgumentSpecs() []ArgSpec                  { return nil }
func (visWrite) OnBind(*BindParams) (*BindResponse, error) { return nil, nil }
func (visWrite) NewState(*ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}
func (visWrite) Process(context.Context, *ProcessParams, *struct{}, arrow.RecordBatch, *vgirpc.OutputCollector) error {
	return nil
}
func (visWrite) Finalize(context.Context, *ProcessParams, *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func scanFn(name string) TableFunction { return AsTableFunction[struct{}](visScan{name}) }
func writeFn(name string) TableInOutFunction {
	return AsTableInOutFunction[struct{}](visWrite{name})
}

var visColumns = arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)

// listedVisibility lists schema main of w's catalog the way the extension
// does and returns each listed function's wire visibility ("" for null).
func listedVisibility(t *testing.T, w *Worker) map[string]string {
	t.Helper()
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	items, ok, err := w.catalog.functionListing("main", "", w.catalogName)
	if err != nil || !ok {
		t.Fatalf("listing main: ok=%t err=%v", ok, err)
	}
	out := map[string]string{}
	for _, item := range items {
		name, vis := decodeVisibility(t, item)
		if _, dup := out[name]; dup {
			t.Fatalf("%s is listed twice", name)
		}
		out[name] = vis
	}
	return out
}

func decodeVisibility(t *testing.T, item []byte) (name, visibility string) {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(item), ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("listed item carries no batch")
	}
	batch := reader.RecordBatch()
	idx := batch.Schema().FieldIndices("visibility")
	if len(idx) != 1 {
		t.Fatalf("FunctionInfo has no visibility column: %v", batch.Schema())
	}
	field := batch.Schema().Field(idx[0])
	if field.Type.ID() != arrow.STRING || !field.Nullable {
		t.Fatalf("visibility must be a nullable utf8 column, got %v nullable=%t", field.Type, field.Nullable)
	}
	name = batch.Column(batch.Schema().FieldIndices("name")[0]).(*array.String).Value(0)
	col := batch.Column(idx[0]).(*array.String)
	if col.IsNull(0) {
		return name, ""
	}
	return name, col.Value(0)
}

func expectVisibility(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for name, vis := range want {
		g, ok := got[name]
		if !ok {
			t.Errorf("%s is not listed; every function a table names must be", name)
			continue
		}
		if g != vis {
			t.Errorf("%s visibility = %q, want %q", name, g, vis)
		}
	}
}

func TestTableScanIsTableOnlyByDefault(t *testing.T) {
	w := NewWorker(WithCatalogName("vis"))
	w.RegisterCatalogTable("main", CatalogTable{
		Name: "items", Columns: visColumns,
		Function:       scanFn("items_scan"),
		InsertFunction: writeFn("items_insert"),
		UpdateFunction: writeFn("items_update"),
		DeleteFunction: writeFn("items_delete"),
	})
	w.RegisterCatalogTable("main", CatalogTable{
		Name: "labels", Columns: visColumns,
		Function:   scanFn("labels_scan"),
		ExposeScan: true,
	})
	w.RegisterTable(scanFn("plain_fn"))
	expectVisibility(t, listedVisibility(t, w), map[string]string{
		"items_scan":   "table_only",
		"items_insert": "table_only",
		"items_update": "table_only",
		"items_delete": "table_only",
		"labels_scan":  "",
		"plain_fn":     "",
	})
}

// An author who also registers a table's scan as an ordinary function gets a
// public function -- whichever order the two registrations come in -- and
// exactly one listing entry for it.
func TestExplicitRegistrationMakesScanPublic(t *testing.T) {
	for _, before := range []bool{true, false} {
		w := NewWorker(WithCatalogName("vis"))
		if before {
			w.RegisterTable(scanFn("s"))
		}
		w.RegisterCatalogTable("main", CatalogTable{Name: "t", Columns: visColumns, Function: scanFn("s")})
		if !before {
			w.RegisterTable(scanFn("s"))
		}
		if n := len(w.tables["s"]); n != 1 {
			t.Fatalf("before=%t: %d registrations of s, want 1", before, n)
		}
		expectVisibility(t, listedVisibility(t, w), map[string]string{"s": ""})
	}
}

// Two tables sharing one scan: if either exposes it, it is public.
func TestSharedScanExposedByEitherTableIsPublic(t *testing.T) {
	w := NewWorker(WithCatalogName("vis"))
	w.RegisterCatalogTable("main", CatalogTable{Name: "a", Columns: visColumns, Function: scanFn("shared")})
	w.RegisterCatalogTable("main", CatalogTable{Name: "b", Columns: visColumns, Function: scanFn("shared"), ExposeScan: true})
	expectVisibility(t, listedVisibility(t, w), map[string]string{"shared": ""})
}

// Write functions are table_only even when the author registered them as
// ordinary table in-out functions too.
func TestWriteFunctionsAreAlwaysTableOnly(t *testing.T) {
	w := NewWorker(WithCatalogName("vis"))
	w.RegisterTableInOut(writeFn("w_ins"))
	w.RegisterCatalogTable("main", CatalogTable{Name: "t", Columns: visColumns, InsertFunction: writeFn("w_ins")})
	expectVisibility(t, listedVisibility(t, w), map[string]string{"w_ins": "table_only"})
}

// RegisterTableUnlisted now means "send as table_only": the function is
// listed (so the extension has its capabilities), not omitted.
func TestRegisterTableUnlistedIsListedTableOnly(t *testing.T) {
	w := NewWorker(WithCatalogName("vis"))
	w.RegisterTableUnlisted(scanFn("hidden"))
	expectVisibility(t, listedVisibility(t, w), map[string]string{"hidden": "table_only"})
}

// A table-only function is never a global function alias.
func TestTableOnlyFunctionIsNotGlobal(t *testing.T) {
	w := NewWorker(WithCatalogName("vis"), WithGlobalFunctions("hidden", "plain_fn"))
	w.RegisterTableUnlisted(scanFn("hidden"))
	w.RegisterTable(scanFn("plain_fn"))
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	items, err := w.serializedGlobalFunctions()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("%d global functions, want 1 (plain_fn)", len(items))
	}
	if name, _ := decodeVisibility(t, items[0]); name != "plain_fn" {
		t.Fatalf("global function %q, want plain_fn", name)
	}
}

// catalog_contents carries table-only functions too.
func TestCatalogContentsListsTableOnlyFunctions(t *testing.T) {
	w := NewWorker(WithCatalogName("vis"))
	w.RegisterCatalogTable("main", CatalogTable{Name: "t", Columns: visColumns, Function: scanFn("t_scan")})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	info := *w.catalog.schemas["main"].info
	contents, err := w.schemaContentsOf([]byte("vis"), &info)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range contents.TableFunctions {
		if name, vis := decodeVisibility(t, item); name == "t_scan" {
			found = true
			if vis != "table_only" {
				t.Errorf("t_scan visibility = %q in catalog_contents, want table_only", vis)
			}
		}
	}
	if !found {
		t.Error("t_scan is missing from catalog_contents")
	}
}

func TestUnknownVisibilityIsRefused(t *testing.T) {
	fi := FunctionInfo{Name: "f", FunctionType: FunctionTypeTable, Visibility: "hidden",
		ArgSchema: arrow.NewSchema(nil, nil), OutputSchema: arrow.NewSchema(nil, nil)}
	if _, err := SerializeFunctionInfo(&fi); err == nil || !strings.Contains(err.Error(), "visibility") {
		t.Fatalf("SerializeFunctionInfo with visibility %q: err = %v, want an unknown-visibility error", fi.Visibility, err)
	}
	fi.Visibility = FunctionVisibilityPublic
	data, err := SerializeFunctionInfo(&fi)
	if err != nil {
		t.Fatal(err)
	}
	if _, vis := decodeVisibility(t, data); vis != "public" {
		t.Fatalf("explicit public visibility encoded as %q", vis)
	}
}

// The internal writable catalog's generic scan/write functions back its
// tables only.
func TestWritableCatalogFunctionsAreTableOnly(t *testing.T) {
	w := NewWorker(WithCatalogName("vis"))
	w.registerWritableFunctions("vis")
	got := listedVisibility(t, w)
	for _, name := range []string{writableScanFunctionName, writableInsertFunctionName, writableUpdateFunctionName, writableDeleteFunctionName} {
		if got[name] != "table_only" {
			t.Errorf("%s visibility = %q, want table_only", name, got[name])
		}
	}
}
