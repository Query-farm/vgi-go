// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// scopedScalar is a minimal scalar whose Name() is supplied at construction, so
// two instances can deliberately collide on one name.
type scopedScalar struct {
	name string
	tag  string
}

func (f *scopedScalar) Name() string { return f.name }

func (f *scopedScalar) Metadata() FunctionMetadata {
	return FunctionMetadata{Description: f.tag, ReturnType: arrow.BinaryTypes.String}
}

func (f *scopedScalar) ArgumentSpecs() []ArgSpec {
	return []ArgSpec{{Name: "value", Position: 0, ArrowType: "int64"}}
}

func (f *scopedScalar) OnBind(*BindParams) (*BindResponse, error) {
	return BindResult(arrow.BinaryTypes.String)
}

func (f *scopedScalar) Process(context.Context, *ProcessParams, arrow.RecordBatch) (arrow.RecordBatch, error) {
	return nil, nil
}

// resolvedTag resolves lk and reports the matched implementation's tag.
func resolvedTag(t *testing.T, w *Worker, lk functionLookup) string {
	t.Helper()
	fn, err := w.resolveFunction(lk)
	if err != nil {
		t.Fatalf("resolveFunction(%+v): %v", lk, err)
	}
	impl, ok := fn.(*scopedScalar)
	if !ok {
		t.Fatalf("resolveFunction(%+v): got %T, want *scopedScalar", lk, fn)
	}
	return impl.tag
}

// One name registered in two schemas must dispatch on the schema the caller
// names — the collision the flat name->impls registry could not break.
func TestResolveFunctionBySchema(t *testing.T) {
	w := NewWorker()
	w.RegisterScalar(&scopedScalar{name: "collide", tag: "main"})
	w.RegisterScalarInSchema("Data", &scopedScalar{name: "collide", tag: "data"})

	base := functionLookup{Name: "collide", Type: FunctionTypeScalar, Args: &Arguments{}}

	if got := resolvedTag(t, w, withSchema(base, "main")); got != "main" {
		t.Errorf("schema main: got %q, want %q", got, "main")
	}
	// Schema matching is case-insensitive in both directions: registration
	// declared "Data", the caller names "DATA".
	if got := resolvedTag(t, w, withSchema(base, "DATA")); got != "data" {
		t.Errorf("schema DATA: got %q, want %q", got, "data")
	}
}

// Naming a schema that does not hold the function reports where it does live,
// rather than the generic unknown-function list.
func TestResolveFunctionWrongSchema(t *testing.T) {
	w := NewWorker()
	w.RegisterScalar(&scopedScalar{name: "collide", tag: "main"})
	w.RegisterScalarInSchema("data", &scopedScalar{name: "collide", tag: "data"})

	_, err := w.resolveFunction(functionLookup{
		Name: "collide", Type: FunctionTypeScalar, Schema: "other", Args: &Arguments{},
	})
	if err == nil {
		t.Fatal("resolveFunction: want an error for a schema that holds no such function")
	}
	for _, want := range []string{"not registered in schema 'other'", "data", "main"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// A caller that names no schema (a COPY handler bind, or a pre-1.1.0 client)
// must not get an arbitrary winner when the name spans schemas.
func TestResolveFunctionCrossSchemaAmbiguity(t *testing.T) {
	w := NewWorker()
	w.RegisterScalar(&scopedScalar{name: "collide", tag: "main"})
	w.RegisterScalarInSchema("data", &scopedScalar{name: "collide", tag: "data"})

	_, err := w.resolveFunction(functionLookup{
		Name: "collide", Type: FunctionTypeScalar, Args: &Arguments{},
	})
	if err == nil {
		t.Fatal("resolveFunction: want a cross-schema ambiguity error")
	}
	if !strings.Contains(err.Error(), "more than one schema") {
		t.Errorf("error %q does not name the ambiguity", err)
	}
}

// Two catalogs served by one worker may each declare the same name in the same
// schema; only the attachment tells them apart.
func TestResolveFunctionByCatalog(t *testing.T) {
	w := NewWorker()
	w.RegisterScalarForCatalog("twin_a", &scopedScalar{name: "collide", tag: "a"})
	w.RegisterScalarForCatalog("twin_b", &scopedScalar{name: "collide", tag: "b"})

	base := functionLookup{Name: "collide", Type: FunctionTypeScalar, Schema: "main", Args: &Arguments{}}

	for _, tc := range []struct{ catalog, want string }{{"twin_a", "a"}, {"twin_b", "b"}} {
		lk := base
		lk.Catalog = tc.catalog
		if got := resolvedTag(t, w, lk); got != tc.want {
			t.Errorf("catalog %s: got %q, want %q", tc.catalog, got, tc.want)
		}
	}

	// A catalog that declares neither must not reach one at random.
	lk := base
	lk.Catalog = "example"
	if _, err := w.resolveFunction(lk); err == nil {
		t.Error("resolveFunction: want an error for a catalog declaring neither implementation")
	}
}

// A registration that names no catalog is homed in the worker's own catalog —
// not made reachable from every catalog.
func TestResolveFunctionDefaultHomeIsOwnCatalog(t *testing.T) {
	w := NewWorker(WithCatalogName("own"))
	w.RegisterScalar(&scopedScalar{name: "plain", tag: "plain"})

	base := functionLookup{Name: "plain", Type: FunctionTypeScalar, Schema: "main", Args: &Arguments{}}

	lk := base
	lk.Catalog = "own"
	if got := resolvedTag(t, w, lk); got != "plain" {
		t.Errorf("own catalog: got %q, want %q", got, "plain")
	}

	lk = base
	lk.Catalog = "elsewhere"
	if _, err := w.resolveFunction(lk); err == nil {
		t.Error("resolveFunction: a function homed in 'own' must not be reachable from 'elsewhere'")
	}
}

// An unlisted function keeps its home; it is hidden from listings only.
func TestUnlistedFunctionKeepsItsHome(t *testing.T) {
	w := NewWorker(WithCatalogName("own"))
	w.RegisterTableUnlisted(AsTableFunction[struct{}](&unlistedTable{}))

	origin := w.originOf(kindTable, "hidden_scan", 0)
	if origin.catalog != "own" || origin.schema != "main" || !origin.unlisted {
		t.Errorf("origin = %+v, want {catalog: own, schema: main, unlisted: true}", origin)
	}
}

// unlistedTable is a table function registered only to back a catalog table.
type unlistedTable struct{}

func (unlistedTable) Name() string { return "hidden_scan" }

func (unlistedTable) Metadata() FunctionMetadata { return FunctionMetadata{} }

func (unlistedTable) ArgumentSpecs() []ArgSpec { return nil }

func (unlistedTable) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil))
}

func (unlistedTable) NewState(*ProcessParams) (*struct{}, error) { return &struct{}{}, nil }

func (unlistedTable) Process(context.Context, *ProcessParams, *struct{}, *vgirpc.OutputCollector) error {
	return nil
}

// withSchema returns a copy of lk naming schema.
func withSchema(lk functionLookup, schema string) functionLookup {
	lk.Schema = schema
	return lk
}

// scopedAgg is a minimal aggregate whose Name() is supplied at construction, so
// two instances can deliberately collide on one name across schemas/catalogs.
type scopedAgg struct {
	name string
	tag  string
}

func (f *scopedAgg) Name() string               { return f.name }
func (f *scopedAgg) Metadata() FunctionMetadata { return FunctionMetadata{Description: f.tag} }
func (f *scopedAgg) ArgumentSpecs() []ArgSpec {
	return []ArgSpec{{Name: "value", Position: 0, ArrowType: "int64"}}
}
func (f *scopedAgg) OnBind(*AggregateBindParams) (*BindResponse, error) { return nil, nil }
func (f *scopedAgg) NewState(*AggregateProcessParams) interface{}       { return nil }
func (f *scopedAgg) Update(map[int64]interface{}, *Int64Slice, []arrow.Array, *AggregateProcessParams) error {
	return nil
}
func (f *scopedAgg) Combine(_, _ interface{}, _ *AggregateProcessParams) (interface{}, error) {
	return nil, nil
}
func (f *scopedAgg) Finalize([]int64, map[int64]interface{}, *AggregateProcessParams) (arrow.RecordBatch, error) {
	return nil, nil
}

// aggTag resolves an aggregate by (name, schema, catalog) and reports its tag.
func aggTag(t *testing.T, w *Worker, name, schema, catalog string) string {
	t.Helper()
	fn, err := w.resolveAggregate(name, schema, catalog)
	if err != nil {
		t.Fatalf("resolveAggregate(%q, %q, %q): %v", name, schema, catalog, err)
	}
	return fn.(*scopedAgg).tag
}

// An aggregate name declared in two schemas of one catalog must resolve on the
// schema the caller named — the runtime half of the 1.2.0 gap, since every
// aggregate RPC re-resolves by name.
func TestResolveAggregateBySchema(t *testing.T) {
	w := NewWorker(WithCatalogName("example"))
	w.RegisterAggregate(&scopedAgg{name: "agg", tag: "main"})
	w.RegisterAggregateInSchema("data", &scopedAgg{name: "agg", tag: "data"})

	if got := aggTag(t, w, "agg", "main", "example"); got != "main" {
		t.Errorf("schema main: got %q, want main", got)
	}
	if got := aggTag(t, w, "agg", "data", "example"); got != "data" {
		t.Errorf("schema data: got %q, want data", got)
	}
}

// A caller that names no schema must not get an arbitrary winner when the
// aggregate name spans schemas.
func TestResolveAggregateCrossSchemaAmbiguity(t *testing.T) {
	w := NewWorker(WithCatalogName("example"))
	w.RegisterAggregate(&scopedAgg{name: "agg", tag: "main"})
	w.RegisterAggregateInSchema("data", &scopedAgg{name: "agg", tag: "data"})

	if _, err := w.resolveAggregate("agg", "", "example"); err == nil {
		t.Fatal("resolveAggregate: want a cross-schema ambiguity error")
	}
}

// A schema-less aggregate call still resolves when the name is unique.
func TestResolveAggregateSchemalessUnique(t *testing.T) {
	w := NewWorker(WithCatalogName("example"))
	w.RegisterAggregate(&scopedAgg{name: "agg", tag: "only"})

	if got := aggTag(t, w, "agg", "", "example"); got != "only" {
		t.Errorf("schemaless unique: got %q, want only", got)
	}
}
