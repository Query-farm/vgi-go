// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package twin_catalogs is the two-catalogs-one-worker collision fixture.
//
// `twin_a` and `twin_b` are separate VGI catalogs served by the same worker
// process. Each declares a schema literally named `main` holding a scalar
// function literally named `test_same_name_catalog` — so neither the function
// name nor the schema name distinguishes them. Only the catalog does.
//
// Attaching both from the same worker LOCATION and calling
// `a.main.test_same_name_catalog(1)` vs `b.main.test_same_name_catalog(1)` must
// reach different implementations. The routing key is the per-attach
// attach_opaque_data, which names the catalog: registration scopes each
// implementation to its own catalog, and both the function listing and bind
// dispatch honour that scope, so a call lands on the catalog the caller
// attached rather than on whichever implementation holds the name first.
//
// Companion to examples/scalar/same_name.go, which collides one name within a
// *single* catalog across two schemas. Backs
// ../vgi/test/sql/integration/scalar/same_name_catalogs.test.
package twin_catalogs

import (
	"context"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Deliberately identical in both catalogs — the collision is the point.
const (
	// FunctionName is the scalar both catalogs declare.
	FunctionName = "test_same_name_catalog"
	// CatalogA is the first colliding catalog.
	CatalogA = "twin_a"
	// CatalogB is the second colliding catalog.
	CatalogB = "twin_b"
)

// tagWithCatalog renders "<catalogName>:<value>" for every row, preserving nulls.
func tagWithCatalog(catalogName string, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	get := vgi.Int64Accessor(batch.Column(0)) // hoist the type switch out of the row loop
	prefix := catalogName + ":"
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(_ arrow.Array, i int) string {
			return prefix + strconv.FormatInt(get(i), 10)
		})
}

// argumentSpecs is the single BIGINT argument both implementations take.
func argumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Integer value to tag"},
	}
}

// TwinAFunction is `test_same_name_catalog` as served by the `twin_a` catalog.
type TwinAFunction struct{}

func (f *TwinAFunction) Name() string { return FunctionName }

func (f *TwinAFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Catalog-disambiguation probe; the twin_a implementation",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT a.main.test_same_name_catalog(1)", Description: "Returns 'twin_a:1'"},
		},
	}
}

func (f *TwinAFunction) ArgumentSpecs() []vgi.ArgSpec { return argumentSpecs() }

func (f *TwinAFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *TwinAFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return tagWithCatalog(CatalogA, params, batch)
}

// TwinBFunction is `test_same_name_catalog` as served by the `twin_b` catalog.
type TwinBFunction struct{}

func (f *TwinBFunction) Name() string { return FunctionName }

func (f *TwinBFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Catalog-disambiguation probe; the twin_b implementation",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT b.main.test_same_name_catalog(1)", Description: "Returns 'twin_b:1'"},
		},
	}
}

func (f *TwinBFunction) ArgumentSpecs() []vgi.ArgSpec { return argumentSpecs() }

func (f *TwinBFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *TwinBFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return tagWithCatalog(CatalogB, params, batch)
}

// Register wires both implementations onto w, each scoped to its own catalog so
// neither leaks into the other's — or the primary catalog's — function listing.
// The caller must also pass vgi.WithCatalogAliases(CatalogA, CatalogB) to
// NewWorker so both names are ATTACHable.
func Register(w *vgi.Worker) {
	w.RegisterScalarForCatalog(CatalogA, &TwinAFunction{})
	w.RegisterScalarForCatalog(CatalogB, &TwinBFunction{})
}
