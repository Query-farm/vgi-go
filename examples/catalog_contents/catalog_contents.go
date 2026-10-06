// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package catalog_contents holds the catalogs that exercise the
// catalog_contents RPC (the whole catalog in one call). They mirror
// vgi-python's vgi/_test_fixtures/catalog_contents.py — the cross-SDK
// contract — and are driven by
// vgi/test/sql/integration/catalog/catalog_contents*.test.
//
// The same static two-schema catalog is served under three names, differing
// only in how they answer catalog_contents:
//
//	contents_probe   advertises supports_catalog_contents and serves it (the
//	                 default static catalog: version-frozen, no etag, built
//	                 once and cached) — loaded in one RPC.
//	contents_broken  advertises it, but catalog_contents fails: the client must
//	                 fall back to catalog_schemas + the per-schema RPCs.
//	contents_legacy  does not advertise it, like an older worker: the client
//	                 must never call catalog_contents.
//
// Three DDL-capable in-memory catalogs (version not frozen) advertise it too.
// Every ATTACH gets its own empty "main" schema, so tests sharing a worker
// never see each other's objects:
//
//	contents_memory  reports catalog_version 0 ("unknown") and no etag: the
//	                 client's version-0 rule.
//	contents_reval   a cheap validator: the etag is "gen-<n>", n = the catalog
//	                 version, bumped by every DDL; a matching if_none_match is
//	                 answered not_modified without building anything.
//	contents_hash    no etag of its own, but the framework content hash: the
//	                 snapshot is built on every call and its SHA-256 is the etag.
//
// The static catalog holds every kind the client seeds from catalog_contents
// (tables, a view, scalar / aggregate / table functions, scalar and table
// macros), split over "main" and "extra". Its functions are the example
// worker's double / vgi_sum / sequence, homed in each sub-catalog.
package catalog_contents

import (
	"errors"
	"strconv"

	"github.com/Query-farm/vgi-go/examples/aggregate"
	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/examples/table"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// Catalog names.
const (
	CatalogProbe  = "contents_probe"
	CatalogBroken = "contents_broken"
	CatalogLegacy = "contents_legacy"
	CatalogMemory = "contents_memory"
	CatalogReval  = "contents_reval"
	CatalogHash   = "contents_hash"
)

// BrokenMessage is the error contents_broken's catalog_contents fails with.
const BrokenMessage = "contents_broken: catalog_contents deliberately fails"

// Register serves the six catalog_contents fixture catalogs from w.
func Register(w *vgi.Worker) {
	w.RegisterSubCatalog(staticCatalog(CatalogProbe))
	w.RegisterSubCatalog(staticCatalog(CatalogBroken,
		vgi.WithCatalogContentsHandler(func(*vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
			return vgi.CatalogContentsResult{}, errors.New(BrokenMessage)
		})))
	w.RegisterSubCatalog(staticCatalog(CatalogLegacy, vgi.WithCatalogContents(false)))

	w.RegisterMemoryCatalog(vgi.NewMemoryCatalog(CatalogMemory,
		vgi.WithMemoryCatalogComment("catalog_contents test catalog (contents_memory)"),
		vgi.WithMemoryCatalogUnversioned()))
	w.RegisterMemoryCatalog(vgi.NewMemoryCatalog(CatalogReval,
		vgi.WithMemoryCatalogComment("catalog_contents test catalog (contents_reval)"),
		vgi.WithMemoryCatalogContentsHandler(GenerationEtag)))
	w.RegisterMemoryCatalog(vgi.NewMemoryCatalog(CatalogHash,
		vgi.WithMemoryCatalogComment("catalog_contents test catalog (contents_hash)"),
		vgi.WithMemoryCatalogContentsEtag(vgi.CatalogContentsEtagContentHash)))
}

// GenerationEtag answers catalog_contents with the etag "gen-<version>",
// short-circuiting a matching if_none_match to not_modified before building
// anything.
func GenerationEtag(call *vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
	etag := "gen-" + strconv.FormatInt(call.CatalogVersion, 10)
	if call.IfNoneMatch != nil && *call.IfNoneMatch == etag {
		return vgi.CatalogContentsResult{Etag: &etag, NotModified: true}, nil
	}
	schemas, err := call.Contents()
	if err != nil {
		return vgi.CatalogContentsResult{}, err
	}
	return vgi.CatalogContentsResult{Schemas: schemas, Etag: &etag}, nil
}

// staticCatalog builds the two-schema static catalog served under name.
func staticCatalog(name string, opts ...vgi.WorkerOption) *vgi.Worker {
	opts = append([]vgi.WorkerOption{
		vgi.WithCatalogName(name),
		vgi.WithCatalogComment("catalog_contents test catalog (" + name + ")"),
		vgi.WithSchemaComments(map[string]string{
			"main":  "Every object kind",
			"extra": "A second schema, tables only",
		}),
	}, opts...)
	c := vgi.NewWorker(opts...)

	sequence := table.NewSequenceFunction()
	c.RegisterScalar(scalar.NewDouble())
	c.RegisterAggregate(&aggregate.SumFunction{})
	c.RegisterTable(sequence)

	c.RegisterCatalogTable("main", vgi.CatalogTable{
		Name:     "ten",
		Comment:  "Integers 0..9",
		Function: sequence,
		FuncArgs: []vgi.CatalogTableArg{{Position: 0, Value: int64(10), Type: arrow.PrimitiveTypes.Int64}},
	})
	c.RegisterCatalogView("main", vgi.CatalogView{
		Name: "answer", Definition: "SELECT 42 AS answer", Comment: "One row",
	})
	c.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name: "contents_triple", MacroType: vgi.MacroTypeScalar,
		Parameters: []string{"x"}, Definition: "x * 3", Comment: "Triple a value",
	})
	c.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name: "contents_range", MacroType: vgi.MacroTypeTable,
		Parameters: []string{"n"}, Definition: "SELECT * FROM range(n)", Comment: "Table macro over range(n)",
	})
	c.RegisterCatalogTable("extra", vgi.CatalogTable{
		Name:     "five",
		Comment:  "Integers 0..4",
		Function: sequence,
		FuncArgs: []vgi.CatalogTableArg{{Position: 0, Value: int64(5), Type: arrow.PrimitiveTypes.Int64}},
	})
	return c
}
