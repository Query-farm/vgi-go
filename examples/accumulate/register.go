// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package accumulate

import "github.com/Query-farm/vgi-go/vgi"

// CatalogAliasInfo is the discovery metadata for the accumulate catalog. Pass
// it to vgi.WithCatalogAliasInfo so the worker advertises the catalog (with its
// data version) and mints a random per-ATTACH scope.
func CatalogAliasInfo() vgi.CatalogInfo {
	v := DataVersion
	return vgi.CatalogInfo{Name: CatalogName, DataVersionSpec: &v}
}

// Register wires the three accumulate functions onto w, scoped to the
// accumulate catalog so they are invisible under any other attached catalog.
// The caller must also pass vgi.WithCatalogAliasInfo(CatalogName, CatalogAliasInfo())
// to NewWorker so the catalog is discoverable and per-ATTACH isolated.
func Register(w *vgi.Worker) {
	w.RegisterTableBufferingForCatalog(CatalogName, &AccumulateFunction{})
	w.RegisterTableForCatalog(CatalogName, NewAccumulateReadFunction())
	w.RegisterTableForCatalog(CatalogName, NewAccumulateClearFunction())
}
