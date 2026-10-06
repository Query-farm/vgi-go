// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import "testing"

// A sub-catalog's functions are dispatched by the parent, homed in the
// sub-catalog: a bind through the sub-catalog's attach reaches them, a bind
// through the parent's own catalog does not.
func TestSubCatalogFunctionsHomedInSubCatalog(t *testing.T) {
	parent := NewWorker(WithCatalogName("parent"))
	child := NewWorker(WithCatalogName("kid"))
	child.RegisterTableInSchema("extra", &countingTable{})
	parent.RegisterSubCatalog(child)
	parent.prepareRoutes()

	routed := encodeRoutedAttach("kid", []byte("inner"))
	if got := parent.catalogOfAttach(routed); got != "kid" {
		t.Fatalf("catalogOfAttach(routed) = %q, want kid", got)
	}
	if name, inner, ok := parseRoutedAttach(routed); !ok || name != "kid" || string(inner) != "inner" {
		t.Fatalf("parseRoutedAttach = %q %q %v", name, inner, ok)
	}
	if _, _, ok := parseRoutedAttach([]byte("kid")); ok {
		t.Fatal("a plain attach parsed as routed")
	}

	cands := parent.candidatesFor("counting", FunctionTypeTable)
	if _, err := parent.scopeToHome(cands, "counting", "extra", "kid"); err != nil {
		t.Fatalf("resolving through the sub-catalog: %v", err)
	}
	if _, err := parent.scopeToHome(cands, "counting", "extra", "parent"); err == nil {
		t.Fatal("the sub-catalog's function resolved through the parent's own catalog")
	}
	// The parent's own catalog does not list it (nor create its schema).
	parent.catalog = NewDefaultReadOnlyCatalog("parent", parent)
	if _, ok := parent.catalog.schemas["extra"]; ok {
		t.Fatal("the sub-catalog's function created a schema in the parent's catalog")
	}
}
