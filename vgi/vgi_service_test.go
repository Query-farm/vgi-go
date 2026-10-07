// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// vgiV2Unimplemented is every vgi.v2 method this SDK hosts only to refuse.
var vgiV2Unimplemented = []string{
	"CatalogIndexCreate",
	"CatalogIndexDrop",
	"CatalogIndexGet",
	"CatalogSchemaContentsIndexes",
	"CatalogTableColumnCommentSet",
}

// workerService embeds unimplementedVgiService, so a method it fails to
// declare -- forgotten, or misspelled -- compiles fine and silently answers
// UNIMPLEMENTED. Pin the set: every vgiService method is declared on
// workerService except exactly vgiV2Unimplemented, and workerService declares
// nothing that is not a vgi.v2 method.
func TestVgiV2UnimplementedSet(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var declared []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			if id, ok := fn.Recv.List[0].Type.(*ast.Ident); ok && id.Name == "workerService" {
				declared = append(declared, fn.Name.Name)
			}
		}
	}
	iface := reflect.TypeFor[vgiService]()
	var protocol, missing []string
	for i := range iface.NumMethod() {
		name := iface.Method(i).Name
		protocol = append(protocol, name)
		if !slices.Contains(declared, name) {
			missing = append(missing, name)
		}
	}
	for _, name := range declared {
		if !slices.Contains(protocol, name) {
			t.Errorf("workerService.%s is not a vgi.v2 method (misspelled? the protocol's name wins, and answers UNIMPLEMENTED)", name)
		}
	}
	if !slices.Equal(missing, vgiV2Unimplemented) {
		t.Errorf("vgi.v2 methods workerService leaves UNIMPLEMENTED = %v, want %v", missing, vgiV2Unimplemented)
	}
}
