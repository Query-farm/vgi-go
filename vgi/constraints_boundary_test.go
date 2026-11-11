// Copyright 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestUnsupportedConstraintsRejectBeforeReplacement(t *testing.T) {
	catalog := NewMemoryCatalog("mem")
	attached, err := catalog.attach(nil, &CatalogAttachRequestWire{Name: "mem"})
	if err != nil {
		t.Fatal(err)
	}
	columns, err := SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	request := TableCreateRequestWire{AttachOpaqueData: attached.AttachOpaqueData, SchemaPath: []string{"main"}, Name: "t", Columns: columns, OnConflict: "ERROR"}
	if _, err := catalog.tableCreate(nil, &request); err != nil {
		t.Fatal(err)
	}
	before, err := catalog.getObject(attached.AttachOpaqueData, []string{"main"}, "t", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, conflict := range []string{"ERROR", "IGNORE", "REPLACE"} {
		request.OnConflict = conflict
		request.ConstraintDefinitions = [][]byte{{1}}
		if _, err := catalog.tableCreate(nil, &request); err == nil {
			t.Fatalf("accepted constraints under %s", conflict)
		}
		// This rejection also precedes any writable-store access or replacement.
		if _, err := (&Worker{}).writableTableCreate(nil, request); err == nil {
			t.Fatal("writable store accepted constraints")
		}
	}
	request.ConstraintDefinitions = nil
	request.OnConflict = "REPLACE"
	for _, field := range []arrow.Field{
		{Name: "a", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "record", Type: arrow.StructOf(arrow.Field{Name: "a", Type: arrow.BinaryTypes.String, Nullable: false}), Nullable: true},
	} {
		request.Columns, err = SerializeSchema(arrow.NewSchema([]arrow.Field{field}, nil))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := catalog.tableCreate(nil, &request); err == nil {
			t.Fatal("accepted nonnullable columns without constraints")
		}
		if _, err := (&Worker{}).writableTableCreate(nil, request); err == nil {
			t.Fatal("writable store accepted nonnullable columns")
		}
	}
	after, err := catalog.getObject(attached.AttachOpaqueData, []string{"main"}, "t", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Items) != 1 || len(after.Items) != 1 || !bytes.Equal(before.Items[0], after.Items[0]) {
		t.Fatal("rejected create changed the table")
	}
	version, err := catalog.versionRPC(nil, &CatalogVersionParams{AttachOpaqueData: attached.AttachOpaqueData})
	if err != nil || version.Version != 2 {
		t.Fatalf("rejected create changed version: %+v, %v", version, err)
	}
}

func TestTableInfoRejectsLegacyConstraintClaims(t *testing.T) {
	for _, info := range []*TableInfo{
		{NotNullConstraints: []int32{0}},
		{UniqueConstraints: [][]int32{{0}}},
		{PrimaryKeyConstraints: [][]int32{{0}}},
		{CheckConstraints: []string{"a > 0"}},
		{ForeignKeyConstraints: [][]byte{{1}}},
	} {
		if _, err := SerializeTableInfo(info); err == nil {
			t.Fatal("legacy constraint claim was silently omitted")
		}
	}
}

// The memory catalog reports catalog_table_create.created: false only when IF
// NOT EXISTS (on_conflict IGNORE) kept an existing table.
func TestMemoryCatalogTableCreateReportsCreated(t *testing.T) {
	catalog := NewMemoryCatalog("mem")
	attached, err := catalog.attach(nil, &CatalogAttachRequestWire{Name: "mem"})
	if err != nil {
		t.Fatal(err)
	}
	columns, err := SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		conflict string
		created  bool
	}{{"ERROR", true}, {"IGNORE", false}, {"REPLACE", true}} {
		request := TableCreateRequestWire{AttachOpaqueData: attached.AttachOpaqueData, SchemaPath: []string{"main"}, Name: "t", Columns: columns, OnConflict: step.conflict}
		got, err := catalog.tableCreate(nil, &request)
		if err != nil || got.Created != step.created {
			t.Fatalf("on_conflict %s: %+v %v, want created=%v", step.conflict, got, err, step.created)
		}
	}
	request := TableCreateRequestWire{AttachOpaqueData: attached.AttachOpaqueData, SchemaPath: []string{"main"}, Name: "t", Columns: columns, OnConflict: "ERROR"}
	if got, err := catalog.tableCreate(nil, &request); err == nil || got.Created {
		t.Fatalf("duplicate create: %+v %v", got, err)
	}
}
