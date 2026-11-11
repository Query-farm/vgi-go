// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package simple_writable

import (
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// createdFixture points the catalog handlers at an in-memory store and
// returns one attach's opaque bytes.
func createdFixture(t *testing.T) []byte {
	t.Helper()
	fs, err := vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	Options(fs)
	t.Cleanup(func() {
		backend.mu.Lock()
		backend.fs = nil
		backend.mu.Unlock()
	})
	return append([]byte(CatalogName+"\x00"), []byte("0123456789abcdef")...)
}

func createRequest(t *testing.T, attach []byte, name, onConflict string, fields ...arrow.Field) *vgi.TableCreateRequestWire {
	t.Helper()
	cols, err := vgi.SerializeSchema(arrow.NewSchema(fields, nil))
	if err != nil {
		t.Fatal(err)
	}
	return &vgi.TableCreateRequestWire{AttachOpaqueData: attach, SchemaPath: []string{SchemaPath}, Name: name, Columns: cols, OnConflict: onConflict}
}

// catalog_table_create reports created: true for a new table and REPLACE,
// false for IF NOT EXISTS on an existing one (left unchanged); ERROR on an
// existing name says "already exists". The created table is listed, gettable
// and scanned/inserted through the created-table functions.
func TestCreatedTableCreateReportsCreated(t *testing.T) {
	attach := createdFixture(t)
	idCol := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	okCol := arrow.Field{Name: "ok", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}

	created, handled, err := TableCreateHandler(createRequest(t, attach, "t1", "ERROR", idCol))
	if err != nil || !handled || !created {
		t.Fatalf("create: created=%v handled=%v err=%v", created, handled, err)
	}
	created, handled, err = TableCreateHandler(createRequest(t, attach, "t1", "IGNORE", idCol, okCol))
	if err != nil || !handled || created {
		t.Fatalf("IF NOT EXISTS: created=%v handled=%v err=%v", created, handled, err)
	}
	st, err := attachStore(attach)
	if err != nil {
		t.Fatal(err)
	}
	if cols, err := createdSchema(st, "t1"); err != nil || cols.NumFields() != 1 {
		t.Fatalf("IF NOT EXISTS changed the table: %v %v", cols, err)
	}
	if _, _, err = TableCreateHandler(createRequest(t, attach, "t1", "ERROR", idCol)); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("duplicate create: %v", err)
	}
	if _, _, err = TableCreateHandler(createRequest(t, attach, "items", "ERROR", idCol)); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create over a fixed table: %v", err)
	}
	if created, _, err = TableCreateHandler(createRequest(t, attach, "items", "IGNORE", idCol)); err != nil || created {
		t.Fatalf("IF NOT EXISTS over a fixed table: created=%v err=%v", created, err)
	}
	created, _, err = TableCreateHandler(createRequest(t, attach, "t1", "REPLACE", idCol, okCol))
	if err != nil || !created {
		t.Fatalf("REPLACE: created=%v err=%v", created, err)
	}
	if cols, _ := createdSchema(st, "t1"); cols.NumFields() != 2 {
		t.Fatalf("REPLACE kept the old columns: %v", cols)
	}

	items, ok := SchemaContentsHandler(attach, vgi.SchemaPath{SchemaPath})
	if !ok || len(items) != len(tableOrder)+1 {
		t.Fatalf("schema contents: %d items, want %d", len(items), len(tableOrder)+1)
	}
	if data, handled, err := AttachTableGetHandler(attach, vgi.SchemaPath{SchemaPath}, "t1", nil, nil); err != nil || !handled || data == nil {
		t.Fatalf("table get: handled=%v err=%v", handled, err)
	}
	if res, handled, _ := AttachScanFunctionGetHandler(attach, vgi.SchemaPath{SchemaPath}, "t1", nil, nil); !handled || res.FunctionName != "simple_writable_created_scan" {
		t.Fatalf("scan function: %+v %v", res, handled)
	}
	if res, handled, _ := AttachWriteFunctionGetHandler(vgi.WriteOpInsert, attach, vgi.SchemaPath{SchemaPath}, "t1"); !handled || res.FunctionName != "simple_writable_created_insert" {
		t.Fatalf("insert function: %+v %v", res, handled)
	}
	if _, handled, _ := AttachWriteFunctionGetHandler(vgi.WriteOpDelete, attach, vgi.SchemaPath{SchemaPath}, "t1"); handled {
		t.Fatal("created tables have no delete function")
	}
}

// catalog_table_drop drops a created table, honours ignore_not_found, and
// refuses the fixed tables.
func TestCreatedTableDrop(t *testing.T) {
	attach := createdFixture(t)
	if _, _, err := TableCreateHandler(createRequest(t, attach, "t2", "ERROR", arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Float64, Nullable: true})); err != nil {
		t.Fatal(err)
	}
	drop := func(name string, ignore bool) error {
		handled, err := TableDropHandler(&vgi.CatalogTableDropParams{AttachOpaqueData: attach, SchemaPath: []string{SchemaPath}, Name: name, IgnoreNotFound: ignore})
		if !handled {
			t.Fatalf("drop %s not handled", name)
		}
		return err
	}
	if err := drop("t2", false); err != nil {
		t.Fatal(err)
	}
	if _, handled, _ := AttachTableGetHandler(attach, vgi.SchemaPath{SchemaPath}, "t2", nil, nil); handled {
		t.Fatal("dropped table still served")
	}
	if err := drop("t2", false); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("drop missing: %v", err)
	}
	if err := drop("t2", true); err != nil {
		t.Fatalf("drop IF EXISTS missing: %v", err)
	}
	if err := drop("items", false); err == nil {
		t.Fatal("dropped a fixed table")
	}
	// Another catalog's attach falls through.
	if handled, _ := TableDropHandler(&vgi.CatalogTableDropParams{AttachOpaqueData: []byte("other"), SchemaPath: []string{SchemaPath}, Name: "t2"}); handled {
		t.Fatal("handled another catalog's drop")
	}
}
