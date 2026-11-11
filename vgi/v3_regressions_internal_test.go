// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

func definitionFor(t *testing.T, kind string, paths ...[]string) []byte {
	t.Helper()
	data, err := encodeCanonicalRow(generated.ConstraintDefinitionSchema, map[string]any{
		"kind": kind, "column_paths": nonNilPaths(paths), "declared_name": nil, "check_expression": nil, "foreign_key": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func createSchema(t *testing.T, fields ...arrow.Field) []byte {
	t.Helper()
	data, err := SerializeSchema(arrow.NewSchema(fields, nil))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func wantError(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error mentioning %q", parts)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("error %q does not mention %q", err, part)
		}
	}
}

// CREATE TABLE ... NOT NULL records an enforced canonical record, and every
// write path refuses a NULL in the column.
func TestWritableCreateNotNullEnforced(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	w := NewWorker()
	c := NewWritableCatalog("nn")
	columns := createSchema(t,
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true})
	req := TableCreateRequestWire{SchemaPath: []string{"main"}, Name: "Things", Columns: columns, OnConflict: "ERROR",
		ConstraintDefinitions: [][]byte{definitionFor(t, "NOT_NULL", []string{"id"})}}
	if _, err := w.writableTableCreate(c, req); err != nil {
		t.Fatal(err)
	}
	table, err := c.store.tableLoad(c.Name, "main", "things")
	if err != nil || table == nil {
		t.Fatalf("table not stored: %v", err)
	}
	required, err := requiredColumns(table.constraints)
	if err != nil || len(required) != 1 || required[0] != "id" {
		t.Fatalf("required columns %v, %v", required, err)
	}
	// Served under the name asked for, with id non-nullable and note nullable.
	data, err := w.writableTableGet(c, SchemaPath{"main"}, "Things")
	if err != nil || len(data) != 1 {
		t.Fatalf("table_get: %v", err)
	}
	served := servedSchemaOf(t, data[0])
	if served.Field(0).Nullable || !served.Field(1).Nullable {
		t.Fatalf("served %v", served)
	}

	if _, err := c.store.rowsAppend(c.Name, "main", "Things", []map[string]interface{}{{"id": int64(1), "note": nil}}, required); err != nil {
		t.Fatal(err)
	}
	_, err = c.store.rowsAppend(c.Name, "main", "Things", []map[string]interface{}{{"id": int64(2)}, {"id": nil, "note": "x"}}, required)
	wantError(t, err, "NOT NULL constraint failed", "id")
	_, err = c.store.rowsAppend(c.Name, "main", "Things", []map[string]interface{}{{"note": "x"}}, required)
	wantError(t, err, "NOT NULL constraint failed")
	rows, err := c.store.rowsScan(c.Name, "main", "Things")
	if err != nil || len(rows) != 1 {
		t.Fatalf("a refused batch wrote rows: %d, %v", len(rows), err)
	}
	_, err = c.store.rowsUpdate(c.Name, "main", "Things", []map[string]interface{}{{rowIDFieldName: int64(0), "id": nil}}, required)
	wantError(t, err, "NOT NULL constraint failed")
	if n, err := c.store.rowsUpdate(c.Name, "main", "Things", []map[string]interface{}{{rowIDFieldName: int64(0), "note": "ok"}}, required); err != nil || n != 1 {
		t.Fatalf("valid update: %d, %v", n, err)
	}
}

// Constraints this catalog does not enforce are refused with an exact message.
func TestWritableCreateRefusesUnenforcedConstraints(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	w := NewWorker()
	c := NewWritableCatalog("nn")
	columns := createSchema(t, arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64})
	for kind, want := range map[string]string{"PRIMARY_KEY": "PRIMARY KEY constraints are not enforced", "UNIQUE": "UNIQUE constraints are not enforced"} {
		_, err := w.writableTableCreate(c, TableCreateRequestWire{SchemaPath: []string{"main"}, Name: "t", Columns: columns, OnConflict: "ERROR",
			ConstraintDefinitions: [][]byte{definitionFor(t, kind, []string{"id"})}})
		wantError(t, err, want, "only NOT NULL")
	}
	// A non-nullable column without a NOT NULL definition is refused too.
	_, err := w.writableTableCreate(c, TableCreateRequestWire{SchemaPath: []string{"main"}, Name: "t", Columns: columns, OnConflict: "ERROR"})
	wantError(t, err, `column "id" is non-nullable without a NOT NULL constraint definition`)
}

// A table stored before vgi.v3 keeps a NOT NULL that its rows satisfy (now
// enforced) and drops one they violate; the migration persists.
func TestWritableLegacyNotNullMigration(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c := NewWritableCatalog("legacy")
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	if err := c.store.tableUpsert(c.Name, "main", &writableTable{name: "old", schema: schema}); err != nil {
		t.Fatal(err)
	}
	var legacy bytes.Buffer
	if err := gob.NewEncoder(&legacy).Encode(writableTableMeta{NotNull: []string{"id", "name"}, PrimaryKey: [][]string{{"id"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.db.Exec(`UPDATE wc_table SET meta_blob=? WHERE catalog=? AND name=?`, legacy.Bytes(), c.Name, "old"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.rowsAppend(c.Name, "main", "old", []map[string]interface{}{{"id": int64(1), "name": nil}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.migrateLegacyConstraints(); err != nil {
		t.Fatal(err)
	}
	table, err := c.store.tableLoad(c.Name, "main", "old")
	if err != nil {
		t.Fatal(err)
	}
	if table.legacy != nil {
		t.Fatal("migration was not persisted")
	}
	required, err := requiredColumns(table.constraints)
	if err != nil || len(required) != 1 || required[0] != "id" {
		t.Fatalf("migrated NOT NULL %v, %v", required, err)
	}
}

func TestWritableAttachNegotiatesConstraintProfiles(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	w := NewWorker()
	client := clientOffer(t, CatalogCapabilities{
		Read: []string{ConstraintRecordsReadProfile, ScalarExpressionReadProfile},
		Ddl:  []string{ConstraintCreateProfile, "vgi.constraint.mutate.v1"}, Write: []string{ConstraintEnforceProfile},
		Transaction: []string{"vgi.transaction.ddl.v1"},
	})
	res, err := w.handleWritableAttach(CatalogAttachRequestWire{Name: "wc", ClientCapabilities: &client}, NewWritableCatalog("wc"))
	if err != nil {
		t.Fatal(err)
	}
	selected, err := DecodeCatalogCapabilities(*res.SelectedCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(selected.Read, ",") != ConstraintRecordsReadProfile || strings.Join(selected.Ddl, ",") != ConstraintCreateProfile ||
		strings.Join(selected.Write, ",") != ConstraintEnforceProfile || len(selected.Transaction) != 0 {
		t.Fatalf("selected %+v", selected)
	}
}

func clientOffer(t *testing.T, offer CatalogCapabilities) []byte {
	t.Helper()
	catalog, err := EncodeCatalogCapabilities(offer)
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeCanonicalRowLoose(generated.ClientCapabilitiesSchema, map[string]any{
		"engine": "duckdb", "native_formats": []string{}, "catalogs": []string{}, "can_stream": true,
		"filter_encodings": []string{}, "catalog": catalog, "query_pushdown": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// encodeCanonicalRowLoose is encodeCanonicalRow for schemas with fields the
// test leaves NULL.
func encodeCanonicalRowLoose(schema *arrow.Schema, values map[string]any) ([]byte, error) {
	for _, field := range schema.Fields() {
		if _, ok := values[field.Name]; !ok {
			values[field.Name] = nil
		}
	}
	return encodeCanonicalRow(schema, values)
}

func servedSchemaOf(t *testing.T, tableInfo []byte) *arrow.Schema {
	t.Helper()
	batch, err := DeserializeRecordBatch(tableInfo)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	schema, err := DeserializeSchema(batch.Column(batch.Schema().FieldIndices("columns")[0]).(*array.Binary).Value(0))
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestProtocolBuilders(t *testing.T) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "compression", Type: arrow.BinaryTypes.String, Nullable: true}}, nil))
	defer b.Release()
	b.Field(0).(*array.StringBuilder).Append("zstd")
	values := b.NewRecordBatch()
	defer values.Release()
	state, err := OptionStateRecord(values, []string{"compression"})
	if err != nil {
		t.Fatal(err)
	}
	if row, err := readCanonicalRow(state, generated.OptionStateSchema); err != nil {
		t.Fatal(err)
	} else {
		row.Release()
	}
	if _, err := OptionStateRecord(values, []string{"missing"}); err == nil {
		t.Fatal("explicit name without a value was accepted")
	}
	if _, err := OptionStateRecord(nil, nil); err != nil {
		t.Fatal(err)
	}

	expression, err := ScalarExpressionRecord("ts", []string{"main"})
	if err != nil {
		t.Fatal(err)
	}
	key, err := SortKeyRecord(expression, "asc", "last")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SortKeyRecord(expression, "ascending", "last"); err == nil {
		t.Fatal("bad direction accepted")
	}
	layout, err := TableLayoutRecord([][]byte{expression}, [][]byte{key}, LayoutAllRows)
	if err != nil {
		t.Fatal(err)
	}
	if row, err := readCanonicalRow(layout, generated.TableLayoutSchema); err != nil {
		t.Fatal(err)
	} else {
		row.Release()
	}
	if _, err := TableLayoutRecord(nil, [][]byte{expression}, LayoutAllRows); err == nil {
		t.Fatal("an expression was accepted as a sort key")
	}

	declared, err := MacroParameterDeclaredType("x", arrow.PrimitiveTypes.Int32, arrow.Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := DeserializeSchema(declared)
	if err != nil || schema.NumFields() != 1 || schema.Field(0).Name != "x" || !schema.Field(0).Nullable {
		t.Fatalf("declared type %v, %v", schema, err)
	}
	if _, err := MacroParameterDeclaredType("x", arrow.Null, arrow.Metadata{}); err == nil {
		t.Fatal("NULL declared type accepted")
	}
	if _, err := DefinitionRevision(""); err == nil {
		t.Fatal("empty revision accepted")
	}
	if _, err := SerializeTableInfo(&TableInfo{Name: "t", SchemaPath: []string{"main"}, DefinitionRevision: []byte{}}); err == nil {
		t.Fatal("empty non-nil revision served")
	}
}

// Protocol 3.3 catalog_table_create.created: true when the call created the
// table (including OR REPLACE), false when IF NOT EXISTS found it and left it
// unchanged. CTAS relies on it not to append into an existing table.
func TestWritableCreateReportsCreated(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	w := NewWorker()
	c := NewWritableCatalog("created")
	columns := createSchema(t, arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	for _, step := range []struct {
		conflict string
		created  bool
	}{{"ERROR", true}, {"IGNORE", false}, {"REPLACE", true}, {"IGNORE", false}} {
		created, err := w.writableTableCreate(c, TableCreateRequestWire{SchemaPath: []string{"main"}, Name: "t", Columns: columns, OnConflict: step.conflict})
		if err != nil || created != step.created {
			t.Fatalf("on_conflict %s: created=%v err=%v, want created=%v", step.conflict, created, err, step.created)
		}
	}
	if created, err := w.writableTableCreate(c, TableCreateRequestWire{SchemaPath: []string{"main"}, Name: "t", Columns: columns, OnConflict: "ERROR"}); err == nil || created {
		t.Fatalf("duplicate create: created=%v err=%v", created, err)
	}
}

// WithAttachTableCreateHandler / WithAttachTableDropHandler answer
// catalog_table_create / catalog_table_drop for handler-served catalogs: the
// handler sees the whole request first, its created flag is the reply, its
// *vgirpc.RpcError passes through with its type, and handled=false falls
// through to the built-in read-only refusal.
func TestAttachTableCreateDropHandlers(t *testing.T) {
	var seen []string
	w := NewWorker(
		WithAttachTableCreateHandler(func(req *TableCreateRequestWire) (bool, bool, error) {
			seen = append(seen, "create:"+req.Name+":"+req.OnConflict)
			switch req.Name {
			case "mine":
				return req.OnConflict != "IGNORE", true, nil
			case "typed":
				return false, true, &vgirpc.RpcError{Type: "NotImplementedError", Message: "nope"}
			}
			return false, false, nil
		}),
		WithAttachTableDropHandler(func(req *CatalogTableDropParams) (bool, error) {
			seen = append(seen, "drop:"+req.Name)
			return req.Name == "mine", nil
		}),
	)
	svc := workerService{Worker: w}
	ctx := context.Background()
	for conflict, want := range map[string]bool{"ERROR": true, "IGNORE": false} {
		res, err := svc.CatalogTableCreate(ctx, nil, TableCreateRequestWire{Name: "mine", OnConflict: conflict, ConstraintDefinitions: [][]byte{{1}}})
		if err != nil || res.Created != want {
			t.Fatalf("%s: %+v %v, want created=%v", conflict, res, err, want)
		}
	}
	_, err := svc.CatalogTableCreate(ctx, nil, TableCreateRequestWire{Name: "typed"})
	var rpcErr *vgirpc.RpcError
	if !errors.As(err, &rpcErr) || rpcErr.Type != "NotImplementedError" {
		t.Fatalf("typed handler error: %v", err)
	}
	if res, err := svc.CatalogTableCreate(ctx, nil, TableCreateRequestWire{Name: "other"}); err == nil || res.Created {
		t.Fatalf("unhandled create on a read-only worker: %+v %v", res, err)
	}
	if err := svc.CatalogTableDrop(ctx, nil, CatalogTableDropParams{Name: "mine"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CatalogTableDrop(ctx, nil, CatalogTableDropParams{Name: "other"}); err == nil {
		t.Fatal("unhandled drop on a read-only worker succeeded")
	}
	if len(seen) != 6 {
		t.Fatalf("handler calls %v", seen)
	}
}
