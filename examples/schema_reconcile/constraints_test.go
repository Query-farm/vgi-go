// Copyright 2026 Query Farm LLC - https://query.farm

package schema_reconcile

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func fixtureRow(t *testing.T, invalid string) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, userSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.TimestampBuilder).Append(0)
	nested := b.Field(2).(*array.StructBuilder)
	nested.Append(true)
	if invalid == "a" {
		nested.FieldBuilder(0).AppendNull()
	} else {
		nested.FieldBuilder(0).(*array.Int32Builder).Append(10)
	}
	nested.FieldBuilder(1).AppendNull()
	nested.FieldBuilder(2).AppendNull()
	tags := b.Field(3).(*array.ListBuilder)
	tags.Append(true)
	item := tags.ValueBuilder().(*array.StructBuilder)
	if invalid == "item" {
		item.AppendNull()
	} else {
		item.Append(true)
		if invalid == "k" {
			item.FieldBuilder(0).AppendNull()
		} else {
			item.FieldBuilder(0).(*array.StringBuilder).Append("env")
		}
		item.FieldBuilder(1).AppendNull()
	}
	batch := b.NewRecordBatch()
	t.Cleanup(batch.Release)
	return batch
}

func fixtureParams(t *testing.T) *vgi.ProcessParams {
	t.Helper()
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.Append("demo")
	arg := b.NewArray()
	t.Cleanup(arg.Release)
	return &vgi.ProcessParams{Args: &vgi.Arguments{Positional: []arrow.Array{arg}}, OutputSchema: countSchema}
}

func fullRow(t *testing.T, batch arrow.RecordBatch) arrow.RecordBatch {
	t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(42)
	rowid := b.NewArray()
	defer rowid.Release()
	full := buildRowWithRowid(batch, rowid, 0, tableSpecs["demo"])
	t.Cleanup(full.Release)
	return full
}

func storeFixtureRow(t *testing.T, batch arrow.RecordBatch) []byte {
	t.Helper()
	payload, err := serializeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	key, err := rowidKey(batch.Column(4), 0)
	if err != nil {
		t.Fatal(err)
	}
	db, err := openDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("INSERT INTO demo_rows (rid_blob,payload_blob) VALUES (?,?)", key, payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestInvalidWritesRejectBeforeStorageEffects(t *testing.T) {
	t.Setenv("VGI_SCHEMA_RECONCILE_DB", filepath.Join(t.TempDir(), "rows.sqlite"))
	valid := fullRow(t, fixtureRow(t, ""))
	want := storeFixtureRow(t, valid)
	params := fixtureParams(t)
	for _, invalid := range []string{"a", "item", "k"} {
		batch := fixtureRow(t, invalid)
		// A nil output is deliberate: rejection must happen before emission.
		err := (insertFn{}).Process(context.Background(), params, &noState{}, batch, nil)
		if err == nil || !strings.Contains(err.Error(), "NULL supplied for non-nullable field '"+invalid+"'") {
			t.Fatalf("insert %s: %v", invalid, err)
		}
		update := fullRow(t, batch)
		err = (updateFn{}).Process(context.Background(), params, &noState{}, update, nil)
		if err == nil || !strings.Contains(err.Error(), "NULL supplied for non-nullable field '"+invalid+"'") {
			t.Fatalf("update %s: %v", invalid, err)
		}
	}
	// A mismatched schema is also rejected before touching the stored row.
	bad := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{userFields[0]}, nil), []arrow.Array{valid.Column(0)}, 1)
	defer bad.Release()
	if err := (insertFn{}).Process(context.Background(), params, &noState{}, bad, nil); err == nil {
		t.Fatal("accepted incomplete schema")
	}
	db, err := openDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	var got []byte
	if err := db.QueryRow("SELECT count(*),payload_blob FROM demo_rows").Scan(&count, &got); err != nil {
		t.Fatal(err)
	}
	if count != 1 || !bytes.Equal(got, want) {
		t.Fatal("rejected write modified storage")
	}
}

func TestNullableAncestorsMaskRequiredChildren(t *testing.T) {
	field := arrow.Field{Name: "parent", Nullable: true, Type: arrow.StructOf(arrow.Field{Name: "a.b", Type: arrow.PrimitiveTypes.Int32})}
	b := array.NewStructBuilder(memory.DefaultAllocator, field.Type.(*arrow.StructType))
	defer b.Release()
	b.AppendNull()
	b.Append(true)
	b.FieldBuilder(0).AppendNull()
	values := b.NewArray()
	defer values.Release()
	if err := validateValue(field, values, 0); err != nil {
		t.Fatalf("null ancestor exposed hidden child: %v", err)
	}
	if err := validateValue(field, values, 1); err == nil {
		t.Fatal("present parent masked required null child")
	}
	// Slice offsets must preserve the same ancestor semantics.
	slice := array.NewSlice(values, 1, 2)
	defer slice.Release()
	if err := validateValue(field, slice, 0); err == nil {
		t.Fatal("sliced present parent masked required null child")
	}
}

func TestStoredInvalidRowsCannotClaimAllRows(t *testing.T) {
	t.Setenv("VGI_SCHEMA_RECONCILE_DB", filepath.Join(t.TempDir(), "old.sqlite"))
	storeFixtureRow(t, fullRow(t, fixtureRow(t, "a")))
	if _, err := AttachValidator(&vgi.CatalogAttachRequestWire{Name: CatalogName}, nil); err == nil {
		t.Fatal("attached old data containing a null required leaf")
	}
	if _, err := serializeTableInfo(tableSpecs["demo"]); err == nil {
		t.Fatal("published ALL_ROWS for invalid stored data")
	}
}

func mustRecord(t *testing.T, schema *arrow.Schema, values map[string]any) []byte {
	t.Helper()
	data, err := encodeRecord(schema, values)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func decodeRecord(t *testing.T, data []byte) arrow.RecordBatch {
	t.Helper()
	batch, err := vgi.DeserializeRecordBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batch.Release)
	return batch
}

func TestCapabilitiesSelectOnlyOfferedPrerequisites(t *testing.T) {
	t.Setenv("VGI_SCHEMA_RECONCILE_DB", filepath.Join(t.TempDir(), "caps.sqlite"))
	for _, test := range []struct {
		read, write bool
	}{{}, {true, false}, {false, true}, {true, true}} {
		read, write := []string{}, []string{}
		if test.read {
			read = append(read, constraintRead)
		}
		if test.write {
			write = append(write, constraintEnforce)
		}
		offer := mustRecord(t, generated.CatalogCapabilitiesSchema, map[string]any{"read": read, "write": write, "ddl": []string{}, "transaction": []string{}})
		client := mustRecord(t, generated.ClientCapabilitiesSchema, map[string]any{
			"engine": "duckdb", "native_formats": []string{}, "catalogs": []string{}, "can_stream": false,
			"filter_encodings": []string{}, "catalog": offer, "query_pushdown": nil,
		})
		decision, err := AttachValidator(&vgi.CatalogAttachRequestWire{Name: CatalogName, ClientCapabilities: &client}, nil)
		if err != nil {
			t.Fatal(err)
		}
		selected := decodeRecord(t, decision.SelectedCapabilities)
		for i, expected := range []int{len(read), 0, 0, 0} {
			if i == 2 && test.read && test.write {
				expected = 1
			}
			list := selected.Column(i).(*array.List)
			start, end := list.ValueOffsets(0)
			if int(end-start) != expected {
				t.Fatalf("read=%v write=%v: selected wrong category %d", test.read, test.write, i)
			}
		}
	}
	decision, err := AttachValidator(&vgi.CatalogAttachRequestWire{Name: "example"}, nil)
	if err != nil || decision != nil {
		t.Fatal("changed another catalog's agreement")
	}
}

func TestCanonicalNotNullDiscoveryRetainsNestedPath(t *testing.T) {
	t.Setenv("VGI_SCHEMA_RECONCILE_DB", filepath.Join(t.TempDir(), "catalog.sqlite"))
	encoded, err := serializeTableInfo(tableSpecs["demo"])
	if err != nil {
		t.Fatal(err)
	}
	info := decodeRecord(t, encoded)
	constraints := info.Column(info.Schema().FieldIndices("constraints")[0]).(*array.List).ListValues().(*array.Binary)
	var paths [][]string
	for i := 0; i < constraints.Len(); i++ {
		constraint := decodeRecord(t, constraints.Value(i))
		for name, want := range map[string]string{"enforcement": "ENFORCED", "validation": "ALL_ROWS"} {
			values := constraint.Column(constraint.Schema().FieldIndices(name)[0]).(*array.Dictionary)
			if got := values.Dictionary().(*array.String).Value(values.GetValueIndex(0)); got != want {
				t.Fatalf("%s=%s", name, got)
			}
		}
		definition := decodeRecord(t, constraint.Column(constraint.Schema().FieldIndices("definition")[0]).(*array.Binary).Value(0))
		list := definition.Column(definition.Schema().FieldIndices("column_paths")[0]).(*array.List).ListValues().(*array.List)
		values := list.ListValues().(*array.String)
		start, end := list.ValueOffsets(0)
		var path []string
		for j := start; j < end; j++ {
			path = append(path, values.Value(int(j)))
		}
		paths = append(paths, path)
	}
	if !reflect.DeepEqual(paths, [][]string{{"id"}, {"ts"}, {"nested"}, {"nested", "a"}, {"tags"}}) {
		t.Fatalf("canonical SQL paths include a pseudocolumn or lose a raw component: %v", paths)
	}
}
