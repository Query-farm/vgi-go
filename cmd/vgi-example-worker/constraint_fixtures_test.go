// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"bytes"
	"testing"

	"github.com/Query-farm/vgi-go/examples/table"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func fixtureBatch(t *testing.T, encoded []byte) arrow.RecordBatch {
	t.Helper()
	batch, err := vgi.DeserializeRecordBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batch.Release)
	return batch
}

func TestConstraintFixtureInvalidRowsCannotEstablishFacts(t *testing.T) {
	cases := []struct {
		name, table, field, want string
		version                  int64
		values                   []any
	}{
		{"not_null", "departments", "name", "fixture NOT NULL violated: name", 0, []any{"a", nil, "c"}},
		{"primary", "products", "id", "fixture PRIMARY_KEY contains duplicate key", 0, []any{int64(1), int64(1), int64(3)}},
		{"unique", "departments", "name", "fixture UNIQUE contains duplicate key", 0, []any{"a", "a", "c"}},
		{"composite", "projects", "project_code", "fixture PRIMARY_KEY contains duplicate key", 0, []any{"P001", "P001", "P003"}},
		{"foreign", "employees", "department_id", "fixture FOREIGN KEY missing referenced row", 0, []any{int64(1), int64(1), int64(2), int64(2), int64(99)}},
		{"check", "departments", "budget", "fixture CHECK budget >= 0 violated", 0, []any{float64(0), float64(-1), float64(100)}},
		{"historical_unique", "versioned_constraints", "email", "fixture UNIQUE contains duplicate key", 2, []any{"a", "a", "c"}},
		{"historical_foreign", "versioned_constraints", "department_id", "fixture FOREIGN KEY missing referenced row", 3, []any{int64(1), int64(99), int64(1)}},
		{"nullable_foreign", "employees", "department_id", "", 0, []any{int64(1), int64(1), int64(2), int64(2), nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := table.ConstraintFixtureRows(tc.table, tc.version)
			defer source.Release()
			index := source.Schema().FieldIndices(tc.field)[0]
			builder := array.NewBuilder(memory.DefaultAllocator, source.Schema().Field(index).Type)
			defer builder.Release()
			for _, value := range tc.values {
				if value == nil {
					builder.AppendNull()
					continue
				}
				switch b := builder.(type) {
				case *array.Int64Builder:
					b.Append(value.(int64))
				case *array.StringBuilder:
					b.Append(value.(string))
				case *array.Float64Builder:
					b.Append(value.(float64))
				default:
					t.Fatal("unexpected test builder")
				}
			}
			replacement := builder.NewArray()
			defer replacement.Release()
			columns := append([]arrow.Array(nil), source.Columns()...)
			columns[index] = replacement
			mutated := array.NewRecordBatch(source.Schema(), columns, source.NumRows())
			defer mutated.Release()
			err := validateFixtureRows(mutated, fixtureDefinitions(tc.table, tc.version))
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
}

func TestConstraintFixtureAttachSelectsOnlyOfferedReadProfiles(t *testing.T) {
	profiles := []string{"vgi.constraint.records.read.v1", "vgi.expression.duckdb.scalar.v1"}
	for count := -1; count <= 3; count++ {
		req := &vgi.CatalogAttachRequestWire{Name: "example"}
		if count >= 0 {
			offered := profiles[:min(count, 2)]
			if count == 3 {
				offered = profiles[1:]
			}
			offer := fixtureRecord(generated.CatalogCapabilitiesSchema, map[string]any{"read": offered, "ddl": []string{}, "write": []string{}, "transaction": []string{}})
			client := fixtureRecord(generated.ClientCapabilitiesSchema, map[string]any{"engine": "duckdb", "native_formats": []string{}, "catalogs": []string{}, "can_stream": true, "filter_encodings": []string{}, "catalog": offer, "query_pushdown": nil})
			req.ClientCapabilities = &client
		}
		decision, err := fixtureAttachValidator(req, nil)
		if err != nil {
			t.Fatal(err)
		}
		selected := fixtureBatch(t, decision.SelectedCapabilities)
		read := fixtureColumn(t, selected, "read").(*array.List)
		a, b := read.ValueOffsets(0)
		expected := max(count, 0)
		if count == 3 {
			expected = 0
		}
		if int(b-a) != expected {
			t.Fatal("selection ignored client offer")
		}
		write := fixtureColumn(t, selected, "write").(*array.List)
		a, b = write.ValueOffsets(0)
		if b != a {
			t.Fatal("read-only fixture advertised mutation enforcement")
		}
	}
	decision, err := fixtureAttachValidator(&vgi.CatalogAttachRequestWire{Name: "unrelated"}, nil)
	if err != nil || decision != nil {
		t.Fatal("unrelated catalog was changed")
	}
}

func fixtureColumn(t *testing.T, batch arrow.RecordBatch, name string) arrow.Array {
	t.Helper()
	index := batch.Schema().FieldIndices(name)
	if len(index) != 1 {
		t.Fatalf("missing field %s", name)
	}
	return batch.Column(index[0])
}

func fixtureEnum(t *testing.T, batch arrow.RecordBatch, name string) string {
	t.Helper()
	values := fixtureColumn(t, batch, name).(*array.Dictionary)
	return values.Dictionary().(*array.String).Value(values.GetValueIndex(0))
}

func TestConstraintFixtureDiscoveryIsValidatedReadOnly(t *testing.T) {
	cases := []struct {
		name   string
		schema *arrow.Schema
		count  int
	}{
		{"departments", table.DepartmentsSchema, 5},
		{"products", table.ProductsSchema, 2},
		{"employees", table.EmployeesSchema, 6},
		{"projects", table.ProjectsSchema, 5},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			records := fixtureConstraints(entry.name, 0)
			encoded, err := vgi.SerializeTableInfo(&vgi.TableInfo{
				Name: entry.name, SchemaPath: []string{"data"},
				Columns: fixtureSchema(entry.schema, entry.name, 0), Constraints: records,
			})
			if err != nil {
				t.Fatal(err)
			}
			info := fixtureBatch(t, encoded)
			columns, err := vgi.DeserializeSchema(fixtureColumn(t, info, "columns").(*array.Binary).Value(0))
			if err != nil {
				t.Fatal(err)
			}
			if !columns.Equal(entry.schema) {
				t.Fatal("catalog and actual scan nullability differ")
			}

			constraints := fixtureColumn(t, info, "constraints").(*array.List)
			items := constraints.ListValues().(*array.Binary)
			if items.Len() != entry.count {
				t.Fatalf("got %d constraints, want %d", items.Len(), entry.count)
			}
			keys := map[string]bool{}
			for i := 0; i < items.Len(); i++ {
				constraint := fixtureBatch(t, items.Value(i))
				if !constraint.Schema().Equal(generated.ConstraintInfoSchema) {
					t.Fatal("wrong canonical record schema")
				}
				if fixtureEnum(t, constraint, "enforcement") != "ENFORCED" || fixtureEnum(t, constraint, "validation") != "ALL_ROWS" {
					t.Fatal("fixture lost validated read-only guarantees")
				}
				ref := fixtureBatch(t, fixtureColumn(t, constraint, "ref").(*array.Binary).Value(0))
				key := fixtureColumn(t, ref, "name").(*array.String).Value(0)
				if keys[key] {
					t.Fatalf("reused fixture identity %s", key)
				}
				keys[key] = true
				if fixtureColumn(t, ref, "owner_table").(*array.String).Value(0) != entry.name {
					t.Fatal("wrong owner")
				}
				definition := fixtureBatch(t, fixtureColumn(t, constraint, "definition").(*array.Binary).Value(0))
				switch fixtureEnum(t, definition, "kind") {
				case "FOREIGN_KEY":
					if !bytes.Equal(fixtureColumn(t, constraint, "referenced_constraint").(*array.Binary).Value(0), fixtureConstraintRef("departments", "fixture:departments:primary")) {
						t.Fatal("FK lost stable target")
					}
				case "CHECK":
					expression := fixtureBatch(t, fixtureColumn(t, definition, "check_expression").(*array.Binary).Value(0))
					context := fixtureColumn(t, expression, "evaluation_context").(*array.Struct)
					if context.IsNull(0) || context.Field(0).(*array.String).Value(0) != "UTC" {
						t.Fatal("CHECK lost explicit context")
					}
				}
			}
		})
	}
}

func TestConstraintFixtureHistoricalSnapshotsRetainStableKeys(t *testing.T) {
	previous := [][]byte{}
	for version := int64(1); version <= 3; version++ {
		records := fixtureConstraints("versioned_constraints", version)
		expected := int(version) + 2
		if version == 1 {
			expected = 1
		}
		if len(records) != expected {
			t.Fatalf("version %d has wrong definition set", version)
		}
		for i := range previous {
			if !bytes.Equal(previous[i], records[i]) {
				t.Fatal("surviving constraint identity or definition changed")
			}
		}
		if !fixtureSchema(table.VersionedConstraintsSchema(version), "versioned_constraints", version).Equal(table.VersionedConstraintsSchema(version)) {
			t.Fatal("historical schema flags differ")
		}
		previous = records
	}
}
