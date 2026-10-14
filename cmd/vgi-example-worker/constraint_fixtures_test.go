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

func TestConstraintFixtureDiscoveryIsMetadataOnly(t *testing.T) {
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
				Columns: fixtureNullableSchema(entry.schema), Constraints: records,
			})
			if err != nil {
				t.Fatal(err)
			}
			info := fixtureBatch(t, encoded)
			columns, err := vgi.DeserializeSchema(fixtureColumn(t, info, "columns").(*array.Binary).Value(0))
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range columns.Fields() {
				if !field.Nullable {
					t.Fatalf("metadata-only field %s claims nonnull", field.Name)
				}
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
				if fixtureEnum(t, constraint, "enforcement") != "METADATA_ONLY" || fixtureEnum(t, constraint, "validation") != "UNVALIDATED" {
					t.Fatal("fixture claims enforcement or validation")
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
		if len(records) != int(version)+2 {
			t.Fatalf("version %d has wrong definition set", version)
		}
		for i := range previous {
			if !bytes.Equal(previous[i], records[i]) {
				t.Fatal("surviving constraint identity or definition changed")
			}
		}
		previous = records
	}
}
