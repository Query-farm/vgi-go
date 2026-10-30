// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"github.com/Query-farm/vgi-go/examples/table"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// Fixed provider identities for immutable, validated read-only fixture rows.
// The fixture tables have no write endpoints and their rows are validated
// against these definitions before registration (fixtureConstraints), so
// every record is attested ENFORCED + ALL_ROWS.
func fixtureConstraint(table, key string, kind vgi.ConstraintKind, paths [][]string, checkSQL string, foreign bool) []byte {
	spec := vgi.ConstraintRecordSpec{
		SchemaPath: []string{"data"}, Table: table, ID: key, Kind: kind, Columns: paths, CheckSQL: checkSQL,
		Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
	}
	if foreign {
		spec.References = &fixtureDepartmentKey
	}
	record, err := vgi.ConstraintRecord(spec)
	if err != nil {
		panic(err)
	}
	return record
}

// fixtureDepartmentKey is the one foreign key target: departments' primary key.
var fixtureDepartmentKey = vgi.ConstraintReference{
	SchemaPath: []string{"data"}, Table: "departments", Columns: [][]string{{"id"}},
	ConstraintID: "fixture:departments:primary",
}

func fixtureDefinitions(table string, version int64) [][]byte {
	switch table {
	case "departments":
		return [][]byte{
			fixtureConstraint(table, "fixture:departments:nn:id", vgi.ConstraintNotNull, [][]string{{"id"}}, "", false),
			fixtureConstraint(table, "fixture:departments:nn:name", vgi.ConstraintNotNull, [][]string{{"name"}}, "", false),
			fixtureConstraint(table, "fixture:departments:primary", vgi.ConstraintPrimaryKey, [][]string{{"id"}}, "", false),
			fixtureConstraint(table, "fixture:departments:unique:name", vgi.ConstraintUnique, [][]string{{"name"}}, "", false),
			fixtureConstraint(table, "fixture:departments:check:budget", vgi.ConstraintCheck, nil, "budget >= 0", false),
		}
	case "products":
		return [][]byte{
			fixtureConstraint(table, "fixture:products:nn:id", vgi.ConstraintNotNull, [][]string{{"id"}}, "", false),
			fixtureConstraint(table, "fixture:products:primary", vgi.ConstraintPrimaryKey, [][]string{{"id"}}, "", false),
		}
	case "employees":
		return [][]byte{
			fixtureConstraint(table, "fixture:employees:nn:id", vgi.ConstraintNotNull, [][]string{{"id"}}, "", false),
			fixtureConstraint(table, "fixture:employees:nn:name", vgi.ConstraintNotNull, [][]string{{"name"}}, "", false),
			fixtureConstraint(table, "fixture:employees:nn:email", vgi.ConstraintNotNull, [][]string{{"email"}}, "", false),
			fixtureConstraint(table, "fixture:employees:primary", vgi.ConstraintPrimaryKey, [][]string{{"id"}}, "", false),
			fixtureConstraint(table, "fixture:employees:unique:email", vgi.ConstraintUnique, [][]string{{"email"}}, "", false),
			fixtureConstraint(table, "fixture:employees:fk:department", vgi.ConstraintForeignKey, [][]string{{"department_id"}}, "", true),
		}
	case "projects":
		return [][]byte{
			fixtureConstraint(table, "fixture:projects:nn:department", vgi.ConstraintNotNull, [][]string{{"department_id"}}, "", false),
			fixtureConstraint(table, "fixture:projects:nn:code", vgi.ConstraintNotNull, [][]string{{"project_code"}}, "", false),
			fixtureConstraint(table, "fixture:projects:nn:title", vgi.ConstraintNotNull, [][]string{{"title"}}, "", false),
			fixtureConstraint(table, "fixture:projects:primary", vgi.ConstraintPrimaryKey, [][]string{{"department_id"}, {"project_code"}}, "", false),
			fixtureConstraint(table, "fixture:projects:fk:department", vgi.ConstraintForeignKey, [][]string{{"department_id"}}, "", true),
		}
	case "versioned_constraints":
		if version < 1 || version > 3 {
			panic("unknown constraint fixture version")
		}
		result := [][]byte{
			fixtureConstraint(table, "fixture:versioned_constraints:nn:id", vgi.ConstraintNotNull, [][]string{{"id"}}, "", false),
		}
		if version >= 2 {
			result = append(result,
				fixtureConstraint(table, "fixture:versioned_constraints:nn:name", vgi.ConstraintNotNull, [][]string{{"name"}}, "", false),
				fixtureConstraint(table, "fixture:versioned_constraints:primary", vgi.ConstraintPrimaryKey, [][]string{{"id"}}, "", false),
				fixtureConstraint(table, "fixture:versioned_constraints:unique:email", vgi.ConstraintUnique, [][]string{{"email"}}, "", false))
		}
		if version >= 3 {
			result = append(result, fixtureConstraint(table, "fixture:versioned_constraints:fk:department", vgi.ConstraintForeignKey, [][]string{{"department_id"}}, "", true))
		}
		return result
	default:
		panic("unknown constraint fixture table: " + table)
	}
}

// Validation uses the arrays the scan actually emits, before trusted metadata
// is registered. These tables have no write endpoints.
func fixtureConstraints(name string, version int64) [][]byte {
	batch := table.ConstraintFixtureRows(name, version)
	defer batch.Release()
	definitions := fixtureDefinitions(name, version)
	if err := validateFixtureRows(batch, definitions); err != nil {
		panic(err)
	}
	return definitions
}

func fixtureSchema(schema *arrow.Schema, name string, version int64) *arrow.Schema {
	required := map[string]bool{}
	for _, encoded := range fixtureDefinitions(name, version) {
		record, err := vgi.ReadConstraintRecord(encoded)
		if err != nil {
			panic(err)
		}
		if record.Kind == vgi.ConstraintNotNull || record.Kind == vgi.ConstraintPrimaryKey {
			for _, path := range record.Columns {
				required[path[0]] = true
			}
		}
	}
	fields := schema.Fields()
	for i := range fields {
		fields[i].Nullable = !required[fields[i].Name]
	}
	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata)
}
