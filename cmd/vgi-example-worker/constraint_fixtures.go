// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// These fixed fixture identities describe metadata, not storage guarantees.
// This worker does not implement constraint enforcement or validation.
func fixtureConstraintRef(table, key string) []byte {
	return fixtureRecord(generated.CatalogObjectRefSchema, map[string]any{
		"kind": "constraint", "schema_path": []string{"data"}, "name": key, "owner_table": table,
	})
}

func fixtureConstraint(table, key, kind string, paths [][]string, check, foreign []byte) []byte {
	var checkValue, foreignValue, referenced any
	if check != nil {
		checkValue = check
	}
	if foreign != nil {
		foreignValue = foreign
		referenced = fixtureConstraintRef("departments", "fixture:departments:primary")
	}
	definition := fixtureRecord(generated.ConstraintDefinitionSchema, map[string]any{
		"kind": kind, "column_paths": paths, "declared_name": nil,
		"check_expression": checkValue, "foreign_key": foreignValue,
	})
	return fixtureRecord(generated.ConstraintInfoSchema, map[string]any{
		"ref": fixtureConstraintRef(table, key), "definition": definition,
		"enforcement": "METADATA_ONLY", "validation": "UNVALIDATED", "referenced_constraint": referenced,
	})
}

func fixtureDepartmentForeignKey() []byte {
	target := fixtureRecord(generated.CatalogObjectRefSchema, map[string]any{
		"kind": "table", "schema_path": []string{"data"}, "name": "departments", "owner_table": nil,
	})
	return fixtureRecord(generated.ForeignKeyDefinitionSchema, map[string]any{
		"referenced_table": target, "referenced_column_paths": [][]string{{"id"}},
		"match": "SIMPLE", "on_update": "NO_ACTION", "on_delete": "NO_ACTION",
	})
}

func fixtureConstraints(table string, version int64) [][]byte {
	switch table {
	case "departments":
		check := fixtureRecord(generated.SqlExpressionSchema, map[string]any{
			"sql": "budget >= 0", "profile": "vgi.expression.duckdb.scalar.v1", "schema_path": []string{"data"},
			"evaluation_context": map[string]any{
				"timezone": "UTC", "collation": "binary", "default_order": "asc",
				"default_null_order": "nulls_last", "preserve_insertion_order": true,
			},
		})
		return [][]byte{
			fixtureConstraint(table, "fixture:departments:nn:id", "NOT_NULL", [][]string{{"id"}}, nil, nil),
			fixtureConstraint(table, "fixture:departments:nn:name", "NOT_NULL", [][]string{{"name"}}, nil, nil),
			fixtureConstraint(table, "fixture:departments:primary", "PRIMARY_KEY", [][]string{{"id"}}, nil, nil),
			fixtureConstraint(table, "fixture:departments:unique:name", "UNIQUE", [][]string{{"name"}}, nil, nil),
			fixtureConstraint(table, "fixture:departments:check:budget", "CHECK", [][]string{}, check, nil),
		}
	case "products":
		return [][]byte{
			fixtureConstraint(table, "fixture:products:nn:id", "NOT_NULL", [][]string{{"id"}}, nil, nil),
			fixtureConstraint(table, "fixture:products:primary", "PRIMARY_KEY", [][]string{{"id"}}, nil, nil),
		}
	case "employees":
		return [][]byte{
			fixtureConstraint(table, "fixture:employees:nn:id", "NOT_NULL", [][]string{{"id"}}, nil, nil),
			fixtureConstraint(table, "fixture:employees:nn:name", "NOT_NULL", [][]string{{"name"}}, nil, nil),
			fixtureConstraint(table, "fixture:employees:nn:email", "NOT_NULL", [][]string{{"email"}}, nil, nil),
			fixtureConstraint(table, "fixture:employees:primary", "PRIMARY_KEY", [][]string{{"id"}}, nil, nil),
			fixtureConstraint(table, "fixture:employees:unique:email", "UNIQUE", [][]string{{"email"}}, nil, nil),
			fixtureConstraint(table, "fixture:employees:fk:department", "FOREIGN_KEY", [][]string{{"department_id"}}, nil, fixtureDepartmentForeignKey()),
		}
	case "projects":
		return [][]byte{
			fixtureConstraint(table, "fixture:projects:nn:department", "NOT_NULL", [][]string{{"department_id"}}, nil, nil),
			fixtureConstraint(table, "fixture:projects:nn:code", "NOT_NULL", [][]string{{"project_code"}}, nil, nil),
			fixtureConstraint(table, "fixture:projects:nn:title", "NOT_NULL", [][]string{{"title"}}, nil, nil),
			fixtureConstraint(table, "fixture:projects:primary", "PRIMARY_KEY", [][]string{{"department_id"}, {"project_code"}}, nil, nil),
			fixtureConstraint(table, "fixture:projects:fk:department", "FOREIGN_KEY", [][]string{{"department_id"}}, nil, fixtureDepartmentForeignKey()),
		}
	case "versioned_constraints":
		if version < 1 || version > 3 {
			panic("unknown constraint fixture version")
		}
		result := [][]byte{
			fixtureConstraint(table, "fixture:versioned_constraints:nn:id", "NOT_NULL", [][]string{{"id"}}, nil, nil),
			fixtureConstraint(table, "fixture:versioned_constraints:nn:name", "NOT_NULL", [][]string{{"name"}}, nil, nil),
			fixtureConstraint(table, "fixture:versioned_constraints:primary", "PRIMARY_KEY", [][]string{{"id"}}, nil, nil),
		}
		if version >= 2 {
			result = append(result, fixtureConstraint(table, "fixture:versioned_constraints:unique:email", "UNIQUE", [][]string{{"email"}}, nil, nil))
		}
		if version >= 3 {
			result = append(result, fixtureConstraint(table, "fixture:versioned_constraints:fk:department", "FOREIGN_KEY", [][]string{{"department_id"}}, nil, fixtureDepartmentForeignKey()))
		}
		return result
	default:
		panic("unknown constraint fixture table: " + table)
	}
}

// Metadata-only constraints cannot strengthen SQL field nullability. Preserve
// types and metadata while making this fixture's user columns nullable.
func fixtureNullableSchema(schema *arrow.Schema) *arrow.Schema {
	fields := schema.Fields()
	for i := range fields {
		fields[i].Nullable = true
	}
	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata)
}

// Use the generated schemas even for these manually assembled fixture values,
// so the actual examples exercise the same IPC contract as external providers.
func fixtureRecord(schema *arrow.Schema, values map[string]any) []byte {
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	for i, field := range schema.Fields() {
		value, ok := values[field.Name]
		if !ok || value == nil && !field.Nullable {
			panic("missing fixture field: " + field.Name)
		}
		appendFixtureValue(builder.Field(i), value)
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	encoded, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		panic(err)
	}
	return encoded
}

func appendFixtureValue(builder array.Builder, value any) {
	if value == nil {
		builder.AppendNull()
		return
	}
	switch b := builder.(type) {
	case *array.StringBuilder:
		b.Append(value.(string))
	case *array.BinaryBuilder:
		b.Append(value.([]byte))
	case *array.BooleanBuilder:
		b.Append(value.(bool))
	case *array.BinaryDictionaryBuilder:
		if err := b.AppendString(value.(string)); err != nil {
			panic(err)
		}
	case *array.ListBuilder:
		b.Append(true)
		switch values := value.(type) {
		case []string:
			for _, item := range values {
				appendFixtureValue(b.ValueBuilder(), item)
			}
		case [][]string:
			for _, item := range values {
				appendFixtureValue(b.ValueBuilder(), item)
			}
		default:
			panic(fmt.Sprintf("unsupported fixture list %T", value))
		}
	case *array.StructBuilder:
		b.Append(true)
		fields := b.Type().(*arrow.StructType).Fields()
		values := value.(map[string]any)
		for i, field := range fields {
			appendFixtureValue(b.FieldBuilder(i), values[field.Name])
		}
	default:
		panic(fmt.Sprintf("unsupported fixture builder %T", builder))
	}
}
