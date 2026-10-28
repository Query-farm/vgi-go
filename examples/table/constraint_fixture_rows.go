// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package table

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// NewRecordBatch retains its arrays. Transfer the factory's references so a
// catalog validation caller can release the complete row snapshot normally.
func constraintFixtureBatch(schema *arrow.Schema, columns []arrow.Array, rows int64) arrow.RecordBatch {
	batch := array.NewRecordBatch(schema, columns, rows)
	for _, column := range columns {
		column.Release()
	}
	return batch
}

// ConstraintFixtureRows returns the same immutable arrays emitted by these
// read-only scans. The caller owns the returned batch.
func ConstraintFixtureRows(name string, version int64) arrow.RecordBatch {
	switch name {
	case "departments":
		return departmentsFixtureRows()
	case "products":
		return productsFixtureRows()
	case "employees":
		return employeesFixtureRows()
	case "projects":
		return projectsFixtureRows()
	case "versioned_constraints":
		return versionedConstraintRows(version)
	default:
		panic("unknown immutable constraint fixture")
	}
}
