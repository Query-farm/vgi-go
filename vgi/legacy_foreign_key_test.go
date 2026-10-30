// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// fkSchema is the generated wire format for a single foreign key constraint.
var fkSchema = generated.ForeignKeyInfoSchema

// serializeForeignKey serializes a ForeignKeyConstraint to the pre-v3
// ForeignKeyInfo record. No vgi.v3 TableInfo field carries it; it lives here
// only so the generated-schema conformance tests still cover the record.
func serializeForeignKey(schemaPath SchemaPath, fk *ForeignKeyConstraint) ([]byte, error) {
	mem := memory.NewGoAllocator()

	// fk_columns
	fkColBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer fkColBuilder.Release()
	fkColBuilder.Append(true)
	fkVB := fkColBuilder.ValueBuilder().(*array.StringBuilder)
	for _, col := range fk.Columns {
		fkVB.Append(col)
	}

	// pk_columns
	pkColBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer pkColBuilder.Release()
	pkColBuilder.Append(true)
	pkVB := pkColBuilder.ValueBuilder().(*array.StringBuilder)
	for _, col := range fk.ReferencedColumns {
		pkVB.Append(col)
	}

	// referenced_table
	refTableBuilder := array.NewStringBuilder(mem)
	defer refTableBuilder.Release()
	refTableBuilder.Append(fk.ReferencedTable)

	// referenced_schema_path
	refSchemaBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer refSchemaBuilder.Release()
	refSchema := fk.ReferencedSchemaPath
	if len(refSchema) == 0 {
		refSchema = schemaPath
	}
	appendSchemaPath(refSchemaBuilder, refSchema)

	cols := []arrow.Array{
		fkColBuilder.NewArray(),
		pkColBuilder.NewArray(),
		refTableBuilder.NewArray(),
		refSchemaBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(fkSchema, cols, 1)
	defer batch.Release()

	return SerializeRecordBatch(batch)
}
