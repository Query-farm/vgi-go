// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TableInfo describes a table in the catalog for wire serialization.
type TableInfo struct {
	Name                  string
	SchemaName            string
	Comment               string
	Tags                  map[string]string
	Columns               *arrow.Schema // serialized as IPC schema bytes
	NotNullConstraints    []int32
	UniqueConstraints     [][]int32
	CheckConstraints      []string
	PrimaryKeyConstraints [][]int32
	ForeignKeyConstraints [][]byte // each []byte is an IPC-serialized FK RecordBatch
}

// Field order matches Python MRO: CatalogObject(comment,tags) + CatalogSchemaObject(name,schema_name) + TableInfo fields
var tableInfoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "comment", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "schema_name", Type: arrow.BinaryTypes.String},
	{Name: "columns", Type: arrow.BinaryTypes.Binary},
	{Name: "not_null_constraints", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
	{Name: "unique_constraints", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32))},
	{Name: "check_constraints", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	{Name: "primary_key_constraints", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32))},
	{Name: "foreign_key_constraints", Type: arrow.ListOf(arrow.BinaryTypes.Binary)},
}, nil)

// SerializeTableInfo serializes a TableInfo to IPC bytes.
func SerializeTableInfo(info *TableInfo) ([]byte, error) {
	mem := memory.NewGoAllocator()

	// comment
	commentBuilder := array.NewStringBuilder(mem)
	defer commentBuilder.Release()
	if info.Comment != "" {
		commentBuilder.Append(info.Comment)
	} else {
		commentBuilder.AppendNull()
	}

	// tags
	tagsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer tagsBuilder.Release()
	tagsBuilder.Append(true)
	if len(info.Tags) > 0 {
		kb := tagsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := tagsBuilder.ItemBuilder().(*array.StringBuilder)
		for k, v := range info.Tags {
			kb.Append(k)
			vb.Append(v)
		}
	}

	// name
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	// schema_name
	schemaNameBuilder := array.NewStringBuilder(mem)
	defer schemaNameBuilder.Release()
	schemaNameBuilder.Append(info.SchemaName)

	// columns (serialized as IPC schema bytes)
	columnsBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer columnsBuilder.Release()
	if info.Columns != nil {
		colBytes, err := SerializeSchema(info.Columns)
		if err != nil {
			return nil, err
		}
		columnsBuilder.Append(colBytes)
	} else {
		columnsBuilder.Append([]byte{})
	}

	// not_null_constraints
	notNullBuilder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer notNullBuilder.Release()
	notNullBuilder.Append(true)
	if len(info.NotNullConstraints) > 0 {
		vb := notNullBuilder.ValueBuilder().(*array.Int32Builder)
		for _, idx := range info.NotNullConstraints {
			vb.Append(idx)
		}
	}

	// unique_constraints (list of list of int32)
	uniqueBuilder := array.NewListBuilder(mem, arrow.ListOf(arrow.PrimitiveTypes.Int32))
	defer uniqueBuilder.Release()
	uniqueBuilder.Append(true)
	if len(info.UniqueConstraints) > 0 {
		innerListBuilder := uniqueBuilder.ValueBuilder().(*array.ListBuilder)
		for _, group := range info.UniqueConstraints {
			innerListBuilder.Append(true)
			innerVB := innerListBuilder.ValueBuilder().(*array.Int32Builder)
			for _, idx := range group {
				innerVB.Append(idx)
			}
		}
	}

	// check_constraints
	checkBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer checkBuilder.Release()
	checkBuilder.Append(true)
	if len(info.CheckConstraints) > 0 {
		vb := checkBuilder.ValueBuilder().(*array.StringBuilder)
		for _, expr := range info.CheckConstraints {
			vb.Append(expr)
		}
	}

	// primary_key_constraints (list of list of int32)
	pkBuilder := array.NewListBuilder(mem, arrow.ListOf(arrow.PrimitiveTypes.Int32))
	defer pkBuilder.Release()
	pkBuilder.Append(true)
	if len(info.PrimaryKeyConstraints) > 0 {
		innerListBuilder := pkBuilder.ValueBuilder().(*array.ListBuilder)
		for _, group := range info.PrimaryKeyConstraints {
			innerListBuilder.Append(true)
			innerVB := innerListBuilder.ValueBuilder().(*array.Int32Builder)
			for _, idx := range group {
				innerVB.Append(idx)
			}
		}
	}

	// foreign_key_constraints (list of binary)
	fkBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	defer fkBuilder.Release()
	fkBuilder.Append(true)
	if len(info.ForeignKeyConstraints) > 0 {
		vb := fkBuilder.ValueBuilder().(*array.BinaryBuilder)
		for _, fkBytes := range info.ForeignKeyConstraints {
			vb.Append(fkBytes)
		}
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaNameBuilder.NewArray(),
		columnsBuilder.NewArray(),
		notNullBuilder.NewArray(),
		uniqueBuilder.NewArray(),
		checkBuilder.NewArray(),
		pkBuilder.NewArray(),
		fkBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(tableInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(tableInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
