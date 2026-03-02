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

// CatalogView describes a view to register in the catalog.
type CatalogView struct {
	// Name is the view name visible in SQL.
	Name string
	// Definition is the SQL query backing the view.
	Definition string
	// Comment is a human-readable description.
	Comment string
}

// ViewInfo describes a view in the catalog for wire serialization.
type ViewInfo struct {
	Name       string
	SchemaName string
	Comment    string
	Tags       map[string]string
	Definition string
}

// Field order matches Python MRO: CatalogObject(comment,tags) + CatalogSchemaObject(name,schema_name) + ViewInfo fields
var viewInfoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "comment", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "schema_name", Type: arrow.BinaryTypes.String},
	{Name: "definition", Type: arrow.BinaryTypes.String},
}, nil)

// SerializeViewInfo serializes a ViewInfo to IPC bytes.
func SerializeViewInfo(info *ViewInfo) ([]byte, error) {
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

	// definition
	defBuilder := array.NewStringBuilder(mem)
	defer defBuilder.Release()
	defBuilder.Append(info.Definition)

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaNameBuilder.NewArray(),
		defBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(viewInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(viewInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
