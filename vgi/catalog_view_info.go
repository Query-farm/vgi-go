// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// CatalogView describes a view to register in the catalog.
type CatalogView struct {
	// Name is the view name visible in SQL.
	Name string
	// Definition is the SQL query backing the view.
	Definition string
	// Comment is a human-readable description.
	Comment string
	// Tags are arbitrary key/value annotations attached to this view;
	// surfaced through ViewInfo.tags.
	Tags map[string]string
	// ColumnComments maps output column names to per-column comments. The
	// C++ extension aligns these by name against the columns DuckDB binds
	// from the view definition and surfaces them via duckdb_columns().comment.
	ColumnComments map[string]string
}

// ViewInfo describes a view in the catalog for wire serialization.
type ViewInfo struct {
	Name           string
	SchemaName     string
	Comment        string
	Tags           map[string]string
	Definition     string
	ColumnComments map[string]string
}

var viewInfoSchema = generated.ViewInfoSchema

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

	// column_comments
	colCommentsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer colCommentsBuilder.Release()
	colCommentsBuilder.Append(true)
	if len(info.ColumnComments) > 0 {
		kb := colCommentsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := colCommentsBuilder.ItemBuilder().(*array.StringBuilder)
		for k, v := range info.ColumnComments {
			kb.Append(k)
			vb.Append(v)
		}
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaNameBuilder.NewArray(),
		defBuilder.NewArray(),
		colCommentsBuilder.NewArray(),
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
