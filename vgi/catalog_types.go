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

// FunctionInfo describes a function in the catalog.
type FunctionInfo struct {
	Name               string
	SchemaName         string
	FunctionType       FunctionType
	ArgSchema          *arrow.Schema // argument schema
	OutputSchema       *arrow.Schema // return schema
	Stability          FunctionStability
	NullHandling       NullHandling
	Description        string
	Comment            string
	Tags               map[string]string
	Categories         []string
	ProjectionPushdown *bool
	FilterPushdown     *bool
	RequiredSecrets    []SecretRequirement
}

var dictType = &arrow.DictionaryType{
	IndexType: arrow.PrimitiveTypes.Int16,
	ValueType: arrow.BinaryTypes.String,
}

// functionInfoSchema matches Python MRO: CatalogObject(comment,tags) + CatalogSchemaObject(name,schema_name) + FunctionInfo fields
var functionInfoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "comment", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "schema_name", Type: arrow.BinaryTypes.String},
	{Name: "function_type", Type: dictType},
	{Name: "arguments", Type: arrow.BinaryTypes.Binary},
	{Name: "output_schema", Type: arrow.BinaryTypes.Binary},
	{Name: "stability", Type: dictType, Nullable: true},
	{Name: "null_handling", Type: dictType, Nullable: true},
	{Name: "description", Type: arrow.BinaryTypes.String},
	{Name: "examples", Type: arrow.ListOf(arrow.BinaryTypes.Binary)},
	{Name: "categories", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	{Name: "projection_pushdown", Type: &arrow.BooleanType{}, Nullable: true},
	{Name: "filter_pushdown", Type: &arrow.BooleanType{}, Nullable: true},
	{Name: "order_preservation", Type: dictType, Nullable: true},
	{Name: "max_workers", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "required_secrets", Type: arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "secret_type", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "secret_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "scope", Type: arrow.BinaryTypes.String, Nullable: true},
	)), Nullable: true},
}, nil)

// SerializeFunctionInfo serializes a FunctionInfo to IPC bytes.
func SerializeFunctionInfo(info *FunctionInfo) ([]byte, error) {
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
	schemaName := info.SchemaName
	if schemaName == "" {
		schemaName = "main"
	}
	schemaNameBuilder.Append(schemaName)

	// function_type
	ftBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer ftBuilder.Release()
	ftBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.FunctionType))

	// arguments
	argBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer argBuilder.Release()
	if info.ArgSchema != nil {
		argBytes, err := SerializeSchema(info.ArgSchema)
		if err != nil {
			return nil, err
		}
		argBuilder.Append(argBytes)
	} else {
		argBuilder.Append([]byte{})
	}

	// output_schema
	outputBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer outputBuilder.Release()
	if info.OutputSchema != nil {
		outBytes, err := SerializeSchema(info.OutputSchema)
		if err != nil {
			return nil, err
		}
		outputBuilder.Append(outBytes)
	} else {
		outputBuilder.Append([]byte{})
	}

	// stability
	stabBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer stabBuilder.Release()
	if info.Stability != "" {
		stabBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.Stability))
	} else {
		stabBuilder.AppendNull()
	}

	// null_handling
	nhBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer nhBuilder.Release()
	if info.NullHandling != "" {
		nhBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.NullHandling))
	} else {
		nhBuilder.AppendNull()
	}

	// description
	descBuilder := array.NewStringBuilder(mem)
	defer descBuilder.Release()
	descBuilder.Append(info.Description)

	// examples (empty list)
	examplesBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	defer examplesBuilder.Release()
	examplesBuilder.Append(true) // non-null empty list

	// categories
	categoriesBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer categoriesBuilder.Release()
	categoriesBuilder.Append(true)
	if len(info.Categories) > 0 {
		vb := categoriesBuilder.ValueBuilder().(*array.StringBuilder)
		for _, cat := range info.Categories {
			vb.Append(cat)
		}
	}

	// projection_pushdown
	ppBuilder := array.NewBooleanBuilder(mem)
	defer ppBuilder.Release()
	if info.ProjectionPushdown != nil {
		ppBuilder.Append(*info.ProjectionPushdown)
	} else {
		ppBuilder.AppendNull()
	}

	// filter_pushdown
	fpBuilder := array.NewBooleanBuilder(mem)
	defer fpBuilder.Release()
	if info.FilterPushdown != nil {
		fpBuilder.Append(*info.FilterPushdown)
	} else {
		fpBuilder.AppendNull()
	}

	// order_preservation
	opBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer opBuilder.Release()
	opBuilder.AppendNull()

	// max_workers
	mwBuilder := array.NewInt32Builder(mem)
	defer mwBuilder.Release()
	mwBuilder.AppendNull()

	// required_secrets
	rsStructType := arrow.StructOf(
		arrow.Field{Name: "secret_type", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "secret_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "scope", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	rsBuilder := array.NewListBuilder(mem, rsStructType)
	defer rsBuilder.Release()
	if len(info.RequiredSecrets) > 0 {
		rsBuilder.Append(true)
		sb := rsBuilder.ValueBuilder().(*array.StructBuilder)
		for _, rs := range info.RequiredSecrets {
			sb.Append(true)
			sb.FieldBuilder(0).(*array.StringBuilder).Append(rs.SecretType)
			if rs.SecretName != "" {
				sb.FieldBuilder(1).(*array.StringBuilder).Append(rs.SecretName)
			} else {
				sb.FieldBuilder(1).(*array.StringBuilder).AppendNull()
			}
			if rs.Scope != "" {
				sb.FieldBuilder(2).(*array.StringBuilder).Append(rs.Scope)
			} else {
				sb.FieldBuilder(2).(*array.StringBuilder).AppendNull()
			}
		}
	} else {
		rsBuilder.AppendNull()
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaNameBuilder.NewArray(),
		ftBuilder.NewArray(),
		argBuilder.NewArray(),
		outputBuilder.NewArray(),
		stabBuilder.NewArray(),
		nhBuilder.NewArray(),
		descBuilder.NewArray(),
		examplesBuilder.NewArray(),
		categoriesBuilder.NewArray(),
		ppBuilder.NewArray(),
		fpBuilder.NewArray(),
		opBuilder.NewArray(),
		mwBuilder.NewArray(),
		rsBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(functionInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(functionInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// SchemaInfo describes a schema in the catalog.
type SchemaInfo struct {
	Name     string
	Comment  string
	Tags     map[string]string
	AttachID []byte
}

// Field order matches Python MRO: CatalogObject(comment, tags) then SchemaInfo(attach_id, name)
var schemaInfoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "comment", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
	{Name: "attach_id", Type: arrow.BinaryTypes.Binary},
	{Name: "name", Type: arrow.BinaryTypes.String},
}, nil)

// SerializeSchemaInfo serializes a SchemaInfo to IPC bytes.
func SerializeSchemaInfo(info *SchemaInfo) ([]byte, error) {
	mem := memory.NewGoAllocator()

	commentBuilder := array.NewStringBuilder(mem)
	defer commentBuilder.Release()
	if info.Comment != "" {
		commentBuilder.Append(info.Comment)
	} else {
		commentBuilder.AppendNull()
	}

	tagsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer tagsBuilder.Release()
	tagsBuilder.Append(true) // always non-null, empty map if no tags
	if len(info.Tags) > 0 {
		kb := tagsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := tagsBuilder.ItemBuilder().(*array.StringBuilder)
		for k, v := range info.Tags {
			kb.Append(k)
			vb.Append(v)
		}
	}

	attachIDBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer attachIDBuilder.Release()
	if info.AttachID != nil {
		attachIDBuilder.Append(info.AttachID)
	} else {
		attachIDBuilder.Append([]byte{})
	}

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		attachIDBuilder.NewArray(),
		nameBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(schemaInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schemaInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
