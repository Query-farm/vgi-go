// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// MacroType identifies the kind of macro.
type MacroType string

const (
	// MacroTypeScalar marks a scalar macro (expands to a SQL expression).
	MacroTypeScalar MacroType = "scalar"
	// MacroTypeTable marks a table macro (expands to a SQL query).
	MacroTypeTable MacroType = "table"
)

// macroKindFilter maps a schema_contents_macros "type" filter value (as sent
// by DuckDB) to a MacroType. Returns "" for an unrecognized value.
func macroKindFilter(s string) MacroType {
	switch s {
	case "scalar_macro", "SCALAR_MACRO", "scalar", "SCALAR":
		return MacroTypeScalar
	case "table_macro", "TABLE_MACRO", "table", "TABLE":
		return MacroTypeTable
	}
	return ""
}

// CatalogMacro describes a macro to register in the catalog.
type CatalogMacro struct {
	// Name is the macro name visible in SQL.
	Name string
	// MacroType is "scalar" or "table".
	MacroType MacroType
	// Parameters lists the parameter names in order.
	Parameters []string
	// ParameterDefaultValues is the serialized Arrow IPC bytes of a 1-row
	// RecordBatch containing default values (nil when no defaults).
	ParameterDefaultValues []byte
	// Definition is the SQL expression (scalar) or query (table).
	Definition string
	// Comment is a human-readable description.
	Comment string
	// Tags are arbitrary key/value annotations attached to this macro;
	// surfaced through MacroInfo.tags.
	Tags map[string]string
	// ParameterDocs is an optional mapping of parameter name to a
	// human/agent-facing description. Keys must appear in Parameters.
	// Descriptions flow over the wire via the macro arguments_schema's
	// vgi_doc field metadata (the same channel functions use for per-argument
	// docs). Empty/nil means no per-parameter docs.
	ParameterDocs map[string]string
}

// MacroInfo describes a macro in the catalog for wire serialization.
type MacroInfo struct {
	Name                   string
	SchemaName             string
	Comment                string
	Tags                   map[string]string
	MacroType              MacroType
	Parameters             []string
	ParameterDefaultValues []byte
	Definition             string
	// ArgumentsSchema is the optional macro arguments schema, serialized as
	// Arrow IPC bytes: one nullable field per parameter, in Parameters order,
	// each carrying its description via the vgi_doc field-metadata key (the
	// same channel functions use). nil when no per-parameter docs are supplied.
	ArgumentsSchema []byte
}

var macroInfoSchema = generated.MacroInfoSchema

// SerializeMacroInfo serializes a MacroInfo to IPC bytes.
func SerializeMacroInfo(info *MacroInfo) ([]byte, error) {
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

	// macro_type (dictionary encoded)
	mtBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer mtBuilder.Release()
	mtBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.MacroType))

	// parameters
	paramsBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer paramsBuilder.Release()
	paramsBuilder.Append(true)
	if len(info.Parameters) > 0 {
		vb := paramsBuilder.ValueBuilder().(*array.StringBuilder)
		for _, p := range info.Parameters {
			vb.Append(p)
		}
	}

	// parameter_default_values (binary, non-null; empty bytes when no defaults)
	pdvBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer pdvBuilder.Release()
	if info.ParameterDefaultValues != nil {
		pdvBuilder.Append(info.ParameterDefaultValues)
	} else {
		pdvBuilder.Append([]byte{})
	}

	// definition
	defBuilder := array.NewStringBuilder(mem)
	defer defBuilder.Release()
	defBuilder.Append(info.Definition)

	// arguments_schema (binary, non-null; empty bytes when no per-parameter docs)
	argsBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer argsBuilder.Release()
	if info.ArgumentsSchema != nil {
		argsBuilder.Append(info.ArgumentsSchema)
	} else {
		argsBuilder.Append([]byte{})
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaNameBuilder.NewArray(),
		mtBuilder.NewArray(),
		paramsBuilder.NewArray(),
		pdvBuilder.NewArray(),
		defBuilder.NewArray(),
		argsBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(macroInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(macroInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// macroInfoFromCatalogMacro builds the wire MacroInfo for a registered
// CatalogMacro in the given schema, including the per-parameter arguments_schema
// (carrying vgi_doc field metadata for documented parameters).
func macroInfoFromCatalogMacro(cm CatalogMacro, schemaName string) (*MacroInfo, error) {
	argsSchema, err := BuildMacroArgumentsSchema(cm.Parameters, cm.ParameterDefaultValues, cm.ParameterDocs)
	if err != nil {
		return nil, err
	}
	return &MacroInfo{
		Name:                   cm.Name,
		SchemaName:             schemaName,
		Comment:                cm.Comment,
		Tags:                   cm.Tags,
		MacroType:              cm.MacroType,
		Parameters:             cm.Parameters,
		ParameterDefaultValues: cm.ParameterDefaultValues,
		Definition:             cm.Definition,
		ArgumentsSchema:        argsSchema,
	}, nil
}

// MacroDefault describes a single parameter default value.
type MacroDefault struct {
	Name  string
	Value interface{}
	Type  arrow.DataType
}

// BuildMacroDefaultValues builds the serialized Arrow IPC bytes for macro
// parameter defaults. Returns a 1-row RecordBatch where column names are
// parameter names and values are typed defaults.
func BuildMacroDefaultValues(defaults []MacroDefault) ([]byte, error) {
	if len(defaults) == 0 {
		return nil, nil
	}
	mem := memory.NewGoAllocator()

	fields := make([]arrow.Field, len(defaults))
	builders := make([]array.Builder, len(defaults))
	for i, d := range defaults {
		fields[i] = arrow.Field{Name: d.Name, Type: d.Type}
		b := array.NewBuilder(mem, d.Type)
		appendValue(b, d.Value)
		builders[i] = b
	}

	schema := arrow.NewSchema(fields, nil)
	cols := make([]arrow.Array, len(builders))
	for i, b := range builders {
		cols[i] = b.NewArray()
		b.Release()
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(schema, cols, 1)
	defer batch.Release()

	data, err := SerializeRecordBatch(batch)
	if err != nil {
		return nil, fmt.Errorf("serializing macro default values: %w", err)
	}
	return data, nil
}

// BuildMacroArgumentsSchema builds the macro arguments_schema describing macro
// parameters, mirroring the function arguments_schema mechanism: one nullable
// Arrow field per parameter, in parameters order. Each parameter's field type is
// the type of its default value when known (derived from parameterDefaultValues,
// the serialized 1-row RecordBatch of typed defaults) else arrow.Null. The
// per-parameter description rides as field metadata under the same vgi_doc key
// functions use (UTF-8, presence-only — the key is omitted entirely when there
// is no doc).
//
// Returns nil when there are no parameters (nothing to describe). The returned
// bytes are Arrow IPC schema bytes suitable for MacroInfo.ArgumentsSchema and
// the macro create-request arguments_schema slot.
func BuildMacroArgumentsSchema(parameters []string, parameterDefaultValues []byte, parameterDocs map[string]string) ([]byte, error) {
	if len(parameters) == 0 {
		return nil, nil
	}

	// Map parameter name -> Arrow type from the typed default values, if any.
	defaultTypes := map[string]arrow.DataType{}
	if len(parameterDefaultValues) > 0 {
		defaultsSchema, err := DeserializeSchema(parameterDefaultValues)
		if err != nil {
			return nil, fmt.Errorf("reading macro default-value schema: %w", err)
		}
		for _, f := range defaultsSchema.Fields() {
			defaultTypes[f.Name] = f.Type
		}
	}

	fields := make([]arrow.Field, len(parameters))
	for i, name := range parameters {
		fieldType, ok := defaultTypes[name]
		if !ok || fieldType == nil {
			fieldType = arrow.Null
		}

		var fieldMeta arrow.Metadata
		if doc := parameterDocs[name]; doc != "" {
			fieldMeta = arrow.NewMetadata([]string{"vgi_doc"}, []string{doc})
		}

		fields[i] = arrow.Field{
			Name:     name,
			Type:     fieldType,
			Nullable: true,
			Metadata: fieldMeta,
		}
	}

	return SerializeSchema(arrow.NewSchema(fields, nil))
}

// MacroParameterDocsFromSchema extracts per-parameter descriptions from a macro
// arguments_schema (Arrow IPC schema bytes). Inverse of
// BuildMacroArgumentsSchema's vgi_doc handling: reads the vgi_doc field metadata
// (UTF-8) for each field. Fields without the key (undocumented) are omitted from
// the result. Returns an empty map when the input is empty or carries no docs.
func MacroParameterDocsFromSchema(argumentsSchema []byte) (map[string]string, error) {
	docs := map[string]string{}
	if len(argumentsSchema) == 0 {
		return docs, nil
	}
	schema, err := DeserializeSchema(argumentsSchema)
	if err != nil {
		return nil, fmt.Errorf("reading macro arguments schema: %w", err)
	}
	for _, f := range schema.Fields() {
		if idx := f.Metadata.FindKey("vgi_doc"); idx >= 0 {
			docs[f.Name] = f.Metadata.Values()[idx]
		}
	}
	return docs, nil
}
