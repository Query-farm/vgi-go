// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"

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
	// Overloads is a complete canonical family; simple-definition fields must be empty.
	Overloads []generated.MacroOverload
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
	// Descriptions populate each canonical parameter's Documentation field.
	// A present empty description is distinct from an absent description.
	ParameterDocs map[string]string
}

// MacroInfo describes a macro in the catalog for wire serialization.
type MacroInfo struct {
	Name       string
	SchemaPath []string
	Comment    string
	Tags       map[string]string
	MacroType  MacroType
	Overloads  []generated.MacroOverload
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
		// Sorted, so the same record always encodes to the same bytes.
		for _, k := range slices.Sorted(maps.Keys(info.Tags)) {
			v := info.Tags[k]
			kb.Append(k)
			vb.Append(v)
		}
	}

	// name
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	// schema_path
	schemaPathBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer schemaPathBuilder.Release()
	appendSchemaPath(schemaPathBuilder, info.SchemaPath)

	// macro_type (dictionary encoded)
	mtBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer mtBuilder.Release()
	mtBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.MacroType))

	if len(info.Overloads) == 0 {
		return nil, fmt.Errorf("macro family must be nonempty")
	}
	overloadsBuilder := array.NewBuilder(mem, macroInfoSchema.Field(5).Type).(*array.ListBuilder)
	defer overloadsBuilder.Release()
	overloadsBuilder.Append(true)
	for _, overload := range info.Overloads {
		if !slices.Equal(overload.SchemaPath, info.SchemaPath) || !strings.EqualFold(overload.MacroType, string(info.MacroType)) {
			return nil, fmt.Errorf("macro overload kind and owner must equal parent macro")
		}
		if err := appendMacroOverload(overloadsBuilder.ValueBuilder().(*array.StructBuilder), overload); err != nil {
			return nil, err
		}
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaPathBuilder.NewArray(),
		mtBuilder.NewArray(),
		overloadsBuilder.NewArray(),
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
func macroInfoFromCatalogMacro(cm CatalogMacro, schemaPath SchemaPath) (*MacroInfo, error) {
	family := cm.Overloads
	if family != nil {
		if len(family) == 0 || len(cm.Parameters) != 0 || cm.Definition != "" || len(cm.ParameterDefaultValues) != 0 || len(cm.ParameterDocs) != 0 {
			return nil, fmt.Errorf("a nonempty macro family cannot coexist with simple-definition fields")
		}
	} else {
		if strings.TrimSpace(cm.Definition) == "" || strings.ContainsRune(cm.Definition, 0) {
			return nil, fmt.Errorf("macro definition must be nonempty SQL without NUL")
		}
		defaults := map[string][]byte{}
		if len(cm.ParameterDefaultValues) > 0 {
			reader, err := ipc.NewReader(bytes.NewReader(cm.ParameterDefaultValues))
			if err != nil {
				return nil, err
			}
			defer reader.Release()
			if !reader.Next() || reader.RecordBatch().NumRows() != 1 {
				return nil, fmt.Errorf("macro defaults require exactly one row")
			}
			batch := reader.RecordBatch()
			for i, field := range batch.Schema().Fields() {
				if !slices.Contains(cm.Parameters, field.Name) {
					return nil, fmt.Errorf("unknown macro default parameter %q", field.Name)
				}
				if _, exists := defaults[field.Name]; exists {
					return nil, fmt.Errorf("duplicate macro default")
				}
				metadata := batch.Schema().Metadata()
				single := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{field}, &metadata), []arrow.Array{batch.Column(i)}, 1)
				payload, err := SerializeRecordBatch(single)
				single.Release()
				if err != nil {
					return nil, err
				}
				defaults[field.Name] = payload
			}
			if reader.Next() || reader.Err() != nil {
				return nil, fmt.Errorf("macro defaults must contain one batch")
			}
		}
		for name := range cm.ParameterDocs {
			if !slices.Contains(cm.Parameters, name) {
				return nil, fmt.Errorf("unknown documented macro parameter %q", name)
			}
		}
		parameters := make([]generated.MacroParameter, 0, len(cm.Parameters))
		seen := map[string]bool{}
		seenDefault := false
		for _, name := range cm.Parameters {
			key := strings.ToLower(name)
			if name == "" || strings.ContainsRune(name, 0) || seen[key] {
				return nil, fmt.Errorf("invalid or duplicate macro parameter")
			}
			seen[key] = true
			value, hasDefault := defaults[name]
			if seenDefault && !hasDefault {
				return nil, fmt.Errorf("required parameter cannot follow a default")
			}
			seenDefault = seenDefault || hasDefault
			parameter := generated.MacroParameter{Name: name}
			if hasDefault {
				parameter.DefaultValue = &value
			}
			if doc, ok := cm.ParameterDocs[name]; ok {
				parameter.Documentation = &doc
			}
			parameters = append(parameters, parameter)
		}
		family = []generated.MacroOverload{{Parameters: parameters, Definition: cm.Definition, Profile: "duckdb.macro.v1",
			SchemaPath: schemaPath, MacroType: string(cm.MacroType), EvaluationContext: generated.QueryEvaluationContext{
				Timezone: "UTC", Collation: "binary", DefaultOrder: "asc", DefaultNullOrder: "nulls_last", PreserveInsertionOrder: true}}}
	}
	return &MacroInfo{Name: cm.Name, SchemaPath: schemaPath, Comment: cm.Comment, Tags: cm.Tags, MacroType: cm.MacroType, Overloads: family}, nil
}

func appendMacroOverload(builder *array.StructBuilder, value generated.MacroOverload) error {
	builder.Append(true)
	parameters := builder.FieldBuilder(0).(*array.ListBuilder)
	parameters.Append(true)
	param := parameters.ValueBuilder().(*array.StructBuilder)
	for _, p := range value.Parameters {
		if p.DefaultExpression != nil {
			return fmt.Errorf("discovery macro defaults must be resolved values")
		}
		param.Append(true)
		param.FieldBuilder(0).(*array.StringBuilder).Append(p.Name)
		for i, payload := range []*[]byte{p.DeclaredType, p.DefaultValue, p.DefaultExpression} {
			b := param.FieldBuilder(i + 1).(*array.BinaryBuilder)
			if payload == nil {
				b.AppendNull()
			} else {
				b.Append(*payload)
			}
		}
		doc := param.FieldBuilder(4).(*array.StringBuilder)
		if p.Documentation == nil {
			doc.AppendNull()
		} else {
			doc.Append(*p.Documentation)
		}
	}
	builder.FieldBuilder(1).(*array.StringBuilder).Append(value.Definition)
	builder.FieldBuilder(2).(*array.StringBuilder).Append(value.Profile)
	appendSchemaPath(builder.FieldBuilder(3).(*array.ListBuilder), value.SchemaPath)
	builder.FieldBuilder(4).(*array.StringBuilder).Append(value.MacroType)
	context := builder.FieldBuilder(5).(*array.StructBuilder)
	context.Append(true)
	for i, item := range []string{value.EvaluationContext.Timezone, value.EvaluationContext.Collation, value.EvaluationContext.DefaultOrder, value.EvaluationContext.DefaultNullOrder} {
		context.FieldBuilder(i).(*array.StringBuilder).Append(item)
	}
	context.FieldBuilder(4).(*array.BooleanBuilder).Append(value.EvaluationContext.PreserveInsertionOrder)
	return nil
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
		fields[i] = arrow.Field{Name: d.Name, Type: d.Type, Nullable: true}
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

// BuildMacroArgumentsSchema builds a local documentation schema describing macro
// parameters, mirroring the function arguments_schema mechanism: one nullable
// Arrow field per parameter, in parameters order. Each parameter's field type is
// the type of its default value when known (derived from parameterDefaultValues,
// the serialized 1-row RecordBatch of typed defaults) else arrow.Null. The
// per-parameter description rides as field metadata under the same vgi_doc key
// functions use (UTF-8, presence-only — the key is omitted entirely when there
// is no doc).
//
// Returns nil when there are no parameters (nothing to describe). The returned
// bytes are for local presentation only. MacroInfo and CREATE carry canonical
// overload parameters, including Documentation and a separate DeclaredType.
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
