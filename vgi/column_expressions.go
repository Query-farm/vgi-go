// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GeneratedExpressionKey stores a canonical serialized generated-column expression.
const GeneratedExpressionKey = "vgi.column.generated.v1"

// GenerationKey identifies the generated column evaluation category.
const GenerationKey = "vgi.column.generation.v1"
const scalarExpressionProfile = "vgi.expression.duckdb.scalar.v1"

func sqlText(value string, maximum int) bool {
	return value != "" && !strings.ContainsRune(value, 0) && utf8.ValidString(value) && len(value) <= maximum
}

// ValidateColumnExpressions checks canonical VIRTUAL metadata without evaluating SQL.
// Writable providers that cannot honor generated columns pass allowGenerated=false.
func ValidateColumnExpressions(schema *arrow.Schema, owner []string, allowGenerated bool) error {
	if schema == nil {
		return nil
	}
	var visitType func(arrow.DataType) error
	visit := func(field arrow.Field) error {
		values := field.Metadata.ToMap()
		for key := range values {
			if (strings.HasPrefix(key, "vgi.column.generated.") && key != GeneratedExpressionKey) || (strings.HasPrefix(key, "vgi.column.generation.") && key != GenerationKey) {
				return fmt.Errorf("unknown generated column metadata version")
			}
		}
		if _, legacy := values[legacyGeneratedKey]; legacy {
			return fmt.Errorf("column %q: legacy generated_expression metadata is not read under vgi.v3; "+
				"use CatalogTable.Generated or vgi.GeneratedExpressionMetadata (SerializeTableInfo translates it automatically when unambiguous)", field.Name)
		}
		encoded, expression := values[GeneratedExpressionKey]
		category, generation := values[GenerationKey]
		if expression != generation {
			return fmt.Errorf("generated expression and category must occur together")
		}
		if expression {
			if category != "virtual" {
				return fmt.Errorf("only VIRTUAL generated columns are supported")
			}
			if _, exists := values["default"]; exists {
				return fmt.Errorf("generated column cannot have a default")
			}
			if _, exists := values["vgi.column.default.v1"]; exists {
				return fmt.Errorf("generated column cannot have a default")
			}
			if err := validateGeneratedExpression(encoded, owner); err != nil {
				return err
			}
			if !allowGenerated {
				return methodNotImplemented("generated columns are unsupported by this writable catalog")
			}
		}
		return visitType(field.Type)
	}
	visitType = func(t arrow.DataType) error {
		if e, ok := t.(arrow.ExtensionType); ok {
			return visitType(e.StorageType())
		}
		if d, ok := t.(*arrow.DictionaryType); ok {
			return visitType(d.ValueType)
		}
		if n, ok := t.(arrow.NestedType); ok {
			for _, f := range n.Fields() {
				if err := visit(f); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, f := range schema.Fields() {
		if err := visit(f); err != nil {
			return err
		}
	}
	return nil
}

func validateGeneratedExpression(encoded string, owner []string) error {
	if len(encoded) == 0 || len(encoded) > 4*((8*1024*1024+2)/3) {
		return fmt.Errorf("invalid generated expression size")
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(payload) != encoded || len(payload) == 0 || len(payload) > 8*1024*1024 {
		return fmt.Errorf("generated expression requires canonical base64 IPC")
	}
	source := bytes.NewReader(payload)
	reader, err := ipc.NewReader(source)
	if err != nil {
		return err
	}
	defer reader.Release()
	if !reader.Next() {
		return fmt.Errorf("generated expression requires one row")
	}
	row := reader.RecordBatch()
	if row.NumRows() != 1 || !row.Schema().Equal(generated.SqlExpressionSchema) {
		return fmt.Errorf("invalid SqlExpression record schema")
	}
	for _, column := range row.Columns() {
		if column.IsNull(0) {
			return fmt.Errorf("required SqlExpression field is null")
		}
	}
	sql := row.Column(0).(*array.String).Value(0)
	profile := row.Column(1).(*array.String).Value(0)
	if !sqlText(sql, 1024*1024) || strings.TrimSpace(sql) == "" || profile != scalarExpressionProfile {
		return fmt.Errorf("invalid or unsupported generated expression")
	}
	path := row.Column(2).(*array.List)
	start, end := path.ValueOffsets(0)
	parts := path.ListValues().(*array.String)
	if end-start > 32 {
		return fmt.Errorf("invalid expression owner")
	}
	names := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		if parts.IsNull(int(i)) || !sqlText(parts.Value(int(i)), 4096) {
			return fmt.Errorf("invalid expression owner")
		}
		names = append(names, parts.Value(int(i)))
	}
	if !slices.Equal(names, owner) {
		return fmt.Errorf("generated expression owner differs from table schema")
	}
	context := row.Column(3).(*array.Struct)
	for i := 0; i < context.NumField(); i++ {
		if context.Field(i).IsNull(0) {
			return fmt.Errorf("required expression context is null")
		}
	}
	// The extension reads only the default explicit evaluation context.
	if !defaultEvaluationContext(context) {
		return fmt.Errorf("unsupported generated expression evaluation context; DuckDB reads only UTC/binary/asc/nulls_last/preserve_insertion_order")
	}
	if reader.Next() || reader.Err() != nil || source.Len() != 0 {
		return fmt.Errorf("generated expression requires exactly one complete IPC batch")
	}
	return nil
}

// GeneratedExpressionMetadata translates a SQL convenience value using an explicit
// owner and the SDK's default evaluation context. It does not bind or execute SQL.
func GeneratedExpressionMetadata(sql string, owner []string) (string, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, generated.SqlExpressionSchema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).Append(sql)
	builder.Field(1).(*array.StringBuilder).Append(scalarExpressionProfile)
	path := builder.Field(2).(*array.ListBuilder)
	path.Append(true)
	for _, part := range owner {
		path.ValueBuilder().(*array.StringBuilder).Append(part)
	}
	context := builder.Field(3).(*array.StructBuilder)
	context.Append(true)
	for i, value := range []string{"UTC", "binary", "asc", "nulls_last"} {
		context.FieldBuilder(i).(*array.StringBuilder).Append(value)
	}
	context.FieldBuilder(4).(*array.BooleanBuilder).Append(true)
	row := builder.NewRecordBatch()
	defer row.Release()
	payload, err := SerializeRecordBatch(row)
	if err != nil {
		return "", err
	}
	encoded := base64.StdEncoding.EncodeToString(payload)
	return encoded, validateGeneratedExpression(encoded, owner)
}

// legacyGeneratedKey is the pre-v3 generated-column metadata key, whose value
// was the bare SQL expression.
const legacyGeneratedKey = "generated_expression"

// translateLegacyGenerated rewrites top-level legacy generated_expression
// metadata into the canonical VIRTUAL form owned by schemaPath. It refuses
// the ambiguous cases: canonical keys alongside the legacy one, or an empty
// expression. Fields without the legacy key are untouched.
func translateLegacyGenerated(schema *arrow.Schema, owner []string) (*arrow.Schema, error) {
	if schema == nil {
		return nil, nil
	}
	var fields []arrow.Field
	for i, field := range schema.Fields() {
		index := field.Metadata.FindKey(legacyGeneratedKey)
		if index < 0 {
			continue
		}
		sql := field.Metadata.Values()[index]
		if field.Metadata.FindKey(GeneratedExpressionKey) >= 0 || field.Metadata.FindKey(GenerationKey) >= 0 {
			return nil, fmt.Errorf("column %q carries both legacy generated_expression and canonical %s metadata", field.Name, GeneratedExpressionKey)
		}
		if strings.TrimSpace(sql) == "" {
			return nil, fmt.Errorf("column %q: legacy generated_expression is empty", field.Name)
		}
		encoded, err := GeneratedExpressionMetadata(sql, owner)
		if err != nil {
			return nil, fmt.Errorf("column %q: translating legacy generated_expression: %w", field.Name, err)
		}
		values := field.Metadata.ToMap()
		delete(values, legacyGeneratedKey)
		values[GeneratedExpressionKey] = encoded
		values[GenerationKey] = "virtual"
		if fields == nil {
			fields = slices.Clone(schema.Fields())
		}
		fields[i].Metadata = arrow.MetadataFrom(values)
	}
	if fields == nil {
		return schema, nil
	}
	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata), nil
}

// validateGeneratedColumns mirrors the extension's restrictions on the
// generated columns of a served table: Boolean, signed integer or string type
// without an extension type, the bounded generated-expression grammar over
// existing generated-primitive columns, and at least one physical column.
// Nullability is served by ServedTableColumns. Canonical metadata must
// already have passed ValidateColumnExpressions.
func validateGeneratedColumns(schema *arrow.Schema) error {
	if schema == nil {
		return nil
	}
	generatedCount, physical := 0, 0
	for _, field := range schema.Fields() {
		index := field.Metadata.FindKey(GeneratedExpressionKey)
		if index < 0 {
			if !isRowIDField(field) {
				physical++
			}
			continue
		}
		generatedCount++
		if !generatedPrimitiveArrow(field) {
			return fmt.Errorf("generated column %q has type %s; DuckDB reads generated columns only as BOOLEAN, TINYINT..BIGINT or VARCHAR", field.Name, field.Type)
		}
		payload, err := base64.StdEncoding.DecodeString(field.Metadata.Values()[index])
		if err != nil {
			return fmt.Errorf("generated column %q: %w", field.Name, err)
		}
		row, err := readCanonicalRow(payload, generated.SqlExpressionSchema)
		if err != nil {
			return fmt.Errorf("generated column %q: %w", field.Name, err)
		}
		sql := rowColumn(row, "sql").(*array.String).Value(0)
		row.Release()
		if err := validateGeneratedSQL(sql, schema, field.Name); err != nil {
			return fmt.Errorf("generated column %q (%s): %w", field.Name, sql, err)
		}
	}
	if generatedCount > 0 && physical == 0 {
		return fmt.Errorf("a table with generated columns needs at least one physical column")
	}
	return nil
}
