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

const GeneratedExpressionKey = "vgi.column.generated.v1"
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
	var visit func(arrow.Field) error
	visit = func(field arrow.Field) error {
		values := field.Metadata.ToMap()
		for key := range values {
			if (strings.HasPrefix(key, "vgi.column.generated.") && key != GeneratedExpressionKey) || (strings.HasPrefix(key, "vgi.column.generation.") && key != GenerationKey) {
				return fmt.Errorf("unknown generated column metadata version")
			}
		}
		if _, legacy := values["generated_expression"]; legacy {
			return fmt.Errorf("legacy generated expression requires explicit canonical translation")
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
	if end-start == 0 || end-start > 32 {
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
	for _, i := range []int{0, 1} {
		if !sqlText(context.Field(i).(*array.String).Value(0), 256) {
			return fmt.Errorf("invalid expression context")
		}
	}
	order := context.Field(2).(*array.String).Value(0)
	nullOrder := context.Field(3).(*array.String).Value(0)
	if order != "asc" && order != "desc" {
		return fmt.Errorf("invalid expression order")
	}
	if !slices.Contains([]string{"nulls_first", "nulls_last", "nulls_first_on_asc_last_on_desc", "nulls_last_on_asc_first_on_desc"}, nullOrder) {
		return fmt.Errorf("invalid expression NULL order")
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
