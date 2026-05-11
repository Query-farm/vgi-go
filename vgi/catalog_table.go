// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// Sql represents a raw SQL expression that should be passed through verbatim
// (e.g. current_timestamp, nextval('seq')).
type Sql string

// CatalogTable describes a table to register in the catalog.
type CatalogTable struct {
	// Name is the table name visible in SQL.
	Name string
	// Comment is a human-readable description.
	Comment string
	// Columns is the explicit column schema. If nil and Function is set,
	// columns are derived from the function's OnBind response.
	Columns *arrow.Schema
	// Function is the backing table function (nil for handler-only tables).
	Function TableFunction
	// FuncArgs are the arguments to pass when calling the backing function.
	FuncArgs []CatalogTableArg
	// NotNull lists column names with NOT NULL constraints.
	NotNull []string
	// Unique lists groups of column names for UNIQUE constraints.
	Unique [][]string
	// Check lists check constraint expressions.
	Check []string
	// PrimaryKey lists groups of column names for PRIMARY KEY constraints.
	PrimaryKey [][]string
	// ForeignKey lists foreign key definitions.
	ForeignKey []ForeignKeyConstraint
	// Defaults maps column names to their default values.
	// Supported types: Sql (raw SQL), string, int/int64/int32, float64/float32, bool, nil.
	Defaults map[string]any
	// ColumnComments maps column names to per-column comments surfaced
	// through duckdb_columns().comment.
	ColumnComments map[string]string
	// Generated maps column names to SQL expressions for generated (virtual)
	// columns. Encoded as `generated_expression` Arrow field metadata.
	Generated map[string]string
	// Statistics holds optimizer hints per column name. When non-empty,
	// the catalog reports supports_column_statistics=true for this table and
	// answers catalog_table_column_statistics_get from this map.
	Statistics map[string]*ColumnStatistics
	// StatisticsCacheMaxAgeSeconds, when set, is emitted as
	// cache_max_age_seconds schema metadata on the stats batch.
	// Nil means cache indefinitely; 0 means do not cache.
	StatisticsCacheMaxAgeSeconds *int64
	// SupportsTimeTravel indicates this table supports AT (VERSION/TIMESTAMP) queries.
	SupportsTimeTravel bool
	// CardinalityEstimate, when non-nil, inlines the table's row-count estimate
	// on TableInfo. The C++ extension uses it directly and skips the per-bind
	// table_function_cardinality RPC. Use only for read-only / slow-changing
	// tables where cardinality is statically known.
	CardinalityEstimate *int64
	// CardinalityMax mirrors CardinalityEstimate for the cardinality upper bound.
	CardinalityMax *int64
}

// ForeignKeyConstraint describes a foreign key relationship.
type ForeignKeyConstraint struct {
	// Columns are the column names in this table.
	Columns []string
	// ReferencedTable is the name of the referenced table.
	ReferencedTable string
	// ReferencedColumns are the column names in the referenced table.
	ReferencedColumns []string
	// ReferencedSchema is the schema of the referenced table (empty = same schema).
	ReferencedSchema string
}

// CatalogTableArg describes a single argument for a function-backed table.
type CatalogTableArg struct {
	// Position is the 0-based positional index, or -1 for named arguments.
	Position int
	// Name is the argument name (for named args).
	Name string
	// Value is the Go value (int64, float64, string, bool, []byte).
	Value interface{}
	// Type is the Arrow type for serialization.
	Type arrow.DataType
}

// ScanFunctionGetHandler is a callback for resolving table scan functions
// that are not backed by a registered CatalogTable with a Function field.
// atUnit and atValue carry time-travel AT clause parameters (both nil when absent).
type ScanFunctionGetHandler func(schemaName, tableName string, atUnit, atValue *string) (*ScanFunctionResult, error)

// TableGetHandler is a callback for customizing catalog_table_get responses,
// e.g. to return version-specific schemas for time-travel queries.
// Return nil to fall through to the default table lookup.
type TableGetHandler func(schemaName, tableName string, atUnit, atValue *string) ([]byte, error)

// ScanFunctionResult describes the function to call when scanning a catalog table.
type ScanFunctionResult struct {
	// FunctionName is the name of the function to invoke.
	FunctionName string
	// PositionalArguments are the positional arguments.
	PositionalArguments []ScanArg
	// NamedArguments are the named arguments.
	NamedArguments map[string]ScanArg
	// RequiredExtensions lists DuckDB extensions that must be loaded.
	RequiredExtensions []string
}

// ScanArg is a single argument value with its Arrow type.
type ScanArg struct {
	Value interface{}
	Type  arrow.DataType
}

// scanFunctionResultSchema is the wire format for ScanFunctionResult.
var scanFunctionResultSchema = generated.ScanFunctionResultSchema

// SerializeScanFunctionResult serializes a ScanFunctionResult to IPC bytes.
func SerializeScanFunctionResult(result *ScanFunctionResult) ([]byte, error) {
	mem := memory.NewGoAllocator()

	// Build the arguments batch
	argBytes, err := serializeScanArgs(mem, result.PositionalArguments, result.NamedArguments)
	if err != nil {
		return nil, fmt.Errorf("serializing scan arguments: %w", err)
	}

	// function_name
	fnNameBuilder := array.NewStringBuilder(mem)
	defer fnNameBuilder.Release()
	fnNameBuilder.Append(result.FunctionName)

	// arguments
	argBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer argBuilder.Release()
	argBuilder.Append(argBytes)

	// required_extensions
	extBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer extBuilder.Release()
	extBuilder.Append(true)
	if len(result.RequiredExtensions) > 0 {
		vb := extBuilder.ValueBuilder().(*array.StringBuilder)
		for _, ext := range result.RequiredExtensions {
			vb.Append(ext)
		}
	}

	cols := []arrow.Array{
		fnNameBuilder.NewArray(),
		argBuilder.NewArray(),
		extBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(scanFunctionResultSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(scanFunctionResultSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// serializeScanArgs builds and serializes the nested arguments batch.
// Positional args are named arg_0, arg_1, ...; named args use their name.
func serializeScanArgs(mem memory.Allocator, positional []ScanArg, named map[string]ScanArg) ([]byte, error) {
	// Count total fields
	nFields := len(positional) + len(named)
	if nFields == 0 {
		// Empty arguments: serialize an empty schema
		emptySchema := arrow.NewSchema(nil, nil)
		return SerializeSchema(emptySchema)
	}

	fields := make([]arrow.Field, 0, nFields)
	builders := make([]array.Builder, 0, nFields)

	// Positional arguments
	for i, arg := range positional {
		name := fmt.Sprintf("arg_%d", i)
		fields = append(fields, arrow.Field{Name: name, Type: arg.Type})
		b := array.NewBuilder(mem, arg.Type)
		appendValue(b, arg.Value)
		builders = append(builders, b)
	}

	// Named arguments
	for name, arg := range named {
		fields = append(fields, arrow.Field{Name: name, Type: arg.Type})
		b := array.NewBuilder(mem, arg.Type)
		appendValue(b, arg.Value)
		builders = append(builders, b)
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

	return SerializeRecordBatch(batch)
}

// appendValue appends a Go value to the appropriate Arrow builder.
func appendValue(b array.Builder, val interface{}) {
	switch v := val.(type) {
	case bool:
		b.(*array.BooleanBuilder).Append(v)
	case int:
		b.(*array.Int64Builder).Append(int64(v))
	case int64:
		b.(*array.Int64Builder).Append(v)
	case int32:
		b.(*array.Int32Builder).Append(v)
	case float64:
		b.(*array.Float64Builder).Append(v)
	case float32:
		b.(*array.Float32Builder).Append(v)
	case string:
		b.(*array.StringBuilder).Append(v)
	case []byte:
		b.(*array.BinaryBuilder).Append(v)
	default:
		b.AppendNull()
	}
}

// defaultToSQL converts a Go default value to a SQL expression string.
func defaultToSQL(value any) string {
	switch v := value.(type) {
	case Sql:
		return string(v)
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// applyGenerated adds "generated_expression" metadata to Arrow schema fields
// for generated/virtual columns. Existing field metadata is preserved.
func applyGenerated(schema *arrow.Schema, generated map[string]string) (*arrow.Schema, error) {
	if len(generated) == 0 {
		return schema, nil
	}

	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		fields[i] = schema.Field(i)
	}

	for colName, expr := range generated {
		idx := -1
		for i, f := range fields {
			if f.Name == colName {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("generated references non-existent column %q", colName)
		}
		existing := fields[idx].Metadata
		keys := append(existing.Keys(), "generated_expression")
		vals := append(existing.Values(), expr)
		fields[idx].Metadata = arrow.NewMetadata(keys, vals)
	}

	md := schema.Metadata()
	return arrow.NewSchema(fields, &md), nil
}

// applyColumnComments adds "comment" metadata to Arrow schema fields for
// columns with per-column descriptions. Existing metadata is preserved.
func applyColumnComments(schema *arrow.Schema, comments map[string]string) (*arrow.Schema, error) {
	if len(comments) == 0 {
		return schema, nil
	}
	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		fields[i] = schema.Field(i)
	}
	for colName, comment := range comments {
		idx := -1
		for i, f := range fields {
			if f.Name == colName {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("column_comment references non-existent column %q", colName)
		}
		existing := fields[idx].Metadata
		keys := append(existing.Keys(), "comment")
		vals := append(existing.Values(), comment)
		fields[idx].Metadata = arrow.NewMetadata(keys, vals)
	}
	md := schema.Metadata()
	return arrow.NewSchema(fields, &md), nil
}

// applyDefaults adds "default" metadata to Arrow schema fields for columns
// that have default values defined. Existing field metadata is preserved.
func applyDefaults(schema *arrow.Schema, defaults map[string]any) (*arrow.Schema, error) {
	if len(defaults) == 0 {
		return schema, nil
	}

	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		fields[i] = schema.Field(i)
	}

	for colName, defVal := range defaults {
		idx := -1
		for i, f := range fields {
			if f.Name == colName {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("default references non-existent column %q", colName)
		}

		sqlExpr := defaultToSQL(defVal)

		// Merge with existing metadata
		existing := fields[idx].Metadata
		keys := existing.Keys()
		vals := existing.Values()
		keys = append(keys, "default")
		vals = append(vals, sqlExpr)
		fields[idx].Metadata = arrow.NewMetadata(keys, vals)
	}

	md := schema.Metadata()
	return arrow.NewSchema(fields, &md), nil
}
