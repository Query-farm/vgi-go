// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// Builders for the opaque []byte protocol records that TableInfo,
// SchemaInfo and MacroOverload carry. Each returns the IPC bytes of one
// canonical record and refuses what the record's contract forbids.

// OptionStateRecord encodes an OptionState record for TableInfo.Options or
// SchemaInfo.Options: values is a one-row batch holding one typed value per
// option the provider exposes, and explicitNames lists the options with a
// stored override (including explicit NULLs). A nil values batch encodes no
// options.
func OptionStateRecord(values arrow.RecordBatch, explicitNames []string) ([]byte, error) {
	var payload []byte
	var names []string
	if values != nil {
		if values.NumRows() != 1 {
			return nil, fmt.Errorf("option values must contain exactly one row, got %d", values.NumRows())
		}
		var err error
		if payload, err = SerializeRecordBatch(values); err != nil {
			return nil, err
		}
		for _, field := range values.Schema().Fields() {
			names = append(names, field.Name)
		}
	} else {
		empty := arrow.NewSchema(nil, nil)
		var err error
		if payload, err = serializeEmptyRow(empty); err != nil {
			return nil, err
		}
	}
	if err := uniqueOptionNames(names, "option value"); err != nil {
		return nil, err
	}
	if err := uniqueOptionNames(explicitNames, "explicit option"); err != nil {
		return nil, err
	}
	for _, name := range explicitNames {
		if !slices.Contains(names, name) {
			return nil, fmt.Errorf("explicit option %q has no value", name)
		}
	}
	return encodeCanonicalRow(generated.OptionStateSchema, map[string]any{
		"values": payload, "explicit_names": nonNilStrings(explicitNames),
	})
}

func uniqueOptionNames(names []string, label string) error {
	seen := map[string]bool{}
	for _, name := range names {
		if name == "" || strings.ContainsRune(name, 0) || seen[name] {
			return fmt.Errorf("%s names must be unique, nonempty and without NUL", label)
		}
		seen[name] = true
	}
	return nil
}

func serializeEmptyRow(schema *arrow.Schema) ([]byte, error) {
	row := array.NewRecordBatch(schema, nil, 1)
	defer row.Release()
	return SerializeRecordBatch(row)
}

// LayoutCoverage states whether existing rows follow a table layout.
type LayoutCoverage string

// Layout coverage values.
const (
	LayoutAllRows      LayoutCoverage = "all_rows"
	LayoutFutureWrites LayoutCoverage = "future_writes"
)

// SortKeyRecord encodes a SortKey for TableLayoutRecord: expression is a
// SqlExpression record (ScalarExpressionRecord), direction "asc" or "desc",
// nullOrder "first" or "last".
func SortKeyRecord(expression []byte, direction, nullOrder string) ([]byte, error) {
	if direction != "asc" && direction != "desc" {
		return nil, fmt.Errorf("sort direction must be asc or desc, got %q", direction)
	}
	if nullOrder != "first" && nullOrder != "last" {
		return nil, fmt.Errorf("sort NULL order must be first or last, got %q", nullOrder)
	}
	if _, err := readScalarExpression(expression); err != nil {
		return nil, err
	}
	return encodeCanonicalRow(generated.SortKeySchema, map[string]any{
		"expression": expression, "direction": direction, "null_order": nullOrder,
	})
}

// TableLayoutRecord encodes a TableLayout for TableInfo.Layout: partitionBy
// holds SqlExpression records (ScalarExpressionRecord), sortedBy SortKey
// records (SortKeyRecord). Empty lists mean no partitioning/sorting policy.
func TableLayoutRecord(partitionBy, sortedBy [][]byte, coverage LayoutCoverage) ([]byte, error) {
	if coverage != LayoutAllRows && coverage != LayoutFutureWrites {
		return nil, fmt.Errorf("layout coverage must be all_rows or future_writes, got %q", coverage)
	}
	for _, expression := range partitionBy {
		if _, err := readScalarExpression(expression); err != nil {
			return nil, fmt.Errorf("partition_by: %w", err)
		}
	}
	for _, key := range sortedBy {
		row, err := readCanonicalRow(key, generated.SortKeySchema)
		if err != nil {
			return nil, fmt.Errorf("sorted_by: SortKey: %w", err)
		}
		row.Release()
	}
	return encodeCanonicalRow(generated.TableLayoutSchema, map[string]any{
		"partition_by": binaryList(partitionBy), "sorted_by": binaryList(sortedBy), "coverage": string(coverage),
	})
}

// ScalarExpressionRecord encodes a SqlExpression record for DuckDB scalar SQL
// owned by schemaPath, under ScalarExpressionReadProfile with the default
// evaluation context (UTC, binary collation, asc, nulls_last, insertion order
// preserved). It does not parse or bind the SQL.
func ScalarExpressionRecord(sql string, schemaPath []string) ([]byte, error) {
	if !sqlText(sql, 1024*1024) || strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("scalar expression SQL must be nonempty UTF-8 without NUL")
	}
	return encodeCanonicalRow(generated.SqlExpressionSchema, map[string]any{
		"sql": sql, "profile": scalarExpressionProfile, "schema_path": nonNilStrings(schemaPath),
		"evaluation_context": map[string]any{
			"timezone": "UTC", "collation": "binary", "default_order": "asc",
			"default_null_order": "nulls_last", "preserve_insertion_order": true,
		},
	})
}

func readScalarExpression(data []byte) (string, error) {
	row, err := readCanonicalRow(data, generated.SqlExpressionSchema)
	if err != nil {
		return "", fmt.Errorf("SqlExpression: %w", err)
	}
	defer row.Release()
	return rowColumn(row, "sql").(*array.String).Value(0), nil
}

// MacroParameterDeclaredType encodes MacroParameter.DeclaredType for a typed
// macro parameter: a singleton schema with one nullable field named after the
// parameter. Field metadata (e.g. an extension type) is kept as logical type
// identity. A Null type is refused: leave DeclaredType nil for an untyped
// parameter.
func MacroParameterDeclaredType(parameter string, dataType arrow.DataType, metadata arrow.Metadata) ([]byte, error) {
	if !validIdentifier(parameter) {
		return nil, fmt.Errorf("macro parameter name must be nonempty without NUL")
	}
	if dataType == nil || dataType.ID() == arrow.NULL {
		return nil, fmt.Errorf("macro parameter %q: a declared type cannot be NULL; leave DeclaredType nil for an untyped parameter", parameter)
	}
	return SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: parameter, Type: dataType, Nullable: true, Metadata: metadata}}, nil))
}

// DefinitionRevision returns an opaque TableInfo/CatalogTable
// DefinitionRevision token: the provider's identity for the table's semantic
// definition, which must change whenever its constraints do. DuckDB accepts
// 1..65536 bytes.
func DefinitionRevision(token string) ([]byte, error) {
	if token == "" || len(token) > 65536 {
		return nil, fmt.Errorf("definition revision must be 1..65536 bytes")
	}
	return []byte(token), nil
}

func binaryList(values [][]byte) [][]byte {
	if values == nil {
		return [][]byte{}
	}
	return values
}
