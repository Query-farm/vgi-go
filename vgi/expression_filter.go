// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ExpressionFilter is a recursive expression tree pushed from DuckDB.
// The worker evaluates it via the registered ExpressionEvaluator (the
// embedded DuckDB one in vgi/exprduckdb, which can load the spatial extension
// for geometry predicates). Mirrors
// vgi-python's ExpressionFilter in table_filter_pushdown.py.
type ExpressionFilter struct {
	columnName  string
	columnIndex int
	Expr        *exprNode
	// Values holds the scalars referenced by constant-node value_ref indices.
	// Populated at parse time from the filter batch's value-ref columns.
	Values []scalarValueRef
}

// scalarValueRef captures one scalar value + its Arrow field metadata
// (so geoarrow.wkb constants can wrap into ST_GeomFromHEXWKB).
type scalarValueRef struct {
	value any    // Go scalar (int64, float64, string, []byte, nil)
	hex   string // hex string for binary values
	wkb   bool   // true if the value comes from a geoarrow.wkb column
}

// ColumnName returns the name of the column the filter applies to.
func (f *ExpressionFilter) ColumnName() string { return f.columnName }

// ColumnIndex returns the index of the column the filter applies to.
func (f *ExpressionFilter) ColumnIndex() int { return f.columnIndex }

// Type returns the filter type, which is always FilterExpression.
func (f *ExpressionFilter) Type() FilterType { return FilterExpression }

// Evaluate renders the expression tree to SQL and evaluates it against the
// batch with the registered ExpressionEvaluator -- the embedded DuckDB one,
// once the worker imports _ "github.com/Query-farm/vgi-go/vgi/exprduckdb" --
// returning a Boolean array of length batch.NumRows(). Without an evaluator it
// returns ErrExpressionEvaluatorNotLinked.
func (f *ExpressionFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	n := int(batch.NumRows())
	if n == 0 {
		return makeBoolArray(true, 0), nil
	}
	sqlExpr, err := f.Expr.toSQL(f.columnName, batch.Schema(), f.Values)
	if err != nil {
		return nil, fmt.Errorf("expression filter: render SQL: %w", err)
	}
	return evalExpressionAgainstBatch(ctx, batch, sqlExpr)
}

// ---------------------------------------------------------------------------
// Expression tree
// ---------------------------------------------------------------------------

type exprNode struct {
	Type         string     `json:"expr_type"`
	Index        int        `json:"index,omitempty"`
	ValueRef     *int       `json:"value_ref,omitempty"`
	FunctionName string     `json:"function_name,omitempty"`
	Children     []exprNode `json:"children,omitempty"`
	Op           string     `json:"op,omitempty"`
	Left         *exprNode  `json:"left,omitempty"`
	Right        *exprNode  `json:"right,omitempty"`
	Conjunction  string     `json:"conjunction_type,omitempty"`
}

var compOpSymbol = map[string]string{
	"eq": "=", "ne": "!=", "gt": ">", "ge": ">=", "lt": "<", "le": "<=",
}

var wkbHexSQLWrapper = map[string]string{
	"geoarrow.wkb": "ST_GeomFromHEXWKB",
}

func isOperatorName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return false
		}
	}
	return true
}

// toSQL converts the expression tree to a SQL string. Column references
// resolve to the (double-quoted) filter column name. Constants are
// rendered from the scalar-values array (resolved at filter-parse time).
func (n *exprNode) toSQL(columnName string, schema *arrow.Schema, values []scalarValueRef) (string, error) {
	switch n.Type {
	case "column_ref":
		escaped := strings.ReplaceAll(columnName, `"`, `""`)
		// When the referenced column is a geoarrow.wkb BLOB in our temp
		// table, wrap it in ST_GeomFromWKB so the spatial function binds.
		if schema != nil {
			for i := 0; i < schema.NumFields(); i++ {
				f := schema.Field(i)
				if f.Name == columnName && isWKBField(f) {
					return `ST_GeomFromWKB("` + escaped + `")`, nil
				}
			}
		}
		return `"` + escaped + `"`, nil
	case "constant":
		return renderConstantRef(n, values)
	case "function":
		parts := make([]string, len(n.Children))
		for i := range n.Children {
			s, err := n.Children[i].toSQL(columnName, schema, values)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		fname := n.FunctionName
		if isOperatorName(fname) && len(parts) == 2 {
			return "(" + parts[0] + " " + fname + " " + parts[1] + ")", nil
		}
		return fname + "(" + strings.Join(parts, ", ") + ")", nil
	case "comparison":
		left, err := n.Left.toSQL(columnName, schema, values)
		if err != nil {
			return "", err
		}
		right, err := n.Right.toSQL(columnName, schema, values)
		if err != nil {
			return "", err
		}
		sym, ok := compOpSymbol[n.Op]
		if !ok {
			return "", fmt.Errorf("unknown comparison op %q", n.Op)
		}
		return "(" + left + " " + sym + " " + right + ")", nil
	case "conjunction":
		parts := make([]string, len(n.Children))
		for i := range n.Children {
			s, err := n.Children[i].toSQL(columnName, schema, values)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		joiner := " AND "
		if strings.EqualFold(n.Conjunction, "or") {
			joiner = " OR "
		}
		return "(" + strings.Join(parts, joiner) + ")", nil
	}
	return "", fmt.Errorf("unsupported expression node type %q", n.Type)
}

// renderConstantRef produces a SQL literal for a constant node by looking
// up its value_ref index in the value array.
func renderConstantRef(n *exprNode, values []scalarValueRef) (string, error) {
	if n.ValueRef == nil {
		return "NULL", nil
	}
	if *n.ValueRef >= len(values) {
		return "", fmt.Errorf("constant value_ref %d out of range (have %d)", *n.ValueRef, len(values))
	}
	v := values[*n.ValueRef]
	if v.wkb && v.hex != "" {
		return "ST_GeomFromHEXWKB('" + v.hex + "')", nil
	}
	switch x := v.value.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int64:
		return fmt.Sprintf("%d", x), nil
	case float64:
		return fmt.Sprintf("%v", x), nil
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'", nil
	case []byte:
		return "'\\x" + hex.EncodeToString(x) + "'::BLOB", nil
	}
	return "", fmt.Errorf("unsupported constant type %T", v.value)
}

// Alternative JSON shape for binary constants: vgi-python may send them as
// base-64 or hex under different keys. UnmarshalJSON below normalises.

// ---------------------------------------------------------------------------
// Expression evaluator hook
// ---------------------------------------------------------------------------

// ExpressionEvaluator evaluates sqlExpr, a DuckDB SQL boolean expression over
// the batch's columns (referenced by quoted name), and returns a Boolean array
// of length batch.NumRows(). See RegisterExpressionEvaluator.
type ExpressionEvaluator func(ctx context.Context, batch arrow.RecordBatch, sqlExpr string) (arrow.Array, error)

var (
	evaluatorMu sync.RWMutex
	evaluator   ExpressionEvaluator
)

// RegisterExpressionEvaluator installs the evaluator ExpressionFilter.Evaluate
// and AutoApplyFilters use. The SDK itself links none, so a worker that never
// evaluates filters builds without cgo (CGO_ENABLED=0) and without embedding
// DuckDB. Link the embedded-DuckDB evaluator with a blank import:
//
//	import _ "github.com/Query-farm/vgi-go/vgi/exprduckdb"
//
// The last registration wins; nil unregisters.
func RegisterExpressionEvaluator(e ExpressionEvaluator) {
	evaluatorMu.Lock()
	evaluator = e
	evaluatorMu.Unlock()
}

// HasExpressionEvaluator reports whether an ExpressionEvaluator is registered
// (normally by importing vgi/exprduckdb).
func HasExpressionEvaluator() bool { return currentEvaluator() != nil }

func currentEvaluator() ExpressionEvaluator {
	evaluatorMu.RLock()
	defer evaluatorMu.RUnlock()
	return evaluator
}

// ErrExpressionEvaluatorNotLinked is returned (wrapped) by Evaluate, and by
// AutoApplyFilters evaluation, when no ExpressionEvaluator is registered.
var ErrExpressionEvaluatorNotLinked = errors.New(
	"vgi: expression evaluator not linked; add `import _ \"github.com/Query-farm/vgi-go/vgi/exprduckdb\"` " +
		"to the worker (it embeds DuckDB and needs cgo)")

// evalExpressionAgainstBatch evaluates sqlExpr against batch through the
// registered ExpressionEvaluator.
func evalExpressionAgainstBatch(ctx context.Context, batch arrow.RecordBatch, sqlExpr string) (arrow.Array, error) {
	e := currentEvaluator()
	if e == nil {
		return nil, ErrExpressionEvaluatorNotLinked
	}
	return e(ctx, batch, sqlExpr)
}

// DuckDBColumnType is the DuckDB column type an evaluator stages an Arrow
// field as: geoarrow.wkb columns as BLOB, dictionaries as their value type,
// lists and structs recursively. It errors for a type the expression evaluator
// does not support.
func DuckDBColumnType(f arrow.Field) (string, error) { return duckDBTypeFor(f) }

func escapeIdent(s string) string { return strings.ReplaceAll(s, `"`, `""`) }

func duckDBTypeFor(f arrow.Field) (string, error) {
	// geoarrow.wkb columns come in as BLOB; the rendered SQL wraps them in
	// ST_GeomFromHEXWKB when referencing the column. Using BLOB here avoids
	// the appender's per-driver quirks with GEOMETRY.
	if isWKBField(f) {
		return "BLOB", nil
	}
	if dictionary, ok := f.Type.(*arrow.DictionaryType); ok {
		return duckDBTypeFor(arrow.Field{Name: f.Name, Type: dictionary.ValueType})
	}
	switch f.Type.ID() {
	case arrow.INT64:
		return "BIGINT", nil
	case arrow.INT32:
		return "INTEGER", nil
	case arrow.INT16:
		return "SMALLINT", nil
	case arrow.INT8:
		return "TINYINT", nil
	case arrow.FLOAT64:
		return "DOUBLE", nil
	case arrow.FLOAT32:
		return "REAL", nil
	case arrow.STRING:
		return "VARCHAR", nil
	case arrow.BOOL:
		return "BOOLEAN", nil
	case arrow.BINARY:
		return "BLOB", nil
	case arrow.LIST:
		lt := f.Type.(*arrow.ListType)
		elem, err := duckDBTypeFor(arrow.Field{Name: lt.Elem().Name(), Type: lt.Elem()})
		if err != nil {
			return "", err
		}
		return elem + "[]", nil
	case arrow.STRUCT:
		st := f.Type.(*arrow.StructType)
		parts := make([]string, st.NumFields())
		for i := 0; i < st.NumFields(); i++ {
			child := st.Field(i)
			childType, err := duckDBTypeFor(child)
			if err != nil {
				return "", err
			}
			parts[i] = fmt.Sprintf(`"%s" %s`, escapeIdent(child.Name), childType)
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")", nil
	}
	return "", fmt.Errorf("unsupported column type %s in expression filter", f.Type)
}

// isWKBField returns true if the Arrow field is flagged with the
// geoarrow.wkb extension name.
func isWKBField(f arrow.Field) bool {
	if !f.HasMetadata() {
		return false
	}
	md := f.Metadata
	for k := 0; k < md.Len(); k++ {
		if md.Keys()[k] == "ARROW:extension:name" && md.Values()[k] == "geoarrow.wkb" {
			return true
		}
	}
	return false
}

// Unused-import silencer if buildCreateTableSQL drops all cases.
var _ = json.Marshal

// collectExprValueRefs walks the expression tree collecting every
// value_ref index referenced by constant nodes.
func collectExprValueRefs(n *exprNode, out map[int]bool) {
	if n == nil {
		return
	}
	if n.Type == "constant" && n.ValueRef != nil {
		out[*n.ValueRef] = true
	}
	for i := range n.Children {
		collectExprValueRefs(&n.Children[i], out)
	}
	if n.Left != nil {
		collectExprValueRefs(n.Left, out)
	}
	if n.Right != nil {
		collectExprValueRefs(n.Right, out)
	}
}

// resolveScalarValueRef extracts one scalar from the filter batch at
// column index ref+1 (column 0 holds the JSON specs).
func resolveScalarValueRef(batch arrow.RecordBatch, ref int) (scalarValueRef, error) {
	colIdx := ref + 1
	if colIdx >= int(batch.NumCols()) {
		return scalarValueRef{}, fmt.Errorf("value_ref %d out of range (batch has %d columns)", ref, batch.NumCols())
	}
	col := batch.Column(colIdx)
	if col.Len() == 0 {
		return scalarValueRef{}, fmt.Errorf("value column %d is empty", colIdx)
	}
	field := batch.Schema().Field(colIdx)
	isWKB := false
	if field.HasMetadata() {
		md := field.Metadata
		for k := 0; k < md.Len(); k++ {
			if md.Keys()[k] == "ARROW:extension:name" && md.Values()[k] == "geoarrow.wkb" {
				isWKB = true
				break
			}
		}
	}
	if col.IsNull(0) {
		return scalarValueRef{value: nil, wkb: isWKB}, nil
	}
	switch a := col.(type) {
	case *array.Int64:
		return scalarValueRef{value: a.Value(0)}, nil
	case *array.Int32:
		return scalarValueRef{value: int64(a.Value(0))}, nil
	case *array.Float64:
		return scalarValueRef{value: a.Value(0)}, nil
	case *array.Float32:
		return scalarValueRef{value: float64(a.Value(0))}, nil
	case *array.String:
		return scalarValueRef{value: a.Value(0)}, nil
	case *array.Boolean:
		return scalarValueRef{value: a.Value(0)}, nil
	case *array.Binary:
		b := a.Value(0)
		sv := scalarValueRef{value: append([]byte{}, b...), wkb: isWKB}
		sv.hex = hex.EncodeToString(b)
		return sv, nil
	}
	return scalarValueRef{}, fmt.Errorf("unsupported value column type %T", col)
}
