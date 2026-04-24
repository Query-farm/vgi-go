// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/marcboeker/go-duckdb/v2"
)

// ExpressionFilter is a recursive expression tree pushed from DuckDB.
// The worker evaluates it via an embedded DuckDB connection (which can
// load the spatial extension for geometry predicates). Mirrors
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

func (f *ExpressionFilter) ColumnName() string { return f.columnName }
func (f *ExpressionFilter) ColumnIndex() int   { return f.columnIndex }
func (f *ExpressionFilter) Type() FilterType   { return FilterExpression }

// Evaluate renders the expression tree to SQL, feeds the input batch to a
// local DuckDB, and runs SELECT (<expr>)::BOOLEAN. Returns a Boolean array
// of length batch.NumRows().
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
		// DuckDB spatial's && operator binds only to arrays; rewrite to the
		// equivalent st_intersects_extent for the geometry-on-geometry case
		// (vgi-python docs treat the two as synonyms).
		if fname == "&&" && len(parts) == 2 {
			return "st_intersects_extent(" + parts[0] + ", " + parts[1] + ")", nil
		}
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

func wkbWrapperForColumn(columnName string, schema *arrow.Schema) string {
	if schema == nil {
		return ""
	}
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		if f.Name != columnName {
			continue
		}
		if !f.HasMetadata() {
			return ""
		}
		md := f.Metadata
		for k := 0; k < md.Len(); k++ {
			if md.Keys()[k] == "ARROW:extension:name" {
				if wrap, ok := wkbHexSQLWrapper[md.Values()[k]]; ok {
					return wrap
				}
			}
		}
	}
	return ""
}

// Alternative JSON shape for binary constants: vgi-python may send them as
// base-64 or hex under different keys. UnmarshalJSON below normalises.

// ---------------------------------------------------------------------------
// Embedded DuckDB evaluator
// ---------------------------------------------------------------------------

var (
	evalOnce sync.Once
	evalDB   *sql.DB
	evalErr  error
)

func ensureEvalDB() (*sql.DB, error) {
	evalOnce.Do(func() {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			evalErr = fmt.Errorf("open duckdb: %w", err)
			return
		}
		// Spatial extension is optional — try load; if missing, spatial
		// predicates will fail with a clear DuckDB error at query time.
		_, _ = db.Exec(`INSTALL spatial`)
		_, _ = db.Exec(`LOAD spatial`)
		evalDB = db
	})
	return evalDB, evalErr
}

// evalExpressionAgainstBatch feeds a RecordBatch to embedded DuckDB as a
// temporary table, runs SELECT (<sqlExpr>)::BOOLEAN, and returns the result
// mask as an Arrow Boolean array.
//
// The batch is staged via a one-row-at-a-time INSERT to avoid depending on
// duckdb's Arrow integration (which requires a binary build matching the
// driver). This is slower than the Python version's from_arrow but
// correctness-first; a future optimisation could use the appender API.
func evalExpressionAgainstBatch(ctx context.Context, batch arrow.RecordBatch, sqlExpr string) (arrow.Array, error) {
	db, err := ensureEvalDB()
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	tableName := fmt.Sprintf("vgi_filter_eval_%d", nextEvalID())
	createSQL, err := buildCreateTableSQL(tableName, batch.Schema())
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("create eval table: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	}()
	if err := insertBatchToDuckDB(ctx, conn, tableName, batch); err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, "SELECT ("+sqlExpr+")::BOOLEAN FROM "+tableName)
	if err != nil {
		return nil, fmt.Errorf("expression eval query: %w", err)
	}
	defer rows.Close()
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	for rows.Next() {
		var v sql.NullBool
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		if !v.Valid {
			b.AppendNull()
		} else {
			b.Append(v.Bool)
		}
	}
	out := b.NewArray()
	if out.Len() != n {
		out.Release()
		return nil, fmt.Errorf("expression eval: got %d rows, expected %d", out.Len(), n)
	}
	return out, nil
}

var evalCounter struct {
	sync.Mutex
	n int64
}

func nextEvalID() int64 {
	evalCounter.Lock()
	defer evalCounter.Unlock()
	evalCounter.n++
	return evalCounter.n
}

func buildCreateTableSQL(name string, schema *arrow.Schema) (string, error) {
	parts := make([]string, 0, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		t, err := duckDBTypeFor(f)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf(`"%s" %s`, escapeIdent(f.Name), t))
	}
	return fmt.Sprintf("CREATE TEMP TABLE %s (%s)", name, strings.Join(parts, ", ")), nil
}

func escapeIdent(s string) string { return strings.ReplaceAll(s, `"`, `""`) }

func duckDBTypeFor(f arrow.Field) (string, error) {
	// geoarrow.wkb columns come in as BLOB; the rendered SQL wraps them in
	// ST_GeomFromHEXWKB when referencing the column. Using BLOB here avoids
	// the appender's per-driver quirks with GEOMETRY.
	if isWKBField(f) {
		return "BLOB", nil
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
	}
	return "", fmt.Errorf("unsupported column type %s in expression filter", f.Type)
}

func insertBatchToDuckDB(ctx context.Context, conn *sql.Conn, name string, batch arrow.RecordBatch) error {
	n := int(batch.NumRows())
	if n == 0 {
		return nil
	}
	// Use the appender API via go-duckdb for efficient insertion.
	app, err := appenderForConn(ctx, conn, name)
	if err != nil {
		return err
	}
	defer app.Close()

	schema := batch.Schema()
	cols := make([]arrow.Array, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		cols[i] = batch.Column(i)
	}
	row := make([]driver_value, schema.NumFields())
	for i := 0; i < n; i++ {
		for j := 0; j < schema.NumFields(); j++ {
			v, err := arrowToDriverValue(cols[j], i, schema.Field(j))
			if err != nil {
				return err
			}
			row[j] = v
		}
		if err := app.AppendRow(row...); err != nil {
			return err
		}
	}
	return app.Flush()
}

type driver_value = driver.Value

func appenderForConn(ctx context.Context, conn *sql.Conn, tableName string) (*duckdb.Appender, error) {
	var app *duckdb.Appender
	if err := conn.Raw(func(raw any) error {
		dc, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("appenderForConn: connection is %T, want driver.Conn", raw)
		}
		a, err := duckdb.NewAppenderFromConn(dc, "", tableName)
		if err != nil {
			return err
		}
		app = a
		return nil
	}); err != nil {
		return nil, fmt.Errorf("create duckdb appender: %w", err)
	}
	return app, nil
}

func arrowToDriverValue(col arrow.Array, i int, field arrow.Field) (interface{}, error) {
	if col.IsNull(i) {
		return nil, nil
	}
	_ = isWKBField(field) // presence matters for type mapping only
	switch a := col.(type) {
	case *array.Int64:
		return a.Value(i), nil
	case *array.Int32:
		return int64(a.Value(i)), nil
	case *array.Int16:
		return int64(a.Value(i)), nil
	case *array.Int8:
		return int64(a.Value(i)), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.Float32:
		return float64(a.Value(i)), nil
	case *array.String:
		return a.Value(i), nil
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Binary:
		return append([]byte{}, a.Value(i)...), nil
	case *array.List:
		start, end := a.ValueOffsets(i)
		inner := a.ListValues()
		out := make([]interface{}, end-start)
		for j := start; j < end; j++ {
			v, err := arrowValueAtForList(inner, int(j))
			if err != nil {
				return nil, err
			}
			out[j-start] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported arrow type for eval: %T", col)
}

// arrowValueAtForList extracts a Go scalar from a list's child array.
func arrowValueAtForList(col arrow.Array, i int) (interface{}, error) {
	if col.IsNull(i) {
		return nil, nil
	}
	switch a := col.(type) {
	case *array.Int64:
		return a.Value(i), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.Boolean:
		return a.Value(i), nil
	}
	return nil, fmt.Errorf("unsupported list element type %T", col)
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
