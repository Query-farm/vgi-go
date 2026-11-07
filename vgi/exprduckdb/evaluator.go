// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package exprduckdb links the embedded-DuckDB expression evaluator into a
// worker. Import it for its side effect:
//
//	import _ "github.com/Query-farm/vgi-go/vgi/exprduckdb"
//
// It registers itself with vgi.RegisterExpressionEvaluator, which
// vgi.ExpressionFilter.Evaluate and AutoApplyFilters need to evaluate pushed
// filters. It embeds DuckDB (haybarn-go), so a worker importing it needs cgo
// and a C toolchain, and its binary carries the DuckDB library; a worker that
// does not evaluate filters leaves it out and builds with CGO_ENABLED=0.
package exprduckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"

	"github.com/Query-farm-haybarn/haybarn-go/v2"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func init() { vgi.RegisterExpressionEvaluator(Evaluate) }

var (
	evalOnce sync.Once
	evalDB   *sql.DB
	evalErr  error
)

func ensureEvalDB() (*sql.DB, error) {
	evalOnce.Do(func() {
		// threads=1: the evaluator runs one small query per filtered batch, and
		// at that size DuckDB's intra-query parallelism is pure scheduling
		// overhead (~40% of an evaluation's CPU for a 100-row batch).
		// Concurrent streams still evaluate concurrently, each on its own
		// connection. Filters compare with binary collation; the session
		// setting is applied once as each connection opens rather than by a
		// statement on every evaluation.
		connector, err := duckdb.NewConnector("?threads=1", func(execer driver.ExecerContext) error {
			if _, err := execer.ExecContext(context.Background(), "SET default_collation = 'binary'", nil); err != nil {
				return fmt.Errorf("configure binary filter collation: %w", err)
			}
			return nil
		})
		if err != nil {
			evalErr = fmt.Errorf("open duckdb: %w", err)
			return
		}
		db := sql.OpenDB(connector)
		// Spatial extension is optional — try load; if missing, spatial
		// predicates will fail with a clear DuckDB error at query time.
		_, _ = db.Exec(`INSTALL spatial`)
		_, _ = db.Exec(`LOAD spatial`)
		evalDB = db
	})
	return evalDB, evalErr
}

// Evaluate feeds a RecordBatch to embedded DuckDB as a
// temporary table, runs SELECT (<sqlExpr>)::BOOLEAN, and returns the result
// mask as an Arrow Boolean array.
//
// The batch is staged via a one-row-at-a-time INSERT to avoid depending on
// duckdb's Arrow integration (which requires a binary build matching the
// driver). This is slower than the Python version's from_arrow but
// correctness-first; a future optimisation could use the appender API.
func Evaluate(ctx context.Context, batch arrow.RecordBatch, sqlExpr string) (arrow.Array, error) {
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
		t, err := vgi.DuckDBColumnType(f)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf(`"%s" %s`, strings.ReplaceAll(f.Name, `"`, `""`), t))
	}
	return fmt.Sprintf("CREATE TEMP TABLE %s (%s)", name, strings.Join(parts, ", ")), nil
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
	switch a := col.(type) {
	case *array.Dictionary:
		valueField := field
		valueField.Type = a.Dictionary().DataType()
		return arrowToDriverValue(a.Dictionary(), a.GetValueIndex(i), valueField)
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
	case *array.Struct:
		st := a.DataType().(*arrow.StructType)
		out := make(map[string]interface{}, st.NumFields())
		for j := 0; j < st.NumFields(); j++ {
			child := st.Field(j)
			value, err := arrowToDriverValue(a.Field(j), i, child)
			if err != nil {
				return nil, err
			}
			out[child.Name] = value
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
