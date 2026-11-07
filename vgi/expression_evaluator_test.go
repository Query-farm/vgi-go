// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Without a registered evaluator, evaluation names the import that links one
// rather than failing obscurely (the SDK no longer embeds DuckDB itself).
func TestEvaluateWithoutEvaluatorNamesTheImport(t *testing.T) {
	saved := currentEvaluator()
	RegisterExpressionEvaluator(nil)
	t.Cleanup(func() { RegisterExpressionEvaluator(saved) })

	b := array.NewInt64Builder(memory.NewGoAllocator())
	b.AppendValues([]int64{1}, nil)
	col := b.NewArray()
	b.Release()
	defer col.Release()
	batch := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, 1)
	defer batch.Release()

	ref := 0
	f := &ExpressionFilter{columnName: "x", Expr: &exprNode{Type: "comparison", Op: "gt",
		Left: &exprNode{Type: "column_ref"}, Right: &exprNode{Type: "constant", ValueRef: &ref}},
		Values: []scalarValueRef{{value: int64(0)}}}
	_, err := f.Evaluate(context.Background(), batch)
	if !errors.Is(err, ErrExpressionEvaluatorNotLinked) || !strings.Contains(err.Error(), "vgi/exprduckdb") {
		t.Fatalf("Evaluate without an evaluator = %v", err)
	}
	if HasExpressionEvaluator() {
		t.Fatal("HasExpressionEvaluator with none registered")
	}

	var gotSQL string
	RegisterExpressionEvaluator(func(_ context.Context, batch arrow.RecordBatch, sqlExpr string) (arrow.Array, error) {
		gotSQL = sqlExpr
		return makeBoolArray(true, int(batch.NumRows())), nil
	})
	mask, err := f.Evaluate(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	mask.Release()
	if gotSQL != `("x" > 0)` {
		t.Fatalf("evaluator got %q", gotSQL)
	}
}

type autoApplyProbe struct{ validateProbe[goodState] }

func (autoApplyProbe) Name() string { return "auto_apply_probe" }
func (autoApplyProbe) Metadata() FunctionMetadata {
	m := DefaultMetadata()
	m.AutoApplyFilters = true
	return m
}

// A worker whose function auto-applies filters refuses to start without an
// evaluator, rather than failing its first filtered scan.
func TestAutoApplyFiltersRequiresAnEvaluator(t *testing.T) {
	w := NewWorker()
	w.RegisterTable(AsTableFunction[goodState](autoApplyProbe{}))
	if err := w.checkExpressionEvaluator(); err != nil {
		t.Fatalf("with the evaluator linked: %v", err)
	}
	saved := currentEvaluator()
	RegisterExpressionEvaluator(nil)
	t.Cleanup(func() { RegisterExpressionEvaluator(saved) })
	err := w.checkExpressionEvaluator()
	if !errors.Is(err, ErrExpressionEvaluatorNotLinked) || !strings.Contains(err.Error(), "auto_apply_probe") {
		t.Fatalf("checkExpressionEvaluator = %v", err)
	}
}
