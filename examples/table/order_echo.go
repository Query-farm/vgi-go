// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

var orderEchoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "s", Type: arrow.BinaryTypes.String},
	{Name: "order_column", Type: arrow.BinaryTypes.String},
	{Name: "order_direction", Type: arrow.BinaryTypes.String},
	{Name: "order_null_order", Type: arrow.BinaryTypes.String},
	{Name: "order_limit", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// OrderEchoFunction echoes ORDER BY + LIMIT pushdown hints in output columns.
//
// Verifies that DuckDB's RowGroupPruner optimizer pushes ORDER BY + LIMIT
// hints down via the set_scan_order callback. The function does NOT apply
// the order/limit itself — DuckDB's operators handle that.
type OrderEchoFunction struct{}

var _ vgi.TypedTableFunc[orderEchoState] = (*OrderEchoFunction)(nil)

func (f *OrderEchoFunction) Name() string { return "order_echo" }

func (f *OrderEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Echoes ORDER BY + LIMIT pushdown hints in output columns",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic"},
	}
}

func (f *OrderEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true, HasDefault: true, DefaultValue: "10"},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size for output", HasDefault: true, DefaultValue: "2048", IsConst: true},
	}
}

func (f *OrderEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(orderEchoOutputSchema)
}

func (f *OrderEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type orderEchoState struct {
	vgi.BatchState
	OrderColumn    string
	OrderDirection string
	OrderNullOrder string
	OrderLimit     int64
}

func (f *OrderEchoFunction) NewState(params *vgi.ProcessParams) (*orderEchoState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	batchSize := vgi.OptionalInt64(params.Args, "batch_size", 2048)

	col, dir, null := "(none)", "(none)", "(none)"
	limit := int64(-1)
	if h := params.OrderByHint; h != nil {
		col = h.ColumnName
		if h.Direction != "" {
			dir = h.Direction
		}
		if h.NullOrder != "" {
			null = h.NullOrder
		}
		limit = h.RowLimit
	}

	return &orderEchoState{
		BatchState:     vgi.NewBatchState(count, batchSize),
		OrderColumn:    col,
		OrderDirection: dir,
		OrderNullOrder: null,
		OrderLimit:     limit,
	}, nil
}

func (f *OrderEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *orderEchoState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, orderEchoOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("s") {
			colMap["s"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("row_%d", start+i) })
		}
		if projected.Contains("order_column") {
			colMap["order_column"] = vgi.BuildStringArray(size, func(_ int64) string { return state.OrderColumn })
		}
		if projected.Contains("order_direction") {
			colMap["order_direction"] = vgi.BuildStringArray(size, func(_ int64) string { return state.OrderDirection })
		}
		if projected.Contains("order_null_order") {
			colMap["order_null_order"] = vgi.BuildStringArray(size, func(_ int64) string { return state.OrderNullOrder })
		}
		if projected.Contains("order_limit") {
			colMap["order_limit"] = vgi.BuildInt64Array(size, func(_ int64) int64 { return state.OrderLimit })
		}
		return colMap, nil
	})
}

func NewOrderEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[orderEchoState](&OrderEchoFunction{})
}
