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

// orderEchoArgs is the typed argument schema for order_echo().
type orderEchoArgs struct {
	Count     int64 `vgi:"pos=0,default=10,doc=Number of rows to generate"`
	BatchSize int64 `vgi:"default=2048,doc=Batch size for output"`
}

func (f *OrderEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(orderEchoArgs{})
}

func (f *OrderEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(orderEchoOutputSchema)
}

func (f *OrderEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args orderEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type orderEchoState struct {
	vgi.BatchState
	OrderColumn    string
	OrderDirection string
	OrderNullOrder string
	OrderLimit     int64
}

func (f *OrderEchoFunction) NewState(params *vgi.ProcessParams) (*orderEchoState, error) {
	var args orderEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

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
		BatchState:     vgi.NewBatchState(args.Count, args.BatchSize),
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
