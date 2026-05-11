// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// vgi_window_sum_batch — windowed running sum using window_batch return.
// Functionally equivalent to WindowSumFunction; exists so the SDK exercises
// both list and pa.Array return types from window_batch. The Go SDK's batch
// dispatch uses arrow.Array exclusively so behavior is identical to
// WindowSumFunction; this is registered to match vgi-python's fixture set.

type WindowSumBatchState struct {
	Total int64
}

type WindowSumBatchFunction struct{}

var _ vgi.AggregateFunction = (*WindowSumBatchFunction)(nil)

func (WindowSumBatchFunction) Name() string { return "vgi_window_sum_batch" }

func (WindowSumBatchFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Windowed sum demonstrating window_batch returning pa.Array",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Int64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
		SupportsWindow:    true,
	}
}

func (WindowSumBatchFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Column to sum"},
	}
}

func (WindowSumBatchFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (WindowSumBatchFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &WindowSumBatchState{}
}

func (WindowSumBatchFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_window_sum_batch: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("vgi_window_sum_batch: value column is %T, expected int64", columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *WindowSumBatchState { return &WindowSumBatchState{} })
		s.Total += col.Value(i)
	}
	return nil
}

func (WindowSumBatchFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*WindowSumBatchState)
	t := target.(*WindowSumBatchState)
	return &WindowSumBatchState{Total: s.Total + t.Total}, nil
}

func (WindowSumBatchFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*WindowSumBatchState); ok {
			b.Append(s.Total)
		} else {
			b.AppendNull()
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}
