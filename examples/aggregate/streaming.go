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

// ============================================================================
// vgi_streaming_sum — streaming-partitioned aggregate (running SUM).
//
// The streaming RPC pipeline (aggregate_streaming_open/_chunk/_close) is not
// yet implemented in the Go SDK. Until it is, this function works correctly
// via the standard update/combine/finalize path — which DuckDB uses for GROUP
// BY queries and for windowed queries when the streaming-window optimizer
// rule is disabled (or when the frame shape is ineligible).
// ============================================================================

type StreamingSumState struct {
	Total int64
}

type StreamingSumFunction struct{}

var _ vgi.AggregateFunction = (*StreamingSumFunction)(nil)

func (StreamingSumFunction) Name() string { return "vgi_streaming_sum" }

func (StreamingSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:          "Running SUM via streaming-partitioned protocol (also works as GROUP BY / window aggregate)",
		Stability:            vgi.StabilityConsistent,
		NullHandling:         vgi.NullHandlingDefault,
		ReturnType:           arrow.PrimitiveTypes.Int64,
		OrderDependent:       "NOT_ORDER_DEPENDENT",
		DistinctDependent:    "NOT_DISTINCT_DEPENDENT",
		SupportsWindow:       true,
		StreamingPartitioned: true,
	}
}

func (StreamingSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Column to sum"},
	}
}

func (StreamingSumFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (StreamingSumFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &StreamingSumState{}
}

func (StreamingSumFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_streaming_sum: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("vgi_streaming_sum: value column is %T, expected int64", columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *StreamingSumState { return &StreamingSumState{} })
		s.Total += col.Value(i)
	}
	return nil
}

func (StreamingSumFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*StreamingSumState)
	t := target.(*StreamingSumState)
	return &StreamingSumState{Total: s.Total + t.Total}, nil
}

func (StreamingSumFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*StreamingSumState); ok {
			b.Append(s.Total)
		} else {
			b.AppendNull()
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}
