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
// vgi_count — nullary aggregate (no input columns).
// ============================================================================

type CountState struct {
	Count int64
}

type CountFunction struct{}

var _ vgi.AggregateFunction = (*CountFunction)(nil)

func (CountFunction) Name() string { return "vgi_count" }

func (CountFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Count rows",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingSpecial,
		ReturnType:        arrow.PrimitiveTypes.Int64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

func (CountFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (CountFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (CountFunction) NewState(*vgi.AggregateProcessParams) interface{} { return &CountState{} }

func (CountFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	for i := 0; i < gids.Len(); i++ {
		s := vgi.EnsureState(states, gids.At(i), func() *CountState { return &CountState{} })
		s.Count++
	}
	return nil
}

func (CountFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*CountState)
	t := target.(*CountState)
	return &CountState{Count: s.Count + t.Count}, nil
}

func (CountFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*CountState); ok {
			b.Append(s.Count)
		} else {
			b.Append(0) // count over zero rows is 0, not NULL
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// ============================================================================
// vgi_sum — single int64 input.
// ============================================================================

type SumState struct {
	Total int64
}

type SumFunction struct{}

var _ vgi.AggregateFunction = (*SumFunction)(nil)

func (SumFunction) Name() string { return "vgi_sum" }

func (SumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Sum integer values",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Int64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// sumArgs is the typed argument schema for vgi_sum().
type sumArgs struct {
	Value int64 `vgi:"pos=0,const=false,doc=Column to sum"`
}

func (SumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(sumArgs{})
}

func (SumFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (SumFunction) NewState(*vgi.AggregateProcessParams) interface{} { return &SumState{} }

func (SumFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_sum: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("vgi_sum: value column is %T, expected int64", columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *SumState { return &SumState{} })
		s.Total += col.Value(i)
	}
	return nil
}

func (SumFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*SumState)
	t := target.(*SumState)
	return &SumState{Total: s.Total + t.Total}, nil
}

func (SumFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*SumState); ok {
			b.Append(s.Total)
		} else {
			b.AppendNull()
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// ============================================================================
// vgi_avg — sum + count state.
// ============================================================================

type AvgState struct {
	Total float64
	Count int64
}

type AvgFunction struct{}

var _ vgi.AggregateFunction = (*AvgFunction)(nil)

func (AvgFunction) Name() string { return "vgi_avg" }

func (AvgFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Average of integer values",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// avgArgs is the typed argument schema for vgi_avg().
type avgArgs struct {
	Value int64 `vgi:"pos=0,const=false,doc=Column to average"`
}

func (AvgFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(avgArgs{})
}

func (AvgFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Float64},
	}, nil))
}

func (AvgFunction) NewState(*vgi.AggregateProcessParams) interface{} { return &AvgState{} }

func (AvgFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_avg: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("vgi_avg: value column is %T, expected int64", columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *AvgState { return &AvgState{} })
		s.Total += float64(col.Value(i))
		s.Count++
	}
	return nil
}

func (AvgFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*AvgState)
	t := target.(*AvgState)
	return &AvgState{Total: s.Total + t.Total, Count: s.Count + t.Count}, nil
}

func (AvgFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		s, ok := states[gid].(*AvgState)
		if !ok || s.Count == 0 {
			b.AppendNull()
			continue
		}
		b.Append(s.Total / float64(s.Count))
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}
