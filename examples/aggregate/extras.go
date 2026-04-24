// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================================
// vgi_weighted_sum — multi-input (value, weight).
// ============================================================================

type WeightedSumState struct {
	Total float64
}

type WeightedSumFunction struct{}

var _ vgi.AggregateFunction = (*WeightedSumFunction)(nil)

func (WeightedSumFunction) Name() string { return "vgi_weighted_sum" }

func (WeightedSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Weighted sum (value * weight)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

func (WeightedSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "double", Doc: "Value"},
		{Name: "weight", Position: 1, ArrowType: "double", Doc: "Weight"},
	}
}

func (WeightedSumFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Float64},
	}, nil))
}

func (WeightedSumFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &WeightedSumState{}
}

func (WeightedSumFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) < 2 {
		return fmt.Errorf("vgi_weighted_sum: need value and weight columns")
	}
	val := columns[0]
	wgt := columns[1]
	n := gids.Len()
	for i := 0; i < n; i++ {
		if val.IsNull(i) || wgt.IsNull(i) {
			continue
		}
		v := scalarFloat(val, i)
		w := scalarFloat(wgt, i)
		s := states[gids.At(i)].(*WeightedSumState)
		s.Total += v * w
	}
	return nil
}

func (WeightedSumFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*WeightedSumState)
	t := target.(*WeightedSumState)
	return &WeightedSumState{Total: s.Total + t.Total}, nil
}

func (WeightedSumFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*WeightedSumState); ok {
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
// vgi_listagg — concatenate strings with separator.
// Order-dependent.
// ============================================================================

type ListAggState struct {
	Items []string
}

type ListAggFunction struct{}

var _ vgi.AggregateFunction = (*ListAggFunction)(nil)

func (ListAggFunction) Name() string { return "vgi_listagg" }

func (ListAggFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Concatenate strings with separator",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.BinaryTypes.String,
		OrderDependent:    "ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

func (ListAggFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "varchar", Doc: "String to concatenate"},
	}
}

func (ListAggFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.String},
	}, nil))
}

func (ListAggFunction) NewState(*vgi.AggregateProcessParams) interface{} { return &ListAggState{} }

func (ListAggFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_listagg: missing value column")
	}
	col, ok := columns[0].(*array.String)
	if !ok {
		return fmt.Errorf("vgi_listagg: value column is %T, expected string", columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := states[gids.At(i)].(*ListAggState)
		s.Items = append(s.Items, col.Value(i))
	}
	return nil
}

func (ListAggFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*ListAggState)
	t := target.(*ListAggState)
	return &ListAggState{Items: append(append([]string{}, t.Items...), s.Items...)}, nil
}

func (ListAggFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	const sep = ","
	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for _, gid := range gids {
		s, ok := states[gid].(*ListAggState)
		if !ok {
			b.AppendNull()
			continue
		}
		b.Append(strings.Join(s.Items, sep))
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// ============================================================================
// vgi_percentile — quantile via collected samples (const param).
// ============================================================================

type PercentileState struct {
	Values []float64
}

type PercentileFunction struct{}

var _ vgi.AggregateFunction = (*PercentileFunction)(nil)

func (PercentileFunction) Name() string { return "vgi_percentile" }

func (PercentileFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Approximate percentile using collected samples",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

func (PercentileFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "double", Doc: "Value column"},
		{Name: "p", Position: 1, ArrowType: "double", Doc: "Percentile (0-1)", IsConst: true},
	}
}

func (PercentileFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Float64},
	}, nil))
}

func (PercentileFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &PercentileState{}
}

func (PercentileFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_percentile: missing value column")
	}
	val := columns[0]
	n := gids.Len()
	for i := 0; i < n; i++ {
		if val.IsNull(i) {
			continue
		}
		s := states[gids.At(i)].(*PercentileState)
		s.Values = append(s.Values, scalarFloat(val, i))
	}
	return nil
}

func (PercentileFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*PercentileState)
	t := target.(*PercentileState)
	merged := append(append([]float64{}, t.Values...), s.Values...)
	return &PercentileState{Values: merged}, nil
}

func (PercentileFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	pct := 0.5
	if p.Args != nil && len(p.Args.Positional) > 1 {
		if v, ok := scalarFloatFromArr(p.Args.Positional[1]); ok {
			pct = v
		}
	}
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		s, ok := states[gid].(*PercentileState)
		if !ok || len(s.Values) == 0 {
			b.AppendNull()
			continue
		}
		sorted := append([]float64{}, s.Values...)
		sort.Float64s(sorted)
		idx := int(pct * float64(len(sorted)-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		b.Append(sorted[idx])
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// ============================================================================
// vgi_sum_all — variadic over all input columns.
// ============================================================================

type SumAllState struct {
	Total float64
}

type SumAllFunction struct{}

var _ vgi.AggregateFunction = (*SumAllFunction)(nil)

func (SumAllFunction) Name() string { return "vgi_sum_all" }

func (SumAllFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Sum all numeric inputs (variadic)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

func (SumAllFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "values", Position: 0, ArrowType: "double", Doc: "Numeric inputs", IsVarargs: true},
	}
}

func (SumAllFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Float64},
	}, nil))
}

func (SumAllFunction) NewState(*vgi.AggregateProcessParams) interface{} { return &SumAllState{} }

func (SumAllFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return nil
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		var anyNonNull bool
		var sum float64
		for _, c := range columns {
			if c.IsNull(i) {
				continue
			}
			anyNonNull = true
			sum += scalarFloat(c, i)
		}
		if !anyNonNull {
			continue
		}
		s := states[gids.At(i)].(*SumAllState)
		s.Total += sum
	}
	return nil
}

func (SumAllFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*SumAllState)
	t := target.(*SumAllState)
	return &SumAllState{Total: s.Total + t.Total}, nil
}

func (SumAllFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*SumAllState); ok {
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
// vgi_generic_sum — ANY return type, returns the running sum as the input type.
// ============================================================================

type GenericSumState struct {
	Total float64
}

type GenericSumFunction struct{}

var _ vgi.AggregateFunction = (*GenericSumFunction)(nil)

func (GenericSumFunction) Name() string { return "vgi_generic_sum" }

func (GenericSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Sum that resolves return type from input at bind time",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

func (GenericSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "any", Doc: "Numeric column"},
	}
}

func (GenericSumFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	dt := arrow.DataType(arrow.PrimitiveTypes.Float64)
	if p.Args != nil && len(p.Args.Positional) > 0 && p.Args.Positional[0] != nil {
		dt = p.Args.Positional[0].DataType()
	}
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: dt},
	}, nil))
}

func (GenericSumFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &GenericSumState{}
}

func (GenericSumFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return nil
	}
	col := columns[0]
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := states[gids.At(i)].(*GenericSumState)
		s.Total += scalarFloat(col, i)
	}
	return nil
}

func (GenericSumFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*GenericSumState)
	t := target.(*GenericSumState)
	return &GenericSumState{Total: s.Total + t.Total}, nil
}

func (GenericSumFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	dt := p.OutputSchema.Field(0).Type
	switch dt.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, gid := range gids {
			if s, ok := states[gid].(*GenericSumState); ok {
				b.Append(int64(s.Total))
			} else {
				b.AppendNull()
			}
		}
		col := b.NewArray()
		defer col.Release()
		return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
	default:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, gid := range gids {
			if s, ok := states[gid].(*GenericSumState); ok {
				b.Append(s.Total)
			} else {
				b.AppendNull()
			}
		}
		col := b.NewArray()
		defer col.Release()
		return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
	}
}

// ============================================================================
// scalarFloat / scalarString — minimal helpers for type-erased input columns.
// ============================================================================

func scalarFloat(arr arrow.Array, i int) float64 {
	switch a := arr.(type) {
	case *array.Int64:
		return float64(a.Value(i))
	case *array.Int32:
		return float64(a.Value(i))
	case *array.Float64:
		return a.Value(i)
	case *array.Float32:
		return float64(a.Value(i))
	}
	return 0
}

func scalarFloatFromArr(arr arrow.Array) (float64, bool) {
	if arr == nil || arr.Len() == 0 || arr.IsNull(0) {
		return 0, false
	}
	return scalarFloat(arr, 0), true
}

func scalarString(arr arrow.Array) (string, bool) {
	if arr == nil || arr.Len() == 0 || arr.IsNull(0) {
		return "", false
	}
	if s, ok := arr.(*array.String); ok {
		return s.Value(0), true
	}
	return "", false
}
