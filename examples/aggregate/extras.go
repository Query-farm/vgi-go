// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package aggregate

import (
	"fmt"
	"math"
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

// weightedSumArgs is the typed argument schema for vgi_weighted_sum().
type weightedSumArgs struct {
	Value  float64 `vgi:"pos=0,const=false,doc=Value"`
	Weight float64 `vgi:"pos=1,const=false,doc=Weight"`
}

func (WeightedSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(weightedSumArgs{})
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
		s := vgi.EnsureState(states, gids.At(i), func() *WeightedSumState { return &WeightedSumState{} })
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

// listAggArgs is the typed argument schema for vgi_list_agg().
type listAggArgs struct {
	Value string `vgi:"pos=0,const=false,doc=String to concatenate"`
}

func (ListAggFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(listAggArgs{})
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
		s := vgi.EnsureState(states, gids.At(i), func() *ListAggState { return &ListAggState{} })
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

// percentileArgs is the typed argument schema for vgi_percentile().
type percentileArgs struct {
	Value float64 `vgi:"pos=0,const=false,doc=Value column"`
	P     float64 `vgi:"pos=1,doc=Percentile,ge=0,le=1"`
}

func (PercentileFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(percentileArgs{})
}

func (PercentileFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	// Validate the constant percentile up front and raise a clear error,
	// rather than crashing later in finalize on NULL/NaN/out-of-range input.
	// The const arrives as a named arg "positional_0" (a NULL literal arrives
	// as a null-typed array; positional decimals collapse NULL to 0).
	if p.Args != nil {
		var constArg arrow.Array
		if a, ok := p.Args.Named["positional_0"]; ok {
			constArg = a
		} else {
			for _, a := range p.Args.Positional {
				if a != nil && a.Len() > 0 {
					constArg = a
					break
				}
			}
		}
		if constArg != nil && constArg.Len() > 0 {
			if constArg.DataType().ID() == arrow.NULL || constArg.IsNull(0) {
				return nil, fmt.Errorf("vgi_percentile: percentile must not be NULL")
			}
			if v, ok := scalarFloatFromArr(constArg); ok {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return nil, fmt.Errorf("vgi_percentile: percentile must be a finite number")
				}
				if v < 0 || v > 1 {
					return nil, fmt.Errorf("vgi_percentile: percentile must be in [0, 1], got %v", v)
				}
			}
		}
	}
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
		s := vgi.EnsureState(states, gids.At(i), func() *PercentileState { return &PercentileState{} })
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
	if p.Args != nil {
		// The const param `p` is the only entry the framework stashes for
		// aggregate_bind args (column inputs go via input_schema, not args).
		// It may land at Positional[0] (only const) or Positional[1] (const
		// at original SQL position).
		for _, a := range p.Args.Positional {
			if a == nil {
				continue
			}
			if v, ok := scalarFloatFromArr(a); ok {
				pct = v
				break
			}
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
		idx := int(pct * float64(len(sorted)))
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

// sumAllArgs is the typed argument schema for vgi_sum_all().
type sumAllArgs struct {
	Values []float64 `vgi:"pos=0,varargs,const=false,doc=Numeric inputs"`
}

func (SumAllFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(sumAllArgs{})
}

func (SumAllFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	if p.InputSchema == nil || p.InputSchema.NumFields() == 0 {
		return nil, fmt.Errorf("vgi_sum_all requires at least 1 value")
	}
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
		s := vgi.EnsureState(states, gids.At(i), func() *SumAllState { return &SumAllState{} })
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

// genericSumArgs is the typed argument schema for vgi_generic_sum().
type genericSumArgs struct {
	Value any `vgi:"pos=0,const=false,doc=Numeric column"`
}

func (GenericSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(genericSumArgs{})
}

func (GenericSumFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	dt := arrow.DataType(arrow.PrimitiveTypes.Float64)
	if p.InputSchema != nil && p.InputSchema.NumFields() > 0 {
		dt = p.InputSchema.Field(0).Type
	} else if p.Args != nil && len(p.Args.Positional) > 0 && p.Args.Positional[0] != nil {
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
		s := vgi.EnsureState(states, gids.At(i), func() *GenericSumState { return &GenericSumState{} })
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
	values := make([]interface{}, 0, len(gids))
	for _, gid := range gids {
		if s, ok := states[gid].(*GenericSumState); ok {
			values = append(values, s.Total)
		} else {
			values = append(values, nil)
		}
	}
	col, err := buildGenericSumColumn(mem, dt, values)
	if err != nil {
		return nil, err
	}
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

func buildGenericSumColumn(mem memory.Allocator, dt arrow.DataType, values []interface{}) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(int64(v.(float64)))
			}
		}
		return b.NewArray(), nil
	case arrow.INT32:
		b := array.NewInt32Builder(mem)
		defer b.Release()
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(int32(v.(float64)))
			}
		}
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(float64))
			}
		}
		return b.NewArray(), nil
	case arrow.FLOAT32:
		b := array.NewFloat32Builder(mem)
		defer b.Release()
		for _, v := range values {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(float32(v.(float64)))
			}
		}
		return b.NewArray(), nil
	}
	return nil, fmt.Errorf("vgi_generic_sum: unsupported output type %s", dt)
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
	case *array.Int16:
		return float64(a.Value(i))
	case *array.Int8:
		return float64(a.Value(i))
	case *array.Uint64:
		return float64(a.Value(i))
	case *array.Uint32:
		return float64(a.Value(i))
	case *array.Float64:
		return a.Value(i)
	case *array.Float32:
		return float64(a.Value(i))
	case *array.Decimal128:
		dt := a.DataType().(*arrow.Decimal128Type)
		v := a.Value(i)
		return float64(v.LowBits()) / pow10(int(dt.Scale))
	}
	return 0
}

func pow10(n int) float64 {
	r := 1.0
	for i := 0; i < n; i++ {
		r *= 10
	}
	return r
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
