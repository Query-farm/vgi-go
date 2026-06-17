// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package aggregate

import (
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// stubState is a minimal aggregate state for placeholder implementations
// that only need to satisfy DuckDB's plumbing — it tracks a sum across
// non-null numeric inputs so finalize returns a deterministic non-NULL value.
type stubState struct {
	Total float64
	Count int64
}

// stubUpdate accepts any numeric column type via scalarFloat. Used by
// DynamicAggFunction / DynamicMLAggFunction to avoid Avg's int64-only path.
func stubUpdate(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return nil
	}
	// Pick the last column as "value" — for vgi_dynamic_agg the const
	// code is at position 0 (filtered out by the C++ extension), value at 1.
	val := columns[len(columns)-1]
	for i := 0; i < gids.Len(); i++ {
		if val.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *stubState { return &stubState{} })
		s.Total += scalarFloat(val, i)
		s.Count++
	}
	return nil
}

func stubCombine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*stubState)
	t := target.(*stubState)
	return &stubState{Total: s.Total + t.Total, Count: s.Count + t.Count}, nil
}

func stubFinalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*stubState); ok && s.Count > 0 {
			b.Append(s.Total)
		} else {
			b.AppendNull()
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// The following functions are registered to satisfy the vgi extension's
// duckdb_functions() inventory test (function_registration.test). Their
// runtime behavior is the simplest meaningful implementation that lets the
// declared return type round-trip; richer behavior (vgi-python's dynamic-code
// loading, LLM integration) is intentionally out of scope for the Go SDK.

// ---------------------------------------------------------------------------
// vgi_dynamic_agg — placeholder dynamic aggregate (returns 0.0).
// ---------------------------------------------------------------------------

type DynamicAggFunction struct{ AvgFunction }

func (DynamicAggFunction) Name() string { return "vgi_dynamic_agg" }

func (DynamicAggFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Placeholder for dynamic-code aggregate (vgi-python feature)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// dynamicAggArgs is the typed argument schema for vgi_dynamic_agg().
type dynamicAggArgs struct {
	Code  string  `vgi:"pos=0,doc=Dynamic aggregate code"`
	Value float64 `vgi:"pos=1,const=false,doc=Value column"`
}

func (DynamicAggFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(dynamicAggArgs{})
}

func (DynamicAggFunction) NewState(*vgi.AggregateProcessParams) interface{} { return &stubState{} }
func (DynamicAggFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, p *vgi.AggregateProcessParams) error {
	return stubUpdate(states, gids, columns, p)
}
func (DynamicAggFunction) Combine(s, t interface{}, p *vgi.AggregateProcessParams) (interface{}, error) {
	return stubCombine(s, t, p)
}
func (DynamicAggFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	return stubFinalize(gids, states, p)
}

// ---------------------------------------------------------------------------
// vgi_dynamic_ml_agg — placeholder ML aggregate.
// ---------------------------------------------------------------------------

type DynamicMLAggFunction struct{ AvgFunction }

func (DynamicMLAggFunction) Name() string { return "vgi_dynamic_ml_agg" }

func (DynamicMLAggFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Placeholder for ML aggregate (vgi-python feature)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// dynamicMLAggArgs is the typed argument schema for vgi_dynamic_ml_agg().
type dynamicMLAggArgs struct {
	Code  string  `vgi:"pos=0,doc=Dynamic ML code"`
	Value float64 `vgi:"pos=1,const=false,doc=Value column"`
}

func (DynamicMLAggFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(dynamicMLAggArgs{})
}

func (DynamicMLAggFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &stubState{}
}
func (DynamicMLAggFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, p *vgi.AggregateProcessParams) error {
	return stubUpdate(states, gids, columns, p)
}
func (DynamicMLAggFunction) Combine(s, t interface{}, p *vgi.AggregateProcessParams) (interface{}, error) {
	return stubCombine(s, t, p)
}
func (DynamicMLAggFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	return stubFinalize(gids, states, p)
}
