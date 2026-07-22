// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Cacheable scalar fixtures — back the extension's scalar per-value
// memoization tests (scalar/per_value.test, scalar/per_value_edge.test,
// cache/per_value_*.test). Each declares FunctionMetadata.CacheControl, which
// the framework rides as vgi.cache.* keys on every output batch's custom
// metadata so the extension can memoize the output per distinct input value.
//
// Per-value memoization is an explicit opt-in (vgi.cache.per_value), and these
// fixtures set it as a TEST choice, not as production advice: the maps here are
// far too cheap to memoize per value — the probe plus decode costs more than the
// call it replaces. They opt in so the tier has coverage.
//
// Pure, deterministic scalars only. Mirrors vgi-python's
// CachedDoubleScalarFunction / CachedAddConstScalarFunction /
// CachedLabelScalarFunction.
package scalar

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// bigintOutputSchema is the single-column BIGINT result schema.
var bigintOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "result", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// varcharOutputSchema is the single-column VARCHAR result schema.
var varcharOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "result", Type: arrow.BinaryTypes.String},
}, nil)

// ---------------------------------------------------------------------------
// cached_double_scalar(value) — doubles a BIGINT, advertises vgi.cache.*
// ---------------------------------------------------------------------------

// CachedDoubleScalarFunction doubles a BIGINT value and advertises vgi.cache.*
// — backs scalar per-value memo tests. A deterministic 1:1 map, so opting into
// the result cache is sound: the extension memoizes the output per distinct
// input value and serves a fully-warm distinct set without the worker.
type CachedDoubleScalarFunction struct{}

var _ vgi.ScalarFunction = (*CachedDoubleScalarFunction)(nil)

func (f *CachedDoubleScalarFunction) Name() string { return "cached_double_scalar" }

func (f *CachedDoubleScalarFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:  "Doubles a BIGINT value (advertises vgi.cache.ttl + per_value for per-value memo)",
		Stability:    vgi.StabilityConsistent,
		ReturnType:   arrow.PrimitiveTypes.Int64,
		CacheControl: &vgi.CacheControl{Ttl: vgi.Seconds(300), PerValue: true},
	}
}

func (f *CachedDoubleScalarFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Value to double"},
	}
}

func (f *CachedDoubleScalarFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: bigintOutputSchema}, nil
}

func (f *CachedDoubleScalarFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(vgi.GetInt64Value(col, i) * 2)
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(bigintOutputSchema, []arrow.Array{arr}, int64(n)), nil
}

// ---------------------------------------------------------------------------
// cached_add_const(value, addend) — const param folded into the cache key
// ---------------------------------------------------------------------------

// CachedAddConstScalarFunction computes value + addend (a CONST param) and is
// cacheable — backs per-value const-param keying tests. Two calls with the
// same value but different addend must NOT cross-serve: the const arg is
// folded into the cache key on the C++ side.
type CachedAddConstScalarFunction struct{}

var _ vgi.ScalarFunction = (*CachedAddConstScalarFunction)(nil)

func (f *CachedAddConstScalarFunction) Name() string { return "cached_add_const" }

func (f *CachedAddConstScalarFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:  "value + const addend (advertises vgi.cache.ttl + per_value)",
		Stability:    vgi.StabilityConsistent,
		ReturnType:   arrow.PrimitiveTypes.Int64,
		CacheControl: &vgi.CacheControl{Ttl: vgi.Seconds(300), PerValue: true},
	}
}

func (f *CachedAddConstScalarFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Value"},
		{Name: "addend", Position: 1, ArrowType: "int64", Doc: "Constant addend", IsConst: true},
	}
}

func (f *CachedAddConstScalarFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: bigintOutputSchema}, nil
}

func (f *CachedAddConstScalarFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	// addend is a positional CONST param (declared position 1); const args ride
	// params.Args.Positional at their declared positions after remap.
	addend, err := params.Args.GetScalarInt64(1)
	if err != nil {
		return nil, fmt.Errorf("cached_add_const: addend: %w", err)
	}
	col := batch.Column(0)
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(vgi.GetInt64Value(col, i) + addend)
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(bigintOutputSchema, []arrow.Array{arr}, int64(n)), nil
}

// ---------------------------------------------------------------------------
// cached_label(value) — VARCHAR + NULL round-trip through the cache
// ---------------------------------------------------------------------------

// CachedLabelScalarFunction returns 'lbl-<value>' for value >= 0, NULL
// otherwise, and is cacheable — exercises a heap-string + NULL round-trip
// through the per-value cache.
type CachedLabelScalarFunction struct{}

var _ vgi.ScalarFunction = (*CachedLabelScalarFunction)(nil)

func (f *CachedLabelScalarFunction) Name() string { return "cached_label" }

func (f *CachedLabelScalarFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:  "value -> 'lbl-<value>' or NULL for negatives (advertises vgi.cache.ttl + per_value)",
		Stability:    vgi.StabilityConsistent,
		NullHandling: vgi.NullHandlingSpecial,
		ReturnType:   arrow.BinaryTypes.String,
		CacheControl: &vgi.CacheControl{Ttl: vgi.Seconds(300), PerValue: true},
	}
}

func (f *CachedLabelScalarFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Value"},
	}
}

func (f *CachedLabelScalarFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: varcharOutputSchema}, nil
}

func (f *CachedLabelScalarFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		v := vgi.GetInt64Value(col, i)
		if v < 0 {
			b.AppendNull()
		} else {
			b.Append("lbl-" + strconv.FormatInt(v, 10))
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(varcharOutputSchema, []arrow.Array{arr}, int64(n)), nil
}
