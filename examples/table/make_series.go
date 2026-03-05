// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

var makeSeriesOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// ---------------------------------------------------------------------------
// make_series(count) — generate 0..count-1
// ---------------------------------------------------------------------------

type MakeSeriesCountFunction struct{}

var _ vgi.TypedTableFunc[makeSeriesState] = (*MakeSeriesCountFunction)(nil)

func (f *MakeSeriesCountFunction) Name() string { return "make_series" }

func (f *MakeSeriesCountFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate integers from 0 to count-1",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakeSeriesCountFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of integers", IsConst: true},
	}
}

func (f *MakeSeriesCountFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makeSeriesOutputSchema)
}

type makeSeriesState struct {
	vgi.BatchState
	Start int64
	Step  int64
}

func (f *MakeSeriesCountFunction) NewState(params *vgi.ProcessParams) (*makeSeriesState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	if count < 0 {
		count = 0
	}
	return &makeSeriesState{
		BatchState: vgi.NewBatchState(count, 1024),
		Start:      0,
		Step:       1,
	}, nil
}

func (f *MakeSeriesCountFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makeSeriesState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Start + state.Index*state.Step
		step := state.Step
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + i*step }),
		}, nil
	})
}

func NewMakeSeriesCountFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makeSeriesState](&MakeSeriesCountFunction{})
}

// ---------------------------------------------------------------------------
// make_series(start, stop) — generate start..stop-1
// ---------------------------------------------------------------------------

type MakeSeriesRangeFunction struct{}

var _ vgi.TypedTableFunc[makeSeriesState] = (*MakeSeriesRangeFunction)(nil)

func (f *MakeSeriesRangeFunction) Name() string { return "make_series" }

func (f *MakeSeriesRangeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate integers from start to stop-1",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakeSeriesRangeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "start", Position: 0, ArrowType: "int64", Doc: "Start value (inclusive)", IsConst: true},
		{Name: "stop", Position: 1, ArrowType: "int64", Doc: "Stop value (exclusive)", IsConst: true},
	}
}

func (f *MakeSeriesRangeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makeSeriesOutputSchema)
}

func (f *MakeSeriesRangeFunction) NewState(params *vgi.ProcessParams) (*makeSeriesState, error) {
	start, _ := params.Args.GetScalarInt64(0)
	stop, _ := params.Args.GetScalarInt64(1)
	count := stop - start
	if count < 0 {
		count = 0
	}
	return &makeSeriesState{
		BatchState: vgi.NewBatchState(count, 1024),
		Start:      start,
		Step:       1,
	}, nil
}

func (f *MakeSeriesRangeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makeSeriesState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Start + state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + i }),
		}, nil
	})
}

func NewMakeSeriesRangeFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makeSeriesState](&MakeSeriesRangeFunction{})
}

// ---------------------------------------------------------------------------
// make_series(start, stop, step) — generate with step
// ---------------------------------------------------------------------------

type MakeSeriesStepFunction struct{}

var _ vgi.TypedTableFunc[makeSeriesState] = (*MakeSeriesStepFunction)(nil)

func (f *MakeSeriesStepFunction) Name() string { return "make_series" }

func (f *MakeSeriesStepFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate integers from start to stop-1 with step",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakeSeriesStepFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "start", Position: 0, ArrowType: "int64", Doc: "Start value (inclusive)", IsConst: true},
		{Name: "stop", Position: 1, ArrowType: "int64", Doc: "Stop value (exclusive)", IsConst: true},
		{Name: "step", Position: 2, ArrowType: "int64", Doc: "Step increment", IsConst: true},
	}
}

func (f *MakeSeriesStepFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makeSeriesOutputSchema)
}

func (f *MakeSeriesStepFunction) NewState(params *vgi.ProcessParams) (*makeSeriesState, error) {
	start, _ := params.Args.GetScalarInt64(0)
	stop, _ := params.Args.GetScalarInt64(1)
	step, _ := params.Args.GetScalarInt64(2)
	if step <= 0 {
		step = 1
	}
	count := (stop - start + step - 1) / step
	if count < 0 {
		count = 0
	}
	return &makeSeriesState{
		BatchState: vgi.NewBatchState(count, 1024),
		Start:      start,
		Step:       step,
	}, nil
}

func (f *MakeSeriesStepFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makeSeriesState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Start + state.Index*state.Step
		step := state.Step
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + i*step }),
		}, nil
	})
}

func NewMakeSeriesStepFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makeSeriesState](&MakeSeriesStepFunction{})
}

// ---------------------------------------------------------------------------
// make_series(csv_string) — parse CSV of ints
// ---------------------------------------------------------------------------

type MakeSeriesCsvFunction struct{}

var _ vgi.TypedTableFunc[makeSeriesCsvState] = (*MakeSeriesCsvFunction)(nil)

func (f *MakeSeriesCsvFunction) Name() string { return "make_series" }

func (f *MakeSeriesCsvFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Parse comma-separated integers into rows",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakeSeriesCsvFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "values", Position: 0, ArrowType: "varchar", Doc: "Comma-separated integers", IsConst: true},
	}
}

func (f *MakeSeriesCsvFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makeSeriesOutputSchema)
}

type makeSeriesCsvState struct {
	vgi.BatchState
	Values []int64
}

func (f *MakeSeriesCsvFunction) NewState(params *vgi.ProcessParams) (*makeSeriesCsvState, error) {
	csv, _ := params.Args.GetScalarString(0)
	parts := strings.Split(csv, ",")
	var values []int64
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			continue
		}
		values = append(values, v)
	}
	return &makeSeriesCsvState{
		BatchState: vgi.NewBatchState(int64(len(values)), 1024),
		Values:     values,
	}, nil
}

func (f *MakeSeriesCsvFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makeSeriesCsvState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		offset := state.Index
		vals := state.Values
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return vals[offset+i] }),
		}, nil
	})
}

func NewMakeSeriesCsvFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makeSeriesCsvState](&MakeSeriesCsvFunction{})
}

// ---------------------------------------------------------------------------
// make_series(step) — generate 10 float values with given step size
// ---------------------------------------------------------------------------

var makeSeriesFloatOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

type MakeSeriesFloatStepFunction struct{}

type makeSeriesFloatState struct {
	vgi.BatchState
	Step float64
}

var _ vgi.TypedTableFunc[makeSeriesFloatState] = (*MakeSeriesFloatStepFunction)(nil)

func (f *MakeSeriesFloatStepFunction) Name() string { return "make_series" }

func (f *MakeSeriesFloatStepFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate 10 float values with given step size",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakeSeriesFloatStepFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "step", Position: 0, ArrowType: "double", Doc: "Step size between values", IsConst: true},
	}
}

func (f *MakeSeriesFloatStepFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makeSeriesFloatOutputSchema)
}

func (f *MakeSeriesFloatStepFunction) NewState(params *vgi.ProcessParams) (*makeSeriesFloatState, error) {
	step, _ := params.Args.GetScalarFloat64(0)
	return &makeSeriesFloatState{
		BatchState: vgi.NewBatchState(10, 1024),
		Step:       step,
	}, nil
}

func (f *MakeSeriesFloatStepFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makeSeriesFloatState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		offset := state.Index
		step := state.Step
		return []arrow.Array{
			vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(offset+i) * step }),
		}, nil
	})
}

func NewMakeSeriesFloatStepFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makeSeriesFloatState](&MakeSeriesFloatStepFunction{})
}
