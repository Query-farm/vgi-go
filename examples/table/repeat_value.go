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

// ---------------------------------------------------------------------------
// repeat_value(count, int_values...) — repeats int64 values count times
// ---------------------------------------------------------------------------

type RepeatValueIntFunction struct{}

var _ vgi.TypedTableFunc[repeatValueIntState] = (*RepeatValueIntFunction)(nil)

func (f *RepeatValueIntFunction) Name() string { return "repeat_value" }

func (f *RepeatValueIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Repeat integer values count times",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *RepeatValueIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows", IsConst: true},
		{Name: "values", Position: 1, ArrowType: "int64", Doc: "Integer values to repeat", IsConst: true, IsVarargs: true},
	}
}

func (f *RepeatValueIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// Dynamic schema: v0, v1, ... for each vararg
	numValues := len(params.Args.Positional) - 1
	if numValues < 1 {
		numValues = 1
	}
	fields := make([]arrow.Field, numValues)
	for i := 0; i < numValues; i++ {
		fields[i] = arrow.Field{Name: fmt.Sprintf("v%d", i), Type: arrow.PrimitiveTypes.Int64}
	}
	return vgi.BindSchema(arrow.NewSchema(fields, nil))
}

type repeatValueIntState struct {
	vgi.BatchState
	Values []int64
}

func (f *RepeatValueIntFunction) NewState(params *vgi.ProcessParams) (*repeatValueIntState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	if count < 0 {
		count = 0
	}
	var values []int64
	for i := 1; i < len(params.Args.Positional); i++ {
		v, _ := params.Args.GetScalarInt64(i)
		values = append(values, v)
	}
	return &repeatValueIntState{
		BatchState: vgi.NewBatchState(count, 1024),
		Values:     values,
	}, nil
}

func (f *RepeatValueIntFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *repeatValueIntState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		arrays := make([]arrow.Array, len(state.Values))
		for vi, val := range state.Values {
			v := val
			arrays[vi] = vgi.BuildInt64Array(size, func(i int64) int64 { return v })
		}
		return arrays, nil
	})
}

func NewRepeatValueIntFunction() vgi.TableFunction {
	return vgi.AsTableFunction[repeatValueIntState](&RepeatValueIntFunction{})
}

// ---------------------------------------------------------------------------
// repeat_value(count, str_values...) — repeats string values count times
// ---------------------------------------------------------------------------

type RepeatValueStrFunction struct{}

var _ vgi.TypedTableFunc[repeatValueStrState] = (*RepeatValueStrFunction)(nil)

func (f *RepeatValueStrFunction) Name() string { return "repeat_value" }

func (f *RepeatValueStrFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Repeat string values count times",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *RepeatValueStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows", IsConst: true},
		{Name: "values", Position: 1, ArrowType: "varchar", Doc: "String values to repeat", IsConst: true, IsVarargs: true},
	}
}

func (f *RepeatValueStrFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	numValues := len(params.Args.Positional) - 1
	if numValues < 1 {
		numValues = 1
	}
	fields := make([]arrow.Field, numValues)
	for i := 0; i < numValues; i++ {
		fields[i] = arrow.Field{Name: fmt.Sprintf("v%d", i), Type: arrow.BinaryTypes.String}
	}
	return vgi.BindSchema(arrow.NewSchema(fields, nil))
}

type repeatValueStrState struct {
	vgi.BatchState
	Values []string
}

func (f *RepeatValueStrFunction) NewState(params *vgi.ProcessParams) (*repeatValueStrState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	if count < 0 {
		count = 0
	}
	var values []string
	for i := 1; i < len(params.Args.Positional); i++ {
		v, _ := params.Args.GetScalarString(i)
		values = append(values, v)
	}
	return &repeatValueStrState{
		BatchState: vgi.NewBatchState(count, 1024),
		Values:     values,
	}, nil
}

func (f *RepeatValueStrFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *repeatValueStrState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		arrays := make([]arrow.Array, len(state.Values))
		for vi, val := range state.Values {
			v := val
			arrays[vi] = vgi.BuildStringArray(size, func(i int64) string { return v })
		}
		return arrays, nil
	})
}

func NewRepeatValueStrFunction() vgi.TableFunction {
	return vgi.AsTableFunction[repeatValueStrState](&RepeatValueStrFunction{})
}
