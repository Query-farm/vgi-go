// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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

// repeatValueIntArgs is the typed argument schema for repeat_value(int, ...).
type repeatValueIntArgs struct {
	Count  int64   `vgi:"pos=0,doc=Number of rows"`
	Values []int64 `vgi:"pos=1,varargs,doc=Integer values to repeat"`
}

func (f *RepeatValueIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(repeatValueIntArgs{})
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
	var args repeatValueIntArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if args.Count < 0 {
		args.Count = 0
	}
	return &repeatValueIntState{
		BatchState: vgi.NewBatchState(args.Count, 1024),
		Values:     args.Values,
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

// repeatValueStrArgs is the typed argument schema for repeat_value(str, ...).
type repeatValueStrArgs struct {
	Count  int64    `vgi:"pos=0,doc=Number of rows"`
	Values []string `vgi:"pos=1,varargs,doc=String values to repeat"`
}

func (f *RepeatValueStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(repeatValueStrArgs{})
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
	var args repeatValueStrArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if args.Count < 0 {
		args.Count = 0
	}
	return &repeatValueStrState{
		BatchState: vgi.NewBatchState(args.Count, 1024),
		Values:     args.Values,
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
