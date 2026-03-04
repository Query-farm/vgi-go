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

// LoggingGeneratorFunction emits log messages during generation.
type LoggingGeneratorFunction struct{}

var _ vgi.TypedTableFunc[loggingGeneratorState] = (*LoggingGeneratorFunction)(nil)

func (f *LoggingGeneratorFunction) Name() string { return "logging_generator" }

func (f *LoggingGeneratorFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits log messages during generation",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *LoggingGeneratorFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of values to generate", IsConst: true},
	}
}

func (f *LoggingGeneratorFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

type loggingGeneratorState struct {
	Index int64
	Count int64
}

func (f *LoggingGeneratorFunction) NewState(params *vgi.ProcessParams) (*loggingGeneratorState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &loggingGeneratorState{Index: 0, Count: count}, nil
}

func (f *LoggingGeneratorFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *loggingGeneratorState, out *vgirpc.OutputCollector) error {
	if state.Index == 0 {
		out.ClientLog(vgirpc.LogInfo, fmt.Sprintf("Starting generation of %d values", state.Count))
	}

	if state.Index >= state.Count {
		out.ClientLog(vgirpc.LogInfo, "Generation complete")
		return out.Finish()
	}

	idx := state.Index
	state.Index++

	arr := vgi.BuildInt64Array(1, func(i int64) int64 { return idx })
	defer arr.Release()

	return out.EmitArrays([]arrow.Array{arr}, 1)
}

func NewLoggingGeneratorFunction() vgi.TableFunction {
	return vgi.AsTableFunction[loggingGeneratorState](&LoggingGeneratorFunction{})
}
