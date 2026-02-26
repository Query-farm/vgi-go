// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// LoggingGeneratorFunction emits log messages during generation.
type LoggingGeneratorFunction struct{}

func (f *LoggingGeneratorFunction) Name() string { return "logging_generator" }

func (f *LoggingGeneratorFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits log messages during generation",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *LoggingGeneratorFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of values to generate"},
	}
}

func (f *LoggingGeneratorFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *LoggingGeneratorFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

type loggingGeneratorState struct {
	index int64
	count int64
}

func (f *LoggingGeneratorFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &loggingGeneratorState{index: 0, count: count}, nil
}

func (f *LoggingGeneratorFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*loggingGeneratorState)

	if s.index == 0 {
		out.ClientLog(vgirpc.LogInfo, fmt.Sprintf("Starting generation of %d values", s.count))
	}

	if s.index >= s.count {
		out.ClientLog(vgirpc.LogInfo, "Generation complete")
		return out.Finish()
	}

	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.Append(s.index)

	arr := builder.NewArray()
	defer arr.Release()

	s.index++
	return out.EmitArrays([]arrow.Array{arr}, 1)
}
