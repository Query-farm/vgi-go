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

// GeneratorExceptionFunction raises an exception after N batches for testing.
type GeneratorExceptionFunction struct{}

func (f *GeneratorExceptionFunction) Name() string { return "generator_exception" }

func (f *GeneratorExceptionFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Raises an exception after N batches for testing",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *GeneratorExceptionFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "fail_after", Position: 0, ArrowType: "int64", Doc: "Number of batches before failure"},
	}
}

func (f *GeneratorExceptionFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *GeneratorExceptionFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

type generatorExceptionState struct {
	batchCount int64
	failAfter  int64
}

func (f *GeneratorExceptionFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	failAfter, _ := params.Args.GetScalarInt64(0)
	return &generatorExceptionState{batchCount: 0, failAfter: failAfter}, nil
}

func (f *GeneratorExceptionFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*generatorExceptionState)

	if s.batchCount >= s.failAfter {
		return fmt.Errorf("Intentional failure after %d batches", s.failAfter)
	}

	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.Append(s.batchCount)

	arr := builder.NewArray()
	defer arr.Release()

	s.batchCount++
	return out.EmitArrays([]arrow.Array{arr}, 1)
}
