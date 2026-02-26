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

// GeneratorExceptionFunction raises an exception after N batches for testing.
type GeneratorExceptionFunction struct{}

var _ vgi.TypedTableFunc[generatorExceptionState] = (*GeneratorExceptionFunction)(nil)

func (f *GeneratorExceptionFunction) Name() string { return "generator_exception" }

func (f *GeneratorExceptionFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Raises an exception after N batches for testing",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"testing"},
	}
}

func (f *GeneratorExceptionFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "fail_after", Position: 0, ArrowType: "int64", Doc: "Number of batches before failure", IsConst: true},
	}
}

func (f *GeneratorExceptionFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

type generatorExceptionState struct {
	batchCount int64
	failAfter  int64
}

func (f *GeneratorExceptionFunction) NewState(params *vgi.ProcessParams) (*generatorExceptionState, error) {
	failAfter, _ := params.Args.GetScalarInt64(0)
	return &generatorExceptionState{batchCount: 0, failAfter: failAfter}, nil
}

func (f *GeneratorExceptionFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *generatorExceptionState, out *vgirpc.OutputCollector) error {
	if state.batchCount >= state.failAfter {
		return fmt.Errorf("Intentional failure after %d batches", state.failAfter)
	}

	idx := state.batchCount
	state.batchCount++

	arr := vgi.BuildInt64Array(1, func(i int64) int64 { return idx })
	defer arr.Release()

	return out.EmitArrays([]arrow.Array{arr}, 1)
}

func NewGeneratorExceptionFunction() vgi.TableFunction {
	return vgi.AsTableFunction[generatorExceptionState](&GeneratorExceptionFunction{})
}
