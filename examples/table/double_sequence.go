// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// DoubleSequenceFunction generates a sequence of floating-point numbers.
type DoubleSequenceFunction struct{}

var _ vgi.TypedTableFunc[doubleSequenceState] = (*DoubleSequenceFunction)(nil)

func (f *DoubleSequenceFunction) Name() string { return "double_sequence" }

func (f *DoubleSequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates a sequence of floating-point numbers from 0 to n-1",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *DoubleSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of values to generate", IsConst: true},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size for output", HasDefault: true, DefaultValue: "1000", IsConst: true},
		{Name: "increment", Position: -1, ArrowType: "double", Doc: "Step between values", HasDefault: true, DefaultValue: "1.0", IsConst: true},
	}
}

var doubleSequenceOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Float64},
}, nil)

func (f *DoubleSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(doubleSequenceOutputSchema)
}

func (f *DoubleSequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type doubleSequenceState struct {
	vgi.BatchState
	increment float64
}

func (f *DoubleSequenceFunction) NewState(params *vgi.ProcessParams) (*doubleSequenceState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &doubleSequenceState{
		BatchState: vgi.NewBatchState(count, vgi.OptionalInt64(params.Args, "batch_size", 1000)),
		increment:  vgi.OptionalFloat64(params.Args, "increment", 1.0),
	}, nil
}

func (f *DoubleSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *doubleSequenceState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * state.increment }),
		}, nil
	})
}

func NewDoubleSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[doubleSequenceState](&DoubleSequenceFunction{})
}
