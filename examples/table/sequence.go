// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// SequenceFunction generates a sequence of integers from 0 to n-1.
type SequenceFunction struct{}

var _ vgi.TypedTableFunc[sequenceState] = (*SequenceFunction)(nil)

func (f *SequenceFunction) Name() string { return "sequence" }

func (f *SequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Generates a sequence of integers from 0 to n-1",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "utility"},
	}
}

func (f *SequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of integers to generate", IsConst: true},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size for output", HasDefault: true, DefaultValue: "1000", IsConst: true},
		{Name: "increment", Position: -1, ArrowType: "int64", Doc: "Step between values", HasDefault: true, DefaultValue: "1", IsConst: true},
	}
}

var sequenceOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
}, nil)

func (f *SequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(sequenceOutputSchema)
}

func (f *SequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type sequenceState struct {
	vgi.BatchState
	Increment int64
}

func (f *SequenceFunction) NewState(params *vgi.ProcessParams) (*sequenceState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &sequenceState{
		BatchState: vgi.NewBatchState(count, vgi.OptionalInt64(params.Args, "batch_size", 1000)),
		Increment:  vgi.OptionalInt64(params.Args, "increment", 1),
	}, nil
}

func (f *SequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *sequenceState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return (start + i) * state.Increment }),
		}, nil
	})
}

func NewSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[sequenceState](&SequenceFunction{})
}
