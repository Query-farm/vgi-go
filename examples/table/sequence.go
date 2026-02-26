// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SequenceFunction generates a sequence of integers from 0 to n-1.
type SequenceFunction struct{}

func (f *SequenceFunction) Name() string { return "sequence" }

func (f *SequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Generates a sequence of integers from 0 to n-1",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
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
	return &vgi.BindResponse{OutputSchema: sequenceOutputSchema}, nil
}

func (f *SequenceFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *SequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type sequenceState struct {
	remaining    int64
	currentIndex int64
	batchSize    int64
	increment    int64
}

func (f *SequenceFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	count, _ := params.Args.GetScalarInt64(0)
	batchSize := int64(1000)
	if !params.Args.IsNull("batch_size") {
		if v, err := params.Args.GetScalarInt64("batch_size"); err == nil {
			batchSize = v
		}
	}
	increment := int64(1)
	if !params.Args.IsNull("increment") {
		if v, err := params.Args.GetScalarInt64("increment"); err == nil {
			increment = v
		}
	}
	return &sequenceState{
		remaining:    count,
		currentIndex: 0,
		batchSize:    batchSize,
		increment:    increment,
	}, nil
}

func (f *SequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*sequenceState)
	if s.remaining <= 0 {
		return out.Finish()
	}

	size := s.batchSize
	if s.remaining < size {
		size = s.remaining
	}

	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	for i := int64(0); i < size; i++ {
		builder.Append(s.currentIndex * s.increment)
		s.currentIndex++
	}

	arr := builder.NewArray()
	defer arr.Release()

	s.remaining -= size
	return out.EmitArrays([]arrow.Array{arr}, size)
}
