// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// MultiplyFunction multiplies a value by a constant factor.
type MultiplyFunction struct{}

func (f *MultiplyFunction) Name() string { return "multiply" }

func (f *MultiplyFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Multiplies a value by a constant factor",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MultiplyFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Integer value to multiply"},
		{Name: "factor", Position: 1, ArrowType: "int64", Doc: "Multiplication factor", IsConst: true},
	}
}

func (f *MultiplyFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *MultiplyFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	// Factor is the only const param, so it's positional_0 in the args struct
	factor, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}

	mem := memory.NewGoAllocator()
	valueCol := batch.Column(0).(*array.Int64)
	n := int(batch.NumRows())

	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	for i := 0; i < n; i++ {
		if valueCol.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(valueCol.Value(i) * factor)
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
