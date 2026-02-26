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

// NullHandlingFunction demonstrates special null handling.
// Returns the input value if not null, or -5000 if null.
type NullHandlingFunction struct{}

func (f *NullHandlingFunction) Name() string { return "null_handling" }

func (f *NullHandlingFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:  "Returns value or -5000 if null",
		Stability:    vgi.StabilityConsistent,
		NullHandling: vgi.NullHandlingSpecial,
	}
}

func (f *NullHandlingFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Integer value to process"},
	}
}

func (f *NullHandlingFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *NullHandlingFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	col := batch.Column(0)
	n := int(batch.NumRows())

	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			builder.Append(-5000)
		} else {
			builder.Append(getInt64Value(col, i))
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
