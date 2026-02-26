// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// UpperCaseFunction converts string values to uppercase.
type UpperCaseFunction struct{}

func (f *UpperCaseFunction) Name() string { return "upper_case" }

func (f *UpperCaseFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Converts string values to uppercase",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *UpperCaseFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "varchar", Doc: "String value to uppercase"},
	}
}

func (f *UpperCaseFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.String},
		}, nil),
	}, nil
}

func (f *UpperCaseFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	valueCol := batch.Column(0).(*array.String)
	n := int(batch.NumRows())

	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < n; i++ {
		if valueCol.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(strings.ToUpper(valueCol.Value(i)))
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
