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

// ConditionalMessageFunction returns a repeated message when condition is true.
type ConditionalMessageFunction struct{}

func (f *ConditionalMessageFunction) Name() string { return "conditional_message" }

func (f *ConditionalMessageFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns repeated message when condition is true",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *ConditionalMessageFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "repeat_count", Position: 0, ArrowType: "int64", Doc: "Number of times to repeat", IsConst: true},
		{Name: "message", Position: 1, ArrowType: "varchar", Doc: "Message to repeat", IsConst: true},
		{Name: "condition", Position: 2, ArrowType: "boolean", Doc: "Apply message condition"},
	}
}

func (f *ConditionalMessageFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.String},
		}, nil),
	}, nil
}

func (f *ConditionalMessageFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	repeatCount, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	message, err := params.Args.GetScalarString(1)
	if err != nil {
		return nil, err
	}

	repeatedMessage := strings.Repeat(message, int(repeatCount))

	mem := memory.NewGoAllocator()
	condCol := batch.Column(0).(*array.Boolean)
	n := int(batch.NumRows())

	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < n; i++ {
		if condCol.IsNull(i) {
			builder.AppendNull()
		} else if condCol.Value(i) {
			builder.Append(repeatedMessage)
		} else {
			builder.Append("")
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
