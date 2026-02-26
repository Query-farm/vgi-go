// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	return vgi.BindResult(arrow.BinaryTypes.String)
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

	// Column 0 in the batch is the "condition" boolean; const params
	// (repeat_count, message) are stripped by DuckDB before reaching Process.
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			if col.(*array.Boolean).Value(i) {
				return repeatedMessage
			}
			return ""
		})
}
