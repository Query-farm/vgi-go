// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DoubleFunction doubles numeric values.
type DoubleFunction struct{}

func (f *DoubleFunction) Name() string { return "double" }

func (f *DoubleFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Doubles numeric values",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *DoubleFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "any", Doc: "Numeric value to double"},
	}
}

func (f *DoubleFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// Determine input type from the input schema (column type)
	var inputType arrow.DataType
	if params.InputSchema != nil && params.InputSchema.NumFields() > 0 {
		inputType = params.InputSchema.Field(0).Type
	}
	if inputType == nil {
		inputType = arrow.PrimitiveTypes.Int64
	}
	outputType := promoteForAddition(inputType)
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: outputType},
		}, nil),
	}, nil
}

func (f *DoubleFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	col := batch.Column(0)
	n := int(batch.NumRows())
	outputType := params.OutputSchema.Field(0).Type

	resultArr, err := multiplyArray(mem, col, 2, outputType, n)
	if err != nil {
		return nil, err
	}
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

func multiplyArray(mem memory.Allocator, col arrow.Array, factor int64, outputType arrow.DataType, n int) (arrow.Array, error) {
	switch outputType.ID() {
	case arrow.INT64:
		return buildArray(array.NewInt64Builder(mem), col, n,
			func(i int) int64 { return getInt64Value(col, i) * factor }), nil
	case arrow.INT32:
		return buildArray(array.NewInt32Builder(mem), col, n,
			func(i int) int32 { return int32(getInt64Value(col, i) * factor) }), nil
	case arrow.INT16:
		return buildArray(array.NewInt16Builder(mem), col, n,
			func(i int) int16 { return int16(getInt64Value(col, i) * factor) }), nil
	case arrow.FLOAT64:
		return buildArray(array.NewFloat64Builder(mem), col, n,
			func(i int) float64 { return getFloat64Value(col, i) * float64(factor) }), nil
	case arrow.FLOAT32:
		return buildArray(array.NewFloat32Builder(mem), col, n,
			func(i int) float32 { return float32(getFloat64Value(col, i) * float64(factor)) }), nil
	default:
		return nil, fmt.Errorf("unsupported output type: %v", outputType)
	}
}
