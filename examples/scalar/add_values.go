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

// AddValuesFunction adds two numeric values together.
type AddValuesFunction struct{}

func (f *AddValuesFunction) Name() string { return "add_values" }

func (f *AddValuesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Adds two numeric values",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *AddValuesFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "col1", Position: 0, ArrowType: "any", Doc: "First numeric value"},
		{Name: "col2", Position: 1, ArrowType: "any", Doc: "Second numeric value"},
	}
}

func (f *AddValuesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// Both col1 and col2 are column parameters — get types from input schema
	var dt1, dt2 arrow.DataType
	if params.InputSchema != nil && params.InputSchema.NumFields() >= 2 {
		dt1 = params.InputSchema.Field(0).Type
		dt2 = params.InputSchema.Field(1).Type
	}
	if dt1 == nil {
		dt1 = arrow.PrimitiveTypes.Int64
	}
	if dt2 == nil {
		dt2 = arrow.PrimitiveTypes.Int64
	}
	outputType := commonTypeForAddition(dt1, dt2)
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: outputType},
		}, nil),
	}, nil
}

func (f *AddValuesFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	col1 := batch.Column(0)
	col2 := batch.Column(1)
	n := int(batch.NumRows())
	outputType := params.OutputSchema.Field(0).Type

	resultArr, err := addArrays(mem, col1, col2, outputType, n)
	if err != nil {
		return nil, err
	}
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

func addArrays(mem memory.Allocator, col1, col2 arrow.Array, outputType arrow.DataType, n int) (arrow.Array, error) {
	switch outputType.ID() {
	case arrow.INT16:
		builder := array.NewInt16Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int16(getInt64Value(col1, i) + getInt64Value(col2, i)))
			}
		}
		return builder.NewArray(), nil
	case arrow.INT32:
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int32(getInt64Value(col1, i) + getInt64Value(col2, i)))
			}
		}
		return builder.NewArray(), nil
	case arrow.INT64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(getInt64Value(col1, i) + getInt64Value(col2, i))
			}
		}
		return builder.NewArray(), nil
	case arrow.UINT16:
		builder := array.NewUint16Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(uint16(getInt64Value(col1, i) + getInt64Value(col2, i)))
			}
		}
		return builder.NewArray(), nil
	case arrow.UINT32:
		builder := array.NewUint32Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(uint32(getInt64Value(col1, i) + getInt64Value(col2, i)))
			}
		}
		return builder.NewArray(), nil
	case arrow.UINT64:
		builder := array.NewUint64Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(uint64(getInt64Value(col1, i) + getInt64Value(col2, i)))
			}
		}
		return builder.NewArray(), nil
	case arrow.FLOAT32:
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float32(getFloat64Value(col1, i) + getFloat64Value(col2, i)))
			}
		}
		return builder.NewArray(), nil
	case arrow.FLOAT64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			if col1.IsNull(i) || col2.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(getFloat64Value(col1, i) + getFloat64Value(col2, i))
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("unsupported output type for addition: %v", outputType)
	}
}
