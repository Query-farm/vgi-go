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

// SumValuesFunction sums multiple numeric values (varargs).
type SumValuesFunction struct{}

func (f *SumValuesFunction) Name() string { return "sum_values" }

func (f *SumValuesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Sum multiple numeric values",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *SumValuesFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "values", Position: 0, ArrowType: "any", Doc: "Numeric values to sum", IsVarargs: true},
	}
}

func (f *SumValuesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// Varargs column params — get type from input schema
	var firstType arrow.DataType
	if params.InputSchema != nil && params.InputSchema.NumFields() > 0 {
		firstType = params.InputSchema.Field(0).Type
	}
	if firstType == nil {
		firstType = arrow.PrimitiveTypes.Int64
	}
	outputType := promoteForAddition(firstType)
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: outputType},
		}, nil),
	}, nil
}

func (f *SumValuesFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	n := int(batch.NumRows())
	numCols := int(batch.NumCols())
	outputType := params.OutputSchema.Field(0).Type

	resultArr, err := sumArrays(mem, batch, numCols, outputType, n)
	if err != nil {
		return nil, err
	}
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

func sumArrays(mem memory.Allocator, batch arrow.RecordBatch, numCols int, outputType arrow.DataType, n int) (arrow.Array, error) {
	switch outputType.ID() {
	case arrow.INT64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			isNull := false
			var sum int64
			for c := 0; c < numCols; c++ {
				if batch.Column(c).IsNull(i) {
					isNull = true
					break
				}
				sum += getInt64Value(batch.Column(c), i)
			}
			if isNull {
				builder.AppendNull()
			} else {
				builder.Append(sum)
			}
		}
		return builder.NewArray(), nil
	case arrow.INT32:
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			isNull := false
			var sum int64
			for c := 0; c < numCols; c++ {
				if batch.Column(c).IsNull(i) {
					isNull = true
					break
				}
				sum += getInt64Value(batch.Column(c), i)
			}
			if isNull {
				builder.AppendNull()
			} else {
				builder.Append(int32(sum))
			}
		}
		return builder.NewArray(), nil
	case arrow.INT16:
		builder := array.NewInt16Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			isNull := false
			var sum int64
			for c := 0; c < numCols; c++ {
				if batch.Column(c).IsNull(i) {
					isNull = true
					break
				}
				sum += getInt64Value(batch.Column(c), i)
			}
			if isNull {
				builder.AppendNull()
			} else {
				builder.Append(int16(sum))
			}
		}
		return builder.NewArray(), nil
	case arrow.UINT64:
		builder := array.NewUint64Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			isNull := false
			var sum int64
			for c := 0; c < numCols; c++ {
				if batch.Column(c).IsNull(i) {
					isNull = true
					break
				}
				sum += getInt64Value(batch.Column(c), i)
			}
			if isNull {
				builder.AppendNull()
			} else {
				builder.Append(uint64(sum))
			}
		}
		return builder.NewArray(), nil
	case arrow.FLOAT64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		for i := 0; i < n; i++ {
			isNull := false
			var sum float64
			for c := 0; c < numCols; c++ {
				if batch.Column(c).IsNull(i) {
					isNull = true
					break
				}
				sum += getFloat64Value(batch.Column(c), i)
			}
			if isNull {
				builder.AppendNull()
			} else {
				builder.Append(sum)
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("unsupported output type for sum: %v", outputType)
	}
}
