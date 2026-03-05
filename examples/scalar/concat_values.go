// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ConcatValuesIntFunction — concat_values(int64...) → sum as string
type ConcatValuesIntFunction struct{}

func (f *ConcatValuesIntFunction) Name() string { return "concat_values" }

func (f *ConcatValuesIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Sum integer varargs and return as string",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *ConcatValuesIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "values", Position: 0, ArrowType: "int64", Doc: "Integer values", IsVarargs: true},
	}
}

func (f *ConcatValuesIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *ConcatValuesIntFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapAllColumns(params, batch, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			var sum int64
			for _, c := range cols {
				sum += vgi.GetInt64Value(c, i)
			}
			return fmt.Sprintf("%d", sum)
		})
}

// ConcatValuesStrFunction — concat_values(varchar...) → concatenated string
type ConcatValuesStrFunction struct{}

func (f *ConcatValuesStrFunction) Name() string { return "concat_values" }

func (f *ConcatValuesStrFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Concatenate string varargs",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *ConcatValuesStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "values", Position: 0, ArrowType: "varchar", Doc: "String values", IsVarargs: true},
	}
}

func (f *ConcatValuesStrFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *ConcatValuesStrFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapAllColumns(params, batch, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			var result string
			for _, c := range cols {
				result += vgi.GetStringValue(c, i)
			}
			return result
		})
}
