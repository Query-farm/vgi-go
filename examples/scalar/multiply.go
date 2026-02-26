// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (f *MultiplyFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	factor, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}

	return vgi.MapColumn(params, batch, 0, array.NewInt64Builder,
		func(col arrow.Array, i int) int64 {
			return col.(*array.Int64).Value(i) * factor
		})
}
