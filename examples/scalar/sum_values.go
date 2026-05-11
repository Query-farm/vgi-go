// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
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
		{Name: "values", Position: 0, ArrowType: "any", Doc: "Numeric values to sum", IsVarargs: true, TypeBound: []vgi.TypeBoundPredicate{vgi.IsAddableType}},
	}
}

func (f *SumValuesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// Zero-arg invocation has an empty input schema; bind without any input
	// columns and surface a clear error before we read field(0) below.
	if params.InputSchema == nil || params.InputSchema.NumFields() == 0 {
		return nil, fmt.Errorf("sum_values requires at least 1 value")
	}
	return vgi.BindResultFromInput(params, 0, arrow.PrimitiveTypes.Int64, vgi.PromoteForAddition)
}

func (f *SumValuesFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.NumericDispatch(params, batch,
		func(cols []arrow.Array, i int) int64 {
			var sum int64
			for _, c := range cols {
				sum += vgi.GetInt64Value(c, i)
			}
			return sum
		},
		func(cols []arrow.Array, i int) float64 {
			var sum float64
			for _, c := range cols {
				sum += vgi.GetFloat64Value(c, i)
			}
			return sum
		})
}
