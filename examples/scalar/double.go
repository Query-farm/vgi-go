// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
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
	return vgi.BindResultFromInput(params, 0, arrow.PrimitiveTypes.Int64, vgi.PromoteForAddition)
}

func (f *DoubleFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.NumericDispatch(params, batch,
		func(cols []arrow.Array, i int) int64 {
			return vgi.GetInt64Value(cols[0], i) * 2
		},
		func(cols []arrow.Array, i int) float64 {
			return vgi.GetFloat64Value(cols[0], i) * 2
		})
}
