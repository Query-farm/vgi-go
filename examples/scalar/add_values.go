// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
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
		{Name: "col1", Position: 0, ArrowType: "any", Doc: "First numeric value", TypeBound: []vgi.TypeBoundPredicate{vgi.IsAddableType}},
		{Name: "col2", Position: 1, ArrowType: "any", Doc: "Second numeric value", TypeBound: []vgi.TypeBoundPredicate{vgi.IsAddableType}},
	}
}

func (f *AddValuesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResultFromInputs(params, []int{0, 1}, arrow.PrimitiveTypes.Int64,
		func(types []arrow.DataType) arrow.DataType {
			return vgi.CommonTypeForAddition(types[0], types[1])
		})
}

func (f *AddValuesFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.NumericDispatch(params, batch,
		func(cols []arrow.Array, i int) int64 {
			return vgi.GetInt64Value(cols[0], i) + vgi.GetInt64Value(cols[1], i)
		},
		func(cols []arrow.Array, i int) float64 {
			return vgi.GetFloat64Value(cols[0], i) + vgi.GetFloat64Value(cols[1], i)
		})
}
