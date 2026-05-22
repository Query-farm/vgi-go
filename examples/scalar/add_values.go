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

type addValuesArgs struct {
	Col1 arrow.Array `vgi:"pos=0,const=false,bound=addable,doc=First numeric value"`
	Col2 arrow.Array `vgi:"pos=1,const=false,bound=addable,doc=Second numeric value"`
}

func (*AddValuesFunction) Name() string { return "add_values" }

func (*AddValuesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Adds two numeric values",
		Stability:   vgi.StabilityConsistent,
	}
}

func (*AddValuesFunction) OnBindTyped(_ *addValuesArgs, params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResultFromInputs(params, []int{0, 1}, arrow.PrimitiveTypes.Int64,
		func(types []arrow.DataType) arrow.DataType {
			return vgi.CommonTypeForAddition(types[0], types[1])
		})
}

func (*AddValuesFunction) ProcessTyped(_ context.Context, args *addValuesArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.NumericDispatch(params, batch,
		func(_ []arrow.Array, i int) int64 {
			return vgi.GetInt64Value(args.Col1, i) + vgi.GetInt64Value(args.Col2, i)
		},
		func(_ []arrow.Array, i int) float64 {
			return vgi.GetFloat64Value(args.Col1, i) + vgi.GetFloat64Value(args.Col2, i)
		})
}

// NewAddValues returns the registration-ready ScalarFunction.
func NewAddValues() vgi.ScalarFunction {
	return vgi.AsScalarFunction[addValuesArgs](&AddValuesFunction{})
}
