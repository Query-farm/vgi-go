// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// SumValuesFunction sums multiple numeric values (varargs).
type SumValuesFunction struct{}

type sumValuesArgs struct {
	Values []any `vgi:"pos=0,const=false,varargs,bound=addable,doc=Numeric values to sum"`
}

func (*SumValuesFunction) Name() string { return "sum_values" }

func (*SumValuesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Sum multiple numeric values",
		Stability:   vgi.StabilityConsistent,
	}
}

func (*SumValuesFunction) OnBindTyped(_ *sumValuesArgs, params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil || params.InputSchema.NumFields() == 0 {
		return nil, fmt.Errorf("sum_values requires at least 1 value")
	}
	return vgi.BindResultFromInput(params, 0, arrow.PrimitiveTypes.Int64, vgi.PromoteForAddition)
}

func (*SumValuesFunction) ProcessTyped(_ context.Context, _ *sumValuesArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
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

// NewSumValues returns the registration-ready ScalarFunction.
func NewSumValues() vgi.ScalarFunction {
	return vgi.AsScalarFunction[sumValuesArgs](&SumValuesFunction{})
}
