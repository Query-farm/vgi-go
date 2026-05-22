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

type multiplyArgs struct {
	Value  *array.Int64 `vgi:"pos=0,const=false,doc=Integer value to multiply"`
	Factor int64        `vgi:"pos=1,doc=Multiplication factor"`
}

func (*MultiplyFunction) Name() string { return "multiply" }

func (*MultiplyFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Multiplies a value by a constant factor",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}

func (*MultiplyFunction) OnBindTyped(_ *multiplyArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (*MultiplyFunction) ProcessTyped(_ context.Context, args *multiplyArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumn(params, batch, 0, array.NewInt64Builder,
		func(_ arrow.Array, i int) int64 {
			return args.Value.Value(i) * args.Factor
		})
}

// NewMultiply returns the registration-ready ScalarFunction.
func NewMultiply() vgi.ScalarFunction {
	return vgi.AsScalarFunction[multiplyArgs](&MultiplyFunction{})
}
