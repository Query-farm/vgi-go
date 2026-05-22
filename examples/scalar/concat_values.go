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

type concatIntArgs struct {
	Values []int64 `vgi:"pos=0,const=false,varargs,type=int64,doc=Integer values"`
}

func (*ConcatValuesIntFunction) Name() string { return "concat_values" }

func (*ConcatValuesIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Sum integer varargs and return as string",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (*ConcatValuesIntFunction) OnBindTyped(_ *concatIntArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (*ConcatValuesIntFunction) ProcessTyped(_ context.Context, _ *concatIntArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapAllColumns(params, batch, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			var sum int64
			for _, c := range cols {
				sum += vgi.GetInt64Value(c, i)
			}
			return fmt.Sprintf("%d", sum)
		})
}

// NewConcatValuesInt returns the registration-ready ScalarFunction.
func NewConcatValuesInt() vgi.ScalarFunction {
	return vgi.AsScalarFunction[concatIntArgs](&ConcatValuesIntFunction{})
}

// ConcatValuesStrFunction — concat_values(varchar...) → concatenated string
type ConcatValuesStrFunction struct{}

type concatStrArgs struct {
	Values []string `vgi:"pos=0,const=false,varargs,type=varchar,doc=String values"`
}

func (*ConcatValuesStrFunction) Name() string { return "concat_values" }

func (*ConcatValuesStrFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Concatenate string varargs",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (*ConcatValuesStrFunction) OnBindTyped(_ *concatStrArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (*ConcatValuesStrFunction) ProcessTyped(_ context.Context, _ *concatStrArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapAllColumns(params, batch, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			var result string
			for _, c := range cols {
				result += vgi.GetStringValue(c, i)
			}
			return result
		})
}

// NewConcatValuesStr returns the registration-ready ScalarFunction.
func NewConcatValuesStr() vgi.ScalarFunction {
	return vgi.AsScalarFunction[concatStrArgs](&ConcatValuesStrFunction{})
}
