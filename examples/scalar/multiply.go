// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	// Fast path: bypass vgi.MapColumn's per-row callback. Read the raw int64 slice
	// directly from the input data buffer, multiply in a tight loop the Go compiler
	// can auto-vectorise, then bulk-append. Null mask preserved via AppendValues'
	// valid argument when any nulls are present.
	n := int(batch.NumRows())
	src := args.Value.Int64Values()
	factor := args.Factor

	out := make([]int64, n)
	for i, v := range src {
		out[i] = v * factor
	}

	bldr := array.NewInt64Builder(memory.NewGoAllocator())
	defer bldr.Release()
	if args.Value.NullN() > 0 {
		valid := make([]bool, n)
		for i := 0; i < n; i++ {
			valid[i] = args.Value.IsValid(i)
		}
		bldr.AppendValues(out, valid)
	} else {
		bldr.AppendValues(out, nil)
	}
	arr := bldr.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, int64(n)), nil
}

// NewMultiply returns the registration-ready ScalarFunction.
func NewMultiply() vgi.ScalarFunction {
	return vgi.AsScalarFunction[multiplyArgs](&MultiplyFunction{})
}
