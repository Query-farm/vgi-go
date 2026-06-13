// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// NullHandlingFunction demonstrates special null handling.
// Returns the input value if not null, or -5000 if null.
type NullHandlingFunction struct{}

type nullHandlingArgs struct {
	Value int64 `vgi:"pos=0,const=false,doc=Integer value to process"`
}

func (*NullHandlingFunction) Name() string { return "null_handling" }

func (*NullHandlingFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:  "Returns value or -5000 if null",
		Stability:    vgi.StabilityConsistent,
		NullHandling: vgi.NullHandlingSpecial,
		ReturnType:   arrow.PrimitiveTypes.Int64,
	}
}

func (*NullHandlingFunction) OnBindTyped(_ *nullHandlingArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (*NullHandlingFunction) ProcessTyped(_ context.Context, _ *nullHandlingArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumnCustomNulls(params, batch, 0, array.NewInt64Builder,
		func(col arrow.Array, i int) int64 {
			if col.IsNull(i) {
				return -5000
			}
			return vgi.GetInt64Value(col, i)
		})
}

// NewNullHandling returns the registration-ready ScalarFunction.
func NewNullHandling() vgi.ScalarFunction {
	return vgi.AsScalarFunction[nullHandlingArgs](&NullHandlingFunction{})
}
