// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// ArgumentNamesProbeFunction verifies the VGI 2.0 resolved bind signature.
type ArgumentNamesProbeFunction struct{}

type argumentNamesProbeArgs struct {
	Left  arrow.Array `vgi:"pos=0,const=false,type=int64,doc=Left value"`
	Right arrow.Array `vgi:"pos=1,const=false,type=int64,doc=Right value"`
	Scale int64       `vgi:"pos=2,default=2,doc=Scale factor"`
}

func (*ArgumentNamesProbeFunction) Name() string { return "argument_names_probe" }

func (*ArgumentNamesProbeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Checks VGI 2.0 bind-time argument names"}
}

func (*ArgumentNamesProbeFunction) OnBindTyped(_ *argumentNamesProbeArgs, params *vgi.BindParams) (*vgi.BindResponse, error) {
	expected := []string{"left", "right", "scale"}
	if len(params.ArgumentNames) != len(expected) {
		return nil, fmt.Errorf("argument_names_probe expected %v, got %v", expected, params.ArgumentNames)
	}
	for i, name := range params.ArgumentNames {
		if name == nil || *name != expected[i] {
			return nil, fmt.Errorf("argument_names_probe expected %v, got %v", expected, params.ArgumentNames)
		}
	}
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (*ArgumentNamesProbeFunction) ProcessTyped(_ context.Context, args *argumentNamesProbeArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	left := vgi.Int64Accessor(args.Left)
	right := vgi.Int64Accessor(args.Right)
	result := vgi.BuildInt64Array(batch.NumRows(), func(i int64) int64 {
		return (left(int(i)) + right(int(i))) * args.Scale
	})
	defer result.Release()
	return vgi.BuildResultBatch(params, result, batch.NumRows()), nil
}

// NewArgumentNamesProbe returns the registration-ready scalar function.
func NewArgumentNamesProbe() vgi.ScalarFunction {
	return vgi.AsScalarFunction[argumentNamesProbeArgs](&ArgumentNamesProbeFunction{})
}
