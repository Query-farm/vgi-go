// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// DoubleSequenceFunction generates a sequence of floating-point numbers.
type DoubleSequenceFunction struct{}

var _ vgi.TypedTableFunc[doubleSequenceState] = (*DoubleSequenceFunction)(nil)

func (f *DoubleSequenceFunction) Name() string { return "double_sequence" }

func (f *DoubleSequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates a sequence of floating-point numbers from 0 to n-1",
		Stability:   vgi.StabilityConsistent,
	}
}

// doubleSequenceArgs is the typed argument schema for double_sequence().
type doubleSequenceArgs struct {
	Count     int64   `vgi:"pos=0,doc=Number of values to generate"`
	BatchSize int64   `vgi:"default=1000,doc=Batch size for output"`
	Increment float64 `vgi:"default=1.0,doc=Step between values"`
}

func (f *DoubleSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(doubleSequenceArgs{})
}

var doubleSequenceOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Float64},
}, nil)

func (f *DoubleSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(doubleSequenceOutputSchema)
}

func (f *DoubleSequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

func (f *DoubleSequenceFunction) Statistics(params *vgi.BindParams) ([]vgi.ColumnStatistics, error) {
	var args doubleSequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil || args.Count <= 0 {
		return nil, nil
	}
	maxValue := float64(args.Count-1) * args.Increment
	return []vgi.ColumnStatistics{{
		ColumnName:    "n",
		Type:          arrow.PrimitiveTypes.Float64,
		Min:           0.0,
		Max:           maxValue,
		HasNull:       false,
		HasNotNull:    true,
		DistinctCount: args.Count,
	}}, nil
}

type doubleSequenceState struct {
	vgi.BatchState
	Increment float64
}

func (f *DoubleSequenceFunction) NewState(params *vgi.ProcessParams) (*doubleSequenceState, error) {
	var args doubleSequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &doubleSequenceState{
		BatchState: vgi.NewBatchState(args.Count, args.BatchSize),
		Increment:  args.Increment,
	}, nil
}

func (f *DoubleSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *doubleSequenceState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * state.Increment }),
		}, nil
	})
}

func NewDoubleSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[doubleSequenceState](&DoubleSequenceFunction{})
}
