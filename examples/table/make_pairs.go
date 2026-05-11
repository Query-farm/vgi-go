// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ---------------------------------------------------------------------------
// make_pairs(start, stop) — int pairs (i, i*2)
// ---------------------------------------------------------------------------

var makePairsIntOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	{Name: "b", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// Typed argument schemas for each make_pairs overload.
type makePairsIntArgs struct {
	Start int64 `vgi:"pos=0,doc=Start value (inclusive)"`
	Stop  int64 `vgi:"pos=1,doc=Stop value (exclusive)"`
}

type makePairsStrArgs struct {
	Prefix string `vgi:"pos=0,doc=Prefix string"`
	Suffix string `vgi:"pos=1,doc=Suffix string"`
}

type makePairsMixedArgs struct {
	Start int64  `vgi:"pos=0,doc=Start integer value"`
	Label string `vgi:"pos=1,doc=String label prefix"`
}

type MakePairsIntFunction struct{}

var _ vgi.TypedTableFunc[makePairsIntState] = (*MakePairsIntFunction)(nil)

func (f *MakePairsIntFunction) Name() string { return "make_pairs" }

func (f *MakePairsIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate integer pairs (i, i*2)",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakePairsIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(makePairsIntArgs{})
}

func (f *MakePairsIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makePairsIntOutputSchema)
}

type makePairsIntState struct {
	vgi.BatchState
	Start int64
}

func (f *MakePairsIntFunction) NewState(params *vgi.ProcessParams) (*makePairsIntState, error) {
	var args makePairsIntArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	count := args.Stop - args.Start
	if count < 0 {
		count = 0
	}
	return &makePairsIntState{
		BatchState: vgi.NewBatchState(count, 1024),
		Start:      args.Start,
	}, nil
}

func (f *MakePairsIntFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makePairsIntState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		offset := state.Start + state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return offset + i }),
			vgi.BuildInt64Array(size, func(i int64) int64 { return (offset + i) * 2 }),
		}, nil
	})
}

func NewMakePairsIntFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makePairsIntState](&MakePairsIntFunction{})
}

// ---------------------------------------------------------------------------
// make_pairs(prefix, suffix) — string pairs (prefix+i, suffix+i) for i in 0..4
// ---------------------------------------------------------------------------

var makePairsStrOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "a", Type: arrow.BinaryTypes.String},
	{Name: "b", Type: arrow.BinaryTypes.String},
}, nil)

type MakePairsStrFunction struct{}

var _ vgi.TypedTableFunc[makePairsStrState] = (*MakePairsStrFunction)(nil)

func (f *MakePairsStrFunction) Name() string { return "make_pairs" }

func (f *MakePairsStrFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate string pairs with prefix and suffix",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakePairsStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(makePairsStrArgs{})
}

func (f *MakePairsStrFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makePairsStrOutputSchema)
}

type makePairsStrState struct {
	vgi.BatchState
	Prefix string
	Suffix string
}

func (f *MakePairsStrFunction) NewState(params *vgi.ProcessParams) (*makePairsStrState, error) {
	var args makePairsStrArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &makePairsStrState{
		BatchState: vgi.NewBatchState(5, 1024),
		Prefix:     args.Prefix,
		Suffix:     args.Suffix,
	}, nil
}

func (f *MakePairsStrFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makePairsStrState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		offset := state.Index
		return []arrow.Array{
			vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("%s%d", state.Prefix, offset+i) }),
			vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("%s%d", state.Suffix, offset+i) }),
		}, nil
	})
}

func NewMakePairsStrFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makePairsStrState](&MakePairsStrFunction{})
}

// ---------------------------------------------------------------------------
// make_pairs(start, label) — mixed int/string pairs (a: INT64, b: VARCHAR)
// ---------------------------------------------------------------------------

var makePairsMixedOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	{Name: "b", Type: arrow.BinaryTypes.String},
}, nil)

type MakePairsMixedFunction struct{}

type makePairsMixedState struct {
	vgi.BatchState
	Start int64
	Label string
}

var _ vgi.TypedTableFunc[makePairsMixedState] = (*MakePairsMixedFunction)(nil)

func (f *MakePairsMixedFunction) Name() string { return "make_pairs" }

func (f *MakePairsMixedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate mixed int/string pairs",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakePairsMixedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(makePairsMixedArgs{})
}

func (f *MakePairsMixedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makePairsMixedOutputSchema)
}

func (f *MakePairsMixedFunction) NewState(params *vgi.ProcessParams) (*makePairsMixedState, error) {
	var args makePairsMixedArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &makePairsMixedState{
		BatchState: vgi.NewBatchState(5, 1024),
		Start:      args.Start,
		Label:      args.Label,
	}, nil
}

func (f *MakePairsMixedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *makePairsMixedState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		offset := state.Index
		start := state.Start
		label := state.Label
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + offset + i }),
			vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("%s%d", label, offset+i) }),
		}, nil
	})
}

func NewMakePairsMixedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[makePairsMixedState](&MakePairsMixedFunction{})
}
