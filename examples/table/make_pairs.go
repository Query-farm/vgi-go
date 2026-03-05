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

type MakePairsIntFunction struct{}

var _ vgi.TypedTableFunc[makePairsIntState] = (*MakePairsIntFunction)(nil)

func (f *MakePairsIntFunction) Name() string { return "make_pairs" }

func (f *MakePairsIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate integer pairs (i, i*2) from start to stop-1",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakePairsIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "start", Position: 0, ArrowType: "int64", Doc: "Start value (inclusive)", IsConst: true},
		{Name: "stop", Position: 1, ArrowType: "int64", Doc: "Stop value (exclusive)", IsConst: true},
	}
}

func (f *MakePairsIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(makePairsIntOutputSchema)
}

type makePairsIntState struct {
	vgi.BatchState
	Start int64
}

func (f *MakePairsIntFunction) NewState(params *vgi.ProcessParams) (*makePairsIntState, error) {
	start, _ := params.Args.GetScalarInt64(0)
	stop, _ := params.Args.GetScalarInt64(1)
	count := stop - start
	if count < 0 {
		count = 0
	}
	return &makePairsIntState{
		BatchState: vgi.NewBatchState(count, 1024),
		Start:      start,
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
		Description: "Generate string pairs (prefix+i, suffix+i) for i in 0..4",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *MakePairsStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "prefix", Position: 0, ArrowType: "varchar", Doc: "Prefix string", IsConst: true},
		{Name: "suffix", Position: 1, ArrowType: "varchar", Doc: "Suffix string", IsConst: true},
	}
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
	prefix, _ := params.Args.GetScalarString(0)
	suffix, _ := params.Args.GetScalarString(1)
	return &makePairsStrState{
		BatchState: vgi.NewBatchState(5, 1024),
		Prefix:     prefix,
		Suffix:     suffix,
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
