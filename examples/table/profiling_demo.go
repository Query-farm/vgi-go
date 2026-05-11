// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ProfilingDemoFunction is a sequence(count) clone whose dynamic_to_string
// hook would surface rows_produced / batches_emitted / elapsed_ms in
// EXPLAIN ANALYZE output. The dynamic_to_string RPC isn't wired in the Go
// SDK yet, so EXPLAIN ANALYZE will show only the intrinsic keys (Function,
// Worker) — the function still produces correct rows.
type ProfilingDemoFunction struct{}

var _ vgi.TypedTableFunc[profilingDemoState] = (*ProfilingDemoFunction)(nil)

func (f *ProfilingDemoFunction) Name() string { return "profiling_demo" }

func (f *ProfilingDemoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Sequence generator publishing diagnostics under EXPLAIN ANALYZE",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "utility"},
	}
}

func (f *ProfilingDemoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Rows per batch", HasDefault: true, DefaultValue: "1000", IsConst: true},
		{Name: "increment", Position: -1, ArrowType: "int64", Doc: "Increment between values", HasDefault: true, DefaultValue: "1", IsConst: true},
	}
}

func (f *ProfilingDemoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (f *ProfilingDemoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type profilingDemoState struct {
	vgi.BatchState
	Increment int64
}

func (f *ProfilingDemoFunction) NewState(params *vgi.ProcessParams) (*profilingDemoState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	batchSize := vgi.OptionalInt64(params.Args, "batch_size", 1000)
	return &profilingDemoState{
		BatchState: vgi.NewBatchState(count, batchSize),
		Increment:  vgi.OptionalInt64(params.Args, "increment", 1),
	}, nil
}

func (f *ProfilingDemoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *profilingDemoState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return (start + i) * state.Increment }),
		}, nil
	})
}

func NewProfilingDemoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[profilingDemoState](&ProfilingDemoFunction{})
}
