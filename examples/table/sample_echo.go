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

var sampleEchoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "s", Type: arrow.BinaryTypes.String},
	{Name: "sample_percentage", Type: arrow.PrimitiveTypes.Float64},
	{Name: "sample_seed", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// SampleEchoFunction echoes TABLESAMPLE pushdown hints in output columns.
//
// DuckDB's SamplingPushdown optimizer pushes only SYSTEM-method TABLESAMPLE
// percentages (and optional REPEATABLE seed) to the worker. BERNOULLI and
// RESERVOIR are always handled by DuckDB's physical operators and never
// reach the worker.
type SampleEchoFunction struct{}

var _ vgi.TypedTableFunc[sampleEchoState] = (*SampleEchoFunction)(nil)

func (f *SampleEchoFunction) Name() string { return "sample_echo" }

func (f *SampleEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Echoes TABLESAMPLE pushdown hints in output columns",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		SamplingPushdown:   true,
		Categories:         []string{"generator", "diagnostic"},
	}
}

// sampleEchoArgs is the typed argument schema for sample_echo().
type sampleEchoArgs struct {
	Count     int64 `vgi:"pos=0,default=10,doc=Number of rows to generate"`
	BatchSize int64 `vgi:"default=2048,doc=Batch size for output"`
}

func (f *SampleEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(sampleEchoArgs{})
}

func (f *SampleEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(sampleEchoOutputSchema)
}

func (f *SampleEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args sampleEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type sampleEchoState struct {
	vgi.BatchState
	Percentage float64
	Seed       int64
}

func (f *SampleEchoFunction) NewState(params *vgi.ProcessParams) (*sampleEchoState, error) {
	var args sampleEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	pct, seed := -1.0, int64(-1)
	if h := params.TableSampleHint; h != nil {
		pct = h.Percentage
		seed = h.Seed
	}
	return &sampleEchoState{
		BatchState: vgi.NewBatchState(args.Count, args.BatchSize),
		Percentage: pct,
		Seed:       seed,
	}, nil
}

func (f *SampleEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *sampleEchoState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, sampleEchoOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("s") {
			colMap["s"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("row_%d", start+i) })
		}
		if projected.Contains("sample_percentage") {
			colMap["sample_percentage"] = vgi.BuildFloat64Array(size, func(_ int64) float64 { return state.Percentage })
		}
		if projected.Contains("sample_seed") {
			colMap["sample_seed"] = vgi.BuildInt64Array(size, func(_ int64) int64 { return state.Seed })
		}
		return colMap, nil
	})
}

func NewSampleEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[sampleEchoState](&SampleEchoFunction{})
}
