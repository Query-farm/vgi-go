// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// DynamicFilterEchoFunction generates descending integers and echoes the
// current per-tick pushdown filter on every batch. Used to exercise DuckDB's
// dynamic filter pushdown from ORDER BY ... LIMIT Top-N.
type DynamicFilterEchoFunction struct{}

var _ vgi.TypedTableFunc[dynamicFilterEchoState] = (*DynamicFilterEchoFunction)(nil)

func (f *DynamicFilterEchoFunction) Name() string { return "dynamic_filter_echo" }

func (f *DynamicFilterEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Generates descending integers, echoes dynamic tick filter per batch",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic"},
	}
}

func (f *DynamicFilterEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size for output", HasDefault: true, DefaultValue: "100", IsConst: true},
	}
}

var dynamicFilterEchoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

func (f *DynamicFilterEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(dynamicFilterEchoSchema)
}

func (f *DynamicFilterEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type dynamicFilterEchoState struct {
	Total     int64
	Index     int64
	BatchSize int64
}

func (f *DynamicFilterEchoFunction) NewState(params *vgi.ProcessParams) (*dynamicFilterEchoState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &dynamicFilterEchoState{
		Total:     count,
		BatchSize: vgi.OptionalInt64(params.Args, "batch_size", 100),
	}, nil
}

func (f *DynamicFilterEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *dynamicFilterEchoState, out *vgirpc.OutputCollector) error {
	if state.Index >= state.Total {
		return out.Finish()
	}
	remaining := state.Total - state.Index
	size := state.BatchSize
	if size > remaining {
		size = remaining
	}

	// Echo the current pushdown filter view (updates as DynamicFilter tightens).
	filterStr := "(none)"
	if params.CurrentPushdownFilters != nil && len(params.CurrentPushdownFilters.Filters) > 0 {
		filterStr = params.CurrentPushdownFilters.Repr()
	}

	start := state.Index
	projected := vgi.ProjectedColumns(params.ProjectionIDs, dynamicFilterEchoSchema)
	colMap := make(map[string]arrow.Array)
	if projected.Contains("n") {
		colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 {
			// Descending so ORDER BY n ASC LIMIT K forces Top-N to tighten.
			return state.Total - 1 - (start + i)
		})
	}
	if projected.Contains("pushed_filters") {
		colMap["pushed_filters"] = vgi.BuildStringArray(size, func(_ int64) string { return filterStr })
	}

	cols := make([]arrow.Array, params.OutputSchema.NumFields())
	for i, field := range params.OutputSchema.Fields() {
		cols[i] = colMap[field.Name]
	}
	state.Index += size
	return out.EmitArrays(cols, size)
}

func NewDynamicFilterEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[dynamicFilterEchoState](&DynamicFilterEchoFunction{})
}
