// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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

// dynamicFilterEchoArgs is the typed argument schema for dynamic_filter_echo().
type dynamicFilterEchoArgs struct {
	Count     int64 `vgi:"pos=0,doc=Number of rows to generate"`
	BatchSize int64 `vgi:"default=100,doc=Batch size for output"`
}

func (f *DynamicFilterEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(dynamicFilterEchoArgs{})
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
	var args dynamicFilterEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &dynamicFilterEchoState{
		Total:     args.Count,
		BatchSize: args.BatchSize,
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
