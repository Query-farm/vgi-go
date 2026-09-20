// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// bool_filter_echo(count) -> {n, flag, pushed_filters}
//
// A table function with a nullable BOOLEAN column, so `WHERE flag` and
// `WHERE NOT flag` can be driven end to end. Nothing else can express the
// shape: filter_echo has no boolean column, so the predicate cannot even be
// written against it, and the table-in-out echo path is never handed a bare
// boolean column as a pushed predicate, so a case written there passes
// whether or not the worker understands one.
//
// pushed_filters echoes the rendering, which covers the half a row count
// cannot see: a shape that decodes and evaluates correctly but renders no SQL
// shows up here as "(none)" — and for a worker that builds a WHERE clause
// from it, that is silently wrong rows, because DuckDB does not re-apply a
// predicate it pushed into a table function.

var boolFilterEchoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

// BoolFilterEchoFunction emits rows with a nullable BOOLEAN column and echoes
// the pushed-down filters.
type BoolFilterEchoFunction struct{}

var _ vgi.TypedTableFunc[boolFilterEchoState] = (*BoolFilterEchoFunction)(nil)

func (f *BoolFilterEchoFunction) Name() string { return "bool_filter_echo" }

func (f *BoolFilterEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Rows with a nullable BOOLEAN column, echoing pushed-down filters",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic"},
	}
}

// boolFilterEchoArgs is the typed argument schema for bool_filter_echo().
type boolFilterEchoArgs struct {
	Count int64 `vgi:"pos=0,doc=Number of rows to generate"`
}

func (f *BoolFilterEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(boolFilterEchoArgs{})
}

func (f *BoolFilterEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(boolFilterEchoOutputSchema)
}

func (f *BoolFilterEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type boolFilterEchoState struct {
	vgi.BatchState
	FilterStr string
}

func (f *BoolFilterEchoFunction) NewState(params *vgi.ProcessParams) (*boolFilterEchoState, error) {
	var args boolFilterEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	filterStr := "(none)"
	if pf := params.CurrentPushdownFilters; pf != nil && len(pf.Filters) > 0 {
		filterStr = formatFiltersInline(pf)
	}

	return &boolFilterEchoState{
		BatchState: vgi.NewBatchState(args.Count, args.Count),
		FilterStr:  filterStr,
	}, nil
}

func (f *BoolFilterEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *boolFilterEchoState, out *vgirpc.OutputCollector) error {
	if params.CurrentPushdownFilters != nil && len(params.CurrentPushdownFilters.Filters) > 0 {
		state.FilterStr = formatFiltersInline(params.CurrentPushdownFilters)
	}
	projected := vgi.ProjectedColumns(params.ProjectionIDs, boolFilterEchoOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("flag") {
			colMap["flag"] = buildCyclingFlagArray(start, size)
		}
		if projected.Contains("pushed_filters") {
			colMap["pushed_filters"] = vgi.BuildStringArray(size, func(_ int64) string { return state.FilterStr })
		}
		return colMap, nil
	})
}

// buildCyclingFlagArray cycles TRUE, FALSE, NULL. The NULL is the point: it is
// what distinguishes `WHERE flag` from `WHERE flag IS NOT FALSE`, and
// `WHERE NOT flag` from `WHERE flag IS NOT TRUE`.
func buildCyclingFlagArray(start, size int64) arrow.Array {
	builder := array.NewBooleanBuilder(memory.NewGoAllocator())
	defer builder.Release()
	builder.Reserve(int(size))
	for i := int64(0); i < size; i++ {
		switch (start + i) % 3 {
		case 0:
			builder.Append(true)
		case 1:
			builder.Append(false)
		default:
			builder.AppendNull()
		}
	}
	return builder.NewArray()
}

func NewBoolFilterEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[boolFilterEchoState](&BoolFilterEchoFunction{})
}
