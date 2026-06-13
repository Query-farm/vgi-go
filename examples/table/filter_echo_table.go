// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// FilterEchoTableScanFunction backs example.data.filter_echo_table — a catalog
// *table* (not a table function) that echoes the pushed-down filters it
// received. It mirrors vgi-python's FilterEchoTableScanFunction and backs
// test/sql/integration/table/filter_pushdown_through_view.test, which
// characterizes filter pushdown directly and through a VIEW.
//
// Unlike filter_echo it takes no positional args (the catalog scan route passes
// none) and opts into expression-filter pushdown (prefix/starts_with), so a
// `LIKE 'prefix%'` predicate is observable in the pushed_filters column. The
// framework auto-applies the filters so results stay correct.

var filterEchoTableOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "s", Type: arrow.BinaryTypes.String},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

// filterEchoTableRows is the fixed dataset size: n in 0..99, s = "row_<n>". The
// "row_" prefix makes LIKE 'row_1%' meaningful (matches row_1 and row_10..19).
const filterEchoTableRows = 100

// FilterEchoTableScanFunction is a no-arg catalog-table scan echoing pushed-down
// filters.
type FilterEchoTableScanFunction struct{}

var _ vgi.TypedTableFunc[filterEchoTableState] = (*FilterEchoTableScanFunction)(nil)

func (f *FilterEchoTableScanFunction) Name() string { return "filter_echo_table_scan" }

func (f *FilterEchoTableScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:                "Catalog-table scan echoing pushed-down filters (backs example.data.filter_echo_table)",
		Stability:                  vgi.StabilityConsistent,
		FilterPushdown:             true,
		AutoApplyFilters:           true,
		ProjectionPushdown:         true,
		SupportedExpressionFilters: []string{"prefix", "starts_with"},
		Categories:                 []string{"generator", "diagnostic", "testing"},
	}
}

func (f *FilterEchoTableScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *FilterEchoTableScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(filterEchoTableOutputSchema)
}

// filterEchoTableState carries the one-shot cursor plus the captured pushed
// filter string. FilterStr is serialized (not transient) so it survives the
// HTTP state-token rehydrate path, which deserializes state without re-running
// NewState.
type filterEchoTableState struct {
	Done      bool
	FilterStr string
}

func (f *FilterEchoTableScanFunction) NewState(params *vgi.ProcessParams) (*filterEchoTableState, error) {
	filterStr := "(none)"
	if params.PushdownFilters != nil {
		pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
		if err == nil && pf != nil && len(pf.Filters) > 0 {
			filterStr = formatFiltersInline(pf)
		}
	}
	return &filterEchoTableState{FilterStr: filterStr}, nil
}

func (f *FilterEchoTableScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *filterEchoTableState, out *vgirpc.OutputCollector) error {
	if params.CurrentPushdownFilters != nil && len(params.CurrentPushdownFilters.Filters) > 0 {
		state.FilterStr = formatFiltersInline(params.CurrentPushdownFilters)
	}
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	const n = int64(filterEchoTableRows)
	cols := make([]arrow.Array, 0, params.OutputSchema.NumFields())
	for _, field := range params.OutputSchema.Fields() {
		switch field.Name {
		case "n":
			cols = append(cols, vgi.BuildInt64Array(n, func(i int64) int64 { return i }))
		case "s":
			cols = append(cols, vgi.BuildStringArray(n, func(i int64) string { return fmt.Sprintf("row_%d", i) }))
		case "pushed_filters":
			cols = append(cols, vgi.BuildStringArray(n, func(_ int64) string { return state.FilterStr }))
		default:
			return fmt.Errorf("filter_echo_table: unexpected projected column %q", field.Name)
		}
	}
	out.Emit(array.NewRecordBatch(params.OutputSchema, cols, n))
	return nil
}

func NewFilterEchoTableScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[filterEchoTableState](&FilterEchoTableScanFunction{})
}
