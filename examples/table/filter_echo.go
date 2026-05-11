// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

var filterEchoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "s", Type: arrow.BinaryTypes.String},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

// FilterEchoFunction echoes pushed-down filter predicates in output for diagnostic purposes.
type FilterEchoFunction struct{}

var _ vgi.TypedTableFunc[filterEchoState] = (*FilterEchoFunction)(nil)

func (f *FilterEchoFunction) Name() string { return "filter_echo" }

func (f *FilterEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Echoes pushed-down filter predicates in output",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic"},
	}
}

func (f *FilterEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size for output", HasDefault: true, DefaultValue: "2048", IsConst: true},
	}
}

func (f *FilterEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(filterEchoOutputSchema)
}

func (f *FilterEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type filterEchoState struct {
	vgi.BatchState
	FilterStr string
}

func (f *FilterEchoFunction) NewState(params *vgi.ProcessParams) (*filterEchoState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	batchSize := vgi.OptionalInt64(params.Args, "batch_size", 2048)

	filterStr := "(none)"
	if params.PushdownFilters != nil {
		pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
		if err == nil && len(pf.Filters) > 0 {
			filterStr = formatFiltersInline(pf)
		}
	}

	return &filterEchoState{
		BatchState: vgi.NewBatchState(count, batchSize),
		FilterStr:  filterStr,
	}, nil
}

func (f *FilterEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *filterEchoState, out *vgirpc.OutputCollector) error {
	if params.CurrentPushdownFilters != nil && len(params.CurrentPushdownFilters.Filters) > 0 {
		state.FilterStr = formatFiltersInline(params.CurrentPushdownFilters)
	}
	projected := vgi.ProjectedColumns(params.ProjectionIDs, filterEchoOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("s") {
			colMap["s"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("row_%d", start+i) })
		}
		if projected.Contains("pushed_filters") {
			colMap["pushed_filters"] = vgi.BuildStringArray(size, func(_ int64) string { return state.FilterStr })
		}
		return colMap, nil
	})
}

func NewFilterEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[filterEchoState](&FilterEchoFunction{})
}

// formatFiltersInline formats pushdown filters as a human-readable SQL-like
// string with values inlined (no placeholders). Large IN lists (>20 values)
// are summarized as "col IN (N values)" to match vgi-python's filter_echo.
func formatFiltersInline(pf *vgi.PushdownFilters) string {
	if pf == nil || len(pf.Filters) == 0 {
		return "(none)"
	}
	parts := make([]string, 0, len(pf.Filters))
	for _, f := range pf.Filters {
		parts = append(parts, formatOneFilter(f))
	}
	return strings.Join(parts, " AND ")
}

func formatOneFilter(f vgi.Filter) string {
	switch ft := f.(type) {
	case *vgi.InFilter:
		if ft.Values.Len() > 20 {
			return fmt.Sprintf("%s IN (%d values)", ft.ColumnName(), ft.Values.Len())
		}
	case *vgi.AndFilter:
		children := make([]string, 0, len(ft.Children))
		for _, c := range ft.Children {
			children = append(children, formatOneFilter(c))
		}
		return "(" + strings.Join(children, " AND ") + ")"
	case *vgi.OrFilter:
		children := make([]string, 0, len(ft.Children))
		for _, c := range ft.Children {
			children = append(children, formatOneFilter(c))
		}
		return "(" + strings.Join(children, " OR ") + ")"
	}
	// Fall back to single-filter SQL rendering with inlined params.
	pf := &vgi.PushdownFilters{Filters: []vgi.Filter{f}}
	sql, params := pf.ToSQL(func(s string) string { return s }, "?")
	return inlineParams(sql, params)
}

// inlineParams replaces ? placeholders with formatted parameter values.
func inlineParams(sql string, params []interface{}) string {
	if len(params) == 0 {
		return sql
	}
	var b strings.Builder
	pi := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] == '?' && pi < len(params) {
			b.WriteString(formatValue(params[pi]))
			pi++
		} else {
			b.WriteByte(sql[i])
		}
	}
	return b.String()
}

// formatValue formats a Go value as a SQL literal.
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", val)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", val)
	}
}
