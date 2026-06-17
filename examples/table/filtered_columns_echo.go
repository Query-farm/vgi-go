// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

var filteredColumnsEchoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "tag", Type: arrow.BinaryTypes.String},
	{Name: "filtered_cols", Type: arrow.BinaryTypes.String},
	{Name: "has_n", Type: &arrow.BooleanType{}},
	{Name: "has_tag", Type: &arrow.BooleanType{}},
	{Name: "tag_values", Type: arrow.BinaryTypes.String},
}, nil)

// FilteredColumnsEchoFunction echoes pushed-filter column introspection: the set
// of filtered columns (filtered_columns), whether a given column is filtered
// (has_filter_for_column), and the discrete value set resolved for the string
// column `tag` (the typed get_column_values accessor).
type FilteredColumnsEchoFunction struct{}

var _ vgi.TypedTableFunc[filteredColumnsEchoState] = (*FilteredColumnsEchoFunction)(nil)

func (f *FilteredColumnsEchoFunction) Name() string { return "filtered_columns_echo" }

func (f *FilteredColumnsEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Echoes filtered_columns / has_filter_for_column / get_column_values",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic", "testing"},
	}
}

// filteredColumnsEchoArgs is the typed argument schema for filtered_columns_echo().
type filteredColumnsEchoArgs struct {
	Count int64 `vgi:"pos=0,const=true,doc=Number of rows to generate"`
}

func (f *FilteredColumnsEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(filteredColumnsEchoArgs{})
}

func (f *FilteredColumnsEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(filteredColumnsEchoOutputSchema)
}

func (f *FilteredColumnsEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type filteredColumnsEchoState struct {
	vgi.BatchState
	FilteredCols string
	HasN         bool
	HasTag       bool
	TagValues    string
}

func (f *FilteredColumnsEchoFunction) NewState(params *vgi.ProcessParams) (*filteredColumnsEchoState, error) {
	var args filteredColumnsEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	count := args.Count
	if count < 0 {
		count = 0
	}

	st := &filteredColumnsEchoState{
		BatchState: vgi.NewBatchState(count, 1024),
		TagValues:  "(none)",
	}

	if params.PushdownFilters != nil {
		pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
		if err == nil && pf != nil {
			cols := make([]string, 0)
			for c := range pf.FilteredColumns() {
				cols = append(cols, c)
			}
			sort.Strings(cols)
			st.FilteredCols = strings.Join(cols, ",")
			st.HasN = pf.HasFilterForColumn("n")
			st.HasTag = pf.HasFilterForColumn("tag")
			if vals := pf.GetColumnValues("tag"); vals != nil {
				st.TagValues = renderColumnValues(vals)
			}
		}
	}

	return st, nil
}

func (f *FilteredColumnsEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *filteredColumnsEchoState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, filteredColumnsEchoOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("tag") {
			colMap["tag"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("t%d", start+i) })
		}
		if projected.Contains("filtered_cols") {
			colMap["filtered_cols"] = vgi.BuildStringArray(size, func(_ int64) string { return state.FilteredCols })
		}
		if projected.Contains("has_n") {
			colMap["has_n"] = vgi.BuildBooleanArray(size, func(_ int64) bool { return state.HasN })
		}
		if projected.Contains("has_tag") {
			colMap["has_tag"] = vgi.BuildBooleanArray(size, func(_ int64) bool { return state.HasTag })
		}
		if projected.Contains("tag_values") {
			colMap["tag_values"] = vgi.BuildStringArray(size, func(_ int64) string { return state.TagValues })
		}
		return colMap, nil
	})
}

// renderColumnValues renders a discrete value array as a sorted, comma-joined
// string of its non-null values.
func renderColumnValues(arr arrow.Array) string {
	vs := make([]string, 0, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			continue
		}
		vs = append(vs, vgi.GetStringValue(arr, i))
	}
	sort.Strings(vs)
	return strings.Join(vs, ",")
}

func NewFilteredColumnsEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[filteredColumnsEchoState](&FilteredColumnsEchoFunction{})
}
