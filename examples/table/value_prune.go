// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// valuePruneOutputSchema mirrors vgi-python's ValuePruneFunction:
// {"n": int64, "resolved": utf8}.
var valuePruneOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "resolved", Type: arrow.BinaryTypes.String},
}, nil)

// ValuePruneFunction exercises PushdownFilters.GetColumnValues("n") — the
// partition-pruning accessor. It resolves the discrete value set for `n` up
// front and emits only those keys; the `resolved` column echoes exactly what
// GetColumnValues returned (the sorted, comma-joined set, or "(scan)" when the
// predicate is not enumerable). This makes a regression in the AND-descent /
// OR-union of that accessor directly observable, independent of any residual
// row-by-row filtering (which filter_echo covers via a different code path).
type ValuePruneFunction struct{}

var _ vgi.TypedTableFunc[valuePruneState] = (*ValuePruneFunction)(nil)

func (f *ValuePruneFunction) Name() string { return "value_prune" }

func (f *ValuePruneFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Prunes the key set via get_column_values('n'); echoes the resolved discrete values",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic"},
	}
}

// valuePruneArgs is the typed argument schema for value_prune().
type valuePruneArgs struct {
	Count     int64 `vgi:"pos=0,doc=Number of candidate rows (keys 0..count-1)"`
	BatchSize int64 `vgi:"default=2048,doc=Batch size for output"`
}

func (f *ValuePruneFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(valuePruneArgs{})
}

func (f *ValuePruneFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(valuePruneOutputSchema)
}

func (f *ValuePruneFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

// valuePruneState carries the resolved key set to emit plus the echoed
// get_column_values result. Both Values and Resolved are gob-serialized with
// the rest of the state, so they survive the HTTP state-token round-trip
// (which deserializes state without re-running NewState).
type valuePruneState struct {
	vgi.BatchState
	Values   []int64
	Resolved string
}

func (f *ValuePruneFunction) NewState(params *vgi.ProcessParams) (*valuePruneState, error) {
	var args valuePruneArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	var discrete arrow.Array
	if params.PushdownFilters != nil {
		pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
		if err == nil && len(pf.Filters) > 0 {
			discrete = pf.GetColumnValues("n")
		}
	}

	resolved := "(scan)"
	var emit []int64
	if discrete != nil {
		vals := int64ArrayValues(discrete)
		sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = strconv.FormatInt(v, 10)
			if v >= 0 && v < args.Count {
				emit = append(emit, v)
			}
		}
		resolved = strings.Join(parts, ",")
	} else {
		emit = make([]int64, args.Count)
		for i := range emit {
			emit[i] = int64(i)
		}
	}

	return &valuePruneState{
		BatchState: vgi.NewBatchState(int64(len(emit)), args.BatchSize),
		Values:     emit,
		Resolved:   resolved,
	}, nil
}

func (f *ValuePruneFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *valuePruneState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, valuePruneOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return state.Values[start+i] })
		}
		if projected.Contains("resolved") {
			colMap["resolved"] = vgi.BuildStringArray(size, func(_ int64) string { return state.Resolved })
		}
		return colMap, nil
	})
}

// int64ArrayValues extracts the non-null elements of an integer Arrow array as
// []int64. DuckDB pushes the `n` predicate as int64 literals, but accept the
// narrower signed widths too in case a backend resolves to them.
func int64ArrayValues(arr arrow.Array) []int64 {
	out := make([]int64, 0, arr.Len())
	switch a := arr.(type) {
	case *array.Int64:
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				out = append(out, a.Value(i))
			}
		}
	case *array.Int32:
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				out = append(out, int64(a.Value(i)))
			}
		}
	case *array.Int16:
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				out = append(out, int64(a.Value(i)))
			}
		}
	case *array.Int8:
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				out = append(out, int64(a.Value(i)))
			}
		}
	}
	return out
}

func NewValuePruneFunction() vgi.TableFunction {
	return vgi.AsTableFunction[valuePruneState](&ValuePruneFunction{})
}
