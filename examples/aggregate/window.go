// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package aggregate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ============================================================================
// vgi_window_sum — windowed integer sum.
// ============================================================================

type WindowSumFunction struct{ SumFunction }

var _ vgi.AggregateWindowFunction = (*WindowSumFunction)(nil)

func (WindowSumFunction) Name() string { return "vgi_window_sum" }

func (WindowSumFunction) Metadata() vgi.FunctionMetadata {
	m := SumFunction{}.Metadata()
	m.SupportsWindow = true
	m.Description = "Windowed sum of integer values"
	return m
}

func (WindowSumFunction) WindowInit(_ *vgi.WindowPartition, _ *vgi.AggregateProcessParams) (interface{}, error) {
	return nil, nil
}

func (WindowSumFunction) Window(rid int64, subframes [][2]int64, partition *vgi.WindowPartition, _ interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	if partition.Inputs.NumCols() == 0 {
		return nil, fmt.Errorf("vgi_window_sum: partition has no input column")
	}
	col, ok := partition.Inputs.Column(0).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("vgi_window_sum: input column is %T, expected int64", partition.Inputs.Column(0))
	}
	var total int64
	var hasValue bool
	for _, sf := range subframes {
		for i := sf[0]; i < sf[1]; i++ {
			if partition.FilterMask != nil && !partition.FilterMask[i] {
				continue
			}
			if col.IsNull(int(i)) {
				continue
			}
			total += col.Value(int(i))
			hasValue = true
		}
	}
	if !hasValue {
		return nil, nil
	}
	return total, nil
}

// ============================================================================
// vgi_window_median — windowed median.
// ============================================================================

type WindowMedianFunction struct{ AvgFunction }

// windowMedianArgs is the typed argument schema for vgi_window_median().
type windowMedianArgs struct {
	Value float64 `vgi:"pos=0,const=false,doc=Numeric column"`
}

func (WindowMedianFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(windowMedianArgs{})
}

// Override Update so non-window calls (DuckDB invokes both update and window
// callbacks for SUPPORTS_WINDOW aggregates) accept a DOUBLE column instead
// of AvgFunction's int64-only path.
func (WindowMedianFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return nil
	}
	col := columns[0]
	for i := 0; i < gids.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *AvgState { return &AvgState{} })
		s.Total += scalarFloat(col, i)
		s.Count++
	}
	return nil
}

var _ vgi.AggregateWindowFunction = (*WindowMedianFunction)(nil)

func (WindowMedianFunction) Name() string { return "vgi_window_median" }

func (WindowMedianFunction) Metadata() vgi.FunctionMetadata {
	m := AvgFunction{}.Metadata()
	m.SupportsWindow = true
	m.Description = "Windowed median of numeric values"
	return m
}

func (WindowMedianFunction) WindowInit(_ *vgi.WindowPartition, _ *vgi.AggregateProcessParams) (interface{}, error) {
	return nil, nil
}

func (WindowMedianFunction) Window(rid int64, subframes [][2]int64, partition *vgi.WindowPartition, _ interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	if partition.Inputs.NumCols() == 0 {
		return nil, fmt.Errorf("vgi_window_median: partition has no input column")
	}
	col := partition.Inputs.Column(0)
	values := make([]float64, 0)
	for _, sf := range subframes {
		for i := sf[0]; i < sf[1]; i++ {
			if partition.FilterMask != nil && !partition.FilterMask[i] {
				continue
			}
			if col.IsNull(int(i)) {
				continue
			}
			values = append(values, scalarFloat(col, int(i)))
		}
	}
	if len(values) == 0 {
		return nil, nil
	}
	sort.Float64s(values)
	n := len(values)
	if n%2 == 1 {
		return values[n/2], nil
	}
	return (values[n/2-1] + values[n/2]) / 2, nil
}

// ============================================================================
// vgi_window_listagg — windowed string concatenation.
// ============================================================================

type WindowListAggFunction struct{ ListAggFunction }

var _ vgi.AggregateWindowFunction = (*WindowListAggFunction)(nil)

func (WindowListAggFunction) Name() string { return "vgi_window_listagg" }

func (WindowListAggFunction) Metadata() vgi.FunctionMetadata {
	m := ListAggFunction{}.Metadata()
	m.SupportsWindow = true
	m.Description = "Windowed string concatenation"
	return m
}

func (WindowListAggFunction) WindowInit(_ *vgi.WindowPartition, _ *vgi.AggregateProcessParams) (interface{}, error) {
	return nil, nil
}

func (WindowListAggFunction) Window(rid int64, subframes [][2]int64, partition *vgi.WindowPartition, _ interface{}, params *vgi.AggregateProcessParams) (interface{}, error) {
	if partition.Inputs.NumCols() == 0 {
		return nil, fmt.Errorf("vgi_window_listagg: partition has no input column")
	}
	col, ok := partition.Inputs.Column(0).(*array.String)
	if !ok {
		return nil, fmt.Errorf("vgi_window_listagg: input column is %T, expected string", partition.Inputs.Column(0))
	}
	sep := ","
	if params.Args != nil && len(params.Args.Positional) > 1 {
		if v, ok := scalarString(params.Args.Positional[1]); ok {
			sep = v
		}
	}
	var parts []string
	for _, sf := range subframes {
		for i := sf[0]; i < sf[1]; i++ {
			if partition.FilterMask != nil && !partition.FilterMask[i] {
				continue
			}
			if col.IsNull(int(i)) {
				continue
			}
			parts = append(parts, col.Value(int(i)))
		}
	}
	if len(parts) == 0 {
		return nil, nil
	}
	return strings.Join(parts, sep), nil
}

// avoid unused-import linter when arrow isn't otherwise referenced
var _ = arrow.PrimitiveTypes.Int64
