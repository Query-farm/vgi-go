// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FilterBySettingFunction filters rows where value column >= threshold setting.
type FilterBySettingFunction struct{}

var _ vgi.TypedTableInOutFunc[filterBySettingState] = (*FilterBySettingFunction)(nil)

func (f *FilterBySettingFunction) Name() string { return "filter_by_setting" }

func (f *FilterBySettingFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Filter rows where value column >= threshold setting",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *FilterBySettingFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *FilterBySettingFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

type filterBySettingState struct {
	Threshold int64
}

func (f *FilterBySettingFunction) NewState(params *vgi.ProcessParams) (*filterBySettingState, error) {
	threshold := int64(0)
	if params.Settings != nil {
		if v, ok := params.Settings["threshold"]; ok {
			if t, ok := v.(int64); ok {
				threshold = t
			}
		}
	}
	return &filterBySettingState{Threshold: threshold}, nil
}

func (f *FilterBySettingFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *filterBySettingState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	valueCol := vgi.FindColumn(batch, "value")
	if valueCol == nil {
		return out.Emit(batch)
	}

	// Find which rows pass the filter
	numRows := int(batch.NumRows())
	var kept []int
	for i := 0; i < numRows; i++ {
		if !valueCol.IsNull(i) && vgi.GetInt64Value(valueCol, i) >= state.Threshold {
			kept = append(kept, i)
		}
	}

	if len(kept) == numRows {
		return out.Emit(batch)
	}
	if len(kept) == 0 {
		return nil
	}

	// Build filtered batch
	mem := memory.NewGoAllocator()
	numCols := int(batch.NumCols())
	cols := make([]arrow.Array, numCols)
	for c := 0; c < numCols; c++ {
		srcCol := batch.Column(c)
		b := array.NewBuilder(mem, srcCol.DataType())
		for _, idx := range kept {
			if srcCol.IsNull(idx) {
				b.AppendNull()
			} else {
				appendValue(b, srcCol, idx)
			}
		}
		cols[c] = b.NewArray()
		b.Release()
	}

	result := array.NewRecordBatch(params.OutputSchema, cols, int64(len(kept)))
	for _, c := range cols {
		c.Release()
	}

	return out.Emit(result)
}

func (f *FilterBySettingFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *filterBySettingState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewFilterBySettingFunction creates a FilterBySettingFunction wrapped for registration.
func NewFilterBySettingFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[filterBySettingState](&FilterBySettingFunction{})
}
