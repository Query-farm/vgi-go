// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var nestedSequenceFullSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "metadata", Type: arrow.StructOf(
		arrow.Field{Name: "index", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
	)},
	{Name: "history", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
}, nil)

// NestedSequenceFunction generates a sequence with nested struct and list columns.
type NestedSequenceFunction struct{}

func (f *NestedSequenceFunction) Name() string { return "nested_sequence" }

func (f *NestedSequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Generates a sequence with nested struct and list columns",
		Stability:         vgi.StabilityConsistent,
		ProjectionPushdown: true,
	}
}

func (f *NestedSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate"},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size for output", HasDefault: true, DefaultValue: "1000", IsConst: true},
		{Name: "history_size", Position: -1, ArrowType: "int64", Doc: "Max items in history list", HasDefault: true, DefaultValue: "20", IsConst: true},
	}
}

func (f *NestedSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: nestedSequenceFullSchema}, nil
}

func (f *NestedSequenceFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *NestedSequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type nestedSequenceState struct {
	remaining    int64
	currentIndex int64
	batchSize    int64
	historySize  int64
}

func (f *NestedSequenceFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	count, _ := params.Args.GetScalarInt64(0)
	batchSize := int64(1000)
	if !params.Args.IsNull("batch_size") {
		if v, err := params.Args.GetScalarInt64("batch_size"); err == nil {
			batchSize = v
		}
	}
	historySize := int64(20)
	if !params.Args.IsNull("history_size") {
		if v, err := params.Args.GetScalarInt64("history_size"); err == nil {
			historySize = v
		}
	}
	return &nestedSequenceState{
		remaining:    count,
		currentIndex: 0,
		batchSize:    batchSize,
		historySize:  historySize,
	}, nil
}

func (f *NestedSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*nestedSequenceState)
	if s.remaining <= 0 {
		return out.Finish()
	}

	size := s.batchSize
	if s.remaining < size {
		size = s.remaining
	}

	mem := memory.NewGoAllocator()
	projectedCols := getProjectedColumnNames(params.ProjectionIDs, nestedSequenceFullSchema)

	// Build only projected columns
	colMap := make(map[string]arrow.Array)

	if _, ok := projectedCols["n"]; ok {
		b := array.NewInt64Builder(mem)
		for i := int64(0); i < size; i++ {
			b.Append(s.currentIndex + i)
		}
		colMap["n"] = b.NewArray()
		b.Release()
	}

	if _, ok := projectedCols["metadata"]; ok {
		structType := arrow.StructOf(
			arrow.Field{Name: "index", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
		)
		sb := array.NewStructBuilder(mem, structType)
		indexBuilder := sb.FieldBuilder(0).(*array.Int64Builder)
		labelBuilder := sb.FieldBuilder(1).(*array.StringBuilder)
		for i := int64(0); i < size; i++ {
			sb.Append(true)
			idx := s.currentIndex + i
			indexBuilder.Append(idx)
			labelBuilder.Append(fmt.Sprintf("row_%d", idx))
		}
		colMap["metadata"] = sb.NewArray()
		sb.Release()
	}

	if _, ok := projectedCols["history"]; ok {
		lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
		valueBuilder := lb.ValueBuilder().(*array.Int64Builder)
		for i := int64(0); i < size; i++ {
			lb.Append(true)
			idx := s.currentIndex + i
			start := idx - s.historySize + 1
			if start < 0 {
				start = 0
			}
			for j := start; j <= idx; j++ {
				valueBuilder.Append(j)
			}
		}
		colMap["history"] = lb.NewArray()
		lb.Release()
	}

	// Build record batch with projected schema
	cols := make([]arrow.Array, params.OutputSchema.NumFields())
	for i, f := range params.OutputSchema.Fields() {
		cols[i] = colMap[f.Name]
	}
	batch := array.NewRecordBatch(params.OutputSchema, cols, size)
	for _, c := range cols {
		c.Release()
	}

	s.currentIndex += size
	s.remaining -= size
	return out.Emit(batch)
}

func getProjectedColumnNames(projectionIDs []int32, fullSchema *arrow.Schema) map[string]struct{} {
	result := make(map[string]struct{})
	if projectionIDs != nil {
		for _, id := range projectionIDs {
			result[fullSchema.Field(int(id)).Name] = struct{}{}
		}
	} else {
		for _, f := range fullSchema.Fields() {
			result[f.Name] = struct{}{}
		}
	}
	return result
}
