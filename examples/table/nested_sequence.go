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

var _ vgi.TypedTableFunc[nestedSequenceState] = (*NestedSequenceFunction)(nil)

func (f *NestedSequenceFunction) Name() string { return "nested_sequence" }

func (f *NestedSequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Generates a sequence with nested struct and list columns",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
	}
}

// nestedSequenceArgs is the typed argument schema for nested_sequence().
type nestedSequenceArgs struct {
	Count       int64 `vgi:"pos=0,doc=Number of rows to generate"`
	BatchSize   int64 `vgi:"default=1000,doc=Batch size for output"`
	HistorySize int64 `vgi:"default=20,doc=Max items in history list"`
}

func (f *NestedSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(nestedSequenceArgs{})
}

func (f *NestedSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(nestedSequenceFullSchema)
}

func (f *NestedSequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args nestedSequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type nestedSequenceState struct {
	vgi.BatchState
	HistorySize int64
}

func (f *NestedSequenceFunction) NewState(params *vgi.ProcessParams) (*nestedSequenceState, error) {
	var args nestedSequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &nestedSequenceState{
		BatchState:  vgi.NewBatchState(args.Count, args.BatchSize),
		HistorySize: args.HistorySize,
	}, nil
}

func (f *NestedSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *nestedSequenceState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, nestedSequenceFullSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		mem := memory.NewGoAllocator()
		start := state.Index
		colMap := make(map[string]arrow.Array)

		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}

		if projected.Contains("metadata") {
			structType := arrow.StructOf(
				arrow.Field{Name: "index", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
			)
			sb := array.NewStructBuilder(mem, structType)
			indexBuilder := sb.FieldBuilder(0).(*array.Int64Builder)
			labelBuilder := sb.FieldBuilder(1).(*array.StringBuilder)
			for i := int64(0); i < size; i++ {
				sb.Append(true)
				idx := start + i
				indexBuilder.Append(idx)
				labelBuilder.Append(fmt.Sprintf("row_%d", idx))
			}
			colMap["metadata"] = sb.NewArray()
			sb.Release()
		}

		if projected.Contains("history") {
			lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
			valueBuilder := lb.ValueBuilder().(*array.Int64Builder)
			for i := int64(0); i < size; i++ {
				lb.Append(true)
				idx := start + i
				histStart := idx - state.HistorySize + 1
				if histStart < 0 {
					histStart = 0
				}
				for j := histStart; j <= idx; j++ {
					valueBuilder.Append(j)
				}
			}
			colMap["history"] = lb.NewArray()
			lb.Release()
		}

		return colMap, nil
	})
}

func NewNestedSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[nestedSequenceState](&NestedSequenceFunction{})
}
