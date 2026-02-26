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
)

var projectedDataFullSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	{Name: "extra", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// ProjectedDataFunction generates data with 4 columns, supporting projection pushdown.
type ProjectedDataFunction struct{}

var _ vgi.TypedTableFunc[projectedDataState] = (*ProjectedDataFunction)(nil)

func (f *ProjectedDataFunction) Name() string { return "projected_data" }

func (f *ProjectedDataFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Generates data with 4 columns, supporting projection pushdown",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
	}
}

func (f *ProjectedDataFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
	}
}

func (f *ProjectedDataFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(projectedDataFullSchema)
}

func (f *ProjectedDataFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type projectedDataState struct {
	vgi.BatchState
}

const projectedDataBatchSize = 1000

func (f *ProjectedDataFunction) NewState(params *vgi.ProcessParams) (*projectedDataState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &projectedDataState{
		BatchState: vgi.NewBatchState(count, projectedDataBatchSize),
	}, nil
}

func (f *ProjectedDataFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *projectedDataState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}

	size := state.BatchSize
	if state.Remaining < size {
		size = state.Remaining
	}

	projected := vgi.ProjectedColumns(params.ProjectionIDs, projectedDataFullSchema)
	start := state.Index
	colMap := make(map[string]arrow.Array)

	if projected.Contains("id") {
		colMap["id"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
	}
	if projected.Contains("name") {
		colMap["name"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("item_%d", start+i) })
	}
	if projected.Contains("value") {
		colMap["value"] = vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * 1.5 })
	}
	if projected.Contains("extra") {
		colMap["extra"] = vgi.BuildInt64Array(size, func(i int64) int64 { v := start + i; return v * v })
	}

	cols := make([]arrow.Array, params.OutputSchema.NumFields())
	for i, f := range params.OutputSchema.Fields() {
		cols[i] = colMap[f.Name]
	}
	batch := array.NewRecordBatch(params.OutputSchema, cols, size)
	for _, c := range cols {
		c.Release()
	}

	state.Remaining -= size
	state.Index += size
	return out.Emit(batch)
}

func NewProjectedDataFunction() vgi.TableFunction {
	return vgi.AsTableFunction[projectedDataState](&ProjectedDataFunction{})
}
