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

var projectedDataFullSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	{Name: "extra", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// ProjectedDataFunction generates data with 4 columns, supporting projection pushdown.
type ProjectedDataFunction struct{}

func (f *ProjectedDataFunction) Name() string { return "projected_data" }

func (f *ProjectedDataFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Generates data with 4 columns, supporting projection pushdown",
		Stability:         vgi.StabilityConsistent,
		ProjectionPushdown: true,
	}
}

func (f *ProjectedDataFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
	}
}

func (f *ProjectedDataFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: projectedDataFullSchema}, nil
}

func (f *ProjectedDataFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *ProjectedDataFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type projectedDataState struct {
	remaining    int64
	currentIndex int64
}

func (f *ProjectedDataFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &projectedDataState{remaining: count, currentIndex: 0}, nil
}

const projectedDataBatchSize = 1000

func (f *ProjectedDataFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*projectedDataState)
	if s.remaining <= 0 {
		return out.Finish()
	}

	size := int64(projectedDataBatchSize)
	if s.remaining < size {
		size = s.remaining
	}

	mem := memory.NewGoAllocator()
	projectedCols := getProjectedColumnNames(params.ProjectionIDs, projectedDataFullSchema)

	colMap := make(map[string]arrow.Array)

	if _, ok := projectedCols["id"]; ok {
		b := array.NewInt64Builder(mem)
		for i := int64(0); i < size; i++ {
			b.Append(s.currentIndex + i)
		}
		colMap["id"] = b.NewArray()
		b.Release()
	}

	if _, ok := projectedCols["name"]; ok {
		b := array.NewStringBuilder(mem)
		for i := int64(0); i < size; i++ {
			b.Append(fmt.Sprintf("item_%d", s.currentIndex+i))
		}
		colMap["name"] = b.NewArray()
		b.Release()
	}

	if _, ok := projectedCols["value"]; ok {
		b := array.NewFloat64Builder(mem)
		for i := int64(0); i < size; i++ {
			b.Append(float64(s.currentIndex+i) * 1.5)
		}
		colMap["value"] = b.NewArray()
		b.Release()
	}

	if _, ok := projectedCols["extra"]; ok {
		b := array.NewInt64Builder(mem)
		for i := int64(0); i < size; i++ {
			v := s.currentIndex + i
			b.Append(v * v)
		}
		colMap["extra"] = b.NewArray()
		b.Release()
	}

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
