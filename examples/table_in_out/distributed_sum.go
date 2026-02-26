// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DistributedSumFunction computes distributed column-wise sums.
// This is equivalent to SumAllColumnsFunction but demonstrates the
// distributed aggregation pattern using storage for state persistence.
type DistributedSumFunction struct{}

func (f *DistributedSumFunction) Name() string { return "sum_all_columns_simple_distributed" }

func (f *DistributedSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Distributed sum using simple callback API",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *DistributedSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *DistributedSumFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil {
		return &vgi.BindResponse{
			OutputSchema: arrow.NewSchema(nil, nil),
		}, nil
	}

	var fields []arrow.Field
	for _, field := range params.InputSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
			arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
			fields = append(fields, arrow.Field{Name: field.Name, Type: arrow.PrimitiveTypes.Int64})
		case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
			fields = append(fields, arrow.Field{Name: field.Name, Type: arrow.PrimitiveTypes.Float64})
		}
	}

	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema(fields, nil),
	}, nil
}

func (f *DistributedSumFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

type distributedSumState struct {
	intSums   map[string]int64
	floatSums map[string]float64
}

func (f *DistributedSumFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	intSums := make(map[string]int64)
	floatSums := make(map[string]float64)

	for _, field := range params.OutputSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			intSums[field.Name] = 0
		case arrow.FLOAT64:
			floatSums[field.Name] = 0
		}
	}

	return &distributedSumState{
		intSums:   intSums,
		floatSums: floatSums,
	}, nil
}

func (f *DistributedSumFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	s := state.(*distributedSumState)

	for _, field := range params.OutputSchema.Fields() {
		colIdx := -1
		for i := 0; i < int(batch.NumCols()); i++ {
			if batch.ColumnName(i) == field.Name {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			continue
		}
		col := batch.Column(colIdx)

		if _, ok := s.intSums[field.Name]; ok {
			s.intSums[field.Name] += sumInt64Column(col)
		}
		if _, ok := s.floatSums[field.Name]; ok {
			s.floatSums[field.Name] += sumFloat64Column(col)
		}
	}

	// Persist partial sums to storage
	if params.Storage != nil {
		sumBatch := buildDistSumBatch(params.OutputSchema, s.intSums, s.floatSums)
		data, err := vgi.SerializeRecordBatch(sumBatch)
		if err == nil {
			params.Storage.Put(data)
		}
	}

	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *DistributedSumFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	intSums := make(map[string]int64)
	floatSums := make(map[string]float64)

	for _, field := range params.OutputSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			intSums[field.Name] = 0
		case arrow.FLOAT64:
			floatSums[field.Name] = 0
		}
	}

	if params.Storage != nil {
		for _, data := range params.Storage.Collect() {
			batch, err := vgi.DeserializeRecordBatch(data)
			if err != nil {
				continue
			}
			for _, field := range params.OutputSchema.Fields() {
				colIdx := -1
				for i := 0; i < int(batch.NumCols()); i++ {
					if batch.ColumnName(i) == field.Name {
						colIdx = i
						break
					}
				}
				if colIdx < 0 {
					continue
				}
				col := batch.Column(colIdx)
				if _, ok := intSums[field.Name]; ok {
					intSums[field.Name] += sumInt64Column(col)
				}
				if _, ok := floatSums[field.Name]; ok {
					floatSums[field.Name] += sumFloat64Column(col)
				}
			}
			batch.Release()
		}
	}

	return []arrow.RecordBatch{buildDistSumBatch(params.OutputSchema, intSums, floatSums)}, nil
}

func buildDistSumBatch(schema *arrow.Schema, intSums map[string]int64, floatSums map[string]float64) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())

	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			b := array.NewInt64Builder(mem)
			b.Append(intSums[field.Name])
			cols[i] = b.NewArray()
			b.Release()
		case arrow.FLOAT64:
			b := array.NewFloat64Builder(mem)
			b.Append(floatSums[field.Name])
			cols[i] = b.NewArray()
			b.Release()
		}
	}

	batch := array.NewRecordBatch(schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return batch
}
