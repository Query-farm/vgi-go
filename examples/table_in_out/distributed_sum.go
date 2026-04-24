// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// DistributedSumFunction computes distributed column-wise sums.
// This is equivalent to SumAllColumnsFunction but demonstrates the
// distributed aggregation pattern using storage for state persistence.
type DistributedSumFunction struct{}

var _ vgi.TypedTableInOutFunc[distributedSumState] = (*DistributedSumFunction)(nil)

func (f *DistributedSumFunction) Name() string { return "sum_all_columns_simple_distributed" }

func (f *DistributedSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Distributed sum using simple callback API",
		Stability:   vgi.StabilityConsistent,
		HasFinalize: true,
	}
}

func (f *DistributedSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *DistributedSumFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return sumColumnsOnBind(params)
}

type distributedSumState struct {
	IntSums   map[string]int64
	FloatSums map[string]float64
}

func (f *DistributedSumFunction) NewState(params *vgi.ProcessParams) (*distributedSumState, error) {
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
		IntSums:   intSums,
		FloatSums: floatSums,
	}, nil
}

func (f *DistributedSumFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *distributedSumState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	for _, field := range params.OutputSchema.Fields() {
		col := vgi.FindColumn(batch, field.Name)
		if col == nil {
			continue
		}

		if _, ok := state.IntSums[field.Name]; ok {
			state.IntSums[field.Name] += sumInt64Column(col)
		}
		if _, ok := state.FloatSums[field.Name]; ok {
			state.FloatSums[field.Name] += sumFloat64Column(col)
		}
	}

	// Persist partial sums to storage
	if params.Storage != nil {
		sumBatch := buildSumBatch(params.OutputSchema, state.IntSums, state.FloatSums)
		data, err := vgi.SerializeRecordBatch(sumBatch)
		if err != nil {
			return err
		}
		if err := params.Storage.Put(data); err != nil {
			return err
		}
	}

	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *DistributedSumFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *distributedSumState) ([]arrow.RecordBatch, error) {
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
		workerData, err := params.Storage.Collect()
		if err != nil {
			return nil, err
		}
		for _, data := range workerData {
			batch, err := vgi.DeserializeRecordBatch(data)
			if err != nil {
				continue
			}
			for _, field := range params.OutputSchema.Fields() {
				col := vgi.FindColumn(batch, field.Name)
				if col == nil {
					continue
				}
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

	return []arrow.RecordBatch{buildSumBatch(params.OutputSchema, intSums, floatSums)}, nil
}

// NewDistributedSumFunction creates a DistributedSumFunction wrapped for registration.
func NewDistributedSumFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[distributedSumState](&DistributedSumFunction{})
}
