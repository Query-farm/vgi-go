// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	partitionChunkSize = 1000
	partitionBatchSize = 1000
)

// PartitionedSequenceFunction generates a partitioned sequence for multi-worker execution.
type PartitionedSequenceFunction struct{}

var _ vgi.TypedTableFunc[partitionedSequenceState] = (*PartitionedSequenceFunction)(nil)

func (f *PartitionedSequenceFunction) Name() string { return "partitioned_sequence" }

func (f *PartitionedSequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates a partitioned sequence for multi-worker execution",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *PartitionedSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Total number of integers to generate", IsConst: true},
		{Name: "increment", Position: -1, ArrowType: "int64", Doc: "Step between values", HasDefault: true, DefaultValue: "1", IsConst: true},
	}
}

func (f *PartitionedSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (f *PartitionedSequenceFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	count, _ := params.Args.GetScalarInt64(0)

	// Create work items for each chunk
	var workItems [][]byte
	for startIdx := int64(0); startIdx < count; startIdx += partitionChunkSize {
		endIdx := startIdx + partitionChunkSize
		if endIdx > count {
			endIdx = count
		}
		item := make([]byte, 16)
		binary.BigEndian.PutUint64(item[0:8], uint64(startIdx))
		binary.BigEndian.PutUint64(item[8:16], uint64(endIdx))
		workItems = append(workItems, item)
	}

	if params.Storage != nil {
		if err := params.Storage.QueuePush(workItems); err != nil {
			return nil, err
		}
	}

	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}

func (f *PartitionedSequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type partitionedSequenceState struct {
	currentStart int64
	currentEnd   int64
	currentIdx   int64
	hasChunk     bool
	increment    int64
}

func (f *PartitionedSequenceFunction) NewState(params *vgi.ProcessParams) (*partitionedSequenceState, error) {
	return &partitionedSequenceState{
		increment: vgi.OptionalInt64(params.Args, "increment", 1),
	}, nil
}

func (f *PartitionedSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *partitionedSequenceState, out *vgirpc.OutputCollector) error {
	// If no current chunk or finished, pop next from queue
	if !state.hasChunk || state.currentIdx >= state.currentEnd {
		if params.Storage == nil {
			return out.Finish()
		}
		workData, err := params.Storage.QueuePop()
		if err != nil {
			return err
		}
		if workData == nil {
			return out.Finish()
		}
		state.currentStart = int64(binary.BigEndian.Uint64(workData[0:8]))
		state.currentEnd = int64(binary.BigEndian.Uint64(workData[8:16]))
		state.currentIdx = state.currentStart
		state.hasChunk = true
	}

	batchEnd := state.currentIdx + partitionBatchSize
	if batchEnd > state.currentEnd {
		batchEnd = state.currentEnd
	}

	start := state.currentIdx
	size := batchEnd - start
	arr := vgi.BuildInt64Array(size, func(i int64) int64 {
		return (start + i) * state.increment
	})
	defer arr.Release()

	state.currentIdx = batchEnd
	return out.EmitArrays([]arrow.Array{arr}, size)
}

func NewPartitionedSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[partitionedSequenceState](&PartitionedSequenceFunction{})
}
