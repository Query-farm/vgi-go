// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	partitionChunkSize = 1000
	partitionBatchSize = 1000
)

// PartitionedSequenceFunction generates a partitioned sequence for multi-worker execution.
type PartitionedSequenceFunction struct{}

func (f *PartitionedSequenceFunction) Name() string { return "partitioned_sequence" }

func (f *PartitionedSequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates a partitioned sequence for multi-worker execution",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *PartitionedSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Total number of integers to generate"},
		{Name: "increment", Position: -1, ArrowType: "int64", Doc: "Step between values", HasDefault: true, DefaultValue: "1", IsConst: true},
	}
}

func (f *PartitionedSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
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
		params.Storage.QueuePush(workItems)
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

func (f *PartitionedSequenceFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	increment := int64(1)
	if !params.Args.IsNull("increment") {
		if v, err := params.Args.GetScalarInt64("increment"); err == nil {
			increment = v
		}
	}
	return &partitionedSequenceState{increment: increment}, nil
}

func (f *PartitionedSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*partitionedSequenceState)

	// If no current chunk or finished, pop next from queue
	if !s.hasChunk || s.currentIdx >= s.currentEnd {
		if params.Storage == nil {
			return out.Finish()
		}
		workData := params.Storage.QueuePop()
		if workData == nil {
			return out.Finish()
		}
		s.currentStart = int64(binary.BigEndian.Uint64(workData[0:8]))
		s.currentEnd = int64(binary.BigEndian.Uint64(workData[8:16]))
		s.currentIdx = s.currentStart
		s.hasChunk = true
	}

	batchEnd := s.currentIdx + partitionBatchSize
	if batchEnd > s.currentEnd {
		batchEnd = s.currentEnd
	}

	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	for idx := s.currentIdx; idx < batchEnd; idx++ {
		builder.Append(idx * s.increment)
	}

	arr := builder.NewArray()
	defer arr.Release()

	size := batchEnd - s.currentIdx
	s.currentIdx = batchEnd
	return out.EmitArrays([]arrow.Array{arr}, size)
}
