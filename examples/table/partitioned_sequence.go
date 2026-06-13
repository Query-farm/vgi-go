// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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

// partitionedSequenceArgs is the typed argument schema for partitioned_sequence().
type partitionedSequenceArgs struct {
	Count     int64 `vgi:"pos=0,doc=Total number of integers to generate"`
	Increment int64 `vgi:"default=1,doc=Step between values"`
}

func (f *PartitionedSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(partitionedSequenceArgs{})
}

func (f *PartitionedSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (f *PartitionedSequenceFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	var args partitionedSequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	// Create work items for each chunk
	var workItems [][]byte
	for startIdx := int64(0); startIdx < args.Count; startIdx += partitionChunkSize {
		endIdx := startIdx + partitionChunkSize
		if endIdx > args.Count {
			endIdx = args.Count
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
	var args partitionedSequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type partitionedSequenceState struct {
	CurrentStart int64
	CurrentEnd   int64
	CurrentIdx   int64
	HasChunk     bool
	Increment    int64
}

func (f *PartitionedSequenceFunction) NewState(params *vgi.ProcessParams) (*partitionedSequenceState, error) {
	var args partitionedSequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &partitionedSequenceState{
		Increment: args.Increment,
	}, nil
}

func (f *PartitionedSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *partitionedSequenceState, out *vgirpc.OutputCollector) error {
	// If no current chunk or finished, pop next from queue
	if !state.HasChunk || state.CurrentIdx >= state.CurrentEnd {
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
		state.CurrentStart = int64(binary.BigEndian.Uint64(workData[0:8]))
		state.CurrentEnd = int64(binary.BigEndian.Uint64(workData[8:16]))
		state.CurrentIdx = state.CurrentStart
		state.HasChunk = true
	}

	batchEnd := state.CurrentIdx + partitionBatchSize
	if batchEnd > state.CurrentEnd {
		batchEnd = state.CurrentEnd
	}

	start := state.CurrentIdx
	size := batchEnd - start
	arr := vgi.BuildInt64Array(size, func(i int64) int64 {
		return (start + i) * state.Increment
	})
	defer arr.Release()

	state.CurrentIdx = batchEnd
	return out.EmitArrays([]arrow.Array{arr}, size)
}

func NewPartitionedSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[partitionedSequenceState](&PartitionedSequenceFunction{})
}
