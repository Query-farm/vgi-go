// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// Multi-worker partitioned sequence variants — one per OrderPreservation mode.
// Each generates 0..count-1, dispatched across workers via the per-invocation
// work queue. The only difference between them is the OrderPreservation value
// on Metadata, which is what these fixtures exist to exercise end-to-end.

type orderModeState struct {
	CurrentStart int64
	CurrentEnd   int64
	CurrentIdx   int64
	HasChunk     bool
}

type partitionedOrderModeFunc struct {
	name              string
	description       string
	orderPreservation vgi.OrderPreservation
}

var _ vgi.TypedTableFunc[orderModeState] = (*partitionedOrderModeFunc)(nil)

func (f *partitionedOrderModeFunc) Name() string { return f.name }

func (f *partitionedOrderModeFunc) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       f.description,
		Stability:         vgi.StabilityConsistent,
		OrderPreservation: f.orderPreservation,
	}
}

// partitionedOrderModeArgs is the typed argument schema for the
// partitioned_*_order family.
type partitionedOrderModeArgs struct {
	Count int64 `vgi:"pos=0,doc=Total number of integers to generate"`
}

func (f *partitionedOrderModeFunc) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(partitionedOrderModeArgs{})
}

func (f *partitionedOrderModeFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (f *partitionedOrderModeFunc) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args partitionedOrderModeArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

func (f *partitionedOrderModeFunc) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	var args partitionedOrderModeArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
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

func (f *partitionedOrderModeFunc) NewState(params *vgi.ProcessParams) (*orderModeState, error) {
	return &orderModeState{}, nil
}

func (f *partitionedOrderModeFunc) Process(ctx context.Context, params *vgi.ProcessParams, state *orderModeState, out *vgirpc.OutputCollector) error {
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
	arr := vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
	defer arr.Release()
	state.CurrentIdx = batchEnd
	return out.EmitArrays([]arrow.Array{arr}, size)
}

func NewPartitionedFixedOrderFunction() vgi.TableFunction {
	return vgi.AsTableFunction[orderModeState](&partitionedOrderModeFunc{
		name:              "partitioned_fixed_order",
		description:       "Multi-worker partitioned sequence; preserves_order=FIXED_ORDER (DuckDB serializes the pipeline).",
		orderPreservation: "FIXED_ORDER",
	})
}

func NewPartitionedPreservesOrderFunction() vgi.TableFunction {
	return vgi.AsTableFunction[orderModeState](&partitionedOrderModeFunc{
		name:              "partitioned_preserves_order",
		description:       "Multi-worker partitioned sequence; preserves_order=PRESERVES_ORDER (maps to DuckDB INSERTION_ORDER).",
		orderPreservation: "PRESERVES_ORDER",
	})
}

func NewPartitionedNoOrderGuaranteeFunction() vgi.TableFunction {
	return vgi.AsTableFunction[orderModeState](&partitionedOrderModeFunc{
		name:              "partitioned_no_order_guarantee",
		description:       "Multi-worker partitioned sequence; preserves_order=NO_ORDER_GUARANTEE (maps to DuckDB NO_ORDER).",
		orderPreservation: "NO_ORDER_GUARANTEE",
	})
}
