// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"fmt"
	"sync"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================================
// vgi_streaming_sum — streaming-partitioned aggregate (running SUM).
//
// The streaming RPC pipeline (aggregate_streaming_open/_chunk/_close) is not
// yet implemented in the Go SDK. Until it is, this function works correctly
// via the standard update/combine/finalize path — which DuckDB uses for GROUP
// BY queries and for windowed queries when the streaming-window optimizer
// rule is disabled (or when the frame shape is ineligible).
// ============================================================================

type StreamingSumState struct {
	Total int64
}

type StreamingSumFunction struct{}

var _ vgi.AggregateFunction = (*StreamingSumFunction)(nil)

func (StreamingSumFunction) Name() string { return "vgi_streaming_sum" }

func (StreamingSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:          "Running SUM via streaming-partitioned protocol (also works as GROUP BY / window aggregate)",
		Stability:            vgi.StabilityConsistent,
		NullHandling:         vgi.NullHandlingDefault,
		ReturnType:           arrow.PrimitiveTypes.Int64,
		OrderDependent:       "NOT_ORDER_DEPENDENT",
		DistinctDependent:    "NOT_DISTINCT_DEPENDENT",
		SupportsWindow:       true,
		StreamingPartitioned: true,
	}
}

func (StreamingSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Column to sum"},
	}
}

func (StreamingSumFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (StreamingSumFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &StreamingSumState{}
}

func (StreamingSumFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_streaming_sum: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("vgi_streaming_sum: value column is %T, expected int64", columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *StreamingSumState { return &StreamingSumState{} })
		s.Total += col.Value(i)
	}
	return nil
}

func (StreamingSumFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*StreamingSumState)
	t := target.(*StreamingSumState)
	return &StreamingSumState{Total: s.Total + t.Total}, nil
}

func (StreamingSumFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*StreamingSumState); ok {
			b.Append(s.Total)
		} else {
			b.AppendNull()
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// streamingSumSession holds per-execution running totals keyed by partition
// hash. The map is accessed from a single goroutine per session (the chunk
// RPC handler), but the framework is free to demote the session to a
// different OS thread between chunks; the mutex makes that safe.
type streamingSumSession struct {
	mu   sync.Mutex
	sums map[uint64]int64
}

var _ vgi.StreamingAggregateFunction = (*StreamingSumFunction)(nil)

func (StreamingSumFunction) StreamingOpen(_ *vgi.AggregateProcessParams) (interface{}, error) {
	return &streamingSumSession{sums: make(map[uint64]int64)}, nil
}

func (StreamingSumFunction) StreamingChunk(state interface{}, chunk arrow.RecordBatch, partitionKeyCount, orderKeyCount int, params *vgi.AggregateProcessParams) (arrow.Array, error) {
	sess := state.(*streamingSumSession)
	// Value column is the first non-key, non-order column.
	valueIdx := partitionKeyCount + orderKeyCount
	if int(chunk.NumCols()) <= valueIdx {
		return nil, fmt.Errorf("vgi_streaming_sum: chunk has %d cols, expected value column at index %d", chunk.NumCols(), valueIdx)
	}
	valCol, ok := chunk.Column(valueIdx).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("vgi_streaming_sum: value column is %T, expected int64", chunk.Column(valueIdx))
	}

	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Reserve(int(chunk.NumRows()))

	sess.mu.Lock()
	defer sess.mu.Unlock()
	for i := 0; i < int(chunk.NumRows()); i++ {
		key := vgi.PartitionKey(chunk, partitionKeyCount, i)
		if !valCol.IsNull(i) {
			sess.sums[key] = sess.sums[key] + valCol.Value(i)
		}
		// Cumulative running sum. Even when the current row's value is NULL,
		// the running total reflects all prior non-null contributions.
		b.Append(sess.sums[key])
	}
	return b.NewArray(), nil
}

func (StreamingSumFunction) StreamingClose(_ interface{}, _ *vgi.AggregateProcessParams) error {
	return nil
}

// AggregateWindowFunction implementation lets DuckDB fall back to the
// per-row window callback when the streaming-window optimizer rule is
// disabled or when the frame shape isn't streaming-eligible (sliding
// frames, etc).
var _ vgi.AggregateWindowFunction = (*StreamingSumFunction)(nil)

func (StreamingSumFunction) WindowInit(_ *vgi.WindowPartition, _ *vgi.AggregateProcessParams) (interface{}, error) {
	return nil, nil
}

func (StreamingSumFunction) Window(_ int64, subframes [][2]int64, partition *vgi.WindowPartition, _ interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	if partition.Inputs.NumCols() == 0 {
		return nil, fmt.Errorf("vgi_streaming_sum.Window: partition has no input column")
	}
	col, ok := partition.Inputs.Column(0).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("vgi_streaming_sum.Window: input column is %T, expected int64", partition.Inputs.Column(0))
	}
	var total int64
	var hasValue bool
	for _, sf := range subframes {
		for i := sf[0]; i < sf[1]; i++ {
			if partition.FilterMask != nil && !partition.FilterMask[i] {
				continue
			}
			if col.IsNull(int(i)) {
				continue
			}
			total += col.Value(int(i))
			hasValue = true
		}
	}
	if !hasValue {
		return nil, nil
	}
	return total, nil
}
