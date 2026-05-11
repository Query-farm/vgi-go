// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"encoding/binary"
	"os"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

var filterEchoPartitionedSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "worker_pid", Type: arrow.PrimitiveTypes.Int64},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

// FilterEchoPartitionedFunction is the multi-worker partner of filter_echo:
// distributes work via a shared queue, each worker independently observes
// the pushed filter spec, and emits batches the framework auto-applies the
// filter to. worker_pid carries os.Getpid() so multi-process engagement is
// visible under subprocess transport.
type FilterEchoPartitionedFunction struct{}

var _ vgi.TypedTableFunc[filterEchoPartitionedState] = (*FilterEchoPartitionedFunction)(nil)

func (f *FilterEchoPartitionedFunction) Name() string { return "filter_echo_partitioned" }

func (f *FilterEchoPartitionedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Multi-worker partitioned sequence that echoes pushed-down filters",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic", "testing"},
	}
}

func (f *FilterEchoPartitionedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Total number of integers to generate", IsConst: true},
	}
}

func (f *FilterEchoPartitionedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(filterEchoPartitionedSchema)
}

func (f *FilterEchoPartitionedFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

func (f *FilterEchoPartitionedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	count, _ := params.Args.GetScalarInt64(0)
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

type filterEchoPartitionedState struct {
	CurrentStart int64
	CurrentEnd   int64
	CurrentIdx   int64
	HasChunk     bool
	FilterStr    string
	WorkerPID    int64
}

func (f *FilterEchoPartitionedFunction) NewState(params *vgi.ProcessParams) (*filterEchoPartitionedState, error) {
	filterStr := "(none)"
	if params.PushdownFilters != nil {
		pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
		if err == nil && len(pf.Filters) > 0 {
			filterStr = formatFiltersInline(pf)
		}
	}
	return &filterEchoPartitionedState{
		FilterStr: filterStr,
		WorkerPID: int64(os.Getpid()),
	}, nil
}

func (f *FilterEchoPartitionedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *filterEchoPartitionedState, out *vgirpc.OutputCollector) error {
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

	projected := vgi.ProjectedColumns(params.ProjectionIDs, filterEchoPartitionedSchema)
	colMap := make(map[string]arrow.Array)
	if projected.Contains("n") {
		colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
	}
	if projected.Contains("worker_pid") {
		pid := state.WorkerPID
		colMap["worker_pid"] = vgi.BuildInt64Array(size, func(_ int64) int64 { return pid })
	}
	if projected.Contains("pushed_filters") {
		colMap["pushed_filters"] = vgi.BuildStringArray(size, func(_ int64) string { return state.FilterStr })
	}

	state.CurrentIdx = batchEnd

	if err := emitColumnMap(out, params.OutputSchema, colMap, size); err != nil {
		return err
	}
	return nil
}

// emitColumnMap orders the columns to match params.OutputSchema (post-projection)
// and emits the batch.
func emitColumnMap(out *vgirpc.OutputCollector, schema *arrow.Schema, colMap map[string]arrow.Array, size int64) error {
	arrs := make([]arrow.Array, 0, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		name := schema.Field(i).Name
		a, ok := colMap[name]
		if !ok {
			// Should not happen if caller respected projection.
			continue
		}
		arrs = append(arrs, a)
	}
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	return out.EmitArrays(arrs, size)
}

func NewFilterEchoPartitionedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[filterEchoPartitionedState](&FilterEchoPartitionedFunction{})
}
