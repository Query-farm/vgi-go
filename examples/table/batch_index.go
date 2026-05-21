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
)

const batchIndexChunk = 1000

// packWorkItem encodes (partitionID, start, end) as three big-endian int64s.
func packWorkItem(partitionID, start, end int64) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b[0:8], uint64(partitionID))
	binary.BigEndian.PutUint64(b[8:16], uint64(start))
	binary.BigEndian.PutUint64(b[16:24], uint64(end))
	return b
}
func unpackWorkItem(b []byte) (partitionID, start, end int64) {
	return int64(binary.BigEndian.Uint64(b[0:8])),
		int64(binary.BigEndian.Uint64(b[8:16])),
		int64(binary.BigEndian.Uint64(b[16:24]))
}

type batchIndexArgs struct {
	Count int64 `vgi:"pos=0,doc=Total number of rows to generate"`
}

// pushBatchIndexWork enqueues one (partition_id, start, end) item per chunk.
func pushBatchIndexWork(params *vgi.InitParams, count, chunk int64) error {
	var items [][]byte
	pid := int64(0)
	for start := int64(0); start < count; start += chunk {
		end := start + chunk
		if end > count {
			end = count
		}
		items = append(items, packWorkItem(pid, start, end))
		pid++
	}
	if params.Storage != nil {
		return params.Storage.QueuePush(items)
	}
	return nil
}

// PartitionedBatchIndexFunction is a parallel sequence whose batches are tagged
// with a per-partition vgi_batch_index. Mirrors vgi-python's
// PartitionedBatchIndexFunction.
type PartitionedBatchIndexFunction struct{}

var _ vgi.TypedTableFunc[batchIndexState] = (*PartitionedBatchIndexFunction)(nil)

func (f *PartitionedBatchIndexFunction) Name() string { return "partitioned_batch_index" }
func (f *PartitionedBatchIndexFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Multi-worker partitioned sequence with per-batch batch_index tagging",
		Stability:          vgi.StabilityConsistent,
		Categories:         []string{"generator", "utility"},
		OrderPreservation:  vgi.OrderPreservationFixedOrder,
		SupportsBatchIndex: true,
	}
}
func (f *PartitionedBatchIndexFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(batchIndexArgs{})
}
func (f *PartitionedBatchIndexFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *PartitionedBatchIndexFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	var args batchIndexArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if err := pushBatchIndexWork(params, args.Count, batchIndexChunk); err != nil {
		return nil, err
	}
	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}
func (f *PartitionedBatchIndexFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args batchIndexArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type batchIndexState struct{}

func (f *PartitionedBatchIndexFunction) NewState(params *vgi.ProcessParams) (*batchIndexState, error) {
	return &batchIndexState{}, nil
}
func (f *PartitionedBatchIndexFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *batchIndexState, out *vgirpc.OutputCollector) error {
	if params.Storage == nil {
		return out.Finish()
	}
	work, err := params.Storage.QueuePop()
	if err != nil {
		return err
	}
	if work == nil {
		return out.Finish()
	}
	pid, start, end := unpackWorkItem(work)
	arr := vgi.BuildInt64Array(end-start, func(i int64) int64 { return start + i })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, end-start)
	return vgi.EmitBatchIndex(out, batch, pid)
}

// NewPartitionedBatchIndexFunction wraps the function for registration.
func NewPartitionedBatchIndexFunction() vgi.TableFunction {
	return vgi.AsTableFunction[batchIndexState](&PartitionedBatchIndexFunction{})
}

// PartitionedBatchIndexMarkedFunction emits (partition_id, seq) rows so tests
// can directly observe partition ordering. Projection pushdown is off.
type PartitionedBatchIndexMarkedFunction struct{}

var _ vgi.TypedTableFunc[batchIndexState] = (*PartitionedBatchIndexMarkedFunction)(nil)

type batchIndexMarkedArgs struct {
	Count     int64 `vgi:"pos=0,doc=Total number of rows to generate"`
	ChunkSize int64 `vgi:"default=1000,doc=Rows per partition"`
}

func (f *PartitionedBatchIndexMarkedFunction) Name() string { return "partitioned_batch_index_marked" }
func (f *PartitionedBatchIndexMarkedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Two-column batch_index demo: rows are (partition_id, seq)",
		Stability:          vgi.StabilityConsistent,
		Categories:         []string{"generator", "utility"},
		OrderPreservation:  vgi.OrderPreservationFixedOrder,
		SupportsBatchIndex: true,
	}
}
func (f *PartitionedBatchIndexMarkedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(batchIndexMarkedArgs{})
}
func (f *PartitionedBatchIndexMarkedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "partition_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "seq", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}
func (f *PartitionedBatchIndexMarkedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	var args batchIndexMarkedArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	chunk := args.ChunkSize
	if chunk <= 0 {
		chunk = 1000
	}
	if err := pushBatchIndexWork(params, args.Count, chunk); err != nil {
		return nil, err
	}
	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}
func (f *PartitionedBatchIndexMarkedFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args batchIndexMarkedArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}
func (f *PartitionedBatchIndexMarkedFunction) NewState(params *vgi.ProcessParams) (*batchIndexState, error) {
	return &batchIndexState{}, nil
}
func (f *PartitionedBatchIndexMarkedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *batchIndexState, out *vgirpc.OutputCollector) error {
	if params.Storage == nil {
		return out.Finish()
	}
	work, err := params.Storage.QueuePop()
	if err != nil {
		return err
	}
	if work == nil {
		return out.Finish()
	}
	pid, start, end := unpackWorkItem(work)
	rows := end - start
	pidArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return pid })
	defer pidArr.Release()
	seqArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return i })
	defer seqArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{pidArr, seqArr}, rows)
	return vgi.EmitBatchIndex(out, batch, pid)
}

// NewPartitionedBatchIndexMarkedFunction wraps the function for registration.
func NewPartitionedBatchIndexMarkedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[batchIndexState](&PartitionedBatchIndexMarkedFunction{})
}

// ---------------------------------------------------------------------------
// Deliberately-broken batch_index fixtures (batch_index_contract.test).
// ---------------------------------------------------------------------------

type brokenBatchState struct{ Emitted bool }

func brokenNBatch(params *vgi.ProcessParams, count int64) arrow.RecordBatch {
	arr := vgi.BuildInt64Array(count, func(i int64) int64 { return i })
	defer arr.Release()
	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, count)
}

func brokenBatchIndexArgSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(batchIndexArgs{}) }
func brokenBatchIndexOnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func brokenBatchIndexCount(params *vgi.ProcessParams) int64 {
	c, _ := params.Args.GetScalarInt64(0)
	return c
}

// MissingBatchIndexTagFunction declares supports_batch_index but emits with no
// tag — the C++ extension's contract check raises.
type MissingBatchIndexTagFunction struct{}

var _ vgi.TypedTableFunc[brokenBatchState] = (*MissingBatchIndexTagFunction)(nil)

func (f *MissingBatchIndexTagFunction) Name() string { return "broken_missing_batch_index_tag" }
func (f *MissingBatchIndexTagFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "BROKEN: supports_batch_index but emits no vgi_batch_index", Categories: []string{"testing", "broken"}, OrderPreservation: vgi.OrderPreservationFixedOrder, SupportsBatchIndex: true}
}
func (f *MissingBatchIndexTagFunction) ArgumentSpecs() []vgi.ArgSpec {
	return brokenBatchIndexArgSpecs()
}
func (f *MissingBatchIndexTagFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return brokenBatchIndexOnBind(params)
}
func (f *MissingBatchIndexTagFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *MissingBatchIndexTagFunction) NewState(params *vgi.ProcessParams) (*brokenBatchState, error) {
	return &brokenBatchState{}, nil
}
func (f *MissingBatchIndexTagFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *brokenBatchState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	batch := brokenNBatch(params, brokenBatchIndexCount(params))
	return out.Emit(batch) // no batch_index — C++ raises
}

// NewMissingBatchIndexTagFunction wraps the function for registration.
func NewMissingBatchIndexTagFunction() vgi.TableFunction {
	return vgi.AsTableFunction[brokenBatchState](&MissingBatchIndexTagFunction{})
}

// NonMonotoneBatchIndexFunction emits batch_index 10 then 3 — C++ raises.
type NonMonotoneBatchIndexFunction struct{}

var _ vgi.TypedTableFunc[brokenBatchState] = (*NonMonotoneBatchIndexFunction)(nil)

func (f *NonMonotoneBatchIndexFunction) Name() string { return "broken_non_monotone_batch_index" }
func (f *NonMonotoneBatchIndexFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "BROKEN: emits strictly decreasing batch_index", Categories: []string{"testing", "broken"}, OrderPreservation: vgi.OrderPreservationFixedOrder, SupportsBatchIndex: true}
}
func (f *NonMonotoneBatchIndexFunction) ArgumentSpecs() []vgi.ArgSpec {
	return brokenBatchIndexArgSpecs()
}
func (f *NonMonotoneBatchIndexFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return brokenBatchIndexOnBind(params)
}
func (f *NonMonotoneBatchIndexFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *NonMonotoneBatchIndexFunction) NewState(params *vgi.ProcessParams) (*brokenBatchState, error) {
	return &brokenBatchState{}, nil
}
func (f *NonMonotoneBatchIndexFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *brokenBatchState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		arr := vgi.BuildInt64Array(1, func(i int64) int64 { return 42 })
		defer arr.Release()
		batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1)
		if err := vgi.EmitBatchIndex(out, batch, 3); err != nil {
			return err
		}
		return out.Finish()
	}
	state.Emitted = true
	batch := brokenNBatch(params, brokenBatchIndexCount(params))
	return vgi.EmitBatchIndex(out, batch, 10)
}

// NewNonMonotoneBatchIndexFunction wraps the function for registration.
func NewNonMonotoneBatchIndexFunction() vgi.TableFunction {
	return vgi.AsTableFunction[brokenBatchState](&NonMonotoneBatchIndexFunction{})
}

// BatchIndexOverflowFunction emits a batch_index above DuckDB's cap — C++ raises.
type BatchIndexOverflowFunction struct{}

var _ vgi.TypedTableFunc[brokenBatchState] = (*BatchIndexOverflowFunction)(nil)

func (f *BatchIndexOverflowFunction) Name() string { return "broken_batch_index_overflow" }
func (f *BatchIndexOverflowFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "BROKEN: emits batch_index above DuckDB's per-pipeline cap", Categories: []string{"testing", "broken"}, OrderPreservation: vgi.OrderPreservationFixedOrder, SupportsBatchIndex: true}
}
func (f *BatchIndexOverflowFunction) ArgumentSpecs() []vgi.ArgSpec { return brokenBatchIndexArgSpecs() }
func (f *BatchIndexOverflowFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return brokenBatchIndexOnBind(params)
}
func (f *BatchIndexOverflowFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *BatchIndexOverflowFunction) NewState(params *vgi.ProcessParams) (*brokenBatchState, error) {
	return &brokenBatchState{}, nil
}
func (f *BatchIndexOverflowFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *brokenBatchState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	batch := brokenNBatch(params, brokenBatchIndexCount(params))
	return vgi.EmitBatchIndex(out, batch, int64(1)<<60)
}

// NewBatchIndexOverflowFunction wraps the function for registration.
func NewBatchIndexOverflowFunction() vgi.TableFunction {
	return vgi.AsTableFunction[brokenBatchState](&BatchIndexOverflowFunction{})
}
