// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/google/uuid"
)

var _ = os.Stderr // keep import used

// ============================================================================
// Wire types — mirror vgi-python's protocol.py AggregateXxxRequest dataclasses.
// ============================================================================

// AggregateBindRequestWire is the wire format for aggregate_bind requests.
type AggregateBindRequestWire struct {
	FunctionName string  `vgirpc:"function_name"`
	Arguments    []byte  `vgirpc:"arguments"`
	InputSchema  *[]byte `vgirpc:"input_schema"`
	Settings     *[]byte `vgirpc:"settings"`
	Secrets      *[]byte `vgirpc:"secrets"`
	AttachID     *[]byte `vgirpc:"attach_id"`
}

// AggregateBindResponseWire is the wire format for aggregate_bind responses.
type AggregateBindResponseWire struct {
	OutputSchema []byte `vgirpc:"output_schema"`
	ExecutionID  []byte `vgirpc:"execution_id"`
}

// AggregateUpdateRequestWire is the wire format for aggregate_update requests.
type AggregateUpdateRequestWire struct {
	FunctionName string  `vgirpc:"function_name"`
	ExecutionID  []byte  `vgirpc:"execution_id"`
	InputBatch   []byte  `vgirpc:"input_batch"`
	AttachID     *[]byte `vgirpc:"attach_id"`
}

// AggregateUpdateResponseWire is the empty ack for aggregate_update.
type AggregateUpdateResponseWire struct{}

// AggregateCombineRequestWire is the wire format for aggregate_combine requests.
type AggregateCombineRequestWire struct {
	FunctionName string  `vgirpc:"function_name"`
	ExecutionID  []byte  `vgirpc:"execution_id"`
	MergeBatch   []byte  `vgirpc:"merge_batch"`
	AttachID     *[]byte `vgirpc:"attach_id"`
}

// AggregateCombineResponseWire is the empty ack for aggregate_combine.
type AggregateCombineResponseWire struct{}

// AggregateFinalizeRequestWire is the wire format for aggregate_finalize requests.
type AggregateFinalizeRequestWire struct {
	FunctionName  string  `vgirpc:"function_name"`
	ExecutionID   []byte  `vgirpc:"execution_id"`
	GroupIDsBatch []byte  `vgirpc:"group_ids_batch"`
	OutputSchema  []byte  `vgirpc:"output_schema"`
	AttachID      *[]byte `vgirpc:"attach_id"`
}

// AggregateFinalizeResponseWire is the wire format for aggregate_finalize responses.
type AggregateFinalizeResponseWire struct {
	ResultBatch []byte `vgirpc:"result_batch"`
}

// AggregateDestructorRequestWire is the wire format for aggregate_destructor requests.
type AggregateDestructorRequestWire struct {
	FunctionName  string  `vgirpc:"function_name"`
	ExecutionID   []byte  `vgirpc:"execution_id"`
	GroupIDsBatch []byte  `vgirpc:"group_ids_batch"`
	AttachID      *[]byte `vgirpc:"attach_id"`
}

// AggregateDestructorResponseWire is the empty ack for aggregate_destructor.
type AggregateDestructorResponseWire struct{}

// AggregateWindowInitRequestWire is the wire format for aggregate_window_init.
type AggregateWindowInitRequestWire struct {
	FunctionName   string  `vgirpc:"function_name"`
	ExecutionID    []byte  `vgirpc:"execution_id"`
	PartitionID    int64   `vgirpc:"partition_id"`
	RowCount       int64   `vgirpc:"row_count"`
	PartitionBatch []byte  `vgirpc:"partition_batch"`
	OutputSchema   []byte  `vgirpc:"output_schema"`
	FilterMask     []byte  `vgirpc:"filter_mask"`
	FrameStats     []byte  `vgirpc:"frame_stats"`
	AllValid       []byte  `vgirpc:"all_valid"`
	AttachID       *[]byte `vgirpc:"attach_id"`
}

// AggregateWindowInitResponseWire is the empty ack.
type AggregateWindowInitResponseWire struct{}

// AggregateWindowRequestWire is the wire format for aggregate_window.
type AggregateWindowRequestWire struct {
	FunctionName string  `vgirpc:"function_name"`
	ExecutionID  []byte  `vgirpc:"execution_id"`
	PartitionID  int64   `vgirpc:"partition_id"`
	RID          int64   `vgirpc:"rid"`
	FrameStarts  []int64 `vgirpc:"frame_starts"`
	FrameEnds    []int64 `vgirpc:"frame_ends"`
	AttachID     *[]byte `vgirpc:"attach_id"`
}

// AggregateWindowResponseWire is the wire format for aggregate_window.
type AggregateWindowResponseWire struct {
	ResultBatch []byte `vgirpc:"result_batch"`
}

// AggregateWindowBatchRequestWire batches multiple aggregate_window calls.
type AggregateWindowBatchRequestWire struct {
	FunctionName string  `vgirpc:"function_name"`
	ExecutionID  []byte  `vgirpc:"execution_id"`
	PartitionID  int64   `vgirpc:"partition_id"`
	RowIdx       int64   `vgirpc:"row_idx"`
	Count        int64   `vgirpc:"count"`
	FramesPerRow []int64 `vgirpc:"frames_per_row"`
	FrameStarts  []int64 `vgirpc:"frame_starts"`
	FrameEnds    []int64 `vgirpc:"frame_ends"`
	AttachID     *[]byte `vgirpc:"attach_id"`
}

// AggregateWindowBatchResponseWire is the wire format for aggregate_window_batch.
type AggregateWindowBatchResponseWire struct {
	ResultBatch []byte `vgirpc:"result_batch"`
}

// AggregateWindowDestructorRequestWire is the wire format for aggregate_window_destructor.
type AggregateWindowDestructorRequestWire struct {
	FunctionName string  `vgirpc:"function_name"`
	ExecutionID  []byte  `vgirpc:"execution_id"`
	PartitionID  int64   `vgirpc:"partition_id"`
	AttachID     *[]byte `vgirpc:"attach_id"`
}

// AggregateWindowDestructorResponseWire is the empty ack.
type AggregateWindowDestructorResponseWire struct{}

// ============================================================================
// Handlers
// ============================================================================

func (w *Worker) registerAggregateRPCs(s *vgirpc.Server) {
	vgirpc.Unary[AggregateBindRequestWire, AggregateBindResponseWire](s, "aggregate_bind", w.handleAggregateBind)
	vgirpc.Unary[AggregateUpdateRequestWire, AggregateUpdateResponseWire](s, "aggregate_update", w.handleAggregateUpdate)
	vgirpc.Unary[AggregateCombineRequestWire, AggregateCombineResponseWire](s, "aggregate_combine", w.handleAggregateCombine)
	vgirpc.Unary[AggregateFinalizeRequestWire, AggregateFinalizeResponseWire](s, "aggregate_finalize", w.handleAggregateFinalize)
	vgirpc.Unary[AggregateDestructorRequestWire, AggregateDestructorResponseWire](s, "aggregate_destructor", w.handleAggregateDestructor)
	vgirpc.Unary[AggregateWindowInitRequestWire, AggregateWindowInitResponseWire](s, "aggregate_window_init", w.handleAggregateWindowInit)
	vgirpc.Unary[AggregateWindowRequestWire, AggregateWindowResponseWire](s, "aggregate_window", w.handleAggregateWindow)
	vgirpc.Unary[AggregateWindowBatchRequestWire, AggregateWindowBatchResponseWire](s, "aggregate_window_batch", w.handleAggregateWindowBatch)
	vgirpc.Unary[AggregateWindowDestructorRequestWire, AggregateWindowDestructorResponseWire](s, "aggregate_window_destructor", w.handleAggregateWindowDestructor)
}

func (w *Worker) lookupAggregate(name string) (AggregateFunction, error) {
	fns, ok := w.aggregates[name]
	if !ok || len(fns) == 0 {
		return nil, &vgirpc.RpcError{Type: "ValueError", Message: fmt.Sprintf("aggregate function %q is not registered", name)}
	}
	// All overloads share state semantics; first one is enough for dispatch.
	return fns[0], nil
}

func (w *Worker) handleAggregateBind(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateBindRequestWire) (AggregateBindResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName)
	if err != nil {
		return AggregateBindResponseWire{}, err
	}

	args, err := ParseArguments(req.Arguments)
	if err != nil {
		return AggregateBindResponseWire{}, fmt.Errorf("aggregate_bind: deserialize arguments: %w", err)
	}

	var inputSchema *arrow.Schema
	if req.InputSchema != nil && len(*req.InputSchema) > 0 {
		inputSchema, err = DeserializeSchema(*req.InputSchema)
		if err != nil {
			return AggregateBindResponseWire{}, fmt.Errorf("aggregate_bind: deserialize input_schema: %w", err)
		}
	}

	var settings map[string]interface{}
	var secrets map[string]map[string]interface{}
	if req.Settings != nil && len(*req.Settings) > 0 {
		if b, err := DeserializeRecordBatch(*req.Settings); err == nil {
			defer b.Release()
			settings = BatchToSettingsMap(b)
		}
	}
	if req.Secrets != nil && len(*req.Secrets) > 0 {
		if b, err := DeserializeRecordBatch(*req.Secrets); err == nil {
			defer b.Release()
			secrets = BatchToSecretsMap(b)
		}
	}

	bp := &AggregateBindParams{
		Args:        args,
		InputSchema: inputSchema,
		Settings:    settings,
		Secrets:     secrets,
		Auth:        callCtx.Auth,
	}
	resp, err := fn.OnBind(bp)
	if err != nil {
		return AggregateBindResponseWire{}, err
	}
	if resp == nil || resp.OutputSchema == nil {
		return AggregateBindResponseWire{}, fmt.Errorf("aggregate_bind: %s.OnBind returned nil output schema", req.FunctionName)
	}
	schemaBytes, err := SerializeSchema(resp.OutputSchema)
	if err != nil {
		return AggregateBindResponseWire{}, fmt.Errorf("aggregate_bind: serialize output_schema: %w", err)
	}

	execID := uuid.New()
	executionID := execID[:]

	// Stash the const args under group_id=-2 in storage so subsequent calls
	// can rebuild AggregateProcessParams.Args without resending them.
	if args != nil && len(args.Positional) > 0 {
		bucket := w.aggStorage.bucket(req.FunctionName, executionID)
		if err := bucket.putConstArgs(req.Arguments); err != nil {
			return AggregateBindResponseWire{}, err
		}
	}

	return AggregateBindResponseWire{OutputSchema: schemaBytes, ExecutionID: executionID}, nil
}

// loadAggArgs returns the bind-time arguments stashed by handleAggregateBind.
func (w *Worker) loadAggArgs(funcName string, execID []byte) *Arguments {
	bucket := w.aggStorage.bucket(funcName, execID)
	data, err := bucket.getConstArgs()
	if err != nil || len(data) == 0 {
		if err != nil {
			slog.Debug("aggregate: failed to load const args", "err", err)
		}
		return nil
	}
	args, err := ParseArguments(data)
	if err != nil {
		slog.Debug("aggregate: failed to deserialize stashed args", "err", err)
		return nil
	}
	return args
}

func (w *Worker) handleAggregateUpdate(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateUpdateRequestWire) (AggregateUpdateResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName)
	if err != nil {
		return AggregateUpdateResponseWire{}, err
	}

	batch, err := DeserializeRecordBatch(req.InputBatch)
	if err != nil {
		return AggregateUpdateResponseWire{}, fmt.Errorf("aggregate_update: deserialize input_batch: %w", err)
	}
	defer batch.Release()

	gidIdx, gids, columns, err := splitGroupIDColumn(batch)
	if err != nil {
		return AggregateUpdateResponseWire{}, err
	}
	_ = gidIdx

	bucket := w.aggStorage.bucket(req.FunctionName, req.ExecutionID)
	params := &AggregateProcessParams{
		Args: w.loadAggArgs(req.FunctionName, req.ExecutionID),
		Auth: callCtx.Auth,
	}
	if req.AttachID != nil {
		params.AttachID = *req.AttachID
	}

	uniqueGIDs := uniqueInt64(gids)

	// Pre-populate states ONLY for groups that already exist in storage.
	// New groups are created lazily by the function (it knows whether the
	// row actually contributes — e.g. non-null with NullHandlingDefault),
	// so all-NULL groups stay absent and finalize() returns NULL.
	stored, err := bucket.loadStates(uniqueGIDs)
	if err != nil {
		return AggregateUpdateResponseWire{}, err
	}
	states := make(map[int64]interface{}, len(uniqueGIDs))
	preUpdateBytes := make(map[int64][]byte, len(uniqueGIDs))
	for gid, data := range stored {
		s, err := gobDecodeState(data)
		if err != nil {
			return AggregateUpdateResponseWire{}, err
		}
		states[gid] = s
		preUpdateBytes[gid] = data
	}

	if err := fn.Update(states, &Int64Slice{Data: gids}, columns, params); err != nil {
		return AggregateUpdateResponseWire{}, err
	}

	// Persist only states that either pre-existed in storage or whose
	// serialized form differs from a newly-created group's initial
	// serialization. Mirrors vgi-python's update handler: new groups whose
	// state matches the initial-serialized bytes are skipped, so
	// finalize() sees no state for them and returns NULL (SQL-standard
	// empty-SUM/AVG/MIN/MAX semantics). Groups that received only NULL
	// inputs with NullHandlingDefault never get added to `states` at all.
	toSave := make(map[int64][]byte, len(states))
	for gid, st := range states {
		b, err := gobEncodeState(st)
		if err != nil {
			return AggregateUpdateResponseWire{}, err
		}
		if _, existed := preUpdateBytes[gid]; !existed {
			initial := fn.NewState(params)
			initBytes, err := gobEncodeState(initial)
			if err != nil {
				return AggregateUpdateResponseWire{}, err
			}
			if bytes.Equal(b, initBytes) {
				continue
			}
		}
		toSave[gid] = b
	}
	if err := bucket.saveStates(toSave); err != nil {
		return AggregateUpdateResponseWire{}, err
	}
	return AggregateUpdateResponseWire{}, nil
}

func (w *Worker) handleAggregateCombine(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateCombineRequestWire) (AggregateCombineResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName)
	if err != nil {
		return AggregateCombineResponseWire{}, err
	}
	batch, err := DeserializeRecordBatch(req.MergeBatch)
	if err != nil {
		return AggregateCombineResponseWire{}, fmt.Errorf("aggregate_combine: deserialize merge_batch: %w", err)
	}
	defer batch.Release()
	if batch.NumRows() == 0 {
		return AggregateCombineResponseWire{}, nil
	}

	srcCol, ok := batch.Column(int(batch.Schema().FieldIndices("source_group_id")[0])).(*array.Int64)
	if !ok {
		return AggregateCombineResponseWire{}, fmt.Errorf("aggregate_combine: source_group_id is not int64")
	}
	tgtCol, ok := batch.Column(int(batch.Schema().FieldIndices("target_group_id")[0])).(*array.Int64)
	if !ok {
		return AggregateCombineResponseWire{}, fmt.Errorf("aggregate_combine: target_group_id is not int64")
	}
	n := int(batch.NumRows())
	srcIDs := make([]int64, n)
	tgtIDs := make([]int64, n)
	for i := 0; i < n; i++ {
		srcIDs[i] = srcCol.Value(i)
		tgtIDs[i] = tgtCol.Value(i)
	}

	bucket := w.aggStorage.bucket(req.FunctionName, req.ExecutionID)
	params := &AggregateProcessParams{
		Args: w.loadAggArgs(req.FunctionName, req.ExecutionID),
		Auth: callCtx.Auth,
	}
	if req.AttachID != nil {
		params.AttachID = *req.AttachID
	}

	allGIDs := uniqueInt64(append(append([]int64{}, srcIDs...), tgtIDs...))
	stored, err := bucket.loadStates(allGIDs)
	if err != nil {
		return AggregateCombineResponseWire{}, err
	}
	states := make(map[int64]interface{}, len(stored))
	for gid, data := range stored {
		s, err := gobDecodeState(data)
		if err != nil {
			return AggregateCombineResponseWire{}, err
		}
		states[gid] = s
	}

	for i := 0; i < n; i++ {
		src, sOK := states[srcIDs[i]]
		tgt, tOK := states[tgtIDs[i]]
		if !sOK && !tOK {
			continue
		}
		if !sOK {
			src = fn.NewState(params)
		}
		if !tOK {
			tgt = fn.NewState(params)
		}
		merged, err := fn.Combine(src, tgt, params)
		if err != nil {
			return AggregateCombineResponseWire{}, err
		}
		states[tgtIDs[i]] = merged
	}

	updatedTargets := uniqueInt64(tgtIDs)
	toSave := make(map[int64][]byte, len(updatedTargets))
	for _, gid := range updatedTargets {
		s, ok := states[gid]
		if !ok {
			continue
		}
		b, err := gobEncodeState(s)
		if err != nil {
			return AggregateCombineResponseWire{}, err
		}
		toSave[gid] = b
	}
	if err := bucket.saveStates(toSave); err != nil {
		return AggregateCombineResponseWire{}, err
	}
	return AggregateCombineResponseWire{}, nil
}

func (w *Worker) handleAggregateFinalize(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateFinalizeRequestWire) (AggregateFinalizeResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName)
	if err != nil {
		return AggregateFinalizeResponseWire{}, err
	}

	gidBatch, err := DeserializeRecordBatch(req.GroupIDsBatch)
	if err != nil {
		return AggregateFinalizeResponseWire{}, fmt.Errorf("aggregate_finalize: deserialize group_ids_batch: %w", err)
	}
	defer gidBatch.Release()
	gidCol, ok := gidBatch.Column(int(gidBatch.Schema().FieldIndices("group_id")[0])).(*array.Int64)
	if !ok {
		return AggregateFinalizeResponseWire{}, fmt.Errorf("aggregate_finalize: group_id column is not int64")
	}
	n := int(gidBatch.NumRows())
	gids := make([]int64, n)
	for i := 0; i < n; i++ {
		gids[i] = gidCol.Value(i)
	}

	outSchema, err := DeserializeSchema(req.OutputSchema)
	if err != nil {
		return AggregateFinalizeResponseWire{}, fmt.Errorf("aggregate_finalize: deserialize output_schema: %w", err)
	}

	bucket := w.aggStorage.bucket(req.FunctionName, req.ExecutionID)
	params := &AggregateProcessParams{
		Args:         w.loadAggArgs(req.FunctionName, req.ExecutionID),
		OutputSchema: outSchema,
		Auth:         callCtx.Auth,
	}
	if req.AttachID != nil {
		params.AttachID = *req.AttachID
	}

	stored, err := bucket.loadStates(gids)
	if err != nil {
		return AggregateFinalizeResponseWire{}, err
	}
	states := make(map[int64]interface{}, len(gids))
	for _, gid := range gids {
		if data, ok := stored[gid]; ok {
			s, err := gobDecodeState(data)
			if err != nil {
				return AggregateFinalizeResponseWire{}, err
			}
			states[gid] = s
		} else {
			states[gid] = nil
		}
	}

	resultBatch, err := fn.Finalize(gids, states, params)
	if err != nil {
		return AggregateFinalizeResponseWire{}, err
	}
	defer resultBatch.Release()
	if int(resultBatch.NumRows()) != len(gids) {
		return AggregateFinalizeResponseWire{}, fmt.Errorf("aggregate_finalize: %s returned %d rows, expected %d", req.FunctionName, resultBatch.NumRows(), len(gids))
	}

	out, err := SerializeRecordBatch(resultBatch)
	if err != nil {
		return AggregateFinalizeResponseWire{}, fmt.Errorf("aggregate_finalize: serialize result: %w", err)
	}
	return AggregateFinalizeResponseWire{ResultBatch: out}, nil
}

func (w *Worker) handleAggregateDestructor(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateDestructorRequestWire) (AggregateDestructorResponseWire, error) {
	bucket := w.aggStorage.bucket(req.FunctionName, req.ExecutionID)
	if err := bucket.clear(); err != nil {
		return AggregateDestructorResponseWire{}, err
	}
	return AggregateDestructorResponseWire{}, nil
}

func (w *Worker) handleAggregateWindowInit(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateWindowInitRequestWire) (AggregateWindowInitResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName)
	if err != nil {
		return AggregateWindowInitResponseWire{}, err
	}
	bucket := w.aggStorage.bucket(req.FunctionName, req.ExecutionID)

	wfn, _ := fn.(AggregateWindowFunction)
	if wfn != nil {
		// Build the partition once to call WindowInit; cache the encoded
		// payload so subsequent Window calls can reload without re-running.
		partition, err := unpackWindowPartition(req)
		if err != nil {
			return AggregateWindowInitResponseWire{}, err
		}
		params := &AggregateProcessParams{
			Args:         w.loadAggArgs(req.FunctionName, req.ExecutionID),
			OutputSchema: partition.OutputSchema,
			Auth:         callCtx.Auth,
		}
		if req.AttachID != nil {
			params.AttachID = *req.AttachID
		}
		ws, err := wfn.WindowInit(partition, params)
		if err != nil {
			return AggregateWindowInitResponseWire{}, err
		}
		// Encode the raw request payload + window_state for later reload.
		var wsBytes []byte
		if ws != nil {
			wsBytes, err = gobEncodeState(ws)
			if err != nil {
				return AggregateWindowInitResponseWire{}, err
			}
		}
		if err := bucket.putWindowPartition(req.PartitionID, encodeWindowPartitionPayload(req, wsBytes)); err != nil {
			return AggregateWindowInitResponseWire{}, err
		}
	} else {
		// Function doesn't override WindowInit — still cache the raw payload.
		if err := bucket.putWindowPartition(req.PartitionID, encodeWindowPartitionPayload(req, nil)); err != nil {
			return AggregateWindowInitResponseWire{}, err
		}
	}
	return AggregateWindowInitResponseWire{}, nil
}

func (w *Worker) handleAggregateWindow(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateWindowRequestWire) (AggregateWindowResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName)
	if err != nil {
		return AggregateWindowResponseWire{}, err
	}
	wfn, ok := fn.(AggregateWindowFunction)
	if !ok {
		return AggregateWindowResponseWire{}, fmt.Errorf("aggregate_window: %s does not implement AggregateWindowFunction", req.FunctionName)
	}

	partition, ws, err := w.loadCachedPartition(req.FunctionName, req.ExecutionID, req.PartitionID)
	if err != nil {
		return AggregateWindowResponseWire{}, err
	}
	params := &AggregateProcessParams{
		Args:         w.loadAggArgs(req.FunctionName, req.ExecutionID),
		OutputSchema: partition.OutputSchema,
		Auth:         callCtx.Auth,
	}
	if req.AttachID != nil {
		params.AttachID = *req.AttachID
	}
	if len(req.FrameStarts) != len(req.FrameEnds) {
		return AggregateWindowResponseWire{}, fmt.Errorf("aggregate_window: frame_starts/frame_ends length mismatch")
	}
	subframes := make([][2]int64, len(req.FrameStarts))
	for i := range req.FrameStarts {
		subframes[i] = [2]int64{req.FrameStarts[i], req.FrameEnds[i]}
	}
	val, err := wfn.Window(req.RID, subframes, partition, ws, params)
	if err != nil {
		return AggregateWindowResponseWire{}, err
	}
	batch, err := buildScalarResultBatch(val, partition.OutputSchema)
	if err != nil {
		return AggregateWindowResponseWire{}, err
	}
	defer batch.Release()
	out, err := SerializeRecordBatch(batch)
	if err != nil {
		return AggregateWindowResponseWire{}, err
	}
	return AggregateWindowResponseWire{ResultBatch: out}, nil
}

func (w *Worker) handleAggregateWindowBatch(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateWindowBatchRequestWire) (AggregateWindowBatchResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName)
	if err != nil {
		return AggregateWindowBatchResponseWire{}, err
	}
	wfn, ok := fn.(AggregateWindowFunction)
	if !ok {
		return AggregateWindowBatchResponseWire{}, fmt.Errorf("aggregate_window_batch: %s does not implement AggregateWindowFunction", req.FunctionName)
	}
	partition, ws, err := w.loadCachedPartition(req.FunctionName, req.ExecutionID, req.PartitionID)
	if err != nil {
		return AggregateWindowBatchResponseWire{}, err
	}
	params := &AggregateProcessParams{
		Args:         w.loadAggArgs(req.FunctionName, req.ExecutionID),
		OutputSchema: partition.OutputSchema,
		Auth:         callCtx.Auth,
	}
	if req.AttachID != nil {
		params.AttachID = *req.AttachID
	}

	if int(req.Count) != len(req.FramesPerRow) {
		return AggregateWindowBatchResponseWire{}, fmt.Errorf("aggregate_window_batch: count=%d but frames_per_row has %d entries", req.Count, len(req.FramesPerRow))
	}
	results := make([]interface{}, 0, req.Count)
	offset := int64(0)
	for i := int64(0); i < req.Count; i++ {
		nf := req.FramesPerRow[i]
		subframes := make([][2]int64, nf)
		for k := int64(0); k < nf; k++ {
			subframes[k] = [2]int64{req.FrameStarts[offset+k], req.FrameEnds[offset+k]}
		}
		offset += nf
		val, err := wfn.Window(req.RowIdx+i, subframes, partition, ws, params)
		if err != nil {
			return AggregateWindowBatchResponseWire{}, err
		}
		results = append(results, val)
	}
	batch, err := buildBatchResult(results, partition.OutputSchema)
	if err != nil {
		return AggregateWindowBatchResponseWire{}, err
	}
	defer batch.Release()
	out, err := SerializeRecordBatch(batch)
	if err != nil {
		return AggregateWindowBatchResponseWire{}, err
	}
	return AggregateWindowBatchResponseWire{ResultBatch: out}, nil
}

func (w *Worker) handleAggregateWindowDestructor(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateWindowDestructorRequestWire) (AggregateWindowDestructorResponseWire, error) {
	bucket := w.aggStorage.bucket(req.FunctionName, req.ExecutionID)
	if err := bucket.deleteWindowPartition(req.PartitionID); err != nil {
		return AggregateWindowDestructorResponseWire{}, err
	}
	return AggregateWindowDestructorResponseWire{}, nil
}

// ============================================================================
// Helpers
// ============================================================================

// splitGroupIDColumn returns the group_id column index, the int64 group_ids,
// and the remaining input columns (excluding group_id).
func splitGroupIDColumn(batch arrow.RecordBatch) (int, []int64, []arrow.Array, error) {
	idx := -1
	for i := 0; i < int(batch.NumCols()); i++ {
		if batch.Schema().Field(i).Name == GroupColumnName {
			idx = i
			break
		}
	}
	if idx < 0 {
		return -1, nil, nil, fmt.Errorf("aggregate_update: missing %s column", GroupColumnName)
	}
	gidCol, ok := batch.Column(idx).(*array.Int64)
	if !ok {
		return -1, nil, nil, fmt.Errorf("aggregate_update: %s is not int64", GroupColumnName)
	}
	n := int(batch.NumRows())
	gids := make([]int64, n)
	for i := 0; i < n; i++ {
		gids[i] = gidCol.Value(i)
	}
	cols := make([]arrow.Array, 0, batch.NumCols()-1)
	for i := 0; i < int(batch.NumCols()); i++ {
		if i == idx {
			continue
		}
		col := batch.Column(i)
		col.Retain()
		cols = append(cols, col)
	}
	return idx, gids, cols, nil
}

func uniqueInt64(in []int64) []int64 {
	seen := make(map[int64]struct{}, len(in))
	out := make([]int64, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

