// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// Table-buffering RPC wire types. Field names match the generated
// TableBuffering*ParamsSchema / *ResultSchema and vgi-python's request
// dataclasses. Note these requests carry transaction_id (not
// transaction_opaque_data).

// TableBufferingProcessRequestWire sinks one input batch.
type TableBufferingProcessRequestWire struct {
	FunctionName     string  `vgirpc:"function_name"`
	ExecutionID      []byte  `vgirpc:"execution_id"`
	InputBatch       []byte  `vgirpc:"input_batch"`
	AttachOpaqueData *[]byte `vgirpc:"attach_opaque_data"`
	TransactionID    *[]byte `vgirpc:"transaction_id"`
	BatchIndex       *int64  `vgirpc:"batch_index"`
}

// TableBufferingProcessResponseWire returns the worker-chosen state_id.
type TableBufferingProcessResponseWire struct {
	StateID []byte `vgirpc:"state_id"`
}

// TableBufferingCombineRequestWire fires once after all process calls.
type TableBufferingCombineRequestWire struct {
	FunctionName     string   `vgirpc:"function_name"`
	ExecutionID      []byte   `vgirpc:"execution_id"`
	StateIDs         [][]byte `vgirpc:"state_ids"`
	AttachOpaqueData *[]byte  `vgirpc:"attach_opaque_data"`
	TransactionID    *[]byte  `vgirpc:"transaction_id"`
}

// TableBufferingCombineResponseWire returns the finalize partition keys.
type TableBufferingCombineResponseWire struct {
	FinalizeStateIDs [][]byte `vgirpc:"finalize_state_ids"`
}

// TableBufferingDestructorRequestWire is the best-effort end-of-query cleanup.
type TableBufferingDestructorRequestWire struct {
	FunctionName     string  `vgirpc:"function_name"`
	ExecutionID      []byte  `vgirpc:"execution_id"`
	AttachOpaqueData *[]byte `vgirpc:"attach_opaque_data"`
	TransactionID    *[]byte `vgirpc:"transaction_id"`
}

// TableBufferingDestructorResponseWire is an empty ack.
type TableBufferingDestructorResponseWire struct{}

// bufferingRecipeKey names the state-log slot where the sink init persists the
// InitRecipe so the unary process/combine RPCs (which carry only
// function_name + execution_id) can cold-load it.
var bufferingRecipeKey = []byte("\x00vgi.buffering.recipe")

func (w *Worker) registerTableBufferingRPCs(s *vgirpc.Server) {
	vgirpc.Unary[TableBufferingProcessRequestWire, TableBufferingProcessResponseWire](
		s, "table_buffering_process", w.handleTableBufferingProcess)
	vgirpc.Unary[TableBufferingCombineRequestWire, TableBufferingCombineResponseWire](
		s, "table_buffering_combine", w.handleTableBufferingCombine)
	vgirpc.Unary[TableBufferingDestructorRequestWire, TableBufferingDestructorResponseWire](
		s, "table_buffering_destructor", w.handleTableBufferingDestructor)
}

// loadBufferingParams cold-loads the InitRecipe persisted at sink init and
// rebuilds the (function, ProcessParams) pair for a process/combine RPC.
func (w *Worker) loadBufferingParams(executionID []byte, shardKey string) (TableBufferingFunction, *ProcessParams, error) {
	storage, err := w.getOrCreateStorage(context.Background(), executionID, shardKey)
	if err != nil {
		return nil, nil, err
	}
	entries, err := storage.StateLogScan(bufferingRecipeKey, -1, 1)
	if err != nil {
		return nil, nil, err
	}
	if len(entries) == 0 {
		return nil, nil, fmt.Errorf("table-buffering: no init recipe for execution (sink init missing)")
	}
	recipe, err := decodeInitRecipe(entries[0].Value)
	if err != nil {
		return nil, nil, err
	}
	fn, params, err := w.rebuildProcessParams(recipe)
	if err != nil {
		return nil, nil, err
	}
	bf, ok := fn.(TableBufferingFunction)
	if !ok {
		return nil, nil, fmt.Errorf("table-buffering: function %q is not a TableBufferingFunction", recipe.FunctionName)
	}
	return bf, params, nil
}

func (w *Worker) handleTableBufferingProcess(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingProcessRequestWire) (TableBufferingProcessResponseWire, error) {
	shardKey, err := w.shardKeyForAttachPtr(req.AttachOpaqueData, cc)
	if err != nil {
		return TableBufferingProcessResponseWire{}, err
	}
	fn, params, err := w.loadBufferingParams(req.ExecutionID, shardKey)
	if err != nil {
		return TableBufferingProcessResponseWire{}, err
	}
	params.Auth = cc.Auth
	params.AttachScope = w.attachScopeForPtr(req.AttachOpaqueData, cc, params.AttachScope)
	params.clientLog = func(level vgirpc.LogLevel, msg string) { cc.ClientLog(level, msg) }
	if req.BatchIndex != nil {
		params.BatchIndex = req.BatchIndex
	}
	batch, err := DeserializeRecordBatch(req.InputBatch)
	if err != nil {
		return TableBufferingProcessResponseWire{}, fmt.Errorf("table_buffering_process: input batch: %w", err)
	}
	defer batch.Release()
	stateID, err := fn.Process(ctx, params, batch)
	if err != nil {
		return TableBufferingProcessResponseWire{}, err
	}
	return TableBufferingProcessResponseWire{StateID: stateID}, nil
}

func (w *Worker) handleTableBufferingCombine(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingCombineRequestWire) (TableBufferingCombineResponseWire, error) {
	shardKey, err := w.shardKeyForAttachPtr(req.AttachOpaqueData, cc)
	if err != nil {
		return TableBufferingCombineResponseWire{}, err
	}
	fn, params, err := w.loadBufferingParams(req.ExecutionID, shardKey)
	if err != nil {
		return TableBufferingCombineResponseWire{}, err
	}
	params.Auth = cc.Auth
	params.AttachScope = w.attachScopeForPtr(req.AttachOpaqueData, cc, params.AttachScope)
	params.clientLog = func(level vgirpc.LogLevel, msg string) { cc.ClientLog(level, msg) }
	finalizeIDs, err := fn.Combine(ctx, params, req.StateIDs)
	if err != nil {
		return TableBufferingCombineResponseWire{}, err
	}
	return TableBufferingCombineResponseWire{FinalizeStateIDs: finalizeIDs}, nil
}

func (w *Worker) handleTableBufferingDestructor(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingDestructorRequestWire) (TableBufferingDestructorResponseWire, error) {
	// Best-effort cleanup: clear all execution-scoped state. Never errors.
	shardKey, _ := w.shardKeyForAttachPtr(req.AttachOpaqueData, cc)
	storage, err := w.getOrCreateStorage(ctx, req.ExecutionID, shardKey)
	if err == nil {
		_ = storage.StateLogClear()
	}
	return TableBufferingDestructorResponseWire{}, nil
}

// initTableBuffering handles the two buffering init phases. TABLE_BUFFERING is
// the sink init: it persists the InitRecipe so process/combine can cold-load
// it, and returns an empty stream (the sink ingests via the unary process RPC).
// TABLE_BUFFERING_FINALIZE opens a producer that emits all batches the function
// returns for one finalize_state_id.
func (w *Worker) initTableBuffering(ctx context.Context, fn TableBufferingFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema, phase Phase, recipe *InitRecipe, finalizeStateID *[]byte) (*vgirpc.StreamResult, error) {
	if initParams.ExecutionID == nil {
		initParams.ExecutionID = newExecutionID()
	}
	execID := initParams.ExecutionID
	processParams.ExecutionID = execID
	recipe.ExecutionID = execID
	storage, err := w.getOrCreateStorage(ctx, execID, recipe.ShardKey)
	if err != nil {
		return nil, err
	}
	processParams.Storage = storage

	header := &GlobalInitResponseWire{ExecutionID: execID, MaxWorkers: 1}

	if phase == PhaseTableBufferingFinalize {
		if finalizeStateID == nil {
			return nil, fmt.Errorf("TABLE_BUFFERING_FINALIZE requires finalize_state_id")
		}
		batches, err := fn.Finalize(ctx, processParams, *finalizeStateID)
		if err != nil {
			return nil, err
		}
		batchIPC := make([][]byte, 0, len(batches))
		for _, b := range batches {
			data, serErr := SerializeRecordBatch(b)
			if serErr != nil {
				return nil, fmt.Errorf("serializing finalize batch: %w", serErr)
			}
			batchIPC = append(batchIPC, data)
		}
		return &vgirpc.StreamResult{
			OutputSchema: outputSchema,
			State:        &FinalizeProducerState{Recipe: *recipe, BatchIPC: batchIPC, batches: batches},
			Header:       header,
		}, nil
	}

	// Sink init (PhaseTableBuffering or unset): persist the recipe for the
	// process/combine RPCs and return an empty stream.
	recipeBytes, err := encodeInitRecipe(recipe)
	if err != nil {
		return nil, err
	}
	if _, err := storage.StateAppend(bufferingRecipeKey, recipeBytes); err != nil {
		return nil, err
	}
	return &vgirpc.StreamResult{
		OutputSchema: outputSchema,
		State:        &FinalizeProducerState{Recipe: *recipe},
		Header:       header,
	}, nil
}

func encodeInitRecipe(r *InitRecipe) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return nil, fmt.Errorf("encoding init recipe: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeInitRecipe(data []byte) (*InitRecipe, error) {
	var r InitRecipe
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&r); err != nil {
		return nil, fmt.Errorf("decoding init recipe: %w", err)
	}
	return &r, nil
}
