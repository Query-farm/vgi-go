// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"time"

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
	// SchemaPath is the catalog schema that declares the function. A name is
	// unique only within a schema, so this is what lets the worker resolve
	// (schema, name) on a request that re-resolves by name; nil when the caller
	// names no schema. Protocol 1.2.0.
	SchemaPath *[]string `vgirpc:"schema_path"`
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
	// SchemaPath is the catalog schema that declares the function. A name is
	// unique only within a schema, so this is what lets the worker resolve
	// (schema, name) on a request that re-resolves by name; nil when the caller
	// names no schema. Protocol 1.2.0.
	SchemaPath *[]string `vgirpc:"schema_path"`
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
	// SchemaPath is the catalog schema that declares the function. A name is
	// unique only within a schema, so this is what lets the worker resolve
	// (schema, name) on a request that re-resolves by name; nil when the caller
	// names no schema. Protocol 1.2.0.
	SchemaPath *[]string `vgirpc:"schema_path"`
}

// TableBufferingDestructorResponseWire is an empty ack.
type TableBufferingDestructorResponseWire struct{}

// bufferingRecipeKey names the state-log slot where the sink init persists the
// InitRecipe so the unary process/combine RPCs (which carry only
// function_name + execution_id) can cold-load it.
var bufferingRecipeKey = []byte("\x00vgi.buffering.recipe")

// bufferingParamsEntry caches the decoded (function, ProcessParams template)
// for one buffering execution. The InitRecipe is written once at sink init and
// never changes, so the decode + rebuild is invariant across the execution's
// process/combine RPCs. Each call gets a shallow copy of params with the
// per-call fields (Auth, AttachScope, attach options, clientLog, BatchIndex)
// taken from the live request; the shared maps and Storage are read-only
// during Process.
type bufferingParamsEntry struct {
	fn           TableBufferingFunction
	functionName string
	params       ProcessParams // template -- copied per call
}

// bufferingCacheKey scopes a cached buffering init to the attach shard as well
// as the execution: a request presenting another attach's credentials misses,
// and never reuses this execution's shard-pinned storage, secrets or
// transaction scope.
type bufferingCacheKey struct {
	shardKey    string
	executionID string
}

const (
	bufferingCacheMaxEntries = 256
	// bufferingCacheTTL bounds how long resolved secrets of an execution whose
	// destructor never arrived stay in memory. A longer-running execution just
	// cold-loads its (sealed) recipe again.
	bufferingCacheTTL = 15 * time.Minute
)

func newBufferingCache() *boundedCache[bufferingCacheKey, *bufferingParamsEntry] {
	return newBoundedCache[bufferingCacheKey, *bufferingParamsEntry](bufferingCacheMaxEntries, bufferingCacheTTL)
}

// bufferingSlot is where the sink init stores the sealed recipe.
func bufferingSlot(shardKey string, executionID []byte) storageSlot {
	return storageSlot{ShardKey: shardKey, ScopeID: executionID, NS: storageNSBufferingInit, Key: bufferingRecipeKey}
}

// bufferingRequest is the authenticated per-call context of a buffering
// process/combine RPC. Routing (shard key), identity (function name, schema
// path) and the attach scope always come from here, never from storage.
type bufferingRequest struct {
	functionName string
	executionID  []byte
	schemaPath   *[]string
	attach       *[]byte
	cc           *vgirpc.CallContext
}

// loadBufferingParams returns the (function, ProcessParams) pair for a
// process/combine RPC. It caches the decoded template per (shard key,
// execution id) so only the first call pays the storage scan, the open of the
// sealed recipe and the schema/arg re-parse; every later batch of the same
// execution gets a cheap shallow copy. The cache is bounded (LRU + TTL) and
// evicted by the destructor RPC.
func (w *Worker) loadBufferingParams(r bufferingRequest) (TableBufferingFunction, *ProcessParams, error) {
	shardKey, err := w.shardKeyForAttachPtr(r.attach, r.cc)
	if err != nil {
		return nil, nil, err
	}
	live, err := w.liveScopeFor(r, shardKey)
	if err != nil {
		return nil, nil, err
	}
	key := bufferingCacheKey{shardKey: shardKey, executionID: hex.EncodeToString(r.executionID)}
	if e, ok := w.bufferingParams.Get(key); ok {
		if e.functionName != r.functionName {
			return nil, nil, fmt.Errorf("table-buffering: execution belongs to %q, not %q", e.functionName, r.functionName)
		}
		p := e.params // shallow copy; caller mutates only its own scalar fields
		live.apply(&p)
		return e.fn, &p, nil
	}

	storage, err := w.getOrCreateStorage(context.Background(), r.executionID, shardKey)
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
	plain, err := w.openStoragePayload(entries[0].Value, bufferingSlot(shardKey, r.executionID))
	if err != nil {
		return nil, nil, err
	}
	recipe, err := decodeInitRecipe(plain)
	if err != nil {
		return nil, nil, err
	}
	if recipe.FunctionName != r.functionName {
		return nil, nil, fmt.Errorf("table-buffering: execution belongs to %q, not %q", recipe.FunctionName, r.functionName)
	}
	fn, params, err := w.rebuildProcessParamsLive(recipe, live)
	if err != nil {
		return nil, nil, err
	}
	bf, ok := fn.(TableBufferingFunction)
	if !ok {
		return nil, nil, fmt.Errorf("table-buffering: function %q is not a TableBufferingFunction", recipe.FunctionName)
	}
	w.bufferingParams.Put(key, &bufferingParamsEntry{fn: bf, functionName: r.functionName, params: *params})
	p := *params // return a copy distinct from the cached template
	live.apply(&p)
	return bf, &p, nil
}

func (w *Worker) handleTableBufferingProcess(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingProcessRequestWire) (TableBufferingProcessResponseWire, error) {
	fn, params, err := w.loadBufferingParams(bufferingRequest{
		functionName: req.FunctionName, executionID: req.ExecutionID,
		schemaPath: req.SchemaPath, attach: req.AttachOpaqueData, cc: cc,
	})
	if err != nil {
		return TableBufferingProcessResponseWire{}, err
	}
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
	fn, params, err := w.loadBufferingParams(bufferingRequest{
		functionName: req.FunctionName, executionID: req.ExecutionID,
		schemaPath: req.SchemaPath, attach: req.AttachOpaqueData, cc: cc,
	})
	if err != nil {
		return TableBufferingCombineResponseWire{}, err
	}
	finalizeIDs, err := fn.Combine(ctx, params, req.StateIDs)
	if err != nil {
		return TableBufferingCombineResponseWire{}, err
	}
	return TableBufferingCombineResponseWire{FinalizeStateIDs: finalizeIDs}, nil
}

func (w *Worker) handleTableBufferingDestructor(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingDestructorRequestWire) (TableBufferingDestructorResponseWire, error) {
	// Best-effort cleanup: clear all execution-scoped state. An attach value
	// that does not open is still refused -- no cleanup on someone else's
	// behalf.
	shardKey, err := w.shardKeyForAttachPtr(req.AttachOpaqueData, cc)
	if err != nil {
		return TableBufferingDestructorResponseWire{}, err
	}
	w.bufferingParams.Delete(bufferingCacheKey{shardKey: shardKey, executionID: hex.EncodeToString(req.ExecutionID)})
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
	processParams.Storage = storage.forSubstream(processParams.SubstreamID)
	if processParams.PushdownFilters != nil {
		parsed, err := deserializeProcessFiltersForMetadata(processParams, fn.Metadata())
		if err != nil {
			return nil, err
		}
		processParams.CurrentPushdownFilters = parsed
	}

	header := &GlobalInitResponseWire{ExecutionID: execID, MaxWorkers: 1}

	if phase == PhaseTableBufferingFinalize {
		if finalizeStateID == nil {
			return nil, fmt.Errorf("TABLE_BUFFERING_FINALIZE requires finalize_state_id")
		}
		batches, err := fn.Finalize(ctx, processParams, *finalizeStateID)
		if err != nil {
			return nil, err
		}
		if fn.Metadata().AutoApplyFilters && processParams.CurrentPushdownFilters != nil {
			for i, batch := range batches {
				filtered, filterErr := processParams.CurrentPushdownFilters.Apply(ctx, batch)
				if filterErr != nil {
					return nil, filterErr
				}
				batch.Release()
				batches[i] = filtered
			}
		}
		for i, batch := range batches {
			if batch.Schema().Equal(outputSchema) {
				continue
			}
			projected, projectErr := selectColumnsByName(batch, outputSchema)
			if projectErr != nil {
				return nil, projectErr
			}
			batch.Release()
			batches[i] = projected
		}
		batchIPC := make([][]byte, 0, len(batches))
		for _, b := range batches {
			data, serErr := SerializeRecordBatch(b)
			if serErr != nil {
				return nil, fmt.Errorf("serializing finalize batch: %w", serErr)
			}
			batchIPC = append(batchIPC, data)
		}
		// Result-cache opt-in: a buffering function declaring
		// Metadata().CacheControl advertises its vgi.cache.* keys on every
		// finalize batch, backing the exchange-mode buffered result cache.
		var cacheMeta map[string]string
		if cc := fn.Metadata().CacheControl; cc != nil {
			if err := cc.Validate(); err != nil {
				return nil, err
			}
			cacheMeta = cc.Metadata()
		}
		return &vgirpc.StreamResult{
			OutputSchema: outputSchema,
			// storage lets the first continuation token offload the flush to
			// the state log (FinalizeProducerState.offloadFlush) instead of
			// carrying every remaining batch in the token, which rides each
			// continuation request and outgrows max_request_bytes.
			State:  &FinalizeProducerState{Recipe: *recipe, BatchIPC: batchIPC, batches: batches, CacheMeta: cacheMeta, storage: storage},
			Header: header,
		}, nil
	}

	// Sink init (PhaseTableBuffering or unset): persist the recipe for the
	// process/combine RPCs and return an empty stream.
	// Sealed (storage_seal.go): the recipe carries the resolved bind-time
	// secrets and the opened transaction scope. The attach scope is left out:
	// every process/combine RPC re-derives it from its own attach value.
	stored := *recipe
	stored.AttachScope = nil
	stored.AttachOptionsScope = nil
	recipeBytes, err := encodeInitRecipe(&stored)
	if err != nil {
		return nil, err
	}
	sealed, err := w.sealStoragePayload(recipeBytes, bufferingSlot(recipe.ShardKey, recipe.ExecutionID))
	if err != nil {
		return nil, err
	}
	if _, err := storage.StateAppend(bufferingRecipeKey, sealed); err != nil {
		return nil, err
	}
	// The process/combine RPCs usually land in this process: hand them the
	// template now instead of having the first one read the recipe back.
	w.bufferingParams.Put(bufferingCacheKey{shardKey: recipe.ShardKey, executionID: hex.EncodeToString(execID)},
		&bufferingParamsEntry{fn: fn, functionName: recipe.FunctionName, params: *processParams})
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
