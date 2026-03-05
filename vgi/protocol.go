// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Wire types for RPC serialization (used with vgirpc struct tags)
// ---------------------------------------------------------------------------

// BindRequestWire is the wire format for bind requests.
type BindRequestWire struct {
	FunctionName            string  `vgirpc:"function_name"`
	Arguments               []byte  `vgirpc:"arguments"`
	FunctionType            string  `vgirpc:"function_type,enum"`
	InputSchema             *[]byte `vgirpc:"input_schema"`
	Settings                *[]byte `vgirpc:"settings"`
	Secrets                 *[]byte `vgirpc:"secrets"`
	AttachID                *[]byte `vgirpc:"attach_id"`
	TransactionID           *[]byte `vgirpc:"transaction_id"`
	ResolvedSecretsProvided bool    `vgirpc:"resolved_secrets_provided"`
}

// BindResponseWire is the wire format for bind responses.
type BindResponseWire struct {
	OutputSchema      *[]byte   `vgirpc:"output_schema"`
	OpaqueData        *[]byte   `vgirpc:"opaque_data"`
	LookupSecretTypes *[]string `vgirpc:"lookup_secret_types"`
	LookupScopes      *[]string `vgirpc:"lookup_scopes"`
	LookupNames       *[]string `vgirpc:"lookup_names"`
}

// InitRequestWire is the wire format for init requests.
type InitRequestWire struct {
	BindCall        []byte   `vgirpc:"bind_call"`
	OutputSchema    []byte   `vgirpc:"output_schema"`
	BindOpaqueData  *[]byte  `vgirpc:"bind_opaque_data"`
	ProjectionIDs   *[]int32 `vgirpc:"projection_ids"`
	PushdownFilters *[]byte  `vgirpc:"pushdown_filters"`
	Phase           *string  `vgirpc:"phase,enum"`
	ExecutionID     *[]byte  `vgirpc:"execution_id"`
	InitOpaqueData  *[]byte  `vgirpc:"init_opaque_data"`
}

// GlobalInitResponseWire is the wire format for global init responses.
type GlobalInitResponseWire struct {
	ExecutionID []byte  `vgirpc:"execution_id"`
	MaxWorkers  int64   `vgirpc:"max_workers"`
	OpaqueData  *[]byte `vgirpc:"opaque_data"`
}

// CardinalityRequestWire is the wire format for cardinality requests.
type CardinalityRequestWire struct {
	BindCall       []byte  `vgirpc:"bind_call"`
	BindOpaqueData *[]byte `vgirpc:"bind_opaque_data"`
}

// ---------------------------------------------------------------------------
// Stream state implementations
// ---------------------------------------------------------------------------

// ScalarExchangeState implements ExchangeState for scalar functions.
type ScalarExchangeState struct {
	Recipe InitRecipe     // exported, serialized
	fn     ScalarFunction // transient
	params *ProcessParams // transient
}

func (s *ScalarExchangeState) Exchange(ctx context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	result, err := s.fn.Process(ctx, s.params, input)
	if err != nil {
		return err
	}
	return out.Emit(result)
}

// TableProducerState implements ProducerState for table functions.
type TableProducerState struct {
	Recipe         InitRecipe       // exported, serialized
	UserStateBytes []byte           // exported, gob-serialized user state
	AutoProjectIDs []int32          // exported
	fn             TableFunction    // transient
	params         *ProcessParams   // transient
	state          interface{}      // transient (reconstructed from UserStateBytes)
	autoApply      *PushdownFilters // transient
}

func (s *TableProducerState) Produce(ctx context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.autoApply != nil || s.AutoProjectIDs != nil {
		if s.AutoProjectIDs != nil {
			// Tell OutputCollector to use the full schema for EmitArrays/EmitMap
			// so functions that emit all columns can build valid batches.
			// The interceptor below will project down before the wire write.
			out.ProcessSchema = s.params.OutputSchema
		}
		out.EmitInterceptor = func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
			result := batch
			if s.AutoProjectIDs != nil {
				result = projectBatch(result, s.AutoProjectIDs)
			}
			if s.autoApply != nil {
				return s.autoApply.Apply(ctx, result)
			}
			return result, nil
		}
	}
	return s.fn.Process(ctx, s.params, s.state, out)
}

// projectBatch selects only the columns at the given indices from a RecordBatch.
func projectBatch(batch arrow.RecordBatch, ids []int32) arrow.RecordBatch {
	fields := make([]arrow.Field, len(ids))
	cols := make([]arrow.Array, len(ids))
	for i, id := range ids {
		fields[i] = batch.Schema().Field(int(id))
		cols[i] = batch.Column(int(id))
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecordBatch(schema, cols, batch.NumRows())
}

// TableInOutExchangeState implements ExchangeState for table-in-out INPUT phase.
type TableInOutExchangeState struct {
	Recipe         InitRecipe         // exported, serialized
	UserStateBytes []byte             // exported, gob-serialized user state
	fn             TableInOutFunction // transient
	params         *ProcessParams     // transient
	state          interface{}        // transient
	autoApply      *PushdownFilters   // transient
}

func (s *TableInOutExchangeState) Exchange(ctx context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.autoApply != nil {
		out.EmitInterceptor = func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
			return s.autoApply.Apply(ctx, batch)
		}
	}
	return s.fn.Process(ctx, s.params, s.state, input, out)
}

// FinalizeProducerState implements ProducerState for table-in-out FINALIZE phase.
type FinalizeProducerState struct {
	Recipe   InitRecipe          // exported, serialized
	BatchIPC [][]byte            // exported, serialized finalize batch IPC bytes
	BatchIdx int                 // exported, current emission position
	batches  []arrow.RecordBatch // transient (deserialized from BatchIPC)
}

func (s *FinalizeProducerState) Produce(ctx context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.BatchIdx >= len(s.batches) {
		s.batches = nil
		return out.Finish()
	}
	batch := s.batches[s.BatchIdx]
	s.batches[s.BatchIdx] = nil // release reference after emission
	s.BatchIdx++
	return out.Emit(batch)
}

// ---------------------------------------------------------------------------
// Protocol handler implementations
// ---------------------------------------------------------------------------

// handleBind processes a bind RPC request.
func (w *Worker) handleBind(ctx context.Context, callCtx *vgirpc.CallContext, req BindRequestWire) (BindResponseWire, error) {
	slog.Debug("bind: received request",
		"function", req.FunctionName,
		"type", req.FunctionType,
		"args_len", len(req.Arguments),
		"has_input_schema", req.InputSchema != nil,
	)
	bindParams, err := w.parseBindRequest(req)
	if err != nil {
		slog.Debug("bind: parse failed", "err", err)
		return BindResponseWire{}, err
	}
	slog.Debug("bind: parsed args",
		"positional", len(bindParams.Args.Positional),
		"named", len(bindParams.Args.Named),
	)

	fn, err := w.resolveFunctionWithOverload(req.FunctionName, FunctionType(req.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		slog.Debug("bind: resolve function failed", "function", req.FunctionName, "err", err)
		return BindResponseWire{}, err
	}
	slog.Debug("bind: resolved function", "function", req.FunctionName, "type", fmt.Sprintf("%T", fn))

	// Remap positional args to original ArgSpec positions
	argSpecs := w.getArgSpecs(fn)
	bindParams.Args.RemapPositionalArgs(argSpecs)

	// Validate type bounds against input schema before calling OnBind
	if err := ValidateTypeBounds(argSpecs, bindParams.InputSchema); err != nil {
		slog.Debug("bind: type bound validation failed", "err", err)
		return BindResponseWire{}, err
	}

	var bindResp *BindResponse

	switch f := fn.(type) {
	case ScalarFunction:
		bindResp, err = f.OnBind(bindParams)
	case TableFunction:
		bindResp, err = f.OnBind(bindParams)
	case TableInOutFunction:
		bindResp, err = f.OnBind(bindParams)
	default:
		return BindResponseWire{}, fmt.Errorf("unknown function type: %T", fn)
	}

	if err != nil {
		return BindResponseWire{}, err
	}

	// Two-phase bind: if the function requests scoped secret lookup, return
	// the lookup columns instead of an output schema.
	if bindResp.SecretScopeRequest != nil {
		slog.Debug("bind: two-phase scope request", "function", req.FunctionName, "lookups", len(bindResp.SecretScopeRequest))
		types := make([]string, len(bindResp.SecretScopeRequest))
		scopes := make([]string, len(bindResp.SecretScopeRequest))
		names := make([]string, len(bindResp.SecretScopeRequest))
		for i, sl := range bindResp.SecretScopeRequest {
			types[i] = sl.SecretType
			scopes[i] = sl.Scope
			names[i] = sl.SecretName
		}
		return BindResponseWire{
			LookupSecretTypes: &types,
			LookupScopes:      &scopes,
			LookupNames:       &names,
		}, nil
	}

	outputSchemaBytes, err := SerializeSchema(bindResp.OutputSchema)
	if err != nil {
		return BindResponseWire{}, fmt.Errorf("serializing output schema: %w", err)
	}

	resp := BindResponseWire{
		OutputSchema: &outputSchemaBytes,
	}
	if len(bindResp.OpaqueData) > 0 {
		resp.OpaqueData = &bindResp.OpaqueData
	}
	return resp, nil
}

// handleInit processes an init RPC request and returns a StreamResult.
func (w *Worker) handleInit(ctx context.Context, callCtx *vgirpc.CallContext, req InitRequestWire) (*vgirpc.StreamResult, error) {
	slog.Debug("init: received request",
		"bind_call_len", len(req.BindCall),
		"output_schema_len", len(req.OutputSchema),
		"phase", req.Phase,
		"exec_id_present", req.ExecutionID != nil,
	)
	// Deserialize the embedded bind call
	bindReq, err := w.deserializeBindRequest(req.BindCall)
	if err != nil {
		slog.Debug("init: deserialize bind_call failed", "err", err)
		return nil, fmt.Errorf("deserializing bind_call: %w", err)
	}
	slog.Debug("init: parsed bind call", "function", bindReq.FunctionName, "type", bindReq.FunctionType)

	// Parse output schema
	outputSchema, err := DeserializeSchema(req.OutputSchema)
	if err != nil {
		slog.Debug("init: deserialize output_schema failed", "err", err)
		return nil, fmt.Errorf("deserializing output_schema: %w", err)
	}
	slog.Debug("init: output schema", "fields", outputSchema.NumFields())

	// Parse bind params for argument access
	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return nil, err
	}

	// Resolve the function with overload resolution
	fn, err := w.resolveFunctionWithOverload(bindReq.FunctionName, FunctionType(bindReq.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		slog.Debug("init: resolve function failed", "function", bindReq.FunctionName, "err", err)
		return nil, err
	}
	slog.Debug("init: resolved function", "type", fmt.Sprintf("%T", fn))

	// Remap positional args to original ArgSpec positions
	bindParams.Args.RemapPositionalArgs(w.getArgSpecs(fn))

	// Determine phase
	phase := ""
	if req.Phase != nil {
		phase = *req.Phase
	}

	// Build init params
	initParams := &InitParams{
		FunctionName: bindReq.FunctionName,
		FunctionType: FunctionType(bindReq.FunctionType),
		Args:         bindParams.Args,
		OutputSchema: outputSchema,
		InputSchema:  bindParams.InputSchema,
		Phase:        phase,
		Settings:     bindParams.Settings,
		Secrets:      bindParams.Secrets,
	}
	if req.ProjectionIDs != nil {
		initParams.ProjectionIDs = *req.ProjectionIDs
	}
	if req.BindOpaqueData != nil {
		initParams.BindOpaqueData = *req.BindOpaqueData
	}
	if req.ExecutionID != nil {
		initParams.ExecutionID = *req.ExecutionID
		initParams.IsSecondary = true
	}
	if req.InitOpaqueData != nil {
		initParams.InitOpaqueData = *req.InitOpaqueData
	}
	if req.PushdownFilters != nil {
		batch, err := DeserializeRecordBatch(*req.PushdownFilters)
		if err == nil {
			initParams.PushdownFilters = batch
		}
	}

	// Apply projection to the wire output schema (what DuckDB expects back).
	projectedSchema := ProjectSchema(initParams.ProjectionIDs, outputSchema)

	// Determine the schema the function's Process method should see.
	// If the function supports projection pushdown, it handles projection itself.
	// Otherwise, give it the full schema and auto-project after emit.
	fnMeta := w.getFunctionMetadata(fn)
	processOutputSchema := projectedSchema
	var autoProjectIDs []int32
	if !fnMeta.ProjectionPushdown && initParams.ProjectionIDs != nil {
		// Function doesn't support projection — let it emit all columns,
		// framework will project down before sending to DuckDB.
		processOutputSchema = outputSchema
		autoProjectIDs = initParams.ProjectionIDs
	}

	// Build process params
	processParams := &ProcessParams{
		FunctionName:    bindReq.FunctionName,
		FunctionType:    FunctionType(bindReq.FunctionType),
		Args:            bindParams.Args,
		OutputSchema:    processOutputSchema,
		ProjectionIDs:   initParams.ProjectionIDs,
		Settings:        bindParams.Settings,
		Secrets:         bindParams.Secrets,
		PushdownFilters: initParams.PushdownFilters,
	}

	// Build InitRecipe for HTTP state serialization
	recipe := InitRecipe{
		BindCallIPC:     req.BindCall,
		OutputSchemaIPC: req.OutputSchema,
		FunctionName:    bindReq.FunctionName,
		FunctionType:    bindReq.FunctionType,
		ProjectionIDs:   initParams.ProjectionIDs,
		ExecutionID:     initParams.ExecutionID,
		BindOpaqueData:  initParams.BindOpaqueData,
		InitOpaqueData:  initParams.InitOpaqueData,
		Phase:           phase,
		IsSecondary:     initParams.IsSecondary,
	}
	if req.PushdownFilters != nil {
		recipe.PushdownFilterIPC = *req.PushdownFilters
	}

	slog.Debug("init: dispatching", "type", fmt.Sprintf("%T", fn))
	switch f := fn.(type) {
	case ScalarFunction:
		result, err := w.initScalar(ctx, f, initParams, processParams, projectedSchema, &recipe)
		if err != nil {
			slog.Debug("init: scalar init failed", "err", err)
		} else {
			slog.Debug("init: scalar init success", "state", fmt.Sprintf("%T", result.State))
		}
		return result, err
	case TableFunction:
		result, err := w.initTable(ctx, f, initParams, processParams, projectedSchema, autoProjectIDs, &recipe)
		if err != nil {
			slog.Debug("init: table init failed", "err", err)
		} else {
			slog.Debug("init: table init success", "state", fmt.Sprintf("%T", result.State))
		}
		return result, err
	case TableInOutFunction:
		result, err := w.initTableInOut(ctx, f, initParams, processParams, projectedSchema, phase, &recipe)
		if err != nil {
			slog.Debug("init: table-in-out init failed", "err", err)
		} else {
			slog.Debug("init: table-in-out init success", "state", fmt.Sprintf("%T", result.State))
		}
		return result, err
	default:
		return nil, fmt.Errorf("unknown function type: %T", fn)
	}
}

func (w *Worker) initScalar(ctx context.Context, fn ScalarFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema, recipe *InitRecipe) (*vgirpc.StreamResult, error) {
	// Scalar functions don't have an init phase — use default response
	resp := &GlobalInitResponse{
		MaxWorkers: 1,
	}
	if initParams.ExecutionID == nil {
		resp.ExecutionID = newExecutionID()
	} else {
		resp.ExecutionID = initParams.ExecutionID
	}
	processParams.ExecutionID = resp.ExecutionID
	recipe.ExecutionID = resp.ExecutionID
	storage, err := w.getOrCreateStorage(ctx, resp.ExecutionID)
	if err != nil {
		return nil, err
	}
	processParams.Storage = storage

	header := &GlobalInitResponseWire{
		ExecutionID: resp.ExecutionID,
		MaxWorkers:  resp.MaxWorkers,
	}

	state := &ScalarExchangeState{
		Recipe: *recipe,
		fn:     fn,
		params: processParams,
	}

	return &vgirpc.StreamResult{
		OutputSchema: outputSchema,
		State:        state,
		InputSchema:  initParams.InputSchema,
		Header:       header,
	}, nil
}

func (w *Worker) initTable(ctx context.Context, fn TableFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema, autoProjectIDs []int32, recipe *InitRecipe) (*vgirpc.StreamResult, error) {
	// Pre-create storage so OnInit can use it
	if initParams.ExecutionID == nil {
		initParams.ExecutionID = newExecutionID()
	}
	initStorage, err := w.getOrCreateStorage(ctx, initParams.ExecutionID)
	if err != nil {
		return nil, err
	}
	initParams.Storage = initStorage

	var resp *GlobalInitResponse
	if !initParams.IsSecondary {
		// Primary init: call OnInit (global_init) to set up work items, etc.
		resp, err = fn.OnInit(initParams)
		if err != nil {
			return nil, err
		}
		if resp.ExecutionID == nil {
			resp.ExecutionID = initParams.ExecutionID
		}
		if resp.MaxWorkers == 0 {
			resp.MaxWorkers = 4
		}
	} else {
		// Secondary init: skip OnInit, reuse execution ID from request
		resp = &GlobalInitResponse{
			ExecutionID: initParams.ExecutionID,
			MaxWorkers:  1,
		}
		if initParams.InitOpaqueData != nil {
			resp.OpaqueData = initParams.InitOpaqueData
		}
	}
	processParams.ExecutionID = resp.ExecutionID
	recipe.ExecutionID = resp.ExecutionID
	recipe.InitOpaqueData = resp.OpaqueData
	processParams.InitOpaqueData = resp.OpaqueData
	processStorage, err := w.getOrCreateStorage(ctx, resp.ExecutionID)
	if err != nil {
		return nil, err
	}
	processParams.Storage = processStorage

	userState, err := fn.NewState(processParams)
	if err != nil {
		return nil, err
	}

	// Gob-encode user state for HTTP serialization
	userStateBytes, err := gobEncode(userState)
	if err != nil {
		return nil, fmt.Errorf("encoding user state: %w", err)
	}

	header := &GlobalInitResponseWire{
		ExecutionID: resp.ExecutionID,
		MaxWorkers:  resp.MaxWorkers,
	}
	if len(resp.OpaqueData) > 0 {
		header.OpaqueData = &resp.OpaqueData
	}

	state := &TableProducerState{
		Recipe:         *recipe,
		UserStateBytes: userStateBytes,
		AutoProjectIDs: autoProjectIDs,
		fn:             fn,
		params:         processParams,
		state:          userState,
	}

	// Set up auto-apply if the function opts in and filters are present
	if fn.Metadata().AutoApplyFilters && processParams.PushdownFilters != nil {
		parsed, err := DeserializeFilters(processParams.PushdownFilters)
		if err == nil && len(parsed.Filters) > 0 {
			state.autoApply = parsed
		}
	}

	return &vgirpc.StreamResult{
		OutputSchema: outputSchema,
		State:        state,
		Header:       header,
	}, nil
}

func (w *Worker) initTableInOut(ctx context.Context, fn TableInOutFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema, phase string, recipe *InitRecipe) (*vgirpc.StreamResult, error) {
	// Pre-create storage so OnInit can use it
	if initParams.ExecutionID == nil {
		initParams.ExecutionID = newExecutionID()
	}
	initStorage, err := w.getOrCreateStorage(ctx, initParams.ExecutionID)
	if err != nil {
		return nil, err
	}
	initParams.Storage = initStorage

	var resp *GlobalInitResponse
	if !initParams.IsSecondary {
		// Primary init: call OnInit (global_init)
		resp, err = fn.OnInit(initParams)
		if err != nil {
			return nil, err
		}
		if resp.ExecutionID == nil {
			resp.ExecutionID = initParams.ExecutionID
		}
		if resp.MaxWorkers == 0 {
			resp.MaxWorkers = 1
		}
	} else {
		// Secondary init: skip OnInit, reuse execution ID
		resp = &GlobalInitResponse{
			ExecutionID: initParams.ExecutionID,
			MaxWorkers:  1,
		}
		if initParams.InitOpaqueData != nil {
			resp.OpaqueData = initParams.InitOpaqueData
		}
	}
	processParams.ExecutionID = resp.ExecutionID
	recipe.ExecutionID = resp.ExecutionID
	recipe.InitOpaqueData = resp.OpaqueData
	processParams.InitOpaqueData = resp.OpaqueData
	processStorage, err := w.getOrCreateStorage(ctx, resp.ExecutionID)
	if err != nil {
		return nil, err
	}
	processParams.Storage = processStorage

	header := &GlobalInitResponseWire{
		ExecutionID: resp.ExecutionID,
		MaxWorkers:  resp.MaxWorkers,
	}
	if len(resp.OpaqueData) > 0 {
		header.OpaqueData = &resp.OpaqueData
	}

	if phase == "FINALIZE" {
		// Finalize phase: call Finalize() to get batches, return as producer
		userState, err := fn.NewState(processParams)
		if err != nil {
			return nil, err
		}
		batches, err := fn.Finalize(ctx, processParams, userState)
		if err != nil {
			return nil, err
		}

		// Serialize each finalize batch to IPC bytes
		var batchIPC [][]byte
		for _, b := range batches {
			data, serErr := SerializeRecordBatch(b)
			if serErr != nil {
				return nil, fmt.Errorf("serializing finalize batch: %w", serErr)
			}
			batchIPC = append(batchIPC, data)
		}

		state := &FinalizeProducerState{
			Recipe:   *recipe,
			BatchIPC: batchIPC,
			batches:  batches,
		}

		return &vgirpc.StreamResult{
			OutputSchema: outputSchema,
			State:        state,
			Header:       header,
		}, nil
	}

	// INPUT phase: exchange mode
	userState, err := fn.NewState(processParams)
	if err != nil {
		return nil, err
	}

	// Gob-encode user state for HTTP serialization
	userStateBytes, err := gobEncode(userState)
	if err != nil {
		return nil, fmt.Errorf("encoding user state: %w", err)
	}

	state := &TableInOutExchangeState{
		Recipe:         *recipe,
		UserStateBytes: userStateBytes,
		fn:             fn,
		params:         processParams,
		state:          userState,
	}

	// Set up auto-apply if the function opts in and filters are present
	if fn.Metadata().AutoApplyFilters && processParams.PushdownFilters != nil {
		parsed, err := DeserializeFilters(processParams.PushdownFilters)
		if err == nil && len(parsed.Filters) > 0 {
			state.autoApply = parsed
		}
	}

	return &vgirpc.StreamResult{
		OutputSchema: outputSchema,
		State:        state,
		InputSchema:  initParams.InputSchema,
		Header:       header,
	}, nil
}

// handleCardinality processes a table_function_cardinality RPC request.
func (w *Worker) handleCardinality(ctx context.Context, callCtx *vgirpc.CallContext, req CardinalityRequestWire) (TableCardinality, error) {
	bindReq, err := w.deserializeBindRequest(req.BindCall)
	if err != nil {
		return TableCardinality{}, fmt.Errorf("deserializing bind_call: %w", err)
	}

	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return TableCardinality{}, err
	}

	fn, err := w.resolveFunctionWithOverload(bindReq.FunctionName, FunctionType(bindReq.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		return TableCardinality{}, err
	}

	tableFn, ok := fn.(TableFunctionWithCardinality)
	if !ok {
		return TableCardinality{Estimate: -1, Max: -1}, nil
	}

	card, err := tableFn.Cardinality(bindParams)
	if err != nil {
		return TableCardinality{}, err
	}
	return *card, nil
}

// ---------------------------------------------------------------------------
// Helper methods
// ---------------------------------------------------------------------------

// parseBindRequest converts a wire bind request into BindParams.
func (w *Worker) parseBindRequest(req BindRequestWire) (*BindParams, error) {
	args, err := ParseArguments(req.Arguments)
	if err != nil {
		return nil, fmt.Errorf("parsing arguments: %w", err)
	}

	params := &BindParams{
		FunctionName: req.FunctionName,
		FunctionType: FunctionType(req.FunctionType),
		Args:         args,
	}

	if req.InputSchema != nil {
		schema, err := DeserializeSchema(*req.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("deserializing input schema: %w", err)
		}
		params.InputSchema = schema
	}

	if req.Settings != nil {
		batch, err := DeserializeRecordBatch(*req.Settings)
		if err == nil {
			params.Settings = BatchToSettingsMap(batch)
			batch.Release()
		}
	}

	if req.Secrets != nil {
		batch, err := DeserializeRecordBatch(*req.Secrets)
		if err == nil {
			params.Secrets = BatchToSecretsMap(batch)
			batch.Release()
		}
	}

	if req.AttachID != nil {
		params.AttachID = *req.AttachID
	}
	if req.TransactionID != nil {
		params.TransactionID = *req.TransactionID
	}

	params.ResolvedSecretsProvided = req.ResolvedSecretsProvided

	return params, nil
}

// deserializeBindRequest deserializes a BindRequestWire from IPC bytes.
func (w *Worker) deserializeBindRequest(data []byte) (*BindRequestWire, error) {
	batch, err := DeserializeRecordBatch(data)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	req := &BindRequestWire{}

	for i := 0; i < int(batch.NumCols()); i++ {
		col := batch.Column(i)
		name := batch.ColumnName(i)
		if col.IsNull(0) {
			continue
		}
		switch name {
		case "function_name":
			if c, ok := col.(*array.String); ok {
				req.FunctionName = c.Value(0)
			}
		case "arguments":
			if c, ok := col.(*array.Binary); ok {
				req.Arguments = c.Value(0)
			}
		case "function_type":
			switch c := col.(type) {
			case *array.Dictionary:
				dict := c.Dictionary().(*array.String)
				v := dict.Value(c.GetValueIndex(0))
				req.FunctionType = v
			case *array.String:
				req.FunctionType = c.Value(0)
			}
		case "input_schema":
			if c, ok := col.(*array.Binary); ok {
				v := c.Value(0)
				req.InputSchema = &v
			}
		case "settings":
			if c, ok := col.(*array.Binary); ok {
				v := c.Value(0)
				req.Settings = &v
			}
		case "secrets":
			if c, ok := col.(*array.Binary); ok {
				v := c.Value(0)
				req.Secrets = &v
			}
		case "attach_id":
			if c, ok := col.(*array.Binary); ok {
				v := c.Value(0)
				req.AttachID = &v
			}
		case "transaction_id":
			if c, ok := col.(*array.Binary); ok {
				v := c.Value(0)
				req.TransactionID = &v
			}
		case "resolved_secrets_provided":
			if c, ok := col.(*array.Boolean); ok {
				req.ResolvedSecretsProvided = c.Value(0)
			}
		}
	}

	return req, nil
}

// normalizeFunctionType converts DuckDB function type strings to our canonical values.
func normalizeFunctionType(ft FunctionType) FunctionType {
	switch ft {
	case "SCALAR", "SCALAR_FUNCTION", "scalar":
		return FunctionTypeScalar
	case "TABLE", "TABLE_FUNCTION", "table":
		return FunctionTypeTable
	case "AGGREGATE", "AGGREGATE_FUNCTION", "aggregate":
		return FunctionTypeAggregate
	default:
		return ft
	}
}

// resolveFunction looks up a function by name and type.
// When multiple overloads exist, returns the first one (used for rehydration
// where the bind call will re-resolve with proper overloading).
func (w *Worker) resolveFunction(name string, ft FunctionType) (interface{}, error) {
	ft = normalizeFunctionType(ft)
	// Search in all registries
	switch ft {
	case FunctionTypeScalar:
		if fns, ok := w.scalars[name]; ok && len(fns) > 0 {
			return fns[0], nil
		}
	case FunctionTypeTable:
		if fns, ok := w.tables[name]; ok && len(fns) > 0 {
			return fns[0], nil
		}
		// Table-in-out functions are also registered as "table" type
		if fns, ok := w.tableInOuts[name]; ok && len(fns) > 0 {
			return fns[0], nil
		}
	case FunctionTypeAggregate:
		// Table-in-out functions can be called as aggregate
		if fns, ok := w.tableInOuts[name]; ok && len(fns) > 0 {
			return fns[0], nil
		}
	}

	// Try all registries
	if fns, ok := w.scalars[name]; ok && len(fns) > 0 {
		return fns[0], nil
	}
	if fns, ok := w.tables[name]; ok && len(fns) > 0 {
		return fns[0], nil
	}
	if fns, ok := w.tableInOuts[name]; ok && len(fns) > 0 {
		return fns[0], nil
	}

	return nil, &vgirpc.RpcError{
		Type:    "ValueError",
		Message: fmt.Sprintf("Unknown function: '%s'", name),
	}
}

// getFunctionMetadata returns the FunctionMetadata for a resolved function.
func (w *Worker) getFunctionMetadata(fn interface{}) FunctionMetadata {
	switch f := fn.(type) {
	case ScalarFunction:
		return f.Metadata()
	case TableFunction:
		return f.Metadata()
	case TableInOutFunction:
		return f.Metadata()
	}
	return DefaultMetadata()
}

// getArgSpecs returns the ArgSpecs for a resolved function.
func (w *Worker) getArgSpecs(fn interface{}) []ArgSpec {
	switch f := fn.(type) {
	case ScalarFunction:
		return f.ArgumentSpecs()
	case TableFunction:
		return f.ArgumentSpecs()
	case TableInOutFunction:
		return f.ArgumentSpecs()
	}
	return nil
}

// ---------------------------------------------------------------------------
// ArrowSerializable implementation for GlobalInitResponseWire
// ---------------------------------------------------------------------------

func (r *GlobalInitResponseWire) ArrowSchema() *arrow.Schema {
	fields := []arrow.Field{
		{Name: "execution_id", Type: arrow.BinaryTypes.Binary},
		{Name: "max_workers", Type: arrow.PrimitiveTypes.Int64},
		{Name: "opaque_data", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}
	return arrow.NewSchema(fields, nil)
}

// BuildGlobalInitBatch creates a 1-row RecordBatch from a GlobalInitResponseWire.
func BuildGlobalInitBatch(resp *GlobalInitResponseWire) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	schema := resp.ArrowSchema()

	execIDBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer execIDBuilder.Release()
	execIDBuilder.Append(resp.ExecutionID)

	maxWorkersBuilder := array.NewInt64Builder(mem)
	defer maxWorkersBuilder.Release()
	maxWorkersBuilder.Append(resp.MaxWorkers)

	opaqueBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer opaqueBuilder.Release()
	if resp.OpaqueData != nil {
		opaqueBuilder.Append(*resp.OpaqueData)
	} else {
		opaqueBuilder.AppendNull()
	}

	cols := []arrow.Array{
		execIDBuilder.NewArray(),
		maxWorkersBuilder.NewArray(),
		opaqueBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	return array.NewRecordBatch(schema, cols, 1)
}
