// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Wire types for RPC serialization (used with vgirpc struct tags)
// ---------------------------------------------------------------------------

// BindRequestWire is the wire format for bind requests. It is deserialized
// two ways: as the top-level params of the bind RPC (via the vgirpc tags) and
// as the nested bind_call struct of init/cardinality/statistics/
// dynamic_to_string (via the arrow tags, which the framework reads for struct
// children).
type BindRequestWire struct {
	FunctionName            string  `vgirpc:"function_name" arrow:"function_name"`
	Arguments               []byte  `vgirpc:"arguments" arrow:"arguments"`
	FunctionType            string  `vgirpc:"function_type,enum" arrow:"function_type,enum"`
	InputSchema             *[]byte `vgirpc:"input_schema" arrow:"input_schema"`
	Settings                *[]byte `vgirpc:"settings" arrow:"settings"`
	Secrets                 *[]byte `vgirpc:"secrets" arrow:"secrets"`
	AttachOpaqueData        *[]byte `vgirpc:"attach_opaque_data" arrow:"attach_opaque_data"`
	TransactionOpaqueData   *[]byte `vgirpc:"transaction_opaque_data" arrow:"transaction_opaque_data"`
	ResolvedSecretsProvided bool    `vgirpc:"resolved_secrets_provided" arrow:"resolved_secrets_provided"`
}

// bindRequestWireSchema is the Arrow struct schema for a bind_call. Field
// names/types must match the arrow tags above.
var bindRequestWireSchema = arrow.NewSchema([]arrow.Field{
	{Name: "function_name", Type: arrow.BinaryTypes.String},
	{Name: "arguments", Type: arrow.BinaryTypes.Binary},
	{Name: "function_type", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}},
	{Name: "input_schema", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "settings", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "secrets", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "attach_opaque_data", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "transaction_opaque_data", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "resolved_secrets_provided", Type: &arrow.BooleanType{}},
}, nil)

// ArrowSchema makes BindRequestWire an ArrowSerializable. The framework then
// encodes a nested bind_call as binary IPC at the parameter level (how the C++
// extension sends it) while still accepting an inline struct column (how the
// Python client sends it) — see vgi-rpc's setFieldFromArrow.
func (BindRequestWire) ArrowSchema() *arrow.Schema {
	return bindRequestWireSchema
}

// BindResponseWire is the wire format for bind responses.
type BindResponseWire struct {
	OutputSchema      []byte   `vgirpc:"output_schema"`
	OpaqueData        []byte   `vgirpc:"opaque_data"`
	LookupSecretTypes []string `vgirpc:"lookup_secret_types"`
	LookupScopes      []string `vgirpc:"lookup_scopes"`
	LookupNames       []string `vgirpc:"lookup_names"`
}

// InitRequestWire is the wire format for init requests.
type InitRequestWire struct {
	BindCall              BindRequestWire `vgirpc:"bind_call"`
	OutputSchema          []byte          `vgirpc:"output_schema"`
	BindOpaqueData        *[]byte         `vgirpc:"bind_opaque_data"`
	ProjectionIDs         *[]int32        `vgirpc:"projection_ids"`
	PushdownFilters       *[]byte         `vgirpc:"pushdown_filters"`
	JoinKeys              *[][]byte       `vgirpc:"join_keys"`
	Phase                 *string         `vgirpc:"phase,enum"`
	ExecutionID           *[]byte         `vgirpc:"execution_id"`
	InitOpaqueData        *[]byte         `vgirpc:"init_opaque_data"`
	OrderByColumnName     *string         `vgirpc:"order_by_column_name"`
	OrderByDirection      *string         `vgirpc:"order_by_direction,enum"`
	OrderByNullOrder      *string         `vgirpc:"order_by_null_order,enum"`
	OrderByLimit          *int64          `vgirpc:"order_by_limit"`
	TablesamplePercentage *float64        `vgirpc:"tablesample_percentage"`
	TablesampleSeed       *int64          `vgirpc:"tablesample_seed"`
	FinalizeStateID       *[]byte         `vgirpc:"finalize_state_id"`
}

// GlobalInitResponseWire is the wire format for global init responses.
type GlobalInitResponseWire struct {
	ExecutionID []byte  `vgirpc:"execution_id"`
	MaxWorkers  int64   `vgirpc:"max_workers"`
	OpaqueData  *[]byte `vgirpc:"opaque_data"`
}

// CardinalityRequestWire is the wire format for cardinality requests.
type CardinalityRequestWire struct {
	BindCall       BindRequestWire `vgirpc:"bind_call"`
	BindOpaqueData *[]byte         `vgirpc:"bind_opaque_data"`
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
	s.params.Auth = callCtx.Auth
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
	s.params.Auth = callCtx.Auth
	// Decode any dynamic filter update carried on this tick's custom metadata.
	// DuckDB's dynamic-filter pushdown ships a fresh, tightened filter batch
	// per tick under the vgi_pushdown_filters key (base64-encoded IPC stream).
	applyTickFilters(s.params, callCtx.InputMetadata)
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
			applied := s.autoApply
			if s.params.CurrentPushdownFilters != nil {
				applied = s.params.CurrentPushdownFilters
			}
			if applied != nil {
				return applied.Apply(ctx, result)
			}
			return result, nil
		}
	}
	return s.fn.Process(ctx, s.params, s.state, out)
}

// applyTickFilters checks the tick-level custom metadata for a dynamic filter
// update and, if present, replaces params.CurrentPushdownFilters with the
// freshly decoded filter state. Silent on decode errors — the previous filter
// state is retained (DuckDB falls back to client-side filtering).
func applyTickFilters(params *ProcessParams, meta arrow.Metadata) {
	if meta.Len() == 0 {
		// First tick after init: seed CurrentPushdownFilters from the static
		// PushdownFilters so handlers see a consistent view.
		if params.CurrentPushdownFilters == nil && params.PushdownFilters != nil {
			if pf, err := DeserializeFilters(params.PushdownFilters, params.JoinKeys); err == nil {
				params.CurrentPushdownFilters = pf
			}
		}
		return
	}
	idx := meta.FindKey("vgi_pushdown_filters")
	if idx < 0 {
		return
	}
	encoded := meta.Values()[idx]
	if encoded == "" {
		// Empty string signals "no dynamic filter yet" — keep the static one.
		return
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return
	}
	batch, err := DeserializeRecordBatch(raw)
	if err != nil {
		return
	}
	pf, err := DeserializeFilters(batch, params.JoinKeys)
	if err != nil {
		return
	}
	params.CurrentPushdownFilters = pf
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
	s.params.Auth = callCtx.Auth
	// Projection pushdown: a function declaring projection_pushdown emits a
	// full-width batch but its output schema is narrowed to the projected
	// columns. Select those columns by name before the (filter) auto-apply and
	// the wire write. Mirrors vgi-python's batch.select(target_names) in emit.
	needsProject := s.params.ProjectionIDs != nil
	if needsProject || s.autoApply != nil {
		out.EmitInterceptor = func(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
			b := batch
			if needsProject && int(b.NumCols()) > s.params.OutputSchema.NumFields() {
				nb, err := selectColumnsByName(b, s.params.OutputSchema)
				if err != nil {
					return nil, err
				}
				b = nb
			}
			if s.autoApply != nil {
				return s.autoApply.Apply(ctx, b)
			}
			return b, nil
		}
	}
	return s.fn.Process(ctx, s.params, s.state, input, out)
}

// selectColumnsByName builds a batch containing only the columns named in
// schema, in schema order, taken from src by field name.
func selectColumnsByName(src arrow.RecordBatch, schema *arrow.Schema) (arrow.RecordBatch, error) {
	srcSchema := src.Schema()
	cols := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		idx := srcSchema.FieldIndices(field.Name)
		if len(idx) == 0 {
			return nil, fmt.Errorf("projection: column %q absent from emitted batch", field.Name)
		}
		cols[i] = src.Column(idx[0])
	}
	return array.NewRecordBatch(schema, cols, src.NumRows()), nil
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
func (w *Worker) handleBind(ctx context.Context, callCtx *vgirpc.CallContext, req BindRequestWire) (resp BindResponseWire, err error) {
	defer RecoverPanic("bind", req.FunctionName, &err)
	LogRPC.Debug("bind: received request",
		"function", req.FunctionName,
		"type", req.FunctionType,
		"args_len", len(req.Arguments),
		"has_input_schema", req.InputSchema != nil,
	)
	bindParams, err := w.parseBindRequest(req)
	if err != nil {
		LogRPC.Debug("bind: parse failed", "err", err)
		return BindResponseWire{}, err
	}
	bindParams.Auth = callCtx.Auth
	if back, ferr := w.functionStorage(); ferr == nil {
		bindParams.txBackend = back
	}
	LogRPC.Debug("bind: parsed args",
		"positional", len(bindParams.Args.Positional),
		"named", len(bindParams.Args.Named),
	)

	fn, err := w.resolveFunctionWithOverload(req.FunctionName, FunctionType(req.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		LogRPC.Debug("bind: resolve function failed", "function", req.FunctionName, "err", err)
		return BindResponseWire{}, err
	}
	LogRPC.Debug("bind: resolved function", "function", req.FunctionName, "type", fmt.Sprintf("%T", fn))

	// Remap positional args to original ArgSpec positions
	argSpecs := w.getArgSpecs(fn)
	bindParams.Args.RemapPositionalArgs(argSpecs)

	// Validate type bounds against input schema before calling OnBind
	if err := ValidateTypeBounds(argSpecs, bindParams.InputSchema); err != nil {
		LogRPC.Debug("bind: type bound validation failed", "err", err)
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
	case TableBufferingFunction:
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
		LogRPC.Debug("bind: two-phase scope request", "function", req.FunctionName, "lookups", len(bindResp.SecretScopeRequest))
		types := make([]string, len(bindResp.SecretScopeRequest))
		scopes := make([]string, len(bindResp.SecretScopeRequest))
		names := make([]string, len(bindResp.SecretScopeRequest))
		for i, sl := range bindResp.SecretScopeRequest {
			types[i] = sl.SecretType
			scopes[i] = sl.Scope
			names[i] = sl.SecretName
		}
		return BindResponseWire{
			LookupSecretTypes: types,
			LookupScopes:      scopes,
			LookupNames:       names,
		}, nil
	}

	outputSchemaBytes, err := SerializeSchema(bindResp.OutputSchema)
	if err != nil {
		return BindResponseWire{}, fmt.Errorf("serializing output schema: %w", err)
	}

	resp = BindResponseWire{
		OutputSchema: outputSchemaBytes,
		OpaqueData:   bindResp.OpaqueData,
	}
	return resp, nil
}

// handleInit processes an init RPC request and returns a StreamResult.
func (w *Worker) handleInit(ctx context.Context, callCtx *vgirpc.CallContext, req InitRequestWire) (result *vgirpc.StreamResult, err error) {
	defer RecoverPanic("init", req.BindCall.FunctionName, &err)
	LogRPC.Debug("init: received request",
		"function", req.BindCall.FunctionName,
		"output_schema_len", len(req.OutputSchema),
		"phase", req.Phase,
		"exec_id_present", req.ExecutionID != nil,
	)
	// bind_call arrives as a nested struct on the wire.
	bindReq := &req.BindCall
	LogRPC.Debug("init: parsed bind call", "function", bindReq.FunctionName, "type", bindReq.FunctionType)

	// Parse output schema
	outputSchema, err := DeserializeSchema(req.OutputSchema)
	if err != nil {
		LogRPC.Debug("init: deserialize output_schema failed", "err", err)
		return nil, fmt.Errorf("deserializing output_schema: %w", err)
	}
	LogRPC.Debug("init: output schema", "fields", outputSchema.NumFields())

	// Parse bind params for argument access
	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return nil, err
	}

	// Resolve the function with overload resolution
	fn, err := w.resolveFunctionWithOverload(bindReq.FunctionName, FunctionType(bindReq.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		LogRPC.Debug("init: resolve function failed", "function", bindReq.FunctionName, "err", err)
		return nil, err
	}
	LogRPC.Debug("init: resolved function", "type", fmt.Sprintf("%T", fn))

	// Remap positional args to original ArgSpec positions
	bindParams.Args.RemapPositionalArgs(w.getArgSpecs(fn))

	// Determine phase
	var phase Phase
	if req.Phase != nil {
		phase = Phase(*req.Phase)
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
	if req.JoinKeys != nil && len(*req.JoinKeys) > 0 {
		initParams.JoinKeys = deserializeJoinKeys(*req.JoinKeys)
	}
	if req.OrderByColumnName != nil {
		hint := &OrderByHint{ColumnName: *req.OrderByColumnName, RowLimit: -1}
		if req.OrderByDirection != nil {
			hint.Direction = OrderByDirection(*req.OrderByDirection)
		}
		if req.OrderByNullOrder != nil {
			hint.NullOrder = OrderByNullOrder(*req.OrderByNullOrder)
		}
		if req.OrderByLimit != nil {
			hint.RowLimit = *req.OrderByLimit
		}
		initParams.OrderByHint = hint
	}
	if req.TablesamplePercentage != nil && *req.TablesamplePercentage >= 0 {
		hint := &TableSampleHint{Percentage: *req.TablesamplePercentage, Seed: -1}
		if req.TablesampleSeed != nil {
			hint.Seed = *req.TablesampleSeed
		}
		initParams.TableSampleHint = hint
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
		JoinKeys:        initParams.JoinKeys,
		OrderByHint:     initParams.OrderByHint,
		TableSampleHint: initParams.TableSampleHint,
	}

	// Build InitRecipe for HTTP state serialization
	recipe := InitRecipe{
		BindCall:        req.BindCall,
		OutputSchemaIPC: req.OutputSchema,
		FunctionName:    bindReq.FunctionName,
		FunctionType:    FunctionType(bindReq.FunctionType),
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

	LogRPC.Debug("init: dispatching", "type", fmt.Sprintf("%T", fn))
	switch f := fn.(type) {
	case ScalarFunction:
		result, err := w.initScalar(ctx, f, initParams, processParams, projectedSchema, &recipe)
		if err != nil {
			LogRPC.Debug("init: scalar init failed", "err", err)
		} else {
			LogRPC.Debug("init: scalar init success", "state", fmt.Sprintf("%T", result.State))
		}
		return result, err
	case TableFunction:
		result, err := w.initTable(ctx, f, initParams, processParams, projectedSchema, autoProjectIDs, &recipe)
		if err != nil {
			LogRPC.Debug("init: table init failed", "err", err)
		} else {
			LogRPC.Debug("init: table init success", "state", fmt.Sprintf("%T", result.State))
		}
		return result, err
	case TableInOutFunction:
		result, err := w.initTableInOut(ctx, f, initParams, processParams, projectedSchema, phase, &recipe)
		if err != nil {
			LogRPC.Debug("init: table-in-out init failed", "err", err)
		} else {
			LogRPC.Debug("init: table-in-out init success", "state", fmt.Sprintf("%T", result.State))
		}
		return result, err
	case TableBufferingFunction:
		result, err := w.initTableBuffering(ctx, f, initParams, processParams, projectedSchema, phase, &recipe, req.FinalizeStateID)
		if err != nil {
			LogRPC.Debug("init: table-buffering init failed", "err", err)
		} else {
			LogRPC.Debug("init: table-buffering init success", "state", fmt.Sprintf("%T", result.State))
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
	// Propagate any bind-phase opaque data to Process. Scalar functions have no
	// OnInit hook, so without this the OpaqueData set by OnBind would have no
	// way to reach Process — mirroring what initTable does via OnInit.
	if len(initParams.BindOpaqueData) > 0 {
		processParams.InitOpaqueData = initParams.BindOpaqueData
		recipe.InitOpaqueData = initParams.BindOpaqueData
	}
	storage, err := w.getOrCreateStorage(ctx, resp.ExecutionID)
	if err != nil {
		return nil, err
	}
	processParams.Storage = storage

	header := &GlobalInitResponseWire{
		ExecutionID: resp.ExecutionID,
		MaxWorkers:  resp.MaxWorkers,
	}
	if len(initParams.BindOpaqueData) > 0 {
		op := initParams.BindOpaqueData
		header.OpaqueData = &op
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
		parsed, err := DeserializeFilters(processParams.PushdownFilters, processParams.JoinKeys)
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

func (w *Worker) initTableInOut(ctx context.Context, fn TableInOutFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema, phase Phase, recipe *InitRecipe) (*vgirpc.StreamResult, error) {
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

	if phase == PhaseFinalize {
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
		parsed, err := DeserializeFilters(processParams.PushdownFilters, processParams.JoinKeys)
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

// handleTableFunctionStatistics processes a table_function_statistics RPC
// request, returning serialized per-column statistics IPC bytes (or nil when
// unknown).
func (w *Worker) handleTableFunctionStatistics(ctx context.Context, callCtx *vgirpc.CallContext, req CardinalityRequestWire) (out []byte, err error) {
	defer RecoverPanic("statistics", req.BindCall.FunctionName, &err)
	bindReq := &req.BindCall
	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return nil, err
	}
	bindParams.Auth = callCtx.Auth

	fn, err := w.resolveFunctionWithOverload(bindReq.FunctionName, FunctionType(bindReq.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		return nil, err
	}
	statsFn, ok := fn.(TableFunctionWithStatistics)
	if !ok {
		return nil, nil
	}
	stats, err := statsFn.Statistics(bindParams)
	if err != nil {
		return nil, err
	}
	if len(stats) == 0 {
		return nil, nil
	}
	return SerializeColumnStatistics(stats, nil)
}

// handleCardinality processes a table_function_cardinality RPC request.
func (w *Worker) handleCardinality(ctx context.Context, callCtx *vgirpc.CallContext, req CardinalityRequestWire) (card TableCardinality, err error) {
	defer RecoverPanic("cardinality", req.BindCall.FunctionName, &err)
	bindReq := &req.BindCall

	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return TableCardinality{}, err
	}
	bindParams.Auth = callCtx.Auth

	fn, err := w.resolveFunctionWithOverload(bindReq.FunctionName, FunctionType(bindReq.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		return TableCardinality{}, err
	}

	switch cf := fn.(type) {
	case TableFunctionWithCardinality:
		card, err := cf.Cardinality(bindParams)
		if err != nil {
			return TableCardinality{}, err
		}
		return *card, nil
	case TableBufferingFunctionWithCardinality:
		card, err := cf.Cardinality(bindParams)
		if err != nil {
			return TableCardinality{}, err
		}
		return *card, nil
	}
	return TableCardinality{Estimate: -1, Max: -1}, nil
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

	if req.AttachOpaqueData != nil {
		params.AttachOpaqueData = *req.AttachOpaqueData
	}
	if req.TransactionOpaqueData != nil {
		params.TransactionOpaqueData = *req.TransactionOpaqueData
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
		case "attach_opaque_data":
			if c, ok := col.(*array.Binary); ok {
				v := c.Value(0)
				req.AttachOpaqueData = &v
			}
		case "transaction_opaque_data":
			if c, ok := col.(*array.Binary); ok {
				v := c.Value(0)
				req.TransactionOpaqueData = &v
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
	case "TABLE_BUFFERING", "table_buffering":
		return FunctionTypeTableBuffering
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
	case TableBufferingFunction:
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
	case TableBufferingFunction:
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
