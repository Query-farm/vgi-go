// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"fmt"

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
	FunctionName  string  `vgirpc:"function_name"`
	Arguments     []byte  `vgirpc:"arguments"`
	FunctionType  string  `vgirpc:"function_type,enum"`
	InputSchema   *[]byte `vgirpc:"input_schema"`
	Settings      *[]byte `vgirpc:"settings"`
	Secrets       *[]byte `vgirpc:"secrets"`
	AttachID      *[]byte `vgirpc:"attach_id"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// BindResponseWire is the wire format for bind responses.
type BindResponseWire struct {
	OutputSchema []byte  `vgirpc:"output_schema"`
	OpaqueData   *[]byte `vgirpc:"opaque_data"`
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
	fn     ScalarFunction
	params *ProcessParams
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
	fn    TableFunction
	params *ProcessParams
	state interface{}
}

func (s *TableProducerState) Produce(ctx context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	return s.fn.Process(ctx, s.params, s.state, out)
}

// TableInOutExchangeState implements ExchangeState for table-in-out INPUT phase.
type TableInOutExchangeState struct {
	fn     TableInOutFunction
	params *ProcessParams
	state  interface{}
}

func (s *TableInOutExchangeState) Exchange(ctx context.Context, input arrow.RecordBatch, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	return s.fn.Process(ctx, s.params, s.state, input, out)
}

// FinalizeProducerState implements ProducerState for table-in-out FINALIZE phase.
type FinalizeProducerState struct {
	batches []arrow.RecordBatch
	index   int
}

func (s *FinalizeProducerState) Produce(ctx context.Context, out *vgirpc.OutputCollector, callCtx *vgirpc.CallContext) error {
	if s.index >= len(s.batches) {
		return out.Finish()
	}
	batch := s.batches[s.index]
	s.index++
	return out.Emit(batch)
}

// ---------------------------------------------------------------------------
// Protocol handler implementations
// ---------------------------------------------------------------------------

// handleBind processes a bind RPC request.
func (w *Worker) handleBind(ctx context.Context, callCtx *vgirpc.CallContext, req BindRequestWire) (BindResponseWire, error) {
	debugLog("handleBind: function_name=%q function_type=%q args_len=%d has_input_schema=%v", req.FunctionName, req.FunctionType, len(req.Arguments), req.InputSchema != nil)
	bindParams, err := w.parseBindRequest(req)
	if err != nil {
		debugLog("handleBind: parseBindRequest error: %v", err)
		return BindResponseWire{}, err
	}
	debugLog("handleBind: parsed args ok, positional=%d named=%d", len(bindParams.Args.Positional), len(bindParams.Args.Named))

	fn, err := w.resolveFunction(req.FunctionName, FunctionType(req.FunctionType))
	if err != nil {
		debugLog("handleBind: resolveFunction error: %v", err)
		return BindResponseWire{}, err
	}
	debugLog("handleBind: resolved function %q as %T", req.FunctionName, fn)

	// Remap positional args to original ArgSpec positions
	bindParams.Args.RemapPositionalArgs(w.getArgSpecs(fn))

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

	outputSchemaBytes, err := SerializeSchema(bindResp.OutputSchema)
	if err != nil {
		return BindResponseWire{}, fmt.Errorf("serializing output schema: %w", err)
	}

	resp := BindResponseWire{
		OutputSchema: outputSchemaBytes,
	}
	if len(bindResp.OpaqueData) > 0 {
		resp.OpaqueData = &bindResp.OpaqueData
	}
	return resp, nil
}

// handleInit processes an init RPC request and returns a StreamResult.
func (w *Worker) handleInit(ctx context.Context, callCtx *vgirpc.CallContext, req InitRequestWire) (*vgirpc.StreamResult, error) {
	debugLog("handleInit: bind_call_len=%d output_schema_len=%d phase=%v exec_id_present=%v", len(req.BindCall), len(req.OutputSchema), req.Phase, req.ExecutionID != nil)
	// Deserialize the embedded bind call
	bindReq, err := w.deserializeBindRequest(req.BindCall)
	if err != nil {
		debugLog("handleInit: deserializeBindRequest error: %v", err)
		return nil, fmt.Errorf("deserializing bind_call: %w", err)
	}
	debugLog("handleInit: function=%q type=%q", bindReq.FunctionName, bindReq.FunctionType)

	// Parse output schema
	outputSchema, err := DeserializeSchema(req.OutputSchema)
	if err != nil {
		debugLog("handleInit: deserialize output_schema error: %v", err)
		return nil, fmt.Errorf("deserializing output_schema: %w", err)
	}
	debugLog("handleInit: output_schema fields=%d", outputSchema.NumFields())

	// Resolve the function
	fn, err := w.resolveFunction(bindReq.FunctionName, FunctionType(bindReq.FunctionType))
	if err != nil {
		debugLog("handleInit: resolveFunction error: %v", err)
		return nil, err
	}
	debugLog("handleInit: resolved function as %T", fn)

	// Parse bind params for argument access
	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return nil, err
	}

	// Remap positional args to original ArgSpec positions
	bindParams.Args.RemapPositionalArgs(w.getArgSpecs(fn))

	// Determine phase
	phase := ""
	if req.Phase != nil {
		phase = *req.Phase
	}

	// Build init params
	initParams := &InitParams{
		FunctionName:   bindReq.FunctionName,
		FunctionType:   FunctionType(bindReq.FunctionType),
		Args:           bindParams.Args,
		OutputSchema:   outputSchema,
		InputSchema:    bindParams.InputSchema,
		Phase:          phase,
		Settings:       bindParams.Settings,
		Secrets:        bindParams.Secrets,
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

	// Apply projection to output schema
	projectedSchema := ProjectSchema(initParams.ProjectionIDs, outputSchema)

	// Build process params
	processParams := &ProcessParams{
		FunctionName:    bindReq.FunctionName,
		FunctionType:    FunctionType(bindReq.FunctionType),
		Args:            bindParams.Args,
		OutputSchema:    projectedSchema,
		ProjectionIDs:   initParams.ProjectionIDs,
		Settings:        bindParams.Settings,
		Secrets:         bindParams.Secrets,
		PushdownFilters: initParams.PushdownFilters,
	}

	debugLog("handleInit: dispatching to %T init", fn)
	switch f := fn.(type) {
	case ScalarFunction:
		result, err := w.initScalar(ctx, f, initParams, processParams, projectedSchema)
		if err != nil {
			debugLog("handleInit: initScalar error: %v", err)
		} else {
			debugLog("handleInit: initScalar success, state=%T", result.State)
		}
		return result, err
	case TableFunction:
		result, err := w.initTable(ctx, f, initParams, processParams, projectedSchema)
		if err != nil {
			debugLog("handleInit: initTable error: %v", err)
		} else {
			debugLog("handleInit: initTable success, state=%T", result.State)
		}
		return result, err
	case TableInOutFunction:
		result, err := w.initTableInOut(ctx, f, initParams, processParams, projectedSchema, phase)
		if err != nil {
			debugLog("handleInit: initTableInOut error: %v", err)
		} else {
			debugLog("handleInit: initTableInOut success, state=%T", result.State)
		}
		return result, err
	default:
		return nil, fmt.Errorf("unknown function type: %T", fn)
	}
}

func (w *Worker) initScalar(ctx context.Context, fn ScalarFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema) (*vgirpc.StreamResult, error) {
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
	processParams.Storage = w.getOrCreateStorage(resp.ExecutionID)

	header := &GlobalInitResponseWire{
		ExecutionID: resp.ExecutionID,
		MaxWorkers:  resp.MaxWorkers,
	}

	state := &ScalarExchangeState{
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

func (w *Worker) initTable(ctx context.Context, fn TableFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema) (*vgirpc.StreamResult, error) {
	// Pre-create storage so OnInit can use it
	if initParams.ExecutionID == nil {
		initParams.ExecutionID = newExecutionID()
	}
	initParams.Storage = w.getOrCreateStorage(initParams.ExecutionID)

	var resp *GlobalInitResponse
	if !initParams.IsSecondary {
		// Primary init: call OnInit (global_init) to set up work items, etc.
		var err error
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
	processParams.InitOpaqueData = resp.OpaqueData
	processParams.Storage = w.getOrCreateStorage(resp.ExecutionID)

	userState, err := fn.NewState(processParams)
	if err != nil {
		return nil, err
	}

	header := &GlobalInitResponseWire{
		ExecutionID: resp.ExecutionID,
		MaxWorkers:  resp.MaxWorkers,
	}
	if len(resp.OpaqueData) > 0 {
		header.OpaqueData = &resp.OpaqueData
	}

	state := &TableProducerState{
		fn:     fn,
		params: processParams,
		state:  userState,
	}

	return &vgirpc.StreamResult{
		OutputSchema: outputSchema,
		State:        state,
		Header:       header,
	}, nil
}

func (w *Worker) initTableInOut(ctx context.Context, fn TableInOutFunction, initParams *InitParams, processParams *ProcessParams, outputSchema *arrow.Schema, phase string) (*vgirpc.StreamResult, error) {
	// Pre-create storage so OnInit can use it
	if initParams.ExecutionID == nil {
		initParams.ExecutionID = newExecutionID()
	}
	initParams.Storage = w.getOrCreateStorage(initParams.ExecutionID)

	var resp *GlobalInitResponse
	if !initParams.IsSecondary {
		// Primary init: call OnInit (global_init)
		var err error
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
	processParams.InitOpaqueData = resp.OpaqueData
	processParams.Storage = w.getOrCreateStorage(resp.ExecutionID)

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

		state := &FinalizeProducerState{
			batches: batches,
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

	state := &TableInOutExchangeState{
		fn:     fn,
		params: processParams,
		state:  userState,
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

	fn, err := w.resolveFunction(bindReq.FunctionName, FunctionType(bindReq.FunctionType))
	if err != nil {
		return TableCardinality{}, err
	}

	tableFn, ok := fn.(TableFunctionWithCardinality)
	if !ok {
		return TableCardinality{Estimate: -1, Max: -1}, nil
	}

	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return TableCardinality{}, err
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
func (w *Worker) resolveFunction(name string, ft FunctionType) (interface{}, error) {
	ft = normalizeFunctionType(ft)
	// Search in all registries
	switch ft {
	case FunctionTypeScalar:
		if fn, ok := w.scalars[name]; ok {
			return fn, nil
		}
	case FunctionTypeTable:
		if fn, ok := w.tables[name]; ok {
			return fn, nil
		}
		// Table-in-out functions are also registered as "table" type
		if fn, ok := w.tableInOuts[name]; ok {
			return fn, nil
		}
	case FunctionTypeAggregate:
		// Table-in-out functions can be called as aggregate
		if fn, ok := w.tableInOuts[name]; ok {
			return fn, nil
		}
	}

	// Try all registries
	if fn, ok := w.scalars[name]; ok {
		return fn, nil
	}
	if fn, ok := w.tables[name]; ok {
		return fn, nil
	}
	if fn, ok := w.tableInOuts[name]; ok {
		return fn, nil
	}

	return nil, &vgirpc.RpcError{
		Type:    "ValueError",
		Message: fmt.Sprintf("Unknown function: '%s'", name),
	}
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
