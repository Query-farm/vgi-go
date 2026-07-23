// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// DynamicToStringHook is the optional interface a TableFunction may implement
// to surface per-execution diagnostics under EXPLAIN ANALYZE. The C++
// extension calls this once per scan thread (in OperatorProfiler::FinishSource);
// the last writer wins for the operator's Extra Info.
//
// The returned key/value pairs are merged into DuckDB's
// InsertionOrderPreservingMap. Order is preserved over the wire via parallel
// keys/values lists.
type DynamicToStringHook interface {
	// DynamicToString returns key/value diagnostics surfaced under EXPLAIN ANALYZE for the current scan.
	DynamicToString(ctx context.Context, params *DynamicToStringParams) (keys []string, values []string, err error)
}

// DynamicToStringParams carries the per-call inputs.
type DynamicToStringParams struct {
	// FunctionName is the table function being profiled.
	FunctionName string
	// AttachOpaqueData identifies the catalog the function was invoked under
	// (nil when the call carried no attachment).
	AttachOpaqueData []byte
	// GlobalExecutionID matches the execution_id returned from init_global
	// for this scan; the function uses it to look up storage written during
	// process().
	GlobalExecutionID []byte
	// Storage is the cross-process scratchpad keyed by GlobalExecutionID.
	// Use Storage.Snapshot() to read every worker's per-tick contribution
	// without draining the table.
	Storage *ExecutionStorage
	// Auth is the authentication context for the call.
	Auth *vgirpc.AuthContext
}

// TableFunctionDynamicToStringRequestWire mirrors vgi-python's
// TableFunctionDynamicToStringRequest dataclass.
type TableFunctionDynamicToStringRequestWire struct {
	BindCall          BindRequestWire `vgirpc:"bind_call"`
	BindOpaqueData    *[]byte         `vgirpc:"bind_opaque_data"`
	GlobalExecutionID []byte          `vgirpc:"global_execution_id"`
}

// TableFunctionDynamicToStringResponseWire is the parallel-list wire format.
type TableFunctionDynamicToStringResponseWire struct {
	Keys   []string `vgirpc:"keys"`
	Values []string `vgirpc:"values"`
}

func (w *Worker) registerDynamicToStringRPCs(s *vgirpc.Server) {
	vgirpc.Unary[TableFunctionDynamicToStringRequestWire, TableFunctionDynamicToStringResponseWire](
		s, "table_function_dynamic_to_string", w.handleTableFunctionDynamicToString)
}

func (w *Worker) handleTableFunctionDynamicToString(ctx context.Context, callCtx *vgirpc.CallContext, req TableFunctionDynamicToStringRequestWire) (TableFunctionDynamicToStringResponseWire, error) {
	// bind_call arrives as a nested struct — we only need the function name to
	// dispatch to the right TableFunction.
	bindReq := &req.BindCall

	fn, err := w.lookupTable(bindReq.FunctionName)
	if err != nil {
		return TableFunctionDynamicToStringResponseWire{}, err
	}
	hook, ok := fn.(DynamicToStringHook)
	if !ok {
		// Function did not opt in — return empty so the C++ side surfaces
		// only the intrinsic Function/Worker keys.
		return TableFunctionDynamicToStringResponseWire{}, nil
	}

	params := &DynamicToStringParams{
		FunctionName:      bindReq.FunctionName,
		GlobalExecutionID: req.GlobalExecutionID,
		Auth:              callCtx.Auth,
	}
	var shardKey string
	if bindReq.AttachOpaqueData != nil {
		// User hook sees the catalog's bytes (uuid stripped); storage shards on the uuid.
		if catalogBytes, err := w.openAttach(*bindReq.AttachOpaqueData, callCtx); err == nil {
			params.AttachOpaqueData = catalogBytes
		}
		shardKey, _ = w.shardKeyForAttach(*bindReq.AttachOpaqueData, callCtx)
	}
	if storage, err := w.getOrCreateStorage(ctx, req.GlobalExecutionID, shardKey); err == nil {
		params.Storage = storage
	}
	keys, values, err := hook.DynamicToString(ctx, params)
	if err != nil {
		return TableFunctionDynamicToStringResponseWire{}, err
	}
	if len(keys) != len(values) {
		return TableFunctionDynamicToStringResponseWire{}, fmt.Errorf("DynamicToString returned mismatched keys/values: %d vs %d", len(keys), len(values))
	}
	return TableFunctionDynamicToStringResponseWire{Keys: keys, Values: values}, nil
}

// lookupTable resolves a registered table function by name (single-overload
// lookup; the dynamic_to_string call always knows the resolved name).
func (w *Worker) lookupTable(name string) (TableFunction, error) {
	fns, ok := w.tables[name]
	if !ok || len(fns) == 0 {
		return nil, fmt.Errorf("table function %q not registered", name)
	}
	return fns[0], nil
}
