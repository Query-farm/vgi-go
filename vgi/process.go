// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ProcessParams holds the parameters available during the process phase.
type ProcessParams struct {
	// FunctionName is the name of the function.
	FunctionName string
	// FunctionType is the type of the function.
	FunctionType FunctionType
	// Args are the parsed function arguments.
	Args *Arguments
	// OutputSchema is the output schema (may be projected).
	OutputSchema *arrow.Schema
	// ProjectionIDs are the projected column indices (nil = all columns).
	ProjectionIDs []int32
	// Settings is a map of DuckDB setting names to their scalar values.
	Settings map[string]interface{}
	// Secrets is a map of secret names to their value maps.
	Secrets map[string]map[string]interface{}
	// ExecutionID is the execution identifier.
	ExecutionID []byte
	// InitOpaqueData is the opaque data from the init response.
	InitOpaqueData []byte
	// PushdownFilters is the pushdown filter batch (nil if none).
	PushdownFilters arrow.RecordBatch
	// JoinKeys maps keys_column name -> Arrow array carrying the join keys
	// referenced by FilterJoinKeys entries in PushdownFilters.
	JoinKeys map[string]arrow.Array
	// AtUnit/AtValue carry the AT (TIMESTAMP|VERSION ...) time-travel clause for
	// this scan, threaded onto the bind request embedded in init. Both nil when
	// the scan has no AT clause. Function-backed time-travel tables resolve the
	// version from these at NewState.
	AtUnit  *string
	AtValue *string
	// CurrentPushdownFilters is the filter state for the *current* Produce
	// tick. It starts at the init-time pushdown filters and is replaced
	// whenever DuckDB's dynamic filter tightens (DynamicFilter pushdown).
	// Functions that want to react to filter updates per batch should read
	// this field; functions that only care about static filters should use
	// PushdownFilters.
	CurrentPushdownFilters *PushdownFilters
	// OrderByHint, when non-nil, carries an ORDER BY + LIMIT pushdown hint.
	OrderByHint *OrderByHint
	// TableSampleHint, when non-nil, carries a TABLESAMPLE pushdown hint.
	TableSampleHint *TableSampleHint
	// Storage provides shared execution storage for cross-phase data.
	Storage *ExecutionStorage
	// Auth is the authentication context for the current request.
	// Always non-nil; unauthenticated requests receive vgirpc.Anonymous().
	Auth *vgirpc.AuthContext
	// BatchIndex is the DuckDB per-chunk batch index threaded into a
	// table_buffering_process call when the function declares
	// RequiresInputBatchIndex. Nil otherwise.
	BatchIndex *int64
	// clientLog forwards an in-band log message to the client (surfaced in
	// duckdb_logs() with type='VGI'). Set by the framework on the unary
	// table-buffering RPCs, which have no streaming OutputCollector. Nil when
	// in-band logging is unavailable.
	clientLog func(level vgirpc.LogLevel, msg string)
}

// ClientLog emits an in-band log message to the client, if the framework wired
// a logging sink for this call (e.g. from table_buffering_process/combine).
func (p *ProcessParams) ClientLog(level vgirpc.LogLevel, msg string) {
	if p.clientLog != nil {
		p.clientLog(level, msg)
	}
}
