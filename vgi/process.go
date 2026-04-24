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
	// OrderByHint, when non-nil, carries an ORDER BY + LIMIT pushdown hint.
	OrderByHint *OrderByHint
	// TableSampleHint, when non-nil, carries a TABLESAMPLE pushdown hint.
	TableSampleHint *TableSampleHint
	// Storage provides shared execution storage for cross-phase data.
	Storage *ExecutionStorage
	// Auth is the authentication context for the current request.
	// Always non-nil; unauthenticated requests receive vgirpc.Anonymous().
	Auth *vgirpc.AuthContext
}
