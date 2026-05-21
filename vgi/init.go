// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import "github.com/apache/arrow-go/v18/arrow"

// InitParams holds the parameters available during the init phase.
type InitParams struct {
	// FunctionName is the name of the function.
	FunctionName string
	// FunctionType is the type of the function.
	FunctionType FunctionType
	// Args are the parsed function arguments.
	Args *Arguments
	// OutputSchema is the output schema resolved during bind.
	OutputSchema *arrow.Schema
	// InputSchema is the input table schema (nil for table functions).
	InputSchema *arrow.Schema
	// ProjectionIDs are the projected column indices (nil = all columns).
	ProjectionIDs []int32
	// Phase is the table-in-out init phase.
	Phase Phase
	// ExecutionID is the execution ID for secondary inits.
	ExecutionID []byte
	// BindOpaqueData is opaque data from the bind phase.
	BindOpaqueData []byte
	// InitOpaqueData is opaque data from a previous global init (secondary inits).
	InitOpaqueData []byte
	// Settings is a map of DuckDB setting names to their scalar values.
	Settings map[string]interface{}
	// Secrets is a map of secret names to their value maps.
	Secrets map[string]map[string]interface{}
	// IsSecondary is true if this is a secondary init (worker init).
	IsSecondary bool
	// PushdownFilters is the pushdown filter batch (nil if none).
	PushdownFilters arrow.RecordBatch
	// JoinKeys maps keys_column name -> Arrow array carrying the join keys
	// referenced by FilterJoinKeys entries in PushdownFilters.
	JoinKeys map[string]arrow.Array
	// OrderByHint, when non-nil, carries an ORDER BY + LIMIT pushdown
	// hint set by DuckDB's RowGroupPruner optimizer.
	OrderByHint *OrderByHint
	// TableSampleHint, when non-nil, carries a TABLESAMPLE pushdown hint.
	TableSampleHint *TableSampleHint
	// Storage provides shared execution storage for cross-phase data.
	Storage *ExecutionStorage
}

// OrderByHint is an ORDER BY + LIMIT hint pushed by the optimizer.
type OrderByHint struct {
	ColumnName string
	Direction  OrderByDirection // "" if unspecified
	NullOrder  OrderByNullOrder // "" if unspecified
	RowLimit   int64            // -1 if unbounded
}

// TableSampleHint is a TABLESAMPLE pushdown hint.
type TableSampleHint struct {
	Percentage float64
	Seed       int64
}

// GlobalInitResponse is returned by a function's OnInit method.
type GlobalInitResponse struct {
	// ExecutionID uniquely identifies this execution.
	ExecutionID []byte
	// MaxWorkers is the maximum number of parallel workers (0 = default/4).
	MaxWorkers int64
	// OpaqueData is optional data passed to secondary inits.
	OpaqueData []byte
}
