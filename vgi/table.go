// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"

	"github.com/Query-farm/vgi-rpc/vgirpc"
)

// TableFunction is the interface for table VGI functions.
// Table functions generate output without receiving input (Producer mode).
type TableFunction interface {
	// Name returns the function name used in SQL.
	Name() string
	// Metadata returns descriptive metadata.
	Metadata() FunctionMetadata
	// ArgumentSpecs returns the function's argument specifications.
	ArgumentSpecs() []ArgSpec
	// OnBind resolves the output schema given the bind parameters.
	OnBind(params *BindParams) (*BindResponse, error)
	// OnInit performs one-time initialization and returns execution parameters.
	OnInit(params *InitParams) (*GlobalInitResponse, error)
	// NewState creates the initial mutable state for this function execution.
	NewState(params *ProcessParams) (interface{}, error)
	// Process generates the next output batch. It must either emit data via
	// out.Emit/EmitArrays or call out.Finish() to signal end-of-stream.
	Process(ctx context.Context, params *ProcessParams, state interface{}, out *vgirpc.OutputCollector) error
}

// TableFunctionWithCardinality extends TableFunction with cardinality estimation.
type TableFunctionWithCardinality interface {
	TableFunction
	// Cardinality returns an estimated row count for query optimization.
	Cardinality(params *BindParams) (*TableCardinality, error)
}

// TableFunctionWithStatistics extends TableFunction with per-column statistics
// that help the optimizer fold or skip filters before running the scan.
type TableFunctionWithStatistics interface {
	TableFunction
	// Statistics returns per-output-column stats (empty slice or nil = unknown).
	Statistics(params *BindParams) ([]ColumnStatistics, error)
}
