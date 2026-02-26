// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// TableInOutFunction is the interface for table-in-out VGI functions.
// Table-in-out functions transform input tables, with an INPUT phase (Exchange)
// and an optional FINALIZE phase (Producer).
type TableInOutFunction interface {
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
	// Process transforms one input batch into output. Must emit exactly one
	// output batch via out.Emit.
	Process(ctx context.Context, params *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error
	// Finalize is called after all input batches have been processed.
	// Returns batches to emit during the FINALIZE phase.
	Finalize(ctx context.Context, params *ProcessParams, state interface{}) ([]arrow.RecordBatch, error)
}

// TableInOutFunctionWithCardinality extends TableInOutFunction with cardinality estimation.
type TableInOutFunctionWithCardinality interface {
	TableInOutFunction
	// Cardinality returns an estimated row count for query optimization.
	Cardinality(params *BindParams) (*TableCardinality, error)
}
