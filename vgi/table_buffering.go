// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
)

// TableBufferingFunction is a sink→source ("table-buffering") VGI function.
// It buffers all input during a sink phase, reshapes it once at end-of-input,
// then streams results during a source phase. Mirrors vgi-python's
// TableBufferingFunction.
//
// Lifecycle (all keyed by the execution_id assigned at sink init):
//   - Process: called once per input batch; persists the batch to
//     execution-scoped storage and returns an opaque state_id.
//   - Combine: called once after all Process calls with every returned
//     state_id; returns the finalize_state_ids that drive the source phase.
//   - Finalize: called once per finalize_state_id; returns the batches to emit
//     for that partition.
//
// Cross-process state MUST live in params.Storage (the sqlite-backed state
// log) — Process and Finalize may run in different worker processes.
type TableBufferingFunction interface {
	// Name returns the function name used in SQL.
	Name() string
	// Metadata returns descriptive metadata (PartitionKind, ordering flags...).
	Metadata() FunctionMetadata
	// ArgumentSpecs returns the function's argument specifications.
	ArgumentSpecs() []ArgSpec
	// OnBind resolves the output schema given the bind parameters.
	OnBind(params *BindParams) (*BindResponse, error)
	// Process buffers one input batch and returns an opaque state_id naming
	// where it was stored.
	Process(ctx context.Context, params *ProcessParams, batch arrow.RecordBatch) ([]byte, error)
	// Combine receives every state_id from every Process call (unordered) and
	// returns the finalize_state_ids the source phase will iterate.
	Combine(ctx context.Context, params *ProcessParams, stateIDs [][]byte) ([][]byte, error)
	// Finalize returns the batches to emit for one finalize_state_id.
	Finalize(ctx context.Context, params *ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error)
}

// TableBufferingFunctionWithCardinality lets a buffering function declare a
// cardinality estimate (e.g. a reducer that always emits one row).
type TableBufferingFunctionWithCardinality interface {
	TableBufferingFunction
	// Cardinality estimates the function's output row count for the optimizer.
	Cardinality(params *BindParams) (*TableCardinality, error)
}

// RegisterTableBuffering registers a table-buffering function.
func (w *Worker) RegisterTableBuffering(f TableBufferingFunction) {
	w.tableBufferings[f.Name()] = append(w.tableBufferings[f.Name()], f)
}

// RegisterTableBufferingForCatalog registers a table-buffering function scoped
// to a single catalog (visible only under that ATTACH). See
// RegisterTableForCatalog for the rationale.
func (w *Worker) RegisterTableBufferingForCatalog(catalogName string, f TableBufferingFunction) {
	w.tableBufferings[f.Name()] = append(w.tableBufferings[f.Name()], f)
	w.catalogFunctionScope[f.Name()] = catalogName
}
