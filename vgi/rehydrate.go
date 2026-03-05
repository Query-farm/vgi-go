// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow"
)

// gobEncode gob-encodes a value to bytes.
func gobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// gobDecode gob-decodes bytes back into an interface{}.
func gobDecode(data []byte) (interface{}, error) {
	var v interface{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// rehydrateState reconstructs non-serializable fields on a deserialized stream
// state. This is the RehydrateFunc callback for the HTTP server.
func (w *Worker) rehydrateState(state interface{}, method string) error {
	switch s := state.(type) {
	case *ScalarExchangeState:
		return w.rehydrateScalar(s)
	case *TableProducerState:
		return w.rehydrateTableProducer(s)
	case *TableInOutExchangeState:
		return w.rehydrateTableInOut(s)
	case *FinalizeProducerState:
		return w.rehydrateFinalize(s)
	default:
		return fmt.Errorf("unknown state type for rehydration: %T", state)
	}
}

func (w *Worker) rehydrateScalar(s *ScalarExchangeState) error {
	fn, params, err := w.rebuildProcessParams(&s.Recipe)
	if err != nil {
		return err
	}
	scalarFn, ok := fn.(ScalarFunction)
	if !ok {
		return fmt.Errorf("resolved function %q is not a ScalarFunction", s.Recipe.FunctionName)
	}
	s.fn = scalarFn
	s.params = params
	return nil
}

func (w *Worker) rehydrateTableProducer(s *TableProducerState) error {
	fn, params, err := w.rebuildProcessParams(&s.Recipe)
	if err != nil {
		return err
	}
	tableFn, ok := fn.(TableFunction)
	if !ok {
		return fmt.Errorf("resolved function %q is not a TableFunction", s.Recipe.FunctionName)
	}
	s.fn = tableFn
	s.params = params

	// Restore user state from gob bytes
	if len(s.UserStateBytes) > 0 {
		userState, err := gobDecode(s.UserStateBytes)
		if err != nil {
			slog.Debug("rehydrate: gob decode user state failed, calling NewState", "err", err)
			userState, err = tableFn.NewState(params)
			if err != nil {
				return fmt.Errorf("NewState fallback: %w", err)
			}
		}
		s.state = userState
	} else {
		userState, err := tableFn.NewState(params)
		if err != nil {
			return fmt.Errorf("NewState: %w", err)
		}
		s.state = userState
	}

	// Restore auto-apply filters
	if tableFn.Metadata().AutoApplyFilters && params.PushdownFilters != nil {
		parsed, err := DeserializeFilters(params.PushdownFilters)
		if err == nil && len(parsed.Filters) > 0 {
			s.autoApply = parsed
		}
	}

	return nil
}

func (w *Worker) rehydrateTableInOut(s *TableInOutExchangeState) error {
	fn, params, err := w.rebuildProcessParams(&s.Recipe)
	if err != nil {
		return err
	}
	tioFn, ok := fn.(TableInOutFunction)
	if !ok {
		return fmt.Errorf("resolved function %q is not a TableInOutFunction", s.Recipe.FunctionName)
	}
	s.fn = tioFn
	s.params = params

	// Restore user state from gob bytes
	if len(s.UserStateBytes) > 0 {
		userState, err := gobDecode(s.UserStateBytes)
		if err != nil {
			slog.Debug("rehydrate: gob decode user state failed, calling NewState", "err", err)
			userState, err = tioFn.NewState(params)
			if err != nil {
				return fmt.Errorf("NewState fallback: %w", err)
			}
		}
		s.state = userState
	} else {
		userState, err := tioFn.NewState(params)
		if err != nil {
			return fmt.Errorf("NewState: %w", err)
		}
		s.state = userState
	}

	// Restore auto-apply filters
	if tioFn.Metadata().AutoApplyFilters && params.PushdownFilters != nil {
		parsed, err := DeserializeFilters(params.PushdownFilters)
		if err == nil && len(parsed.Filters) > 0 {
			s.autoApply = parsed
		}
	}

	return nil
}

func (w *Worker) rehydrateFinalize(s *FinalizeProducerState) error {
	// Deserialize batches from IPC bytes
	s.batches = make([]arrow.RecordBatch, 0, len(s.BatchIPC))
	for _, data := range s.BatchIPC {
		batch, err := DeserializeRecordBatch(data)
		if err != nil {
			return fmt.Errorf("deserializing finalize batch: %w", err)
		}
		s.batches = append(s.batches, batch)
	}
	return nil
}

// rebuildProcessParams reconstructs ProcessParams from an InitRecipe.
// It also returns the resolved function (via overload resolution) to avoid
// a redundant second resolution by the caller.
func (w *Worker) rebuildProcessParams(recipe *InitRecipe) (interface{}, *ProcessParams, error) {
	// Deserialize the bind call
	bindReq, err := w.deserializeBindRequest(recipe.BindCallIPC)
	if err != nil {
		return nil, nil, fmt.Errorf("deserializing bind_call: %w", err)
	}

	// Parse bind params for argument access
	bindParams, err := w.parseBindRequest(*bindReq)
	if err != nil {
		return nil, nil, err
	}

	// Resolve function with overload-aware resolution
	fn, err := w.resolveFunctionWithOverload(recipe.FunctionName, FunctionType(recipe.FunctionType), bindParams.Args, bindParams.InputSchema)
	if err != nil {
		return nil, nil, err
	}
	bindParams.Args.RemapPositionalArgs(w.getArgSpecs(fn))

	// Parse output schema
	outputSchema, err := DeserializeSchema(recipe.OutputSchemaIPC)
	if err != nil {
		return nil, nil, fmt.Errorf("deserializing output_schema: %w", err)
	}

	// Apply projection
	projectedSchema := ProjectSchema(recipe.ProjectionIDs, outputSchema)
	fnMeta := w.getFunctionMetadata(fn)
	processOutputSchema := projectedSchema
	if !fnMeta.ProjectionPushdown && recipe.ProjectionIDs != nil {
		processOutputSchema = outputSchema
	}

	params := &ProcessParams{
		FunctionName:   recipe.FunctionName,
		FunctionType:   FunctionType(recipe.FunctionType),
		Args:           bindParams.Args,
		OutputSchema:   processOutputSchema,
		ProjectionIDs:  recipe.ProjectionIDs,
		Settings:       bindParams.Settings,
		Secrets:        bindParams.Secrets,
		ExecutionID:    recipe.ExecutionID,
		InitOpaqueData: recipe.InitOpaqueData,
	}

	// Restore pushdown filters
	if len(recipe.PushdownFilterIPC) > 0 {
		batch, err := DeserializeRecordBatch(recipe.PushdownFilterIPC)
		if err == nil {
			params.PushdownFilters = batch
		}
	}

	// Restore storage
	ctx := context.Background()
	storage, err := w.getOrCreateStorage(ctx, recipe.ExecutionID)
	if err != nil {
		return nil, nil, err
	}
	params.Storage = storage

	return fn, params, nil
}
