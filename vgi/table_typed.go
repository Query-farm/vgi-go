// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
)

// TypedTableFunc is the recommended interface for table functions.
// It provides compile-time type safety for state management, eliminating
// the unsafe state.(*myType) assertions required by the lower-level
// TableFunction interface.
//
// Use the lower-level TableFunction interface only for advanced use cases
// that need non-standard state patterns.
//
// Implementations may also satisfy OnIniter (custom OnInit) and/or
// CardinalityEstimator (cardinality estimation). These are detected
// automatically by AsTableFunction.
type TypedTableFunc[S any] interface {
	Name() string
	Metadata() FunctionMetadata
	ArgumentSpecs() []ArgSpec
	OnBind(params *BindParams) (*BindResponse, error)
	NewState(params *ProcessParams) (*S, error)
	Process(ctx context.Context, params *ProcessParams, state *S, out *vgirpc.OutputCollector) error
}

// OnIniter is an optional interface for TypedTableFunc implementations that
// need custom OnInit behavior (e.g., multi-worker partitioning with work queues).
// If not implemented, the adapter defaults to DefaultInit() (MaxWorkers: 1).
type OnIniter interface {
	OnInit(params *InitParams) (*GlobalInitResponse, error)
}

// CardinalityEstimator is an optional interface for TypedTableFunc
// implementations that can estimate their output row count for query optimization.
type CardinalityEstimator interface {
	Cardinality(params *BindParams) (*TableCardinality, error)
}

// AsTableFunction wraps a TypedTableFunc into a TableFunction for registration
// with Worker.RegisterTable. The adapter:
//   - Provides type-safe state casting (returns error instead of panic)
//   - Defaults OnInit to DefaultInit() (MaxWorkers: 1) unless OnIniter is implemented
//   - Delegates Cardinality if CardinalityEstimator is implemented
//
// Usage:
//
//	func NewSequenceFunction() vgi.TableFunction {
//	    return vgi.AsTableFunction[sequenceState](&SequenceFunction{})
//	}
func AsTableFunction[S any](f TypedTableFunc[S]) TableFunction {
	gob.Register(new(S))
	base := &typedTableAdapter[S]{inner: f}

	if init, ok := any(f).(OnIniter); ok {
		base.onInit = init.OnInit
	}

	if card, ok := any(f).(CardinalityEstimator); ok {
		return &typedTableAdapterWithCard[S]{
			typedTableAdapter: base,
			card:              card,
		}
	}
	return base
}

// typedTableAdapter implements TableFunction by delegating to a TypedTableFunc[S].
type typedTableAdapter[S any] struct {
	inner  TypedTableFunc[S]
	onInit func(*InitParams) (*GlobalInitResponse, error)
}

func (a *typedTableAdapter[S]) Name() string               { return a.inner.Name() }
func (a *typedTableAdapter[S]) Metadata() FunctionMetadata { return a.inner.Metadata() }
func (a *typedTableAdapter[S]) ArgumentSpecs() []ArgSpec   { return a.inner.ArgumentSpecs() }

func (a *typedTableAdapter[S]) OnBind(params *BindParams) (*BindResponse, error) {
	return a.inner.OnBind(params)
}

func (a *typedTableAdapter[S]) OnInit(params *InitParams) (*GlobalInitResponse, error) {
	if a.onInit != nil {
		return a.onInit(params)
	}
	return DefaultInit()
}

func (a *typedTableAdapter[S]) NewState(params *ProcessParams) (interface{}, error) {
	return a.inner.NewState(params)
}

func (a *typedTableAdapter[S]) Process(ctx context.Context, params *ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s, ok := state.(*S)
	if !ok {
		return fmt.Errorf("invalid state type: expected *%T, got %T", new(S), state)
	}
	return a.inner.Process(ctx, params, s, out)
}

// typedTableAdapterWithCard embeds typedTableAdapter and additionally
// implements TableFunctionWithCardinality, so the type assertion in
// protocol.go (fn.(TableFunctionWithCardinality)) succeeds.
type typedTableAdapterWithCard[S any] struct {
	*typedTableAdapter[S]
	card CardinalityEstimator
}

func (a *typedTableAdapterWithCard[S]) Cardinality(params *BindParams) (*TableCardinality, error) {
	return a.card.Cardinality(params)
}
