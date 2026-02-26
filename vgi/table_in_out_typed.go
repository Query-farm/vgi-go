// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// TypedTableInOutFunc is the recommended interface for table-in-out functions.
// It provides compile-time type safety for state management, eliminating
// the unsafe state.(*myType) assertions required by the lower-level
// TableInOutFunction interface.
//
// Implementations may also satisfy OnIniter (custom OnInit). This is detected
// automatically by AsTableInOutFunction.
type TypedTableInOutFunc[S any] interface {
	Name() string
	Metadata() FunctionMetadata
	ArgumentSpecs() []ArgSpec
	OnBind(params *BindParams) (*BindResponse, error)
	NewState(params *ProcessParams) (*S, error)
	Process(ctx context.Context, params *ProcessParams, state *S,
		batch arrow.RecordBatch, out *vgirpc.OutputCollector) error
	Finalize(ctx context.Context, params *ProcessParams, state *S) ([]arrow.RecordBatch, error)
}

// AsTableInOutFunction wraps a TypedTableInOutFunc into a TableInOutFunction
// for registration with Worker.RegisterTableInOut. The adapter:
//   - Provides type-safe state casting (returns error instead of panic)
//   - Defaults OnInit to DefaultInit() (MaxWorkers: 1) unless OnIniter is implemented
//
// Usage:
//
//	func NewEchoFunction() vgi.TableInOutFunction {
//	    return vgi.AsTableInOutFunction[struct{}](&EchoFunction{})
//	}
func AsTableInOutFunction[S any](f TypedTableInOutFunc[S]) TableInOutFunction {
	base := &typedTableInOutAdapter[S]{inner: f}

	if init, ok := any(f).(OnIniter); ok {
		base.onInit = init.OnInit
	}

	return base
}

// typedTableInOutAdapter implements TableInOutFunction by delegating to a TypedTableInOutFunc[S].
type typedTableInOutAdapter[S any] struct {
	inner  TypedTableInOutFunc[S]
	onInit func(*InitParams) (*GlobalInitResponse, error)
}

func (a *typedTableInOutAdapter[S]) Name() string               { return a.inner.Name() }
func (a *typedTableInOutAdapter[S]) Metadata() FunctionMetadata { return a.inner.Metadata() }
func (a *typedTableInOutAdapter[S]) ArgumentSpecs() []ArgSpec   { return a.inner.ArgumentSpecs() }

func (a *typedTableInOutAdapter[S]) OnBind(params *BindParams) (*BindResponse, error) {
	return a.inner.OnBind(params)
}

func (a *typedTableInOutAdapter[S]) OnInit(params *InitParams) (*GlobalInitResponse, error) {
	if a.onInit != nil {
		return a.onInit(params)
	}
	return DefaultInit()
}

func (a *typedTableInOutAdapter[S]) NewState(params *ProcessParams) (interface{}, error) {
	return a.inner.NewState(params)
}

func (a *typedTableInOutAdapter[S]) Process(ctx context.Context, params *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	s, ok := state.(*S)
	if !ok {
		return fmt.Errorf("invalid state type: expected *%T, got %T", new(S), state)
	}
	return a.inner.Process(ctx, params, s, batch, out)
}

func (a *typedTableInOutAdapter[S]) Finalize(ctx context.Context, params *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	s, ok := state.(*S)
	if !ok {
		return nil, fmt.Errorf("invalid state type: expected *%T, got %T", new(S), state)
	}
	return a.inner.Finalize(ctx, params, s)
}
