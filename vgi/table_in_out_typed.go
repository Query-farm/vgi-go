// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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
	gob.Register(new(S))
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

// Name forwards to the wrapped typed function's Name.
func (a *typedTableInOutAdapter[S]) Name() string { return a.inner.Name() }

// Metadata forwards to the wrapped typed function's Metadata.
func (a *typedTableInOutAdapter[S]) Metadata() FunctionMetadata { return a.inner.Metadata() }

// ArgumentSpecs forwards to the wrapped typed function's ArgumentSpecs.
func (a *typedTableInOutAdapter[S]) ArgumentSpecs() []ArgSpec { return a.inner.ArgumentSpecs() }

// OnBind forwards to the wrapped typed function's OnBind.
func (a *typedTableInOutAdapter[S]) OnBind(params *BindParams) (*BindResponse, error) {
	return a.inner.OnBind(params)
}

// OnInit invokes the optional OnIniter hook if present, otherwise returns DefaultInit.
func (a *typedTableInOutAdapter[S]) OnInit(params *InitParams) (*GlobalInitResponse, error) {
	if a.onInit != nil {
		return a.onInit(params)
	}
	return DefaultInit()
}

// NewState forwards to the wrapped typed function's NewState, returning the
// typed state as an untyped interface{}.
func (a *typedTableInOutAdapter[S]) NewState(params *ProcessParams) (interface{}, error) {
	return a.inner.NewState(params)
}

// Process type-asserts the untyped state to *S and forwards to the wrapped
// typed function's Process, returning an error on a state type mismatch.
func (a *typedTableInOutAdapter[S]) Process(ctx context.Context, params *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	s, ok := state.(*S)
	if !ok {
		return fmt.Errorf("invalid state type: expected *%T, got %T", new(S), state)
	}
	return a.inner.Process(ctx, params, s, batch, out)
}

// Finalize type-asserts the untyped state to *S and forwards to the wrapped
// typed function's Finalize, returning an error on a state type mismatch.
func (a *typedTableInOutAdapter[S]) Finalize(ctx context.Context, params *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	s, ok := state.(*S)
	if !ok {
		return nil, fmt.Errorf("invalid state type: expected *%T, got %T", new(S), state)
	}
	return a.inner.Finalize(ctx, params, s)
}
