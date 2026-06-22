// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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
	// Name returns the function name used to invoke it in SQL.
	Name() string
	// Metadata returns descriptive metadata for the function.
	Metadata() FunctionMetadata
	// ArgumentSpecs returns the function's argument specifications.
	ArgumentSpecs() []ArgSpec
	// OnBind resolves the output schema and bind state from the bind parameters.
	OnBind(params *BindParams) (*BindResponse, error)
	// NewState creates a fresh, typed per-scan state value.
	NewState(params *ProcessParams) (*S, error)
	// Process generates output rows for one scan step, emitting them via out.
	Process(ctx context.Context, params *ProcessParams, state *S, out *vgirpc.OutputCollector) error
}

// OnIniter is an optional interface for TypedTableFunc implementations that
// need custom OnInit behavior (e.g., multi-worker partitioning with work queues).
// If not implemented, the adapter defaults to DefaultInit() (MaxWorkers: 1).
type OnIniter interface {
	// OnInit performs global initialization (e.g. worker count, partitioning).
	OnInit(params *InitParams) (*GlobalInitResponse, error)
}

// CardinalityEstimator is an optional interface for TypedTableFunc
// implementations that can estimate their output row count for query optimization.
type CardinalityEstimator interface {
	// Cardinality estimates the function's output row count for the optimizer.
	Cardinality(params *BindParams) (*TableCardinality, error)
}

// StatisticsProvider is an optional interface for TypedTableFunc
// implementations that can report per-column output statistics to the
// optimizer (min/max, null-ness, distinct count, string length).
type StatisticsProvider interface {
	// Statistics reports per-column output statistics to the optimizer.
	Statistics(params *BindParams) ([]ColumnStatistics, error)
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
//
// implementsGobEncoder reports whether t (or *t) defines its own gob encoding,
// in which case it manages its own serialization and is exempt from inspection.
func implementsGobEncoder(t reflect.Type) bool {
	gobEncoder := reflect.TypeOf((*gob.GobEncoder)(nil)).Elem()
	return t.Implements(gobEncoder) || reflect.PointerTo(t).Implements(gobEncoder)
}

// validateGobState fails fast (at registration) when the per-scan state type S
// cannot be gob-encoded for HTTP rehydration. gob otherwise surfaces these only
// mid-query, on the first HTTP continuation, with a cryptic message. Two pitfalls
// are caught by walking S's exported, gob-reachable fields:
//
//   - a struct whose fields are all unexported (e.g. `struct{ done bool }`): gob
//     encodes nothing and reports "type ... has no exported fields".
//   - an exported field of a kind gob cannot encode — an interface (the common
//     case being an Arrow `arrow.Record`/`arrow.Array` stashed in state to emit
//     later), a chan, a func, or an unsafe.Pointer. The fix is to store plain
//     serializable Go values (slices, scalars) in state and rebuild the Arrow
//     batch in Process, mirroring the SDK's static_data.go example.
//
// A truly empty `struct{}` is fine, and any type providing its own
// gob.GobEncoder is exempt (at the top level or as a nested field). Mirrors
// vgi-python enforcing serializable state at class-definition time.
func validateGobState[S any]() {
	t := reflect.TypeOf((*S)(nil)).Elem()
	assertGobEncodable(t, t.String(), map[reflect.Type]bool{})
}

// assertGobEncodable panics if t — reachable from a state field via path — holds
// a value gob cannot encode. seen guards against self-referential types.
func assertGobEncodable(t reflect.Type, path string, seen map[reflect.Type]bool) {
	if t == nil || seen[t] {
		return
	}
	seen[t] = true
	// A type that defines its own gob serialization handles itself.
	if implementsGobEncoder(t) {
		return
	}
	switch t.Kind() {
	case reflect.Interface, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		panic(fmt.Sprintf(
			"vgi.AsTableFunction: state field %s has type %s (kind %s), which cannot be "+
				"gob-encoded for HTTP rehydration. Arrow values (arrow.Record/arrow.Array) and "+
				"other interface/chan/func fields must not live in table-function state — store "+
				"plain serializable Go values (slices, scalars) and rebuild them in Process (see "+
				"the static_data.go example), or implement gob.GobEncoder/GobDecoder.",
			path, t.String(), t.Kind()))
	case reflect.Ptr, reflect.Slice, reflect.Array:
		assertGobEncodable(t.Elem(), path+"[]", seen)
	case reflect.Map:
		assertGobEncodable(t.Key(), path+".<key>", seen)
		assertGobEncodable(t.Elem(), path+".<value>", seen)
	case reflect.Struct:
		if t.NumField() == 0 {
			return // a truly empty struct gob-encodes as nothing
		}
		exported := 0
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue // gob skips unexported fields, so they cannot break encoding
			}
			exported++
			assertGobEncodable(f.Type, path+"."+f.Name, seen)
		}
		if exported == 0 {
			panic(fmt.Sprintf(
				"vgi.AsTableFunction: state type %s has no exported fields and cannot be "+
					"gob-encoded for HTTP rehydration — export its fields (e.g. `Done bool`) or "+
					"implement gob.GobEncoder/GobDecoder.",
				path))
		}
	}
}

func AsTableFunction[S any](f TypedTableFunc[S]) TableFunction {
	validateGobState[S]()
	gob.Register(new(S))
	base := &typedTableAdapter[S]{inner: f}

	if init, ok := any(f).(OnIniter); ok {
		base.onInit = init.OnInit
	}
	if stats, ok := any(f).(StatisticsProvider); ok {
		base.statsProvider = stats
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
	inner         TypedTableFunc[S]
	onInit        func(*InitParams) (*GlobalInitResponse, error)
	statsProvider StatisticsProvider
}

// DynamicToString forwards to the inner typed function when it implements the
// hook. The framework's RPC handler does the interface check on the
// adapter (the user-facing TableFunction); without this forwarder a typed
// function's hook would be invisible.
func (a *typedTableAdapter[S]) DynamicToString(ctx context.Context, params *DynamicToStringParams) ([]string, []string, error) {
	if h, ok := any(a.inner).(DynamicToStringHook); ok {
		return h.DynamicToString(ctx, params)
	}
	return nil, nil, nil
}

// Statistics delegates to the wrapped function's StatisticsProvider if it
// implements one, returning nil otherwise.
func (a *typedTableAdapter[S]) Statistics(params *BindParams) ([]ColumnStatistics, error) {
	if a.statsProvider == nil {
		return nil, nil
	}
	return a.statsProvider.Statistics(params)
}

// Name forwards to the wrapped typed function's Name.
func (a *typedTableAdapter[S]) Name() string { return a.inner.Name() }

// Metadata forwards to the wrapped typed function's Metadata.
func (a *typedTableAdapter[S]) Metadata() FunctionMetadata { return a.inner.Metadata() }

// ArgumentSpecs forwards to the wrapped typed function's ArgumentSpecs.
func (a *typedTableAdapter[S]) ArgumentSpecs() []ArgSpec { return a.inner.ArgumentSpecs() }

// OnBind forwards to the wrapped typed function's OnBind.
func (a *typedTableAdapter[S]) OnBind(params *BindParams) (*BindResponse, error) {
	return a.inner.OnBind(params)
}

// OnInit invokes the optional OnIniter hook if present, otherwise returns DefaultInit.
func (a *typedTableAdapter[S]) OnInit(params *InitParams) (*GlobalInitResponse, error) {
	if a.onInit != nil {
		return a.onInit(params)
	}
	return DefaultInit()
}

// NewState forwards to the wrapped typed function's NewState, returning the
// typed state as an untyped interface{}.
func (a *typedTableAdapter[S]) NewState(params *ProcessParams) (interface{}, error) {
	return a.inner.NewState(params)
}

// Process type-asserts the untyped state to *S and forwards to the wrapped
// typed function's Process, returning an error on a state type mismatch.
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

// Cardinality forwards to the wrapped CardinalityEstimator's Cardinality.
func (a *typedTableAdapterWithCard[S]) Cardinality(params *BindParams) (*TableCardinality, error) {
	return a.card.Cardinality(params)
}
