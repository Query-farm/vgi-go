// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
)

// arrowArrayType is the reflect type of the arrow.Array interface, used to
// detect column-arg struct fields in TypedScalarFunc args.
var arrowArrayType = reflect.TypeOf((*arrow.Array)(nil)).Elem()

// TypedScalarFunc is the declarative variant of ScalarFunction. The argument
// schema is described once by the type parameter A — a struct whose `vgi:"..."`
// tags drive both ArgumentSpecs (via DeriveArgSpecs) and per-call argument
// binding (via BindArgs). Use AsScalarFunction to wrap an implementation for
// registration with Worker.RegisterScalar.
//
// Column arguments (`vgi:"const=false,..."`) are left at zero on the bound
// struct — the function should read column values from the batch directly.
// Constant scalar arguments are populated.
type TypedScalarFunc[A any] interface {
	// Name returns the function name used in SQL.
	Name() string
	// Metadata returns descriptive metadata.
	Metadata() FunctionMetadata
	// OnBindTyped resolves the output schema given the bind parameters and
	// the bound argument struct. args is populated from params.Args via
	// BindArgs; column-arg fields are left at their zero value.
	OnBindTyped(args *A, params *BindParams) (*BindResponse, error)
	// ProcessTyped transforms an input batch into an output batch using the
	// bound argument struct.
	ProcessTyped(ctx context.Context, args *A, params *ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error)
}

// AsScalarFunction wraps a TypedScalarFunc[A] into a ScalarFunction so it can
// be passed to Worker.RegisterScalar. ArgumentSpecs is derived once from A's
// struct tags; OnBind and Process bind A from params.Args on each call.
//
// Column args (fields tagged `const=false`) are auto-populated from the input
// batch at Process time. Two field shapes are supported:
//
//   - arrow.Array — accepts any column type, advertises arrow_type="any" plus
//     whatever `bound=` predicate is declared. This is the polymorphic mode
//     used by add_values, double, sum_values, etc. — output type is computed
//     in OnBindTyped from the actual input.
//
//   - *array.Int64 / *array.String / *array.Float64 / ... — accepts only that
//     concrete fixed type. Advertises the matching arrow_type and lets the
//     body read values without a type assertion. Used by multiply, etc.
//
// Varargs slice fields ([]arrow.Array) consume every remaining batch column.
//
// Nested or parametric column types (struct, list, fixed_list, decimal,
// timestamp, duration) are not in the concrete-type registry because the Go
// pointer doesn't carry the inner shape. Declare those as arrow.Array and
// validate the shape in OnBindTyped — same approach as vgi-python.
func AsScalarFunction[A any](f TypedScalarFunc[A]) ScalarFunction {
	specs := DeriveArgSpecs(*new(A))
	columnBindings, err := columnFieldBindings(reflect.TypeOf((*A)(nil)).Elem())
	if err != nil {
		panic(fmt.Errorf("vgi.AsScalarFunction: %w", err))
	}
	return &typedScalarAdapter[A]{inner: f, specs: specs, columnBindings: columnBindings}
}

type typedScalarAdapter[A any] struct {
	inner          TypedScalarFunc[A]
	specs          []ArgSpec
	columnBindings []columnFieldBinding
}

// columnFieldBinding describes one struct field that maps to an input batch
// column (rather than a const scalar argument).
type columnFieldBinding struct {
	FieldIndex int
	FieldType  reflect.Type
	Varargs    bool // true when the field is []arrow.Array
}

// columnFieldBindings returns the subset of fieldBinding entries that refer
// to column args (IsConst=false) whose Go type the binder can populate:
//   - the arrow.Array interface (catch-all, polymorphic)
//   - any concrete *array.X type that implements arrow.Array (strict)
//   - []arrow.Array for varargs columns
//
// Other column-arg field types are silently ignored — BindArgs leaves them
// at the zero value.
func columnFieldBindings(t reflect.Type) ([]columnFieldBinding, error) {
	bindings, err := parseArgBindings(t)
	if err != nil {
		return nil, err
	}
	var out []columnFieldBinding
	for _, b := range bindings {
		if b.Spec.IsConst {
			continue
		}
		ft := b.Field.Type
		switch {
		case b.Spec.IsVarargs && ft.Kind() == reflect.Slice && ft.Elem() == arrowArrayType:
			out = append(out, columnFieldBinding{FieldIndex: b.FieldIndex, FieldType: ft, Varargs: true})
		case ft == arrowArrayType || ft.Implements(arrowArrayType):
			out = append(out, columnFieldBinding{FieldIndex: b.FieldIndex, FieldType: ft})
		}
	}
	return out, nil
}

// bindColumnArgs populates column-arg fields of args from batch columns in
// declaration order. Field-type-vs-column-type mismatches are impossible if
// ValidateTypeBounds at bind time agrees with the field's declared type;
// any such mismatch panics through reflect.Set and is caught by the dispatch
// recovery in protocol.go, surfacing as a WorkerPanicError.
func (a *typedScalarAdapter[A]) bindColumnArgs(target *A, batch arrow.RecordBatch) {
	if len(a.columnBindings) == 0 || batch == nil {
		return
	}
	v := reflect.ValueOf(target).Elem()
	colIdx := 0
	numCols := int(batch.NumCols())
	for _, b := range a.columnBindings {
		f := v.Field(b.FieldIndex)
		if b.Varargs {
			if colIdx >= numCols {
				f.Set(reflect.Zero(b.FieldType))
				continue
			}
			rest := make([]arrow.Array, 0, numCols-colIdx)
			for ; colIdx < numCols; colIdx++ {
				rest = append(rest, batch.Column(colIdx))
			}
			f.Set(reflect.ValueOf(rest))
			continue
		}
		if colIdx >= numCols {
			colIdx++
			continue
		}
		f.Set(reflect.ValueOf(batch.Column(colIdx)))
		colIdx++
	}
}

func (a *typedScalarAdapter[A]) Name() string               { return a.inner.Name() }
func (a *typedScalarAdapter[A]) Metadata() FunctionMetadata { return a.inner.Metadata() }
func (a *typedScalarAdapter[A]) ArgumentSpecs() []ArgSpec   { return a.specs }

func (a *typedScalarAdapter[A]) OnBind(params *BindParams) (*BindResponse, error) {
	var args A
	if err := BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return a.inner.OnBindTyped(&args, params)
}

func (a *typedScalarAdapter[A]) Process(ctx context.Context, params *ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	var args A
	if err := BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	a.bindColumnArgs(&args, batch)
	return a.inner.ProcessTyped(ctx, &args, params, batch)
}
