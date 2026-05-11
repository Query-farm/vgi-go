// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// ---------------------------------------------------------------------------
// High-level pushdown filter accessors
//
// These collapse the "is the filter on this column a single = or IN?" question
// into one call, so workers can take a fast path without writing the
// ConstantFilter / InFilter type-switch by hand.
//
// Mirrors the ergonomics of vgi-python's `filters.get_column_values(col)`.
// ---------------------------------------------------------------------------

// FilterPrimitive constrains the value type that EqualOrInValues / EqualValue
// can extract. Covers the scalar types DuckDB serializes through the pushdown
// filter wire protocol.
type FilterPrimitive interface {
	~string | ~int64 | ~int32 | ~int16 | ~int8 |
		~uint64 | ~uint32 | ~uint16 | ~uint8 |
		~float64 | ~float32 | ~bool
}

// EqualOrInValues returns the values from a single `column = X` ConstantFilter,
// a single `column IN (...)` InFilter, or an AND-conjunction of such filters
// (all on the same column). Returns ok=false if the column has any other
// filter shape (range comparisons, IS NULL/NOT NULL, OR, nested struct, etc.)
// — callers should fall back to PushdownFilters.Apply for those.
//
// The fast path eliminates the per-worker type-switch over Filter.(type) and
// the repeated scalar-to-Go-value conversion code that workers otherwise
// write to support filter pushdown on string / integer key columns.
//
// Returns a deduplicated, order-preserving slice of values.
func EqualOrInValues[T FilterPrimitive](pf *PushdownFilters, column string) ([]T, bool) {
	if pf == nil {
		return nil, false
	}
	filters := pf.GetColumnFilters(column)
	if len(filters) == 0 {
		return nil, false
	}
	seen := make(map[T]struct{})
	out := make([]T, 0)
	for _, f := range filters {
		if !collectEqOrIn(f, &out, seen) {
			return nil, false
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// EqualValue is the single-value variant: returns the value of a single
// `column = X` ConstantFilter, or ok=false otherwise.
func EqualValue[T FilterPrimitive](pf *PushdownFilters, column string) (T, bool) {
	var zero T
	if pf == nil {
		return zero, false
	}
	filters := pf.GetColumnFilters(column)
	if len(filters) != 1 {
		return zero, false
	}
	cf, ok := filters[0].(*ConstantFilter)
	if !ok || cf.Op != OpEQ {
		return zero, false
	}
	v, ok := filterScalarTo[T](cf.Value)
	if !ok {
		return zero, false
	}
	return v, true
}

// HasOnlyEqualOrInOn returns true when every filter in pf is an eq/in (or
// AND-of-those) on one of the named columns. Useful for "either I can fast-
// path everything, or fall back to a slow path entirely" decisions.
func HasOnlyEqualOrInOn(pf *PushdownFilters, columns ...string) bool {
	if pf == nil || len(pf.Filters) == 0 {
		return false
	}
	allowed := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		allowed[c] = struct{}{}
	}
	for _, f := range pf.Filters {
		if !filterIsEqOrInOn(f, allowed) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// internal helpers
// ---------------------------------------------------------------------------

// collectEqOrIn walks one filter, appending typed values to *out. Returns
// false if the filter (or any AND child) is not an eq/in. AND of eq/in is
// treated as the intersection-by-listing; callers usually want a single
// column filter so this composes naturally.
func collectEqOrIn[T FilterPrimitive](f Filter, out *[]T, seen map[T]struct{}) bool {
	switch ft := f.(type) {
	case *ConstantFilter:
		if ft.Op != OpEQ {
			return false
		}
		v, ok := filterScalarTo[T](ft.Value)
		if !ok {
			return false
		}
		if _, dup := seen[v]; !dup {
			seen[v] = struct{}{}
			*out = append(*out, v)
		}
		return true
	case *InFilter:
		vs, ok := filterArrayTo[T](ft.Values)
		if !ok {
			return false
		}
		for _, v := range vs {
			if _, dup := seen[v]; !dup {
				seen[v] = struct{}{}
				*out = append(*out, v)
			}
		}
		return true
	case *AndFilter:
		for _, child := range ft.Children {
			if !collectEqOrIn(child, out, seen) {
				return false
			}
		}
		return true
	}
	return false
}

func filterIsEqOrInOn(f Filter, allowed map[string]struct{}) bool {
	switch ft := f.(type) {
	case *ConstantFilter:
		if _, ok := allowed[ft.ColumnName()]; !ok {
			return false
		}
		return ft.Op == OpEQ
	case *InFilter:
		_, ok := allowed[ft.ColumnName()]
		return ok
	case *AndFilter:
		for _, child := range ft.Children {
			if !filterIsEqOrInOn(child, allowed) {
				return false
			}
		}
		return true
	}
	return false
}

// filterScalarTo converts an Arrow scalar.Scalar to a concrete Go value of
// type T. Returns ok=false for type mismatches or invalid scalars.
func filterScalarTo[T FilterPrimitive](s scalar.Scalar) (T, bool) {
	var zero T
	if s == nil || !s.IsValid() {
		return zero, false
	}
	v, ok := scalarToAny(s)
	if !ok {
		return zero, false
	}
	out, ok := v.(T)
	return out, ok
}

// scalarToAny extracts the underlying Go value from an Arrow scalar, returning
// values typed as the matching Go primitive for the FilterPrimitive set.
func scalarToAny(s scalar.Scalar) (any, bool) {
	switch v := s.(type) {
	case *scalar.String:
		return string(v.Value.Bytes()), true
	case *scalar.LargeString:
		return string(v.Value.Bytes()), true
	case *scalar.Int8:
		return int8(v.Value), true
	case *scalar.Int16:
		return int16(v.Value), true
	case *scalar.Int32:
		return int32(v.Value), true
	case *scalar.Int64:
		return int64(v.Value), true
	case *scalar.Uint8:
		return uint8(v.Value), true
	case *scalar.Uint16:
		return uint16(v.Value), true
	case *scalar.Uint32:
		return uint32(v.Value), true
	case *scalar.Uint64:
		return uint64(v.Value), true
	case *scalar.Float32:
		return float32(v.Value), true
	case *scalar.Float64:
		return float64(v.Value), true
	case *scalar.Boolean:
		return v.Value, true
	}
	return nil, false
}

// filterArrayTo extracts all non-null values from an Arrow array as []T.
// Returns ok=false on type mismatch with the target Go type.
func filterArrayTo[T FilterPrimitive](a arrow.Array) ([]T, bool) {
	if a == nil {
		return nil, false
	}
	out := make([]T, 0, a.Len())
	switch arr := a.(type) {
	case *array.String:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.LargeString:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Int8:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Int16:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Int32:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Int64:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Uint8:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Uint16:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Uint32:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Uint64:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Float32:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Float64:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	case *array.Boolean:
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v, ok := any(arr.Value(i)).(T)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	}
	return nil, false
}
