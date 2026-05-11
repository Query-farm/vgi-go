// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"reflect"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// ---------------------------------------------------------------------------
// Test helpers — build PushdownFilters values directly so we don't have to go
// through the JSON wire format.
// ---------------------------------------------------------------------------

func stringScalar(s string) scalar.Scalar {
	return scalar.NewStringScalar(s)
}

func int64Scalar(n int64) scalar.Scalar {
	return scalar.NewInt64Scalar(n)
}

func stringArray(vs ...string) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for _, v := range vs {
		b.Append(v)
	}
	return b.NewArray()
}

func int64Array(vs ...int64) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, v := range vs {
		b.Append(v)
	}
	return b.NewArray()
}

// constantFilter and inFilter use unexported fields, so build via the
// existing types directly. Note: columnIndex doesn't matter for these helpers.
func eqFilter(column string, v scalar.Scalar) Filter {
	return &ConstantFilter{columnName: column, columnIndex: 0, Op: OpEQ, Value: v}
}

func inFilter(column string, values arrow.Array) Filter {
	return &InFilter{columnName: column, columnIndex: 0, Values: values}
}

func gtFilter(column string, v scalar.Scalar) Filter {
	return &ConstantFilter{columnName: column, columnIndex: 0, Op: OpGT, Value: v}
}

func andFilter(column string, children ...Filter) Filter {
	return &AndFilter{columnName: column, columnIndex: 0, Children: children}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestEqualOrInValues_SingleEq(t *testing.T) {
	pf := &PushdownFilters{Filters: []Filter{eqFilter("station", stringScalar("UT"))}}
	got, ok := EqualOrInValues[string](pf, "station")
	if !ok || !reflect.DeepEqual(got, []string{"UT"}) {
		t.Errorf("single eq: got %v ok=%v", got, ok)
	}
}

func TestEqualOrInValues_InFilter(t *testing.T) {
	arr := stringArray("UT", "ASD", "RTD")
	defer arr.Release()
	pf := &PushdownFilters{Filters: []Filter{inFilter("station", arr)}}
	got, ok := EqualOrInValues[string](pf, "station")
	if !ok {
		t.Fatal("expected ok=true")
	}
	sort.Strings(got)
	want := []string{"ASD", "RTD", "UT"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IN: got %v want %v", got, want)
	}
}

func TestEqualOrInValues_IntColumn(t *testing.T) {
	arr := int64Array(10, 20, 30)
	defer arr.Release()
	pf := &PushdownFilters{Filters: []Filter{inFilter("id", arr)}}
	got, ok := EqualOrInValues[int64](pf, "id")
	if !ok {
		t.Fatal("expected ok=true")
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !reflect.DeepEqual(got, []int64{10, 20, 30}) {
		t.Errorf("got %v", got)
	}
}

func TestEqualOrInValues_AndOfEqs(t *testing.T) {
	pf := &PushdownFilters{Filters: []Filter{
		andFilter("station",
			eqFilter("station", stringScalar("UT")),
			eqFilter("station", stringScalar("ASD")),
		),
	}}
	got, ok := EqualOrInValues[string](pf, "station")
	if !ok {
		t.Fatal("expected ok=true for AND of eqs")
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"ASD", "UT"}) {
		t.Errorf("got %v", got)
	}
}

func TestEqualOrInValues_GtFails(t *testing.T) {
	pf := &PushdownFilters{Filters: []Filter{gtFilter("id", int64Scalar(5))}}
	_, ok := EqualOrInValues[int64](pf, "id")
	if ok {
		t.Errorf("expected ok=false for `id > 5`")
	}
}

func TestEqualOrInValues_OtherColumnFails(t *testing.T) {
	pf := &PushdownFilters{Filters: []Filter{eqFilter("name", stringScalar("X"))}}
	_, ok := EqualOrInValues[string](pf, "station")
	if ok {
		t.Errorf("expected ok=false when target column has no filter")
	}
}

func TestEqualOrInValues_TypeMismatch(t *testing.T) {
	pf := &PushdownFilters{Filters: []Filter{eqFilter("id", int64Scalar(5))}}
	_, ok := EqualOrInValues[string](pf, "id")
	if ok {
		t.Errorf("expected ok=false when scalar type doesn't match T")
	}
}

func TestEqualOrInValues_Dedupe(t *testing.T) {
	arr := stringArray("UT", "ASD", "UT")
	defer arr.Release()
	pf := &PushdownFilters{Filters: []Filter{
		eqFilter("station", stringScalar("UT")),
		inFilter("station", arr),
	}}
	got, ok := EqualOrInValues[string](pf, "station")
	if !ok {
		t.Fatal()
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"ASD", "UT"}) {
		t.Errorf("dedupe: got %v", got)
	}
}

func TestEqualValue(t *testing.T) {
	pf := &PushdownFilters{Filters: []Filter{eqFilter("id", int64Scalar(42))}}
	got, ok := EqualValue[int64](pf, "id")
	if !ok || got != 42 {
		t.Errorf("EqualValue: got %d ok=%v", got, ok)
	}

	// Two filters → not a single eq.
	pf2 := &PushdownFilters{Filters: []Filter{
		eqFilter("id", int64Scalar(1)),
		eqFilter("id", int64Scalar(2)),
	}}
	if _, ok := EqualValue[int64](pf2, "id"); ok {
		t.Errorf("expected ok=false for multi-filter")
	}

	// In-filter → not a single eq.
	arr := int64Array(1, 2)
	defer arr.Release()
	pf3 := &PushdownFilters{Filters: []Filter{inFilter("id", arr)}}
	if _, ok := EqualValue[int64](pf3, "id"); ok {
		t.Errorf("expected ok=false for IN filter")
	}
}

func TestHasOnlyEqualOrInOn(t *testing.T) {
	pf := &PushdownFilters{Filters: []Filter{
		eqFilter("station", stringScalar("UT")),
		eqFilter("category", stringScalar("ic")),
	}}
	if !HasOnlyEqualOrInOn(pf, "station", "category") {
		t.Errorf("should accept eq on listed columns")
	}
	if HasOnlyEqualOrInOn(pf, "station") {
		t.Errorf("should reject filter on unlisted column")
	}

	pf2 := &PushdownFilters{Filters: []Filter{gtFilter("id", int64Scalar(5))}}
	if HasOnlyEqualOrInOn(pf2, "id") {
		t.Errorf("should reject gt filter")
	}
}

func TestEqualOrInValues_NilSafe(t *testing.T) {
	if _, ok := EqualOrInValues[string](nil, "x"); ok {
		t.Errorf("nil PushdownFilters should return ok=false")
	}
	if _, ok := EqualValue[int64](nil, "x"); ok {
		t.Errorf("nil PushdownFilters should return ok=false")
	}
}
