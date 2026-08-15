// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The accessors used to answer 0 (or "") for a column outside their contract.
// A function advertising bound=numeric, returning a fixed int64 and reading its
// column with GetInt64Value then accepted a DOUBLE argument, produced a column
// of zeros, and reported nothing. These tests pin the loud behaviour.

func float64Col(t *testing.T, vals ...float64) arrow.Array {
	t.Helper()
	b := array.NewFloat64Builder(memory.NewGoAllocator())
	defer b.Release()
	b.AppendValues(vals, nil)
	return b.NewArray()
}

func int64Col(t *testing.T, vals ...int64) arrow.Array {
	t.Helper()
	b := array.NewInt64Builder(memory.NewGoAllocator())
	defer b.Release()
	b.AppendValues(vals, nil)
	return b.NewArray()
}

func TestGetInt64ValuePanicsOnFloatColumn(t *testing.T) {
	col := float64Col(t, 21.5)
	defer col.Release()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic for a float64 column, got none")
		}
		err, ok := r.(*UnsupportedColumnTypeError)
		if !ok {
			t.Fatalf("expected *UnsupportedColumnTypeError, got %T", r)
		}
		if err.Helper != "GetInt64Value" {
			t.Errorf("Helper = %q, want GetInt64Value", err.Helper)
		}
		if !strings.Contains(err.Error(), "float64") {
			t.Errorf("message should name the column type, got %q", err.Error())
		}
		if !strings.Contains(err.Error(), "NumericDispatch") {
			t.Errorf("message should point at the way out, got %q", err.Error())
		}
	}()
	_ = GetInt64Value(col, 0)
}

func TestGetStringValuePanicsOnNonStringColumn(t *testing.T) {
	col := int64Col(t, 1)
	defer col.Release()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic for an int64 column, got none")
		} else if _, ok := r.(*UnsupportedColumnTypeError); !ok {
			t.Fatalf("expected *UnsupportedColumnTypeError, got %T", r)
		}
	}()
	_ = GetStringValue(col, 0)
}

func TestAccessorsPanicOnUnsupportedColumn(t *testing.T) {
	strCol := func() arrow.Array {
		b := array.NewStringBuilder(memory.NewGoAllocator())
		defer b.Release()
		b.Append("x")
		return b.NewArray()
	}()
	defer strCol.Release()

	for _, tc := range []struct {
		name string
		call func()
	}{
		{"Int64Accessor", func() { Int64Accessor(strCol) }},
		{"Float64Accessor", func() { Float64Accessor(strCol) }},
		{"StringAccessor", func() { StringAccessor(int64Col(t, 1)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected a panic, got none")
				} else if _, ok := r.(*UnsupportedColumnTypeError); !ok {
					t.Fatalf("expected *UnsupportedColumnTypeError, got %T", r)
				}
			}()
			tc.call()
		})
	}
}

func TestAccessorsStillWorkInContract(t *testing.T) {
	col := int64Col(t, 21)
	defer col.Release()
	if got := GetInt64Value(col, 0); got != 21 {
		t.Errorf("GetInt64Value = %d, want 21", got)
	}
	// Floats are in GetFloat64Value's contract, ints included.
	if got := GetFloat64Value(col, 0); got != 21 {
		t.Errorf("GetFloat64Value = %v, want 21", got)
	}
	f := float64Col(t, 1.5)
	defer f.Release()
	if got := GetFloat64Value(f, 0); got != 1.5 {
		t.Errorf("GetFloat64Value = %v, want 1.5", got)
	}
}

func TestMapColumnReturnsUnsupportedColumnAsError(t *testing.T) {
	// The point of the recovery: the documented pattern surfaces this as an
	// error the engine reports, not as a dead worker process.
	col := float64Col(t, 21.5)
	defer col.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{col}, 1)
	defer batch.Release()

	params := &ProcessParams{
		OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int64}}, nil),
	}

	_, err := MapColumn(params, batch, 0, array.NewInt64Builder,
		func(c arrow.Array, i int) int64 { return GetInt64Value(c, i) * 2 })
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var ucte *UnsupportedColumnTypeError
	if !errors.As(err, &ucte) {
		t.Fatalf("expected *UnsupportedColumnTypeError, got %T: %v", err, err)
	}
}

func TestRecoverUnsupportedColumnTypeRepanicsOtherValues(t *testing.T) {
	// A genuine bug in a transform must not be swallowed.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected the original panic to propagate")
		} else if s, ok := r.(string); !ok || s != "boom" {
			t.Fatalf("expected the original panic value, got %#v", r)
		}
	}()
	func() (err error) {
		defer RecoverUnsupportedColumnType(&err)
		panic("boom")
	}()
}
