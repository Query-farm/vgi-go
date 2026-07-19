// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// BindResult creates a BindResponse with a single "result" column of the given type.
func BindResult(outputType arrow.DataType) (*BindResponse, error) {
	return &BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: outputType},
		}, nil),
	}, nil
}

// BindResultFromInput derives the output type from a single input schema field,
// applying promoteFn to determine the result type. fieldIndex must be >= 0.
func BindResultFromInput(params *BindParams, fieldIndex int, defaultType arrow.DataType, promoteFn func(arrow.DataType) arrow.DataType) (*BindResponse, error) {
	var inputType arrow.DataType
	if fieldIndex >= 0 && params.InputSchema != nil && params.InputSchema.NumFields() > fieldIndex {
		inputType = params.InputSchema.Field(fieldIndex).Type
	}
	if inputType == nil {
		inputType = defaultType
	}
	return BindResult(promoteFn(inputType))
}

// BindResultFromInputs derives the output type from multiple input schema fields,
// applying combineFn to determine the result type. All fieldIndices must be >= 0.
func BindResultFromInputs(params *BindParams, fieldIndices []int, defaultType arrow.DataType, combineFn func([]arrow.DataType) arrow.DataType) (*BindResponse, error) {
	types := make([]arrow.DataType, len(fieldIndices))
	for i, idx := range fieldIndices {
		if idx >= 0 && params.InputSchema != nil && params.InputSchema.NumFields() > idx {
			types[i] = params.InputSchema.Field(idx).Type
		}
		if types[i] == nil {
			types[i] = defaultType
		}
	}
	return BindResult(combineFn(types))
}

// GetInt64Value extracts an int64 from any integer column type.
// For uint64 values exceeding math.MaxInt64, the result wraps to negative.
func GetInt64Value(col arrow.Array, i int) int64 {
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(i)
	case *array.Int32:
		return int64(c.Value(i))
	case *array.Int16:
		return int64(c.Value(i))
	case *array.Int8:
		return int64(c.Value(i))
	case *array.Uint64:
		return int64(c.Value(i))
	case *array.Uint32:
		return int64(c.Value(i))
	case *array.Uint16:
		return int64(c.Value(i))
	case *array.Uint8:
		return int64(c.Value(i))
	default:
		return 0
	}
}

// GetFloat64Value extracts a float64 from any numeric column type.
// Large int64/uint64 values may lose precision when converted to float64.
func GetFloat64Value(col arrow.Array, i int) float64 {
	switch c := col.(type) {
	case *array.Float64:
		return c.Value(i)
	case *array.Float32:
		return float64(c.Value(i))
	case *array.Int64:
		return float64(c.Value(i))
	case *array.Int32:
		return float64(c.Value(i))
	case *array.Int16:
		return float64(c.Value(i))
	case *array.Int8:
		return float64(c.Value(i))
	case *array.Uint64:
		return float64(c.Value(i))
	case *array.Uint32:
		return float64(c.Value(i))
	case *array.Uint16:
		return float64(c.Value(i))
	case *array.Uint8:
		return float64(c.Value(i))
	case *array.Decimal128:
		dt := c.DataType().(*arrow.Decimal128Type)
		return c.Value(i).ToFloat64(dt.Scale)
	case *array.Decimal256:
		dt := c.DataType().(*arrow.Decimal256Type)
		return c.Value(i).ToFloat64(dt.Scale)
	default:
		return 0
	}
}

// GetStringValue extracts a string from a String or Dictionary column.
func GetStringValue(col arrow.Array, i int) string {
	switch c := col.(type) {
	case *array.String:
		return c.Value(i)
	case *array.Dictionary:
		dict := c.Dictionary().(*array.String)
		return dict.Value(c.GetValueIndex(i))
	default:
		return ""
	}
}

// Int64Accessor resolves the concrete column type once and returns a per-row
// accessor over it. It is the hoisted form of GetInt64Value: call it once
// before a per-row loop and invoke the returned closure inside the loop, so the
// type switch runs once per column instead of once per row (and the closure
// closes over the concrete array, letting the compiler inline Value(i)).
// Unsupported column types yield an accessor that returns 0, mirroring
// GetInt64Value's default.
func Int64Accessor(col arrow.Array) func(i int) int64 {
	switch c := col.(type) {
	case *array.Int64:
		return c.Value
	case *array.Int32:
		return func(i int) int64 { return int64(c.Value(i)) }
	case *array.Int16:
		return func(i int) int64 { return int64(c.Value(i)) }
	case *array.Int8:
		return func(i int) int64 { return int64(c.Value(i)) }
	case *array.Uint64:
		return func(i int) int64 { return int64(c.Value(i)) }
	case *array.Uint32:
		return func(i int) int64 { return int64(c.Value(i)) }
	case *array.Uint16:
		return func(i int) int64 { return int64(c.Value(i)) }
	case *array.Uint8:
		return func(i int) int64 { return int64(c.Value(i)) }
	default:
		return func(int) int64 { return 0 }
	}
}

// Float64Accessor is the hoisted form of GetFloat64Value (see Int64Accessor).
// For decimal columns it also captures the scale once, avoiding the per-row
// DataType() assertion that GetFloat64Value repeats.
func Float64Accessor(col arrow.Array) func(i int) float64 {
	switch c := col.(type) {
	case *array.Float64:
		return c.Value
	case *array.Float32:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Int64:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Int32:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Int16:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Int8:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Uint64:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Uint32:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Uint16:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Uint8:
		return func(i int) float64 { return float64(c.Value(i)) }
	case *array.Decimal128:
		scale := c.DataType().(*arrow.Decimal128Type).Scale
		return func(i int) float64 { return c.Value(i).ToFloat64(scale) }
	case *array.Decimal256:
		scale := c.DataType().(*arrow.Decimal256Type).Scale
		return func(i int) float64 { return c.Value(i).ToFloat64(scale) }
	default:
		return func(int) float64 { return 0 }
	}
}

// StringAccessor is the hoisted form of GetStringValue (see Int64Accessor). For
// a dictionary column it captures the decoded dictionary once.
func StringAccessor(col arrow.Array) func(i int) string {
	switch c := col.(type) {
	case *array.String:
		return c.Value
	case *array.Dictionary:
		dict := c.Dictionary().(*array.String)
		return func(i int) string { return dict.Value(c.GetValueIndex(i)) }
	default:
		return func(int) string { return "" }
	}
}

// AsTyped safely casts an arrow.Array to a specific concrete type.
// Returns (zero, false) if the cast fails. Use MustTyped for error-returning variant.
func AsTyped[T any](col arrow.Array) (T, bool) {
	result, ok := any(col).(T)
	return result, ok
}

// MustTyped safely casts an arrow.Array to a specific concrete type,
// returning an error if the cast fails.
func MustTyped[T any](col arrow.Array) (T, error) {
	result, ok := any(col).(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("expected %T, got %T", zero, col)
	}
	return result, nil
}
