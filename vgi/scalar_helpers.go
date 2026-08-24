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
		panic(unsupportedColumn("GetInt64Value", col, numericHint))
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
		panic(unsupportedColumn("GetFloat64Value", col, "GetFloat64Value covers every numeric column; a non-numeric one needs its own accessor"))
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
		panic(unsupportedColumn("GetStringValue", col, "GetStringValue reads String and Dictionary columns"))
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
		panic(unsupportedColumn("Int64Accessor", col, numericHint))
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
		panic(unsupportedColumn("Float64Accessor", col, "Float64Accessor covers every numeric column; a non-numeric one needs its own accessor"))
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
		panic(unsupportedColumn("StringAccessor", col, "StringAccessor reads String and Dictionary columns"))
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

// UnsupportedColumnTypeError is raised by the typed column accessors when they
// are handed a column outside their contract.
//
// The accessors used to return a zero value instead — 0 for the numeric ones,
// "" for the string one. That is the worst possible answer: a function that
// advertises `bound=numeric` and reads its column with GetInt64Value accepts a
// DOUBLE argument, produces a column of zeros, and reports no error anywhere.
// The query succeeds and the numbers are wrong.
//
// It is raised as a panic because the accessors return a bare value with
// nowhere to put an error, and because a column whose type the function did not
// plan for is a programming mistake rather than a data condition — the Go
// convention for exactly this case. The SDK's own per-row entry points
// (MapColumn and friends, NumericDispatch) recover it and return it as an
// error, so the documented patterns surface it cleanly. A hand-rolled loop over
// GetInt64Value gets the panic, and should either handle the type or defer
// RecoverPanic.
type UnsupportedColumnTypeError struct {
	// Helper is the accessor that was called, e.g. "GetInt64Value".
	Helper string
	// Actual is the column type it was handed.
	Actual arrow.DataType
	// Hint names the way out.
	Hint string
}

// Error renders the accessor, the column type it was handed, and the hint.
func (e *UnsupportedColumnTypeError) Error() string {
	actual := "<nil>"
	if e.Actual != nil {
		actual = e.Actual.String()
	}
	msg := fmt.Sprintf("%s: column type %s is not supported", e.Helper, actual)
	if e.Hint != "" {
		msg += " (" + e.Hint + ")"
	}
	return msg
}

// unsupportedColumn builds the panic value for an out-of-contract column.
func unsupportedColumn(helper string, col arrow.Array, hint string) *UnsupportedColumnTypeError {
	var dt arrow.DataType
	if col != nil {
		dt = col.DataType()
	}
	return &UnsupportedColumnTypeError{Helper: helper, Actual: dt, Hint: hint}
}

const numericHint = "for a function that accepts both integers and floats, bind a " +
	"dynamic output type with BindResultFromInput and read the column through " +
	"NumericDispatch, which picks the int or float callback for you"

// RecoverUnsupportedColumnType converts a panicking typed accessor into a
// returned error, and re-panics anything else so genuine bugs still surface.
// Used by the SDK's per-row helpers; exported so a hand-rolled Process can do
// the same:
//
//	func (f *MyFn) Process(...) (err error) {
//	    defer vgi.RecoverUnsupportedColumnType(&err)
//	    ...
//	}
func RecoverUnsupportedColumnType(errOut *error) {
	r := recover()
	if r == nil {
		return
	}
	if e, ok := r.(*UnsupportedColumnTypeError); ok {
		*errOut = e
		return
	}
	panic(r)
}
