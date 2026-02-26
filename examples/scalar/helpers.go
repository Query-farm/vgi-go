// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// promoteForAddition returns an appropriate output type for addition.
func promoteForAddition(dt arrow.DataType) arrow.DataType {
	switch dt.ID() {
	case arrow.INT8:
		return arrow.PrimitiveTypes.Int16
	case arrow.INT16:
		return arrow.PrimitiveTypes.Int32
	case arrow.INT32, arrow.INT64:
		return arrow.PrimitiveTypes.Int64
	case arrow.UINT8:
		return arrow.PrimitiveTypes.Uint16
	case arrow.UINT16:
		return arrow.PrimitiveTypes.Uint32
	case arrow.UINT32, arrow.UINT64:
		return arrow.PrimitiveTypes.Uint64
	case arrow.FLOAT32, arrow.FLOAT64:
		return dt
	default:
		return dt
	}
}

// getInt64Value extracts an int64 from any integer column type.
func getInt64Value(col arrow.Array, i int) int64 {
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

// getFloat64Value extracts a float64 from any numeric column type.
func getFloat64Value(col arrow.Array, i int) float64 {
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
	default:
		return 0
	}
}

// commonTypeForAddition determines the output type when adding two types.
func commonTypeForAddition(dt1, dt2 arrow.DataType) arrow.DataType {
	if isFloatingType(dt1) || isFloatingType(dt2) {
		return arrow.PrimitiveTypes.Float64
	}
	wider := dt1
	if typeSize(dt2) > typeSize(dt1) {
		wider = dt2
	}
	return promoteForAddition(wider)
}

// typeSize returns the byte size of a numeric type for comparison.
func typeSize(dt arrow.DataType) int {
	switch dt.ID() {
	case arrow.INT8, arrow.UINT8:
		return 1
	case arrow.INT16, arrow.UINT16:
		return 2
	case arrow.INT32, arrow.UINT32, arrow.FLOAT32:
		return 4
	case arrow.INT64, arrow.UINT64, arrow.FLOAT64:
		return 8
	default:
		return 0
	}
}

// isNumericType checks if an Arrow type is numeric.
func isNumericType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

// isIntegerType checks if an Arrow type is an integer type.
func isIntegerType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	default:
		return false
	}
}

// numericBuilder is a constraint for Arrow typed builders.
type numericBuilder[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// buildArray constructs an Arrow array by applying a transform to each non-null row.
// Null rows in col are propagated as nulls in the output.
func buildArray[T any, B numericBuilder[T]](builder B, col arrow.Array, n int, transform func(int) T) arrow.Array {
	defer builder.Release()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(transform(i))
		}
	}
	return builder.NewArray()
}

// isFloatingType checks if an Arrow type is a floating point type.
func isFloatingType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}
