// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import "github.com/apache/arrow-go/v18/arrow"

// PromoteForAddition returns the promoted output type for addition operations.
// Integer types promote to the next wider type; floating point types stay the same.
func PromoteForAddition(dt arrow.DataType) arrow.DataType {
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

// CommonTypeForAddition determines the output type when adding two numeric types.
// If either type is floating point, the result is float64. Otherwise, the wider
// integer type is promoted.
func CommonTypeForAddition(dt1, dt2 arrow.DataType) arrow.DataType {
	if IsFloatingType(dt1) || IsFloatingType(dt2) {
		return arrow.PrimitiveTypes.Float64
	}
	wider := dt1
	if NumericTypeSize(dt2) > NumericTypeSize(dt1) {
		wider = dt2
	}
	return PromoteForAddition(wider)
}

// IsNumericType checks if an Arrow type is numeric (integer or floating point).
func IsNumericType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

// IsIntegerType checks if an Arrow type is an integer type.
func IsIntegerType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	default:
		return false
	}
}

// IsFloatingType checks if an Arrow type is a floating point type.
func IsFloatingType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

// NumericTypeSize returns the byte size of a numeric type for comparison.
func NumericTypeSize(dt arrow.DataType) int {
	switch dt.ID() {
	case arrow.INT8, arrow.UINT8:
		return 1
	case arrow.INT16, arrow.UINT16, arrow.FLOAT16:
		return 2
	case arrow.INT32, arrow.UINT32, arrow.FLOAT32:
		return 4
	case arrow.INT64, arrow.UINT64, arrow.FLOAT64:
		return 8
	default:
		return 0
	}
}
