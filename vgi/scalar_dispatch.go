// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// NumericDispatch creates a result batch by dispatching to the appropriate
// numeric builder based on the output schema type. For integer output types,
// intFn is called; for floating point types, floatFn is called.
// Null propagation is handled automatically across all input columns.
//
// Supported output types: INT8 through UINT64, FLOAT32, FLOAT64. FLOAT16 is
// not supported. The int64/float64 results from intFn/floatFn are cast to the
// output type; callers are responsible for ensuring values fit (overflow silently
// truncates, matching Go's standard integer conversion behavior).
func NumericDispatch(
	params *ProcessParams,
	batch arrow.RecordBatch,
	intFn func(cols []arrow.Array, i int) int64,
	floatFn func(cols []arrow.Array, i int) float64,
) (arrow.RecordBatch, error) {
	mem := defaultAllocator
	n := int(batch.NumRows())
	numCols := int(batch.NumCols())
	outputType := params.OutputSchema.Field(0).Type

	cols := make([]arrow.Array, numCols)
	for c := 0; c < numCols; c++ {
		cols[c] = batch.Column(c)
	}

	anyNull := func(i int) bool {
		for _, c := range cols {
			if c.IsNull(i) {
				return true
			}
		}
		return false
	}

	var resultArr arrow.Array
	switch outputType.ID() {
	case arrow.INT8:
		resultArr = numericBuild(array.NewInt8Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) int8 { return int8(intFn(cols, i)) })
	case arrow.INT16:
		resultArr = numericBuild(array.NewInt16Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) int16 { return int16(intFn(cols, i)) })
	case arrow.INT32:
		resultArr = numericBuild(array.NewInt32Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) int32 { return int32(intFn(cols, i)) })
	case arrow.INT64:
		resultArr = numericBuild(array.NewInt64Builder(mem), cols, n, anyNull, intFn)
	case arrow.UINT8:
		resultArr = numericBuild(array.NewUint8Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) uint8 { return uint8(intFn(cols, i)) })
	case arrow.UINT16:
		resultArr = numericBuild(array.NewUint16Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) uint16 { return uint16(intFn(cols, i)) })
	case arrow.UINT32:
		resultArr = numericBuild(array.NewUint32Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) uint32 { return uint32(intFn(cols, i)) })
	case arrow.UINT64:
		resultArr = numericBuild(array.NewUint64Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) uint64 { return uint64(intFn(cols, i)) })
	case arrow.FLOAT32:
		resultArr = numericBuild(array.NewFloat32Builder(mem), cols, n, anyNull,
			func(cols []arrow.Array, i int) float32 { return float32(floatFn(cols, i)) })
	case arrow.FLOAT64:
		resultArr = numericBuild(array.NewFloat64Builder(mem), cols, n, anyNull, floatFn)
	case arrow.DECIMAL128:
		// Decimal arithmetic via element-wise add. The user's intFn is
		// designed for int64 outputs; for decimal we replay the same shape:
		//   double(x)   — single decimal column → emit value + value
		//   sum_values  — N decimal columns     → emit sum of all column values
		// (Both are "addition of inputs"; PromoteForAddition is the gatekeeper
		// for which scalar functions land here at all.) Mixed decimal/non-decimal
		// inputs fall back through intFn and are coerced via FromI64.
		decType := outputType.(*arrow.Decimal128Type)
		decBuilder := array.NewDecimal128Builder(mem, decType)
		defer decBuilder.Release()

		// Detect the "double" pattern: one input column whose type matches the
		// output exactly modulo the +1 precision bump we applied. In that case
		// we add the value to itself instead of just appending it once.
		doubleSemantics := false
		if len(cols) == 1 {
			if d, ok := cols[0].DataType().(*arrow.Decimal128Type); ok && d.Scale == decType.Scale {
				doubleSemantics = true
			}
		}

		for i := 0; i < n; i++ {
			if anyNull(i) {
				decBuilder.AppendNull()
				continue
			}
			var sum decimal128.Num
			for _, c := range cols {
				if d, ok := c.(*array.Decimal128); ok {
					sum = sum.Add(d.Value(i))
				} else {
					sum = sum.Add(decimal128.FromI64(intFn(cols, i)))
					break
				}
			}
			if doubleSemantics {
				if d, ok := cols[0].(*array.Decimal128); ok {
					sum = sum.Add(d.Value(i))
				}
			}
			if !sum.FitsInPrecision(decType.Precision) {
				return nil, fmt.Errorf("decimal value does not fit in precision %d", decType.Precision)
			}
			decBuilder.Append(sum)
		}
		resultArr = decBuilder.NewArray()
	default:
		return nil, fmt.Errorf("unsupported numeric output type: %v", outputType)
	}

	defer resultArr.Release()
	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

// numericBuild is a generic helper for NumericDispatch that builds an array
// with null propagation.
func numericBuild[T any, B ArrayBuilder[T]](
	builder B,
	cols []arrow.Array,
	n int,
	anyNull func(int) bool,
	transform func([]arrow.Array, int) T,
) arrow.Array {
	defer builder.Release()
	for i := 0; i < n; i++ {
		if anyNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(transform(cols, i))
		}
	}
	return builder.NewArray()
}
