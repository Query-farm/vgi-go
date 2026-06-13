// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// BatchBuilder: row-oriented record-batch construction
//
// Equivalent to pa.RecordBatch.from_pydict in pyarrow, but row-at-a-time so
// loops over decoded records stay flat:
//
//   b := vgi.NewBatchBuilder(schema)
//   defer b.Release()
//   for _, rec := range records {
//       b.AppendRow(map[string]any{
//           "train_number":    "1234",
//           "planned_time":    plannedTime,            // time.Time
//           "actual_time":     nil,                    // → AppendNull
//           "delay_minutes":   int32(5),
//           "via":             []string{"X", "Y"},     // list<string>
//           "messages":        nil,                    // list column → null
//           "platform_changed": false,
//       })
//   }
//   batch, err := b.Build()
//
// Dispatch is driven by the *schema's* column type, not the Go value type.
// nil values (including missing map keys, typed nil pointers, and untyped nil)
// always produce a NULL. Pointer values are auto-dereferenced; nil pointers
// produce a NULL. Slices map to list columns; lists with primitive element
// types dispatch through the same scalar rules as their column counterparts.
//
// The builder owns the underlying Arrow builders. Call Release() if you
// abandon the batch without Build()ing; Build() releases automatically and
// returns a fully-owned arrow.RecordBatch.
// ---------------------------------------------------------------------------

// BatchBuilder accumulates rows for a single output schema and emits an
// arrow.RecordBatch.
type BatchBuilder struct {
	schema   *arrow.Schema
	mem      memory.Allocator
	columns  []columnBuilder
	rowCount int64
}

// columnBuilder pairs a typed Arrow builder with the field it serves. The
// appender closure encapsulates the type-specific dispatch path.
type columnBuilder struct {
	field    arrow.Field
	builder  array.Builder
	appender func(v any) error
}

// NewBatchBuilder constructs a BatchBuilder for the given schema using the
// default Arrow allocator.
func NewBatchBuilder(schema *arrow.Schema) *BatchBuilder {
	return NewBatchBuilderWithAllocator(schema, memory.NewGoAllocator())
}

// NewBatchBuilderWithAllocator constructs a BatchBuilder with a caller-provided
// memory allocator. Useful when integrating with an existing arena.
func NewBatchBuilderWithAllocator(schema *arrow.Schema, mem memory.Allocator) *BatchBuilder {
	b := &BatchBuilder{schema: schema, mem: mem, columns: make([]columnBuilder, schema.NumFields())}
	for i, f := range schema.Fields() {
		bld := array.NewBuilder(mem, f.Type)
		b.columns[i] = columnBuilder{
			field:    f,
			builder:  bld,
			appender: makeAppender(bld, f.Type),
		}
	}
	return b
}

// Schema returns the output schema.
func (b *BatchBuilder) Schema() *arrow.Schema { return b.schema }

// Rows returns the number of rows appended so far.
func (b *BatchBuilder) Rows() int64 { return b.rowCount }

// AppendRow writes one row from a name-keyed value map. Missing keys, nil
// values, and nil pointers produce NULLs. Returns the first per-column error
// it encounters; the row is left in a partially-appended state if that
// happens (call Release() to discard).
func (b *BatchBuilder) AppendRow(row map[string]any) error {
	for _, col := range b.columns {
		v, ok := row[col.field.Name]
		if !ok || isNil(v) {
			col.builder.AppendNull()
			continue
		}
		if err := col.appender(v); err != nil {
			return fmt.Errorf("column %q: %w", col.field.Name, err)
		}
	}
	b.rowCount++
	return nil
}

// AppendNullRow appends a row of all NULLs. Useful for placeholders.
func (b *BatchBuilder) AppendNullRow() {
	for _, col := range b.columns {
		col.builder.AppendNull()
	}
	b.rowCount++
}

// Build finalizes the accumulated rows into a RecordBatch and releases the
// underlying builders. The returned batch is owned by the caller and must
// be Released by them when no longer needed.
func (b *BatchBuilder) Build() (arrow.RecordBatch, error) {
	cols := make([]arrow.Array, len(b.columns))
	for i, c := range b.columns {
		cols[i] = c.builder.NewArray()
	}
	batch := array.NewRecordBatch(b.schema, cols, b.rowCount)
	// NewRecordBatch retained references; release the local handles.
	for _, c := range cols {
		c.Release()
	}
	for _, c := range b.columns {
		c.builder.Release()
	}
	b.columns = nil
	return batch, nil
}

// Release discards any accumulated state without producing a batch. Safe to
// call after Build() (no-op).
func (b *BatchBuilder) Release() {
	for _, c := range b.columns {
		c.builder.Release()
	}
	b.columns = nil
}

// ---------------------------------------------------------------------------
// Per-column appender construction
// ---------------------------------------------------------------------------

// makeAppender returns a closure that appends a single value to `builder`,
// dispatching on the field's Arrow type. The closure is built once per column
// at construction time so AppendRow's hot path is one map lookup + one virtual
// call per column.
func makeAppender(builder array.Builder, dt arrow.DataType) func(any) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		return func(v any) error {
			x, ok := bbToBool(v)
			if !ok {
				return typeErr(v, "bool")
			}
			b.Append(x)
			return nil
		}
	case *array.Int8Builder:
		return func(v any) error {
			n, ok := bbToInt64(v)
			if !ok {
				return typeErr(v, "int8")
			}
			b.Append(int8(n))
			return nil
		}
	case *array.Int16Builder:
		return func(v any) error {
			n, ok := bbToInt64(v)
			if !ok {
				return typeErr(v, "int16")
			}
			b.Append(int16(n))
			return nil
		}
	case *array.Int32Builder:
		return func(v any) error {
			n, ok := bbToInt64(v)
			if !ok {
				return typeErr(v, "int32")
			}
			b.Append(int32(n))
			return nil
		}
	case *array.Int64Builder:
		return func(v any) error {
			n, ok := bbToInt64(v)
			if !ok {
				return typeErr(v, "int64")
			}
			b.Append(n)
			return nil
		}
	case *array.Uint8Builder:
		return func(v any) error {
			n, ok := bbToUint64(v)
			if !ok {
				return typeErr(v, "uint8")
			}
			b.Append(uint8(n))
			return nil
		}
	case *array.Uint16Builder:
		return func(v any) error {
			n, ok := bbToUint64(v)
			if !ok {
				return typeErr(v, "uint16")
			}
			b.Append(uint16(n))
			return nil
		}
	case *array.Uint32Builder:
		return func(v any) error {
			n, ok := bbToUint64(v)
			if !ok {
				return typeErr(v, "uint32")
			}
			b.Append(uint32(n))
			return nil
		}
	case *array.Uint64Builder:
		return func(v any) error {
			n, ok := bbToUint64(v)
			if !ok {
				return typeErr(v, "uint64")
			}
			b.Append(n)
			return nil
		}
	case *array.Float32Builder:
		return func(v any) error {
			f, ok := bbToFloat64(v)
			if !ok {
				return typeErr(v, "float32")
			}
			b.Append(float32(f))
			return nil
		}
	case *array.Float64Builder:
		return func(v any) error {
			f, ok := bbToFloat64(v)
			if !ok {
				return typeErr(v, "float64")
			}
			b.Append(f)
			return nil
		}
	case *array.StringBuilder:
		return func(v any) error {
			s, ok := bbToString(v)
			if !ok {
				return typeErr(v, "string")
			}
			b.Append(s)
			return nil
		}
	case *array.BinaryBuilder:
		return func(v any) error {
			x, ok := bbToBytes(v)
			if !ok {
				return typeErr(v, "binary")
			}
			b.Append(x)
			return nil
		}
	case *array.TimestampBuilder:
		return func(v any) error {
			t, ok := bbToTime(v)
			if !ok {
				return typeErr(v, "timestamp")
			}
			b.AppendTime(t)
			return nil
		}
	case *array.ListBuilder:
		// List columns: dispatch each element through its own appender so
		// nested primitives reuse the conversion rules.
		elemBuilder := b.ValueBuilder()
		elemAppender := makeAppender(elemBuilder, dt.(*arrow.ListType).Elem())
		return func(v any) error {
			items, ok := bbToSliceAny(v)
			if !ok {
				return typeErr(v, "list")
			}
			b.Append(true)
			for i, e := range items {
				if isNil(e) {
					elemBuilder.AppendNull()
					continue
				}
				if err := elemAppender(e); err != nil {
					return fmt.Errorf("element %d: %w", i, err)
				}
			}
			return nil
		}
	}
	// Fallback: type not specifically supported. Surface a clear error so
	// callers know to extend the builder.
	return func(v any) error {
		return fmt.Errorf("BatchBuilder: unsupported Arrow type %v (value %T); add a case in makeAppender", dt, v)
	}
}

// ---------------------------------------------------------------------------
// Value coercion helpers
// ---------------------------------------------------------------------------

// isNil returns true for untyped nil, nil interface, nil pointer, nil slice,
// nil map, and nil chan/func. Anything else is non-nil — including a non-nil
// pointer to a zero value.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.Interface:
		return rv.IsNil()
	}
	return false
}

// deref dereferences pointer types so toX helpers can accept *string, *int64, etc.
func deref(v any) any {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		return rv.Elem().Interface()
	}
	return v
}

func typeErr(v any, target string) error {
	return fmt.Errorf("cannot convert %T to %s", v, target)
}

func bbToBool(v any) (bool, bool) {
	switch x := deref(v).(type) {
	case bool:
		return x, true
	}
	return false, false
}

func bbToInt64(v any) (int64, bool) {
	switch x := deref(v).(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	case float32:
		return int64(x), true
	case float64:
		return int64(x), true
	}
	return 0, false
}

func bbToUint64(v any) (uint64, bool) {
	switch x := deref(v).(type) {
	case int:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int64:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int32:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case uint:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint64:
		return x, true
	}
	return 0, false
}

func bbToFloat64(v any) (float64, bool) {
	switch x := deref(v).(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}

func bbToString(v any) (string, bool) {
	switch x := deref(v).(type) {
	case string:
		return x, true
	}
	return "", false
}

func bbToBytes(v any) ([]byte, bool) {
	switch x := deref(v).(type) {
	case []byte:
		return x, true
	case string:
		return []byte(x), true
	}
	return nil, false
}

func bbToTime(v any) (time.Time, bool) {
	switch x := deref(v).(type) {
	case time.Time:
		return x, true
	}
	return time.Time{}, false
}

// toSliceAny coerces []T (for any T) and []any to a []any view via reflection.
// Returns ok=false for non-slices.
func bbToSliceAny(v any) ([]any, bool) {
	if s, ok := v.([]any); ok {
		return s, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}
