// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ColumnStatistics describes optimizer hints for one column. Match the
// Python ColumnStatistics dataclass in vgi-python/vgi/catalog/catalog_interface.py.
//
// Min/Max are stored as Go scalars; SerializeColumnStatistics handles the
// Arrow sparse-union encoding the wire protocol expects.
type ColumnStatistics struct {
	ColumnName    string
	Type          arrow.DataType // Arrow type of Min/Max (required if either is set)
	Min           interface{}    // e.g. int64(1), float64(0.99), "Accounting", nil
	Max           interface{}
	HasNull       bool
	HasNotNull    bool
	DistinctCount int64 // 0 treated as unknown
	// ContainsUnicode and MaxStringLength apply only to string/binary columns.
	ContainsUnicode  *bool
	MaxStringLength  *int64
	distinctCountSet bool // zero-value disambiguator
}

// SetDistinctCount sets the distinct-count estimator (including 0 as a valid value).
func (c *ColumnStatistics) SetDistinctCount(n int64) {
	c.DistinctCount = n
	c.distinctCountSet = true
}

// TableColumnStatisticsResult is the full reply to catalog_table_column_statistics_get.
type TableColumnStatisticsResult struct {
	Statistics []ColumnStatistics
	// CacheMaxAgeSeconds: nil means cache indefinitely; 0 means no cache.
	CacheMaxAgeSeconds *int64
}

// SerializeColumnStatistics encodes per-column stats as the sparse-union IPC
// batch DuckDB's VGI extension expects. See vgi-python's
// catalog_interface.serialize_column_statistics for the reference layout.
func SerializeColumnStatistics(stats []ColumnStatistics, cacheMaxAgeSeconds *int64) ([]byte, error) {
	mem := memory.NewGoAllocator()
	n := len(stats)

	// Collect distinct Arrow types in insertion order.
	typeIndex := map[string]int{}
	typeOrder := []arrow.DataType{}
	rowTypeCodes := make([]int8, n)
	for i, s := range stats {
		t := s.Type
		if t == nil {
			t = arrow.Null
		}
		key := t.String()
		if _, ok := typeIndex[key]; !ok {
			typeIndex[key] = len(typeOrder)
			typeOrder = append(typeOrder, t)
		}
		rowTypeCodes[i] = int8(typeIndex[key])
	}
	if len(typeOrder) == 0 {
		typeOrder = []arrow.DataType{arrow.Null}
	}

	// Per-type child arrays of length n (sparse union requires each child array
	// be the same length as the parent).
	fieldNames := make([]string, len(typeOrder))
	typeCodes := make([]arrow.UnionTypeCode, len(typeOrder))
	minChildren := make([]arrow.Array, len(typeOrder))
	maxChildren := make([]arrow.Array, len(typeOrder))
	for code, t := range typeOrder {
		fieldNames[code] = fmt.Sprintf("%d", code)
		typeCodes[code] = arrow.UnionTypeCode(code)
		minArr, err := buildStatChildArray(mem, t, int8(code), rowTypeCodes, stats, true)
		if err != nil {
			return nil, err
		}
		maxArr, err := buildStatChildArray(mem, t, int8(code), rowTypeCodes, stats, false)
		if err != nil {
			minArr.Release()
			return nil, err
		}
		minChildren[code] = minArr
		maxChildren[code] = maxArr
	}
	defer func() {
		for _, a := range minChildren {
			a.Release()
		}
		for _, a := range maxChildren {
			a.Release()
		}
	}()

	// Construct the union type and its union-codes array.
	unionType := arrow.SparseUnionOf(
		makeUnionFields(typeOrder, fieldNames),
		typeCodes,
	)
	codesArr := buildInt8Array(mem, rowTypeCodes)
	defer codesArr.Release()

	minUnion, err := array.NewSparseUnionFromArraysWithFieldCodes(codesArr, minChildren, fieldNames, typeCodes)
	if err != nil {
		return nil, err
	}
	defer minUnion.Release()
	maxUnion, err := array.NewSparseUnionFromArraysWithFieldCodes(codesArr, maxChildren, fieldNames, typeCodes)
	if err != nil {
		return nil, err
	}
	defer maxUnion.Release()

	_ = unionType

	// Flat columns.
	colNameB := array.NewStringBuilder(mem)
	defer colNameB.Release()
	hasNullB := array.NewBooleanBuilder(mem)
	defer hasNullB.Release()
	hasNotNullB := array.NewBooleanBuilder(mem)
	defer hasNotNullB.Release()
	distinctB := array.NewInt64Builder(mem)
	defer distinctB.Release()
	containsUniB := array.NewBooleanBuilder(mem)
	defer containsUniB.Release()
	maxStrLenB := array.NewUint64Builder(mem)
	defer maxStrLenB.Release()
	for _, s := range stats {
		colNameB.Append(s.ColumnName)
		hasNullB.Append(s.HasNull)
		hasNotNullB.Append(s.HasNotNull)
		if s.DistinctCount > 0 || s.distinctCountSet {
			distinctB.Append(s.DistinctCount)
		} else {
			distinctB.AppendNull()
		}
		if s.ContainsUnicode == nil {
			containsUniB.AppendNull()
		} else {
			containsUniB.Append(*s.ContainsUnicode)
		}
		if s.MaxStringLength == nil {
			maxStrLenB.AppendNull()
		} else {
			maxStrLenB.Append(uint64(*s.MaxStringLength))
		}
	}

	cols := []arrow.Array{
		colNameB.NewArray(),
		minUnion,
		maxUnion,
		hasNullB.NewArray(),
		hasNotNullB.NewArray(),
		distinctB.NewArray(),
		containsUniB.NewArray(),
		maxStrLenB.NewArray(),
	}
	defer func() {
		// union children already released above; release the flat ones.
		for _, c := range []arrow.Array{cols[0], cols[3], cols[4], cols[5], cols[6], cols[7]} {
			c.Release()
		}
	}()
	// Retain the unions once more so they live past the batch.
	minUnion.Retain()
	maxUnion.Retain()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "column_name", Type: arrow.BinaryTypes.String},
		{Name: "min", Type: unionType},
		{Name: "max", Type: unionType},
		{Name: "has_null", Type: &arrow.BooleanType{}},
		{Name: "has_not_null", Type: &arrow.BooleanType{}},
		{Name: "distinct_count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "contains_unicode", Type: &arrow.BooleanType{}, Nullable: true},
		{Name: "max_string_length", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
	}, nil)

	if cacheMaxAgeSeconds != nil {
		md := arrow.NewMetadata(
			[]string{"cache_max_age_seconds"},
			[]string{fmt.Sprintf("%d", *cacheMaxAgeSeconds)},
		)
		schema = arrow.NewSchema(schema.Fields(), &md)
	}
	batchWithMeta := array.NewRecordBatch(schema, cols, int64(n))
	defer batchWithMeta.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(batchWithMeta); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func makeUnionFields(types []arrow.DataType, names []string) []arrow.Field {
	out := make([]arrow.Field, len(types))
	for i, t := range types {
		out[i] = arrow.Field{Name: names[i], Type: t, Nullable: true}
	}
	return out
}

func buildInt8Array(mem memory.Allocator, data []int8) arrow.Array {
	b := array.NewInt8Builder(mem)
	defer b.Release()
	for _, v := range data {
		b.Append(v)
	}
	return b.NewArray()
}

// buildStatChildArray builds the per-type child array of a sparse union.
// Only rows whose typeCode matches the target are populated with the real
// value; others get null.
func buildStatChildArray(mem memory.Allocator, t arrow.DataType, target int8, codes []int8, stats []ColumnStatistics, wantMin bool) (arrow.Array, error) {
	switch t.ID() {
	case arrow.NULL:
		b := array.NewNullBuilder(mem)
		defer b.Release()
		for range stats {
			b.AppendNull()
		}
		return b.NewArray(), nil
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for i, s := range stats {
			v := pickScalar(s, wantMin)
			if codes[i] != target || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(toInt64(v))
		}
		return b.NewArray(), nil
	case arrow.INT32:
		b := array.NewInt32Builder(mem)
		defer b.Release()
		for i, s := range stats {
			v := pickScalar(s, wantMin)
			if codes[i] != target || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(int32(toInt64(v)))
		}
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for i, s := range stats {
			v := pickScalar(s, wantMin)
			if codes[i] != target || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(toFloat64(v))
		}
		return b.NewArray(), nil
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for i, s := range stats {
			v := pickScalar(s, wantMin)
			if codes[i] != target || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(toString(v))
		}
		return b.NewArray(), nil
	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for i, s := range stats {
			v := pickScalar(s, wantMin)
			if codes[i] != target || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(v.(bool))
		}
		return b.NewArray(), nil
	case arrow.BINARY:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		for i, s := range stats {
			v := pickScalar(s, wantMin)
			if codes[i] != target || v == nil {
				b.AppendNull()
				continue
			}
			switch x := v.(type) {
			case []byte:
				b.Append(x)
			case string:
				b.Append([]byte(x))
			default:
				return nil, fmt.Errorf("stats binary child: unsupported %T", v)
			}
		}
		return b.NewArray(), nil
	}
	return nil, fmt.Errorf("column statistics: unsupported Arrow type %s", t)
}

func pickScalar(s ColumnStatistics, wantMin bool) interface{} {
	if wantMin {
		return s.Min
	}
	return s.Max
}
