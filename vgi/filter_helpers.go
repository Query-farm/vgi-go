// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// ColumnBounds represents numeric/comparable bounds for a column
// extracted from comparison filters.
type ColumnBounds struct {
	// MinValue is the minimum bound value, or nil if unbounded below.
	MinValue scalar.Scalar
	// MinInclusive is true if MinValue is inclusive (>=), false if exclusive (>).
	MinInclusive bool
	// MaxValue is the maximum bound value, or nil if unbounded above.
	MaxValue scalar.Scalar
	// MaxInclusive is true if MaxValue is inclusive (<=), false if exclusive (<).
	MaxInclusive bool
}

// GetColumnBounds extracts numeric bounds from comparison filters on the
// named column. Returns nil if no bounds can be determined.
func (pf *PushdownFilters) GetColumnBounds(name string) *ColumnBounds {
	var minVal scalar.Scalar
	minInc := true
	var maxVal scalar.Scalar
	maxInc := true

	for _, f := range pf.collectColumnFilters(name) {
		cf, ok := f.(*ConstantFilter)
		if !ok {
			continue
		}
		switch cf.Op {
		case OpEQ:
			return &ColumnBounds{
				MinValue:     cf.Value,
				MinInclusive: true,
				MaxValue:     cf.Value,
				MaxInclusive: true,
			}
		case OpGT:
			if minVal == nil || scalarGreater(cf.Value, minVal) {
				minVal = cf.Value
				minInc = false
			}
		case OpGE:
			if minVal == nil || scalarGreaterEqual(cf.Value, minVal) {
				minVal = cf.Value
				minInc = true
			}
		case OpLT:
			if maxVal == nil || scalarLess(cf.Value, maxVal) {
				maxVal = cf.Value
				maxInc = false
			}
		case OpLE:
			if maxVal == nil || scalarLessEqual(cf.Value, maxVal) {
				maxVal = cf.Value
				maxInc = true
			}
		}
	}

	if minVal == nil && maxVal == nil {
		return nil
	}
	return &ColumnBounds{
		MinValue:     minVal,
		MinInclusive: minInc,
		MaxValue:     maxVal,
		MaxInclusive: maxInc,
	}
}

// GetColumnConstant returns the constant value if the column has an equality
// filter, or nil if no equality filter exists.
func (pf *PushdownFilters) GetColumnConstant(name string) scalar.Scalar {
	for _, f := range pf.Filters {
		if f.ColumnName() == name {
			if cf, ok := f.(*ConstantFilter); ok && cf.Op == OpEQ {
				return cf.Value
			}
		}
	}
	return nil
}

// GetColumnInValues returns the IN filter values for a column, or nil if
// no IN filter exists.
func (pf *PushdownFilters) GetColumnInValues(name string) arrow.Array {
	for _, f := range pf.Filters {
		if f.ColumnName() == name {
			if inf, ok := f.(*InFilter); ok {
				return inf.Values
			}
		}
	}
	return nil
}

// GetColumnValues returns discrete values a column could have based on
// equality or IN filters. For EQ, wraps the value in a 1-element array.
// Returns nil if no discrete values can be determined.
//
// Descends one level into AndFilter children (via collectColumnFilters),
// consistent with GetColumnBounds: DuckDB pushes `col = v` / `col IN (...)`
// conjoined with derived range bounds as a single AndFilter (e.g. a semi-join
// emits `col IN (...) AND col >= min AND col <= max`). Without the descent the
// discrete-value fast path silently misses those and pruning callers fall back
// to scanning every partition.
//
// An OrFilter resolves to the UNION of its branches, but only when every branch
// pins this column to discrete values; if any branch is a range/IS NULL or
// constrains a different column the set is not enumerable and we return nil.
func (pf *PushdownFilters) GetColumnValues(name string) arrow.Array {
	for _, f := range pf.collectColumnFilters(name) {
		switch ft := f.(type) {
		case *ConstantFilter:
			if ft.Op == OpEQ {
				return scalarToArray(ft.Value)
			}
		case *InFilter:
			return ft.Values
		case *OrFilter:
			if union := orDiscreteValues(ft, name); union != nil {
				return union
			}
		}
	}
	return nil
}

// orDiscreteValues returns the deduplicated union of discrete values for the
// named column across all OR branches, or nil if any branch leaves the column
// unbounded (a range/IS NULL branch, or a branch constraining a different
// column). Unlike the AND case, returning one branch's values would be an
// unsafe subset — a pruning caller would skip the other branches' rows.
// Descends one level only, consistent with collectColumnFilters.
func orDiscreteValues(or *OrFilter, name string) arrow.Array {
	var scalars []scalar.Scalar
	for _, child := range or.Children {
		if child.ColumnName() != name {
			return nil
		}
		switch ct := child.(type) {
		case *ConstantFilter:
			if ct.Op != OpEQ {
				return nil
			}
			scalars = append(scalars, ct.Value)
		case *InFilter:
			for i := 0; i < ct.Values.Len(); i++ {
				s, err := scalar.GetScalar(ct.Values, i)
				if err != nil {
					return nil
				}
				scalars = append(scalars, s)
			}
		default:
			return nil
		}
	}
	if len(scalars) == 0 {
		return nil
	}

	// Deduplicate by Go value, preserving first-seen order for stable output.
	seen := make(map[interface{}]struct{}, len(scalars))
	deduped := make([]scalar.Scalar, 0, len(scalars))
	for _, s := range scalars {
		key := scalarToGo(s)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, s)
	}

	mem := memory.NewGoAllocator()
	b := array.NewBuilder(mem, deduped[0].DataType())
	defer b.Release()
	for _, s := range deduped {
		if err := scalar.Append(b, s); err != nil {
			b.AppendNull()
		}
	}
	return b.NewArray()
}

// Repr returns a Python-style repr of the filter set, matching the vgi-python
// PushdownFilters.__repr__ output. Useful for diagnostic display (e.g., the
// dynamic_filter_echo example) and stable regression testing. Format:
//
//	PushdownFilters([])                              when empty
//	PushdownFilters([Filter1, Filter2, ...])         otherwise
func (pf *PushdownFilters) Repr() string {
	if pf == nil || len(pf.Filters) == 0 {
		return "PushdownFilters([])"
	}
	parts := make([]string, len(pf.Filters))
	for i, f := range pf.Filters {
		parts[i] = reprFilter(f)
	}
	return "PushdownFilters([" + strings.Join(parts, ", ") + "])"
}

func reprFilter(f Filter) string {
	switch ft := f.(type) {
	case *ConstantFilter:
		return fmt.Sprintf("ConstantFilter(%s %s %v)", ft.columnName, ft.Op.Symbol(), scalarToGo(ft.Value))
	case *IsNullFilter:
		return fmt.Sprintf("IsNullFilter(%s IS NULL)", ft.columnName)
	case *IsNotNullFilter:
		return fmt.Sprintf("IsNotNullFilter(%s IS NOT NULL)", ft.columnName)
	case *InFilter:
		vals := arrayToGoSlice(ft.Values)
		n := len(vals)
		var preview string
		if n > 5 {
			head := make([]string, 0, 3)
			for i := 0; i < 3 && i < n; i++ {
				head = append(head, fmt.Sprintf("%v", vals[i]))
			}
			preview = fmt.Sprintf("[%s]...(%d total)", strings.Join(head, ", "), n)
		} else {
			parts := make([]string, n)
			for i, v := range vals {
				parts[i] = fmt.Sprintf("%v", v)
			}
			preview = "[" + strings.Join(parts, ", ") + "]"
		}
		return fmt.Sprintf("InFilter(%s IN %s)", ft.columnName, preview)
	case *AndFilter:
		parts := make([]string, len(ft.Children))
		for i, c := range ft.Children {
			parts[i] = reprFilter(c)
		}
		return "AndFilter(" + strings.Join(parts, " AND ") + ")"
	case *OrFilter:
		parts := make([]string, len(ft.Children))
		for i, c := range ft.Children {
			parts[i] = reprFilter(c)
		}
		return "OrFilter(" + strings.Join(parts, " OR ") + ")"
	case *StructFilter:
		return fmt.Sprintf("StructFilter(%s.%s: %s)", ft.columnName, ft.ChildName, reprFilter(ft.ChildFilter))
	case *ExpressionFilter:
		return fmt.Sprintf("ExpressionFilter(%s)", ft.columnName)
	default:
		return fmt.Sprintf("%T(%s)", f, f.ColumnName())
	}
}

// ToSQL converts filters to a SQL WHERE clause with parameters.
// The quoteIdentifier function is used to quote column names (default: double quotes).
// The placeholder is the parameter placeholder style ("?", "%s", etc.).
// Returns the clause (excluding "WHERE" keyword) and parameter values.
func (pf *PushdownFilters) ToSQL(quoteIdentifier func(string) string, placeholder string) (string, []interface{}) {
	if len(pf.Filters) == 0 {
		return "", nil
	}

	if quoteIdentifier == nil {
		quoteIdentifier = func(s string) string { return fmt.Sprintf("%q", s) }
	}

	var conditions []string
	var params []interface{}

	for _, f := range pf.Filters {
		sql, ps := filterToSQL(f, quoteIdentifier, placeholder)
		conditions = append(conditions, sql)
		params = append(params, ps...)
	}

	return strings.Join(conditions, " AND "), params
}

// collectColumnFilters collects filters for a column from top-level and
// direct AND children.
func (pf *PushdownFilters) collectColumnFilters(name string) []Filter {
	var result []Filter
	for _, f := range pf.Filters {
		if f.ColumnName() == name {
			if af, ok := f.(*AndFilter); ok {
				for _, c := range af.Children {
					if c.ColumnName() == name {
						result = append(result, c)
					}
				}
			} else {
				result = append(result, f)
			}
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// SQL generation helper
// ---------------------------------------------------------------------------

func filterToSQL(f Filter, quote func(string) string, placeholder string) (string, []interface{}) {
	col := quote(f.ColumnName())

	switch ft := f.(type) {
	case *ConstantFilter:
		return fmt.Sprintf("%s %s %s", col, ft.Op.Symbol(), placeholder), []interface{}{scalarToGo(ft.Value)}

	case *IsNullFilter:
		return fmt.Sprintf("%s IS NULL", col), nil

	case *IsNotNullFilter:
		return fmt.Sprintf("%s IS NOT NULL", col), nil

	case *InFilter:
		placeholders := make([]string, ft.Values.Len())
		for i := range placeholders {
			placeholders[i] = placeholder
		}
		vals := arrayToGoSlice(ft.Values)
		return fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", ")), vals

	case *AndFilter:
		var parts []string
		var params []interface{}
		for _, child := range ft.Children {
			sql, ps := filterToSQL(child, quote, placeholder)
			parts = append(parts, sql)
			params = append(params, ps...)
		}
		return fmt.Sprintf("(%s)", strings.Join(parts, " AND ")), params

	case *OrFilter:
		var parts []string
		var params []interface{}
		for _, child := range ft.Children {
			sql, ps := filterToSQL(child, quote, placeholder)
			parts = append(parts, sql)
			params = append(params, ps...)
		}
		return fmt.Sprintf("(%s)", strings.Join(parts, " OR ")), params

	case *StructFilter:
		nestedCol := fmt.Sprintf("%s.%s", f.ColumnName(), ft.ChildName)
		return filterToSQL(ft.ChildFilter, func(_ string) string { return quote(nestedCol) }, placeholder)

	default:
		return "1=1", nil
	}
}

// ---------------------------------------------------------------------------
// Scalar comparison helpers
// ---------------------------------------------------------------------------

func scalarToGo(s scalar.Scalar) interface{} {
	if s == nil || !s.IsValid() {
		return nil
	}
	switch v := s.(type) {
	case *scalar.Int8:
		return v.Value
	case *scalar.Int16:
		return v.Value
	case *scalar.Int32:
		return v.Value
	case *scalar.Int64:
		return v.Value
	case *scalar.Uint8:
		return v.Value
	case *scalar.Uint16:
		return v.Value
	case *scalar.Uint32:
		return v.Value
	case *scalar.Uint64:
		return v.Value
	case *scalar.Float32:
		return v.Value
	case *scalar.Float64:
		return v.Value
	case *scalar.String:
		return string(v.Value.Bytes())
	case *scalar.Boolean:
		return v.Value
	default:
		return s.String()
	}
}

func scalarToFloat64(s scalar.Scalar) (float64, bool) {
	switch v := s.(type) {
	case *scalar.Int8:
		return float64(v.Value), true
	case *scalar.Int16:
		return float64(v.Value), true
	case *scalar.Int32:
		return float64(v.Value), true
	case *scalar.Int64:
		return float64(v.Value), true
	case *scalar.Uint8:
		return float64(v.Value), true
	case *scalar.Uint16:
		return float64(v.Value), true
	case *scalar.Uint32:
		return float64(v.Value), true
	case *scalar.Uint64:
		return float64(v.Value), true
	case *scalar.Float32:
		return float64(v.Value), true
	case *scalar.Float64:
		return v.Value, true
	default:
		return 0, false
	}
}

func scalarGreater(a, b scalar.Scalar) bool {
	av, aok := scalarToFloat64(a)
	bv, bok := scalarToFloat64(b)
	if aok && bok {
		return av > bv
	}
	return false
}

func scalarGreaterEqual(a, b scalar.Scalar) bool {
	av, aok := scalarToFloat64(a)
	bv, bok := scalarToFloat64(b)
	if aok && bok {
		return av >= bv
	}
	return false
}

func scalarLess(a, b scalar.Scalar) bool {
	av, aok := scalarToFloat64(a)
	bv, bok := scalarToFloat64(b)
	if aok && bok {
		return av < bv
	}
	return false
}

func scalarLessEqual(a, b scalar.Scalar) bool {
	av, aok := scalarToFloat64(a)
	bv, bok := scalarToFloat64(b)
	if aok && bok {
		return av <= bv
	}
	return false
}

func scalarToArray(s scalar.Scalar) arrow.Array {
	mem := memory.NewGoAllocator()
	switch v := s.(type) {
	case *scalar.Int64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		b.Append(v.Value)
		return b.NewArray()
	case *scalar.Int32:
		b := array.NewInt32Builder(mem)
		defer b.Release()
		b.Append(v.Value)
		return b.NewArray()
	case *scalar.Float64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		b.Append(v.Value)
		return b.NewArray()
	case *scalar.String:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Append(string(v.Value.Bytes()))
		return b.NewArray()
	case *scalar.Boolean:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		b.Append(v.Value)
		return b.NewArray()
	default:
		// Fallback: build from the general builder
		b := array.NewBuilder(mem, s.DataType())
		defer b.Release()
		if err := scalar.Append(b, s); err != nil {
			b.AppendNull()
		}
		return b.NewArray()
	}
}

func arrayToGoSlice(arr arrow.Array) []interface{} {
	result := make([]interface{}, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			result[i] = nil
			continue
		}
		s, err := scalar.GetScalar(arr, i)
		if err != nil {
			result[i] = nil
			continue
		}
		result[i] = scalarToGo(s)
	}
	return result
}
