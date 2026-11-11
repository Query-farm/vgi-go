// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"errors"
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
		// An IN set implies bounds — [min(values), max(values)] — and a join-key
		// filter IS an IN set once its side batch is resolved. Skipping them meant
		// a worker pruning by range got NOTHING from a join-key pushdown, which is
		// the single most valuable pushdown a scan receives: it read every row and
		// let the engine filter above it, silently, with the pushdown having
		// arrived and been discarded one call short of use.
		if inf, ok := f.(*InFilter); ok && inf.Values != nil {
			lo, hi, found := arrayBoundsScalar(inf.Values)
			if !found {
				continue
			}
			if minVal == nil || scalarGreaterEqual(lo, minVal) {
				// Deliberately NOT tightened: an IN set's min is the tightest
				// lower bound already, and it is inclusive by construction.
				minVal, minInc = lo, true
			}
			if maxVal == nil || scalarLessEqual(hi, maxVal) {
				maxVal, maxInc = hi, true
			}
			continue
		}
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

// arrayBoundsScalar returns the (min, max) of an array as scalars, skipping
// nulls. Used to derive range bounds from an IN / join-key value set.
func arrayBoundsScalar(arr arrow.Array) (scalar.Scalar, scalar.Scalar, bool) {
	var lo, hi scalar.Scalar
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			continue
		}
		v, err := scalar.GetScalar(arr, i)
		if err != nil {
			continue
		}
		if lo == nil || scalarLess(v, lo) {
			lo = v
		}
		if hi == nil || scalarGreater(v, hi) {
			hi = v
		}
	}
	return lo, hi, lo != nil && hi != nil
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

// ErrFilterNotTranslatable is returned (wrapped) by [PushdownFilters.ToSQLExact]
// when a pushed filter has no exact SQL rendering. Test for it with errors.Is.
var ErrFilterNotTranslatable = errors.New("vgi: filter has no exact SQL translation")

// ToSQL converts filters to a SQL WHERE clause with parameters.
// The quoteIdentifier function is used to quote column names (default: double quotes).
// The placeholder is the parameter placeholder style ("?", "%s", etc.).
// Returns the clause (excluding "WHERE" keyword) and parameter values.
//
// Deprecated: ToSQL WIDENS any filter it cannot translate to "1=1" (or, under
// NOT, drops the whole negation to "1=1"). DuckDB removes every predicate it
// pushes into a table function from its own plan and never re-checks the rows
// the function returns, so a worker that builds its query from ToSQL silently
// returns rows the query excludes whenever a filter is untranslatable. Use
// [PushdownFilters.ToSQLExact], which returns an error naming the
// untranslatable predicate instead, or set FunctionMetadata.AutoApplyFilters so
// the SDK re-applies the filters to the rows the worker emits. ToSQL is kept
// for source compatibility; for every filter ToSQLExact accepts, the two return
// the same clause and parameters.
func (pf *PushdownFilters) ToSQL(quoteIdentifier func(string) string, placeholder string) (string, []interface{}) {
	sql, params, _ := pf.toSQL(quoteIdentifier, placeholder, true)
	return sql, params
}

// ToSQLExact converts filters to a SQL WHERE clause (excluding the "WHERE"
// keyword) with parameter values, exactly: the clause keeps precisely the rows
// the pushed predicates keep, or ToSQLExact returns an error. It never widens
// a required predicate.
//
// An advisory predicate (Filter Encoding v2 mode "advisory", e.g. the
// `geom && <constant>` DuckDB derives from ST_Intersects) is optional: DuckDB
// keeps an exact residual for it, so a worker may ignore it. ToSQLExact
// includes an advisory predicate only when it translates exactly and otherwise
// omits it from the clause rather than failing; if every predicate is omitted
// the clause is "". Only an untranslatable required predicate is an error.
//
// quoteIdentifier quotes column names (nil means Go-style double quotes) and
// placeholder is the parameter marker repeated for each value ("?", "%s", ...).
// An empty filter set yields ("", nil, nil).
//
// Translated shapes: comparisons of a column with a literal, IS [NOT] NULL,
// IN and NOT IN over a literal or resolved join-key set (including DuckDB's
// `not(in(...))` form and `in` with negated=true), boolean-column predicates,
// AND / OR of translatable children, NOT of a translatable child, and struct
// field filters. Any other predicate — e.g. an opaque v2 expression such as a
// function call, arithmetic, a cast, or a column-to-column comparison — makes
// ToSQLExact return an error wrapping [ErrFilterNotTranslatable] that names the
// predicate, if it is required. The caller should then fall back to filtering the rows itself
// (FunctionMetadata.AutoApplyFilters, or [PushdownFilters.Apply]) and may still
// push a weaker clause on its own responsibility.
//
// NULL semantics follow standard SQL three-valued logic, which DuckDB also
// implements: `col NOT IN (a, NULL)` keeps no rows, a NULL column value never
// satisfies IN / NOT IN over a non-empty set, and NOT of a NULL is NULL. A NULL
// set element is passed as a nil parameter, which database/sql binds as SQL
// NULL. An empty set renders as the constant "1=0" (IN) or "1=1" (NOT IN),
// matching DuckDB, rather than the non-portable `col IN ()`.
//
// What ToSQLExact cannot guarantee is that the target database compares values
// the way DuckDB does: collation and case sensitivity, implicit type coercion
// (e.g. SQLite type affinity), NaN ordering, and timestamp/time-zone handling
// are properties of the target engine. Where those differ, pair ToSQLExact with
// AutoApplyFilters.
func (pf *PushdownFilters) ToSQLExact(quoteIdentifier func(string) string, placeholder string) (string, []any, error) {
	return pf.toSQL(quoteIdentifier, placeholder, false)
}

func (pf *PushdownFilters) toSQL(quoteIdentifier func(string) string, placeholder string, lenient bool) (string, []any, error) {
	if pf == nil || len(pf.Filters) == 0 {
		return "", nil, nil
	}

	if quoteIdentifier == nil {
		quoteIdentifier = func(s string) string { return fmt.Sprintf("%q", s) }
	}

	var conditions []string
	var params []any

	// pf.Filters projects pf.v2Predicates one-to-one; a hand-built filter set
	// has no predicate modes and every filter in it is treated as required.
	modes := len(pf.v2Predicates) == len(pf.Filters)
	for i, f := range pf.Filters {
		if modes && pf.v2Predicates[i].Mode == "advisory" {
			// Optional: keep it only if it translates exactly, in ToSQL too,
			// so the two agree on every clause ToSQLExact returns.
			sql, ps, err := filterToSQL(f, quoteIdentifier, placeholder, false)
			if err == nil {
				conditions = append(conditions, sql)
				params = append(params, ps...)
			}
			continue
		}
		sql, ps, err := filterToSQL(f, quoteIdentifier, placeholder, lenient)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, sql)
		params = append(params, ps...)
	}

	return strings.Join(conditions, " AND "), params, nil
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

// filterToSQL renders one filter. With lenient=false an untranslatable filter
// is an error wrapping ErrFilterNotTranslatable. With lenient=true (the
// deprecated ToSQL behaviour) it widens to "1=1" instead. Widening is only
// monotone outside NOT — under NOT "1=1" would become FALSE and drop every
// row — so a NOT whose child is untranslatable widens as a whole.
func filterToSQL(f Filter, quote func(string) string, placeholder string, lenient bool) (string, []any, error) {
	switch ft := f.(type) {
	case *ConstantFilter:
		return fmt.Sprintf("%s %s %s", quote(f.ColumnName()), ft.Op.Symbol(), placeholder), []any{scalarToGo(ft.Value)}, nil

	case *IsNullFilter:
		return fmt.Sprintf("%s IS NULL", quote(f.ColumnName())), nil, nil

	case *IsNotNullFilter:
		return fmt.Sprintf("%s IS NOT NULL", quote(f.ColumnName())), nil, nil

	case *InFilter:
		if ft.Values == nil {
			break
		}
		sql, params := inListSQL(quote(f.ColumnName()), ft.Values, false, placeholder)
		return sql, params, nil

	case *AndFilter, *OrFilter:
		var children []Filter
		joiner := " AND "
		if af, ok := ft.(*AndFilter); ok {
			children = af.Children
		} else {
			children = ft.(*OrFilter).Children
			joiner = " OR "
		}
		parts := make([]string, 0, len(children))
		var params []any
		for _, child := range children {
			sql, ps, err := filterToSQL(child, quote, placeholder, lenient)
			if err != nil {
				return "", nil, err
			}
			parts = append(parts, sql)
			params = append(params, ps...)
		}
		return fmt.Sprintf("(%s)", strings.Join(parts, joiner)), params, nil

	case *StructFilter:
		nestedCol := fmt.Sprintf("%s.%s", f.ColumnName(), ft.ChildName)
		return filterToSQL(ft.ChildFilter, func(_ string) string { return quote(nestedCol) }, placeholder, lenient)

	case *v2FilterView:
		if sql, params, ok := v2ViewToSQL(ft.expr, quote, placeholder); ok {
			return sql, params, nil
		}
	}
	if lenient {
		return "1=1", nil, nil
	}
	return "", nil, fmt.Errorf("%w: %s", ErrFilterNotTranslatable, describeFilter(f))
}

// v2ViewToSQL renders the v2 shapes publicV2Filter deliberately leaves as an
// opaque view because no ergonomic Filter type expresses them — today, the
// negations. It reports ok=false for anything it cannot render exactly. The
// child of a NOT is always rendered strictly: there is no exact negation of a
// widened clause.
func v2ViewToSQL(e *v2Expr, quote func(string) string, placeholder string) (string, []any, bool) {
	if e == nil {
		return "", nil, false
	}
	switch e.Node {
	case "in":
		// publicV2Filter projects only the positive form onto InFilter.
		if e.Negated && e.Expression != nil && e.Expression.Node == "column_ref" && e.Set != nil && e.Set.Values != nil {
			sql, params := inListSQL(quote(e.Expression.ColumnName), e.Set.Values, true, placeholder)
			return sql, params, true
		}
	case "not":
		if e.Expression == nil {
			return "", nil, false
		}
		child := publicV2Filter(e.Expression)
		// NOT (col IN set) is col NOT IN set under three-valued logic; render
		// the idiomatic form. NOT (col NOT IN set) is the positive IN.
		if in, ok := child.(*InFilter); ok && in.Values != nil {
			sql, params := inListSQL(quote(in.ColumnName()), in.Values, true, placeholder)
			return sql, params, true
		}
		if view, ok := child.(*v2FilterView); ok && view.expr != nil && view.expr.Node == "in" && view.expr.Negated &&
			view.expr.Expression != nil && view.expr.Expression.Node == "column_ref" && view.expr.Set != nil && view.expr.Set.Values != nil {
			sql, params := inListSQL(quote(view.expr.Expression.ColumnName), view.expr.Set.Values, false, placeholder)
			return sql, params, true
		}
		sql, params, err := filterToSQL(child, quote, placeholder, false)
		if err != nil {
			return "", nil, false
		}
		return "NOT (" + sql + ")", params, true
	}
	return "", nil, false
}

// inListSQL renders col [NOT] IN (?, ...). An empty set is a constant, as in
// DuckDB: IN () keeps nothing and NOT IN () keeps everything, NULLs included.
func inListSQL(col string, values arrow.Array, negated bool, placeholder string) (string, []any) {
	if values.Len() == 0 {
		if negated {
			return "1=1", nil
		}
		return "1=0", nil
	}
	placeholders := make([]string, values.Len())
	for i := range placeholders {
		placeholders[i] = placeholder
	}
	op := "IN"
	if negated {
		op = "NOT IN"
	}
	return fmt.Sprintf("%s %s (%s)", col, op, strings.Join(placeholders, ", ")), arrayToGoSlice(values)
}

// describeFilter names a filter for an error message: its Repr, plus the v2
// expression shape for an opaque v2 view (whose Repr is only its type name).
func describeFilter(f Filter) string {
	if view, ok := f.(*v2FilterView); ok {
		return fmt.Sprintf("%s v2 expression %s", reprFilter(f), describeV2Expr(view.expr))
	}
	return reprFilter(f)
}

// describeV2Expr renders the node structure of a v2 expression, e.g.
// not(in(column_ref(region), set[2])). Values are summarised, not printed.
func describeV2Expr(e *v2Expr) string {
	if e == nil {
		return "<nil>"
	}
	var args []string
	switch e.Node {
	case "column_ref":
		return "column_ref(" + e.ColumnName + ")"
	case "field_ref":
		return "field_ref(" + describeV2Expr(e.Expression) + "." + e.FieldName + ")"
	case "literal":
		return "literal"
	case "call":
		args = append(args, e.Function)
		for _, a := range e.Arguments {
			args = append(args, describeV2Expr(a))
		}
	default:
		if e.Op != "" {
			args = append(args, e.Op)
		}
		for _, child := range []*v2Expr{e.Expression, e.Left, e.Right} {
			if child != nil {
				args = append(args, describeV2Expr(child))
			}
		}
		for _, child := range e.Children {
			args = append(args, describeV2Expr(child))
		}
		if e.Set != nil && e.Set.Values != nil {
			args = append(args, fmt.Sprintf("set[%d]", e.Set.Values.Len()))
		}
		if e.Negated {
			args = append(args, "negated")
		}
	}
	return e.Node + "(" + strings.Join(args, ", ") + ")"
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
