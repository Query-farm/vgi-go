// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// supportedFilterVersion is the filter protocol version we handle.
const supportedFilterVersion = "1"

// ---------------------------------------------------------------------------
// Filter type and comparison operator enums
// ---------------------------------------------------------------------------

// FilterType identifies the kind of pushdown filter.
type FilterType string

const (
	FilterConstant  FilterType = "constant"
	FilterIsNull    FilterType = "is_null"
	FilterIsNotNull FilterType = "is_not_null"
	FilterIn        FilterType = "in"
	FilterAnd       FilterType = "and"
	FilterOr        FilterType = "or"
	FilterStruct    FilterType = "struct"
)

// ComparisonOp identifies a comparison operator used in constant filters.
type ComparisonOp string

const (
	OpEQ ComparisonOp = "eq"
	OpNE ComparisonOp = "ne"
	OpGT ComparisonOp = "gt"
	OpGE ComparisonOp = "ge"
	OpLT ComparisonOp = "lt"
	OpLE ComparisonOp = "le"
)

// Symbol returns the SQL operator string for this comparison op.
func (op ComparisonOp) Symbol() string {
	switch op {
	case OpEQ:
		return "="
	case OpNE:
		return "!="
	case OpGT:
		return ">"
	case OpGE:
		return ">="
	case OpLT:
		return "<"
	case OpLE:
		return "<="
	default:
		return string(op)
	}
}

// computeFuncName returns the arrow compute function name for this operator.
func (op ComparisonOp) computeFuncName() string {
	switch op {
	case OpEQ:
		return "equal"
	case OpNE:
		return "not_equal"
	case OpGT:
		return "greater"
	case OpGE:
		return "greater_equal"
	case OpLT:
		return "less"
	case OpLE:
		return "less_equal"
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// Filter interface
// ---------------------------------------------------------------------------

// Filter is the interface all pushdown filter types implement.
type Filter interface {
	// ColumnName returns the name of the column this filter applies to.
	ColumnName() string
	// ColumnIndex returns the index of the column in the output schema.
	ColumnIndex() int
	// Type returns the filter type identifier.
	Type() FilterType
	// Evaluate evaluates the filter against a record batch, returning a
	// boolean array where true indicates the row passes the filter.
	Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error)
}

// ---------------------------------------------------------------------------
// ConstantFilter — column <op> value
// ---------------------------------------------------------------------------

// ConstantFilter compares a column against a constant value.
type ConstantFilter struct {
	columnName  string
	columnIndex int
	Op          ComparisonOp
	Value       scalar.Scalar
}

func (f *ConstantFilter) ColumnName() string { return f.columnName }
func (f *ConstantFilter) ColumnIndex() int   { return f.columnIndex }
func (f *ConstantFilter) Type() FilterType   { return FilterConstant }

func (f *ConstantFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	col := batch.Column(f.columnIndex)
	funcName := f.Op.computeFuncName()
	if funcName == "" {
		return nil, fmt.Errorf("unknown comparison operator: %s", f.Op)
	}

	colDatum := compute.NewDatum(col)
	scalarDatum := compute.NewDatum(f.Value)
	defer colDatum.Release()
	defer scalarDatum.Release()

	result, err := compute.CallFunction(ctx, funcName, nil, colDatum, scalarDatum)
	if err != nil {
		return nil, fmt.Errorf("evaluating %s filter on column %q: %w", funcName, f.columnName, err)
	}
	defer result.Release()

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// ---------------------------------------------------------------------------
// IsNullFilter — column IS NULL
// ---------------------------------------------------------------------------

// IsNullFilter checks whether a column value is null.
type IsNullFilter struct {
	columnName  string
	columnIndex int
}

func (f *IsNullFilter) ColumnName() string { return f.columnName }
func (f *IsNullFilter) ColumnIndex() int   { return f.columnIndex }
func (f *IsNullFilter) Type() FilterType   { return FilterIsNull }

func (f *IsNullFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	col := batch.Column(f.columnIndex)
	colDatum := compute.NewDatum(col)
	defer colDatum.Release()

	result, err := compute.CallFunction(ctx, "is_null", nil, colDatum)
	if err != nil {
		return nil, fmt.Errorf("evaluating is_null on column %q: %w", f.columnName, err)
	}
	defer result.Release()

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// ---------------------------------------------------------------------------
// IsNotNullFilter — column IS NOT NULL
// ---------------------------------------------------------------------------

// IsNotNullFilter checks whether a column value is not null.
type IsNotNullFilter struct {
	columnName  string
	columnIndex int
}

func (f *IsNotNullFilter) ColumnName() string { return f.columnName }
func (f *IsNotNullFilter) ColumnIndex() int   { return f.columnIndex }
func (f *IsNotNullFilter) Type() FilterType   { return FilterIsNotNull }

func (f *IsNotNullFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	col := batch.Column(f.columnIndex)
	colDatum := compute.NewDatum(col)
	defer colDatum.Release()

	// "is_valid" in arrow compute is the equivalent of IS NOT NULL
	result, err := compute.CallFunction(ctx, "is_valid", nil, colDatum)
	if err != nil {
		return nil, fmt.Errorf("evaluating is_valid on column %q: %w", f.columnName, err)
	}
	defer result.Release()

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// ---------------------------------------------------------------------------
// InFilter — column IN (v1, v2, ...)
// ---------------------------------------------------------------------------

// InFilter checks whether a column value is in a set of values.
type InFilter struct {
	columnName  string
	columnIndex int
	Values      arrow.Array
}

func (f *InFilter) ColumnName() string { return f.columnName }
func (f *InFilter) ColumnIndex() int   { return f.columnIndex }
func (f *InFilter) Type() FilterType   { return FilterIn }

func (f *InFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	col := batch.Column(f.columnIndex)
	colDatum := compute.NewDatum(col)
	defer colDatum.Release()

	valueSetDatum := compute.NewDatum(f.Values)
	defer valueSetDatum.Release()

	result, err := compute.IsIn(ctx, compute.SetOptions{ValueSet: valueSetDatum}, colDatum)
	if err != nil {
		return nil, fmt.Errorf("evaluating is_in on column %q: %w", f.columnName, err)
	}
	defer result.Release()

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// ---------------------------------------------------------------------------
// AndFilter — conjunction of child filters
// ---------------------------------------------------------------------------

// AndFilter is a conjunction of child filters. All children must pass.
type AndFilter struct {
	columnName  string
	columnIndex int
	Children    []Filter
}

func (f *AndFilter) ColumnName() string { return f.columnName }
func (f *AndFilter) ColumnIndex() int   { return f.columnIndex }
func (f *AndFilter) Type() FilterType   { return FilterAnd }

func (f *AndFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	if len(f.Children) == 0 {
		return makeBoolArray(true, int(batch.NumRows())), nil
	}

	result, err := f.Children[0].Evaluate(ctx, batch)
	if err != nil {
		return nil, err
	}

	for _, child := range f.Children[1:] {
		childResult, err := child.Evaluate(ctx, batch)
		if err != nil {
			result.Release()
			return nil, err
		}

		lhs := compute.NewDatum(result)
		rhs := compute.NewDatum(childResult)
		combined, err := compute.CallFunction(ctx, "and_kleene", nil, lhs, rhs)
		lhs.Release()
		rhs.Release()
		result.Release()
		childResult.Release()
		if err != nil {
			return nil, fmt.Errorf("evaluating and_kleene: %w", err)
		}
		result = combined.(*compute.ArrayDatum).MakeArray()
		combined.Release()
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// OrFilter — disjunction of child filters
// ---------------------------------------------------------------------------

// OrFilter is a disjunction of child filters. At least one child must pass.
type OrFilter struct {
	columnName  string
	columnIndex int
	Children    []Filter
}

func (f *OrFilter) ColumnName() string { return f.columnName }
func (f *OrFilter) ColumnIndex() int   { return f.columnIndex }
func (f *OrFilter) Type() FilterType   { return FilterOr }

func (f *OrFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	if len(f.Children) == 0 {
		return makeBoolArray(false, int(batch.NumRows())), nil
	}

	result, err := f.Children[0].Evaluate(ctx, batch)
	if err != nil {
		return nil, err
	}

	for _, child := range f.Children[1:] {
		childResult, err := child.Evaluate(ctx, batch)
		if err != nil {
			result.Release()
			return nil, err
		}

		lhs := compute.NewDatum(result)
		rhs := compute.NewDatum(childResult)
		combined, err := compute.CallFunction(ctx, "or_kleene", nil, lhs, rhs)
		lhs.Release()
		rhs.Release()
		result.Release()
		childResult.Release()
		if err != nil {
			return nil, fmt.Errorf("evaluating or_kleene: %w", err)
		}
		result = combined.(*compute.ArrayDatum).MakeArray()
		combined.Release()
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// StructFilter — nested struct field filter
// ---------------------------------------------------------------------------

// StructFilter filters on a nested field within a struct column.
type StructFilter struct {
	columnName  string
	columnIndex int
	ChildIndex  int
	ChildName   string
	ChildFilter Filter
}

func (f *StructFilter) ColumnName() string { return f.columnName }
func (f *StructFilter) ColumnIndex() int   { return f.columnIndex }
func (f *StructFilter) Type() FilterType   { return FilterStruct }

func (f *StructFilter) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	structCol, ok := batch.Column(f.columnIndex).(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("column %q (index %d) is not a struct", f.columnName, f.columnIndex)
	}

	fieldArr := structCol.Field(f.ChildIndex)

	// Create a single-column record batch wrapping the struct field
	fieldSchema := arrow.NewSchema([]arrow.Field{
		{Name: f.ChildName, Type: fieldArr.DataType()},
	}, nil)
	fieldBatch := array.NewRecordBatch(fieldSchema, []arrow.Array{fieldArr}, int64(fieldArr.Len()))
	defer fieldBatch.Release()

	// Evaluate child filter with column_index=0 against the single-column batch
	adjustedChild := withColumnIndex(f.ChildFilter, 0)
	return adjustedChild.Evaluate(ctx, fieldBatch)
}

// ---------------------------------------------------------------------------
// PushdownFilters container
// ---------------------------------------------------------------------------

// PushdownFilters holds the deserialized pushdown filters for a function call.
type PushdownFilters struct {
	Filters []Filter
	Version string
}

// Evaluate evaluates all filters against the batch, returning a boolean mask.
// Filters are combined with AND at the top level.
func (pf *PushdownFilters) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	if len(pf.Filters) == 0 {
		return makeBoolArray(true, int(batch.NumRows())), nil
	}

	result, err := pf.Filters[0].Evaluate(ctx, batch)
	if err != nil {
		return nil, err
	}

	for _, f := range pf.Filters[1:] {
		childResult, err := f.Evaluate(ctx, batch)
		if err != nil {
			result.Release()
			return nil, err
		}

		lhs := compute.NewDatum(result)
		rhs := compute.NewDatum(childResult)
		combined, err := compute.CallFunction(ctx, "and_kleene", nil, lhs, rhs)
		lhs.Release()
		rhs.Release()
		result.Release()
		childResult.Release()
		if err != nil {
			return nil, fmt.Errorf("evaluating and_kleene for top-level filter combination: %w", err)
		}
		result = combined.(*compute.ArrayDatum).MakeArray()
		combined.Release()
	}
	return result, nil
}

// Apply applies all filters to the batch, returning a filtered batch.
func (pf *PushdownFilters) Apply(ctx context.Context, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mask, err := pf.Evaluate(ctx, batch)
	if err != nil {
		return nil, err
	}
	defer mask.Release()

	filtered, err := compute.FilterRecordBatch(ctx, batch, mask, compute.DefaultFilterOptions())
	if err != nil {
		return nil, fmt.Errorf("filtering record batch: %w", err)
	}
	return filtered, nil
}

// FilteredColumns returns the set of column names that have filters applied.
func (pf *PushdownFilters) FilteredColumns() map[string]struct{} {
	result := make(map[string]struct{})
	for _, f := range pf.Filters {
		result[f.ColumnName()] = struct{}{}
	}
	return result
}

// HasFilterForColumn returns true if any filter constrains the given column.
func (pf *PushdownFilters) HasFilterForColumn(name string) bool {
	for _, f := range pf.Filters {
		if f.ColumnName() == name {
			return true
		}
	}
	return false
}

// GetColumnFilters returns all top-level filters for a specific column.
func (pf *PushdownFilters) GetColumnFilters(name string) []Filter {
	var result []Filter
	for _, f := range pf.Filters {
		if f.ColumnName() == name {
			result = append(result, f)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Deserialization
// ---------------------------------------------------------------------------

// DeserializeFilters deserializes a pushdown filters record batch into a
// PushdownFilters container with a typed filter AST.
func DeserializeFilters(batch arrow.RecordBatch) (*PushdownFilters, error) {
	if batch.NumCols() == 0 {
		return nil, fmt.Errorf("filter batch has no columns")
	}

	// Check version from field 0 metadata
	field0 := batch.Schema().Field(0)
	version := ""
	if field0.Metadata.Len() > 0 {
		idx := field0.Metadata.FindKey("vgi_filter_version")
		if idx >= 0 {
			version = field0.Metadata.Values()[idx]
		}
	}
	if version != supportedFilterVersion {
		return nil, fmt.Errorf("unsupported filter version: %q (expected %q)", version, supportedFilterVersion)
	}

	// Parse JSON specs from column 0, row 0
	jsonCol, ok := batch.Column(0).(*array.String)
	if !ok {
		return nil, fmt.Errorf("filter column 0 is not a string column")
	}
	if jsonCol.Len() == 0 {
		return nil, fmt.Errorf("filter column 0 is empty")
	}

	var specs []filterSpec
	if err := json.Unmarshal([]byte(jsonCol.Value(0)), &specs); err != nil {
		return nil, fmt.Errorf("parsing filter JSON: %w", err)
	}

	// Value resolver: value_ref N → scalar from column N+1
	getValue := func(ref int) (scalar.Scalar, error) {
		colIdx := ref + 1
		if colIdx >= int(batch.NumCols()) {
			return nil, fmt.Errorf("value_ref %d out of range (batch has %d columns)", ref, batch.NumCols())
		}
		col := batch.Column(colIdx)
		if col.Len() == 0 {
			return nil, fmt.Errorf("value column %d is empty", colIdx)
		}
		return scalar.GetScalar(col, 0)
	}

	filters := make([]Filter, len(specs))
	for i, spec := range specs {
		f, err := parseFilter(spec, getValue)
		if err != nil {
			return nil, fmt.Errorf("parsing filter %d: %w", i, err)
		}
		filters[i] = f
	}

	return &PushdownFilters{
		Filters: filters,
		Version: version,
	}, nil
}

// ---------------------------------------------------------------------------
// JSON filter spec types
// ---------------------------------------------------------------------------

type filterSpec struct {
	Type        string       `json:"type"`
	ColumnName  string       `json:"column_name"`
	ColumnIndex int          `json:"column_index"`
	Op          string       `json:"op,omitempty"`
	ValueRef    *int         `json:"value_ref,omitempty"`
	Children    []filterSpec `json:"children,omitempty"`
	ChildIndex  int          `json:"child_index,omitempty"`
	ChildName   string       `json:"child_name,omitempty"`
	ChildFilter *filterSpec  `json:"child_filter,omitempty"`
}

func parseFilter(spec filterSpec, getValue func(int) (scalar.Scalar, error)) (Filter, error) {
	switch FilterType(spec.Type) {
	case FilterConstant:
		if spec.ValueRef == nil {
			return nil, fmt.Errorf("constant filter missing value_ref")
		}
		val, err := getValue(*spec.ValueRef)
		if err != nil {
			return nil, fmt.Errorf("resolving value_ref %d: %w", *spec.ValueRef, err)
		}
		return &ConstantFilter{
			columnName:  spec.ColumnName,
			columnIndex: spec.ColumnIndex,
			Op:          ComparisonOp(spec.Op),
			Value:       val,
		}, nil

	case FilterIsNull:
		return &IsNullFilter{
			columnName:  spec.ColumnName,
			columnIndex: spec.ColumnIndex,
		}, nil

	case FilterIsNotNull:
		return &IsNotNullFilter{
			columnName:  spec.ColumnName,
			columnIndex: spec.ColumnIndex,
		}, nil

	case FilterIn:
		if spec.ValueRef == nil {
			return nil, fmt.Errorf("in filter missing value_ref")
		}
		listScalar, err := getValue(*spec.ValueRef)
		if err != nil {
			return nil, fmt.Errorf("resolving value_ref %d for IN filter: %w", *spec.ValueRef, err)
		}
		ls, ok := listScalar.(scalar.ListScalar)
		if !ok {
			return nil, fmt.Errorf("IN filter value_ref %d is not a list scalar (got %T)", *spec.ValueRef, listScalar)
		}
		values := ls.GetList()
		values.Retain()
		return &InFilter{
			columnName:  spec.ColumnName,
			columnIndex: spec.ColumnIndex,
			Values:      values,
		}, nil

	case FilterAnd:
		children := make([]Filter, len(spec.Children))
		for i, childSpec := range spec.Children {
			child, err := parseFilter(childSpec, getValue)
			if err != nil {
				return nil, fmt.Errorf("parsing AND child %d: %w", i, err)
			}
			children[i] = child
		}
		return &AndFilter{
			columnName:  spec.ColumnName,
			columnIndex: spec.ColumnIndex,
			Children:    children,
		}, nil

	case FilterOr:
		children := make([]Filter, len(spec.Children))
		for i, childSpec := range spec.Children {
			child, err := parseFilter(childSpec, getValue)
			if err != nil {
				return nil, fmt.Errorf("parsing OR child %d: %w", i, err)
			}
			children[i] = child
		}
		return &OrFilter{
			columnName:  spec.ColumnName,
			columnIndex: spec.ColumnIndex,
			Children:    children,
		}, nil

	case FilterStruct:
		if spec.ChildFilter == nil {
			return nil, fmt.Errorf("struct filter missing child_filter")
		}
		childFilter, err := parseFilter(*spec.ChildFilter, getValue)
		if err != nil {
			return nil, fmt.Errorf("parsing struct child filter: %w", err)
		}
		return &StructFilter{
			columnName:  spec.ColumnName,
			columnIndex: spec.ColumnIndex,
			ChildIndex:  spec.ChildIndex,
			ChildName:   spec.ChildName,
			ChildFilter: childFilter,
		}, nil

	default:
		return nil, fmt.Errorf("unknown filter type: %q", spec.Type)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeBoolArray creates a boolean array of constant value.
func makeBoolArray(value bool, length int) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	for i := 0; i < length; i++ {
		b.Append(value)
	}
	return b.NewArray()
}

// withColumnIndex returns a new filter with the column index adjusted.
// This is used by StructFilter to evaluate child filters at index 0.
func withColumnIndex(f Filter, idx int) Filter {
	switch ft := f.(type) {
	case *ConstantFilter:
		return &ConstantFilter{columnName: ft.columnName, columnIndex: idx, Op: ft.Op, Value: ft.Value}
	case *IsNullFilter:
		return &IsNullFilter{columnName: ft.columnName, columnIndex: idx}
	case *IsNotNullFilter:
		return &IsNotNullFilter{columnName: ft.columnName, columnIndex: idx}
	case *InFilter:
		return &InFilter{columnName: ft.columnName, columnIndex: idx, Values: ft.Values}
	case *AndFilter:
		children := make([]Filter, len(ft.Children))
		for i, c := range ft.Children {
			children[i] = withColumnIndex(c, idx)
		}
		return &AndFilter{columnName: ft.columnName, columnIndex: idx, Children: children}
	case *OrFilter:
		children := make([]Filter, len(ft.Children))
		for i, c := range ft.Children {
			children[i] = withColumnIndex(c, idx)
		}
		return &OrFilter{columnName: ft.columnName, columnIndex: idx, Children: children}
	case *StructFilter:
		return &StructFilter{columnName: ft.columnName, columnIndex: idx, ChildIndex: ft.ChildIndex, ChildName: ft.ChildName, ChildFilter: ft.ChildFilter}
	default:
		return f
	}
}
