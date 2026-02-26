// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import "github.com/apache/arrow-go/v18/arrow"

// DefaultInit returns a standard single-worker GlobalInitResponse (MaxWorkers: 1).
// Use this in OnInit for table functions that don't need parallel execution.
func DefaultInit() (*GlobalInitResponse, error) {
	return &GlobalInitResponse{MaxWorkers: 1}, nil
}

// BindSchema creates a BindResponse from an output schema.
// This is the table-function equivalent of BindResult (which creates a single-column schema).
func BindSchema(schema *arrow.Schema) (*BindResponse, error) {
	return &BindResponse{OutputSchema: schema}, nil
}

// OptionalInt64 extracts a named int64 argument, returning defaultVal if the
// argument is null, not present, or cannot be converted to int64.
// This is a convenience helper that never returns an error — type mismatches
// silently return the default value.
func OptionalInt64(args *Arguments, name string, defaultVal int64) int64 {
	if args.IsNull(name) {
		return defaultVal
	}
	if v, err := args.GetScalarInt64(name); err == nil {
		return v
	}
	return defaultVal
}

// OptionalFloat64 extracts a named float64 argument, returning defaultVal if
// the argument is null, not present, or cannot be converted to float64.
func OptionalFloat64(args *Arguments, name string, defaultVal float64) float64 {
	if args.IsNull(name) {
		return defaultVal
	}
	if v, err := args.GetScalarFloat64(name); err == nil {
		return v
	}
	return defaultVal
}

// OptionalString extracts a named string argument, returning defaultVal if
// the argument is null, not present, or cannot be converted to string.
func OptionalString(args *Arguments, name string, defaultVal string) string {
	if args.IsNull(name) {
		return defaultVal
	}
	if v, err := args.GetScalarString(name); err == nil {
		return v
	}
	return defaultVal
}

// OptionalBool extracts a named bool argument, returning defaultVal if the
// argument is null, not present, or cannot be converted to bool.
func OptionalBool(args *Arguments, name string, defaultVal bool) bool {
	if args.IsNull(name) {
		return defaultVal
	}
	if v, err := args.GetScalarBool(name); err == nil {
		return v
	}
	return defaultVal
}

// BindInputSchema creates a BindResponse that passes through the input schema
// as the output schema. This is the common pattern for table-in-out functions
// that don't transform the schema. InputSchema is expected to be non-nil for
// table-in-out functions.
func BindInputSchema(params *BindParams) (*BindResponse, error) {
	return &BindResponse{OutputSchema: params.InputSchema}, nil
}

// FindColumn returns the column array from a RecordBatch by field name,
// or nil if not found.
func FindColumn(batch arrow.RecordBatch, name string) arrow.Array {
	for i := 0; i < int(batch.NumCols()); i++ {
		if batch.ColumnName(i) == name {
			return batch.Column(i)
		}
	}
	return nil
}

// ColumnSet is a set of column names, typically used for projection pushdown.
type ColumnSet map[string]struct{}

// Contains returns true if the named column is in the set.
func (cs ColumnSet) Contains(name string) bool {
	_, ok := cs[name]
	return ok
}

// ProjectedColumns returns the set of column names that should be generated,
// given projection IDs and the full (unprojected) schema. If projectionIDs
// is nil, all columns are included.
func ProjectedColumns(projectionIDs []int32, fullSchema *arrow.Schema) ColumnSet {
	result := make(ColumnSet)
	if projectionIDs != nil {
		for _, id := range projectionIDs {
			result[fullSchema.Field(int(id)).Name] = struct{}{}
		}
	} else {
		for _, f := range fullSchema.Fields() {
			result[f.Name] = struct{}{}
		}
	}
	return result
}
