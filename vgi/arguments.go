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

// Arguments holds parsed function arguments from an Arrow IPC payload.
// Arguments arrive as Arrow IPC bytes with metadata markers on each field:
// vgi_arg (positional index), vgi_type (type category), vgi_const (constant flag),
// vgi_varargs (variadic flag).
type Arguments struct {
	// Positional contains arguments indexed by position.
	Positional []arrow.Array
	// Named contains arguments keyed by name.
	Named map[string]arrow.Array
	// Schema is the original argument schema with VGI metadata.
	Schema *arrow.Schema
	// Batch is the underlying record batch (1 row for scalars).
	Batch arrow.RecordBatch
}

// ParseArguments deserializes Arrow IPC bytes into Arguments.
// The arguments IPC contains ALL declared arguments in the schema. Each field
// may have VGI metadata: vgi_arg="named" for named args, vgi_const="true" for
// constant params. The batch row contains scalar values for const params and
// null/placeholder values for column params.
func ParseArguments(data []byte) (*Arguments, error) {
	if len(data) == 0 {
		return &Arguments{
			Named: make(map[string]arrow.Array),
		}, nil
	}

	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("reading arguments IPC: %w", err)
	}
	defer reader.Release()

	schema := reader.Schema()

	if !reader.Next() {
		// Schema-only (no batch) — valid for functions with no arguments
		return &Arguments{
			Schema: schema,
			Named:  make(map[string]arrow.Array),
		}, nil
	}

	batch := reader.RecordBatch()
	batch.Retain()

	debugLog("ParseArguments: schema fields=%d batch cols=%d rows=%d", schema.NumFields(), batch.NumCols(), batch.NumRows())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		meta := ""
		if f.HasMetadata() {
			meta = fmt.Sprintf(" meta=%v", f.Metadata)
		}
		debugLog("  arg[%d]: name=%q type=%v%s", i, f.Name, f.Type, meta)
	}

	args := &Arguments{
		Schema: schema,
		Batch:  batch,
		Named:  make(map[string]arrow.Array),
	}

	// DuckDB wraps arguments in a single "args" struct column.
	// The struct fields are named "positional_0", "positional_1", etc. for
	// positional args, and by name for named args.
	if batch.NumCols() == 1 && batch.ColumnName(0) == "args" {
		if structArr, ok := batch.Column(0).(*array.Struct); ok {
			structType := structArr.DataType().(*arrow.StructType)

			// Collect positional args sorted by index
			type posArg struct {
				idx int
				arr arrow.Array
			}
			var positionals []posArg

			for fi := 0; fi < structType.NumFields(); fi++ {
				fieldName := structType.Field(fi).Name
				fieldArr := structArr.Field(fi)

				if len(fieldName) > 11 && fieldName[:11] == "positional_" {
					idx := 0
					for _, c := range fieldName[11:] {
						idx = idx*10 + int(c-'0')
					}
					positionals = append(positionals, posArg{idx, fieldArr})
					args.Named[fieldName] = fieldArr
				} else if len(fieldName) > 6 && fieldName[:6] == "named_" {
					// DuckDB prefixes named parameters with "named_"
					actualName := fieldName[6:]
					args.Named[actualName] = fieldArr
					args.Named[fieldName] = fieldArr // also store with prefix for compatibility
				} else {
					args.Named[fieldName] = fieldArr
				}
			}

			// Sort positionals by index and fill the Positional slice
			if len(positionals) > 0 {
				maxIdx := 0
				for _, p := range positionals {
					if p.idx > maxIdx {
						maxIdx = p.idx
					}
				}
				args.Positional = make([]arrow.Array, maxIdx+1)
				for _, p := range positionals {
					args.Positional[p.idx] = p.arr
				}
			}

			debugLog("ParseArguments: unwrapped args struct: positional=%d named=%d", len(args.Positional), len(args.Named))
			return args, nil
		}
	}

	// Fallback: direct column mapping
	for i := 0; i < int(batch.NumCols()); i++ {
		col := batch.Column(i)
		name := schema.Field(i).Name
		args.Named[name] = col
		args.Positional = append(args.Positional, col)
	}

	return args, nil
}

// NumArgs returns the total number of arguments.
func (a *Arguments) NumArgs() int {
	return len(a.Positional)
}

// GetScalarInt64 returns an int64 scalar value from the argument at the given
// position or name. For named access, pass a string key.
func (a *Arguments) GetScalarInt64(key interface{}) (int64, error) {
	col, err := a.getColumn(key)
	if err != nil {
		return 0, err
	}
	if col.IsNull(0) {
		return 0, fmt.Errorf("argument is null")
	}
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(0), nil
	case *array.Int32:
		return int64(c.Value(0)), nil
	case *array.Int16:
		return int64(c.Value(0)), nil
	case *array.Int8:
		return int64(c.Value(0)), nil
	case *array.Uint64:
		return int64(c.Value(0)), nil
	case *array.Uint32:
		return int64(c.Value(0)), nil
	case *array.Uint16:
		return int64(c.Value(0)), nil
	case *array.Uint8:
		return int64(c.Value(0)), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", col)
	}
}

// GetScalarFloat64 returns a float64 scalar value.
func (a *Arguments) GetScalarFloat64(key interface{}) (float64, error) {
	col, err := a.getColumn(key)
	if err != nil {
		return 0, err
	}
	if col.IsNull(0) {
		return 0, fmt.Errorf("argument is null")
	}
	switch c := col.(type) {
	case *array.Float64:
		return c.Value(0), nil
	case *array.Float32:
		return float64(c.Value(0)), nil
	case *array.Int64:
		return float64(c.Value(0)), nil
	case *array.Int32:
		return float64(c.Value(0)), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", col)
	}
}

// GetScalarString returns a string scalar value.
func (a *Arguments) GetScalarString(key interface{}) (string, error) {
	col, err := a.getColumn(key)
	if err != nil {
		return "", err
	}
	if col.IsNull(0) {
		return "", fmt.Errorf("argument is null")
	}
	switch c := col.(type) {
	case *array.String:
		return c.Value(0), nil
	case *array.Dictionary:
		dict := c.Dictionary().(*array.String)
		return dict.Value(c.GetValueIndex(0)), nil
	default:
		return "", fmt.Errorf("cannot convert %T to string", col)
	}
}

// GetScalarBool returns a bool scalar value.
func (a *Arguments) GetScalarBool(key interface{}) (bool, error) {
	col, err := a.getColumn(key)
	if err != nil {
		return false, err
	}
	if col.IsNull(0) {
		return false, fmt.Errorf("argument is null")
	}
	if c, ok := col.(*array.Boolean); ok {
		return c.Value(0), nil
	}
	return false, fmt.Errorf("cannot convert %T to bool", col)
}

// GetScalarBytes returns a []byte scalar value.
func (a *Arguments) GetScalarBytes(key interface{}) ([]byte, error) {
	col, err := a.getColumn(key)
	if err != nil {
		return nil, err
	}
	if col.IsNull(0) {
		return nil, fmt.Errorf("argument is null")
	}
	if c, ok := col.(*array.Binary); ok {
		return c.Value(0), nil
	}
	return nil, fmt.Errorf("cannot convert %T to bytes", col)
}

// IsNull checks if the argument at the given position/name is null.
func (a *Arguments) IsNull(key interface{}) bool {
	col, err := a.getColumn(key)
	if err != nil {
		return true
	}
	return col.IsNull(0)
}

// GetColumn returns the Arrow array for the given key (int for positional, string for named).
func (a *Arguments) GetColumn(key interface{}) (arrow.Array, error) {
	return a.getColumn(key)
}

func (a *Arguments) getColumn(key interface{}) (arrow.Array, error) {
	switch k := key.(type) {
	case int:
		if k < 0 || k >= len(a.Positional) {
			return nil, fmt.Errorf("argument index %d out of range (have %d)", k, len(a.Positional))
		}
		return a.Positional[k], nil
	case string:
		col, ok := a.Named[k]
		if !ok {
			return nil, fmt.Errorf("argument %q not found", k)
		}
		return col, nil
	default:
		return nil, fmt.Errorf("argument key must be int or string, got %T", key)
	}
}

// RemapPositionalArgs remaps DuckDB's sequential const-arg numbering back to
// the original ArgSpec positions. DuckDB only sends const params in the args
// struct and numbers them sequentially (0, 1, ...), but functions expect them
// at their declared positions. For example, if specs are:
//
//	position 0: const (header)
//	position 1: non-const (payload) — not in args
//	position 2: const (config)
//
// DuckDB sends positional_0=header, positional_1=config. This method remaps
// so that Positional[0]=header, Positional[2]=config, allowing functions to
// access args by their declared positions.
func (a *Arguments) RemapPositionalArgs(specs []ArgSpec) {
	if len(a.Positional) == 0 || len(specs) == 0 {
		return
	}

	// Build list of const arg positions in declaration order
	var constPositions []int
	maxPos := 0
	for _, spec := range specs {
		if spec.Position >= 0 && spec.IsConst {
			constPositions = append(constPositions, spec.Position)
		}
		if spec.Position > maxPos {
			maxPos = spec.Position
		}
	}

	// If there are no non-const params, positions already match
	if len(constPositions) == len(specs) {
		return
	}

	// Only remap if DuckDB sent fewer args than the max position
	if len(a.Positional) > maxPos {
		return // positions already cover the full range
	}

	// Build the expanded positional slice
	expanded := make([]arrow.Array, maxPos+1)
	for i, origPos := range constPositions {
		if i < len(a.Positional) {
			expanded[origPos] = a.Positional[i]
		}
	}
	a.Positional = expanded
}

// Release releases the underlying batch.
func (a *Arguments) Release() {
	if a.Batch != nil {
		a.Batch.Release()
	}
}

// SerializeArguments serializes arguments to Arrow IPC bytes.
func SerializeArguments(schema *arrow.Schema, values []arrow.Array) ([]byte, error) {
	mem := memory.NewGoAllocator()
	_ = mem
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if len(values) > 0 {
		batch := array.NewRecordBatch(schema, values, 1)
		defer batch.Release()
		if err := w.Write(batch); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
