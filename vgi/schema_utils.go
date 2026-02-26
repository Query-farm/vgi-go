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

// SchemaFromFields creates an Arrow schema from name/type pairs.
func SchemaFromFields(fields map[string]arrow.DataType) *arrow.Schema {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for name, dt := range fields {
		arrowFields = append(arrowFields, arrow.Field{Name: name, Type: dt})
	}
	return arrow.NewSchema(arrowFields, nil)
}

// SchemaFromOrderedFields creates an Arrow schema preserving insertion order.
func SchemaFromOrderedFields(names []string, types []arrow.DataType) *arrow.Schema {
	fields := make([]arrow.Field, len(names))
	for i := range names {
		fields[i] = arrow.Field{Name: names[i], Type: types[i]}
	}
	return arrow.NewSchema(fields, nil)
}

// ProjectSchema returns a new schema with only the fields at the given indices.
func ProjectSchema(projectionIDs []int32, schema *arrow.Schema) *arrow.Schema {
	if projectionIDs == nil {
		return schema
	}
	fields := make([]arrow.Field, len(projectionIDs))
	for i, id := range projectionIDs {
		fields[i] = schema.Field(int(id))
	}
	return arrow.NewSchema(fields, nil)
}

// SerializeSchema serializes an Arrow schema to IPC bytes.
func SerializeSchema(schema *arrow.Schema) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeserializeSchema reads an Arrow schema from IPC bytes.
func DeserializeSchema(data []byte) (*arrow.Schema, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty schema IPC data")
	}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("reading schema IPC: %w", err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}

// DeserializeRecordBatch reads a RecordBatch from IPC bytes.
func DeserializeRecordBatch(data []byte) (arrow.RecordBatch, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("reading batch IPC: %w", err)
	}
	defer reader.Release()
	if !reader.Next() {
		return nil, fmt.Errorf("no batch in IPC stream")
	}
	batch := reader.RecordBatch()
	batch.Retain()
	return batch, nil
}

// SerializeRecordBatch serializes a RecordBatch to IPC bytes.
func SerializeRecordBatch(batch arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// EmptyBatch creates a zero-row batch with the given schema.
func EmptyBatch(schema *arrow.Schema) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		b := array.NewBuilder(mem, f.Type)
		cols[i] = b.NewArray()
		b.Release()
	}
	batch := array.NewRecordBatch(schema, cols, 0)
	for _, c := range cols {
		c.Release()
	}
	return batch
}

// BuildArgSchema creates an Arrow schema from ArgSpecs with VGI metadata markers.
func BuildArgSchema(specs []ArgSpec) *arrow.Schema {
	if len(specs) == 0 {
		return arrow.NewSchema(nil, nil)
	}

	fields := make([]arrow.Field, len(specs))
	for i, spec := range specs {
		arrowType := argTypeToArrowType(spec.ArrowType)
		meta := make(map[string]string)

		// Named argument marker
		if spec.Position < 0 {
			meta["vgi_arg"] = "named"
		}

		// Type markers: non-const column params are always "any" in the catalog.
		// DuckDB resolves actual types at bind time via input_schema.
		// Const params with concrete types (int64, varchar, etc.) keep their
		// specific types for DuckDB validation. Const params with complex/dynamic
		// types (struct, any) must also be "any" since Go can't express
		// parameterized types (e.g., STRUCT(label VARCHAR, version BIGINT)).
		if !spec.IsConst {
			switch spec.ArrowType {
			case "table":
				meta["vgi_type"] = "table"
			default:
				meta["vgi_type"] = "any"
				arrowType = arrow.Null // placeholder for any/flexible type
			}
		} else if spec.ArrowType == "struct" || spec.ArrowType == "any" || spec.ArrowType == "" {
			// Const params with complex/dynamic types need "any" marker
			meta["vgi_type"] = "any"
			arrowType = arrow.Null
		}

		// Constant parameter marker
		if spec.IsConst {
			meta["vgi_const"] = "true"
		}

		// Varargs marker
		if spec.IsVarargs {
			meta["vgi_varargs"] = "true"
		}

		var fieldMeta arrow.Metadata
		if len(meta) > 0 {
			keys := make([]string, 0, len(meta))
			vals := make([]string, 0, len(meta))
			for k, v := range meta {
				keys = append(keys, k)
				vals = append(vals, v)
			}
			fieldMeta = arrow.NewMetadata(keys, vals)
		}

		fields[i] = arrow.Field{
			Name:     spec.Name,
			Type:     arrowType,
			Metadata: fieldMeta,
		}
	}

	return arrow.NewSchema(fields, nil)
}

// argTypeToArrowType converts a VGI arg type string to an Arrow DataType.
func argTypeToArrowType(t string) arrow.DataType {
	switch t {
	case "int8":
		return arrow.PrimitiveTypes.Int8
	case "int16":
		return arrow.PrimitiveTypes.Int16
	case "int32":
		return arrow.PrimitiveTypes.Int32
	case "int64":
		return arrow.PrimitiveTypes.Int64
	case "uint8":
		return arrow.PrimitiveTypes.Uint8
	case "uint16":
		return arrow.PrimitiveTypes.Uint16
	case "uint32":
		return arrow.PrimitiveTypes.Uint32
	case "uint64":
		return arrow.PrimitiveTypes.Uint64
	case "float", "float32":
		return arrow.PrimitiveTypes.Float32
	case "double", "float64":
		return arrow.PrimitiveTypes.Float64
	case "varchar", "string":
		return arrow.BinaryTypes.String
	case "bool", "boolean":
		return &arrow.BooleanType{}
	case "blob", "binary":
		return arrow.BinaryTypes.Binary
	case "any", "", "struct":
		return arrow.Null // placeholder for any/flexible type
	case "table":
		return arrow.Null // placeholder for table input
	default:
		return arrow.BinaryTypes.String
	}
}

// BatchToSettingsMap converts a single-row settings RecordBatch to a map.
func BatchToSettingsMap(batch arrow.RecordBatch) map[string]interface{} {
	if batch == nil || batch.NumRows() == 0 {
		return nil
	}
	result := make(map[string]interface{})
	for i := 0; i < int(batch.NumCols()); i++ {
		name := batch.ColumnName(i)
		col := batch.Column(i)
		if col.IsNull(0) {
			continue
		}
		result[name] = extractScalarValue(col, 0)
	}
	return result
}

// BatchToSecretsMap converts a secrets RecordBatch to a map of maps.
func BatchToSecretsMap(batch arrow.RecordBatch) map[string]map[string]interface{} {
	if batch == nil || batch.NumRows() == 0 {
		return nil
	}
	result := make(map[string]map[string]interface{})
	for i := 0; i < int(batch.NumCols()); i++ {
		name := batch.ColumnName(i)
		col := batch.Column(i)
		if col.IsNull(0) {
			continue
		}
		// Secrets come as struct arrays
		if structArr, ok := col.(*array.Struct); ok {
			secretMap := make(map[string]interface{})
			structType := structArr.DataType().(*arrow.StructType)
			for fi := 0; fi < structType.NumFields(); fi++ {
				fieldName := structType.Field(fi).Name
				fieldArr := structArr.Field(fi)
				if !fieldArr.IsNull(0) {
					secretMap[fieldName] = extractScalarValue(fieldArr, 0)
				}
			}
			result[name] = secretMap
		}
	}
	return result
}

// extractScalarValue extracts a Go value from an Arrow array at the given index.
func extractScalarValue(col arrow.Array, idx int) interface{} {
	if col.IsNull(idx) {
		return nil
	}
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(idx)
	case *array.Int32:
		return int64(c.Value(idx))
	case *array.Int16:
		return int64(c.Value(idx))
	case *array.Int8:
		return int64(c.Value(idx))
	case *array.Uint64:
		return c.Value(idx)
	case *array.Uint32:
		return uint32(c.Value(idx))
	case *array.Float64:
		return c.Value(idx)
	case *array.Float32:
		return float64(c.Value(idx))
	case *array.String:
		return c.Value(idx)
	case *array.Boolean:
		return c.Value(idx)
	case *array.Binary:
		return c.Value(idx)
	case *array.Dictionary:
		dict := c.Dictionary().(*array.String)
		return dict.Value(c.GetValueIndex(idx))
	default:
		return fmt.Sprintf("%v", col)
	}
}
