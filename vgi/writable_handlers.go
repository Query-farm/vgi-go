// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var _ = bytes.Buffer{}

// writableByAttachID returns the writable catalog whose attach_id matches.
// nil if no match (the caller should treat as the default read-only catalog).
func (w *Worker) writableByAttachID(attachID []byte) *WritableCatalog {
	if len(attachID) == 0 {
		return nil
	}
	for _, c := range w.extraCatalogs {
		if bytes.Equal(c.AttachID(), attachID) {
			return c
		}
	}
	return nil
}

// handleWritableAttach serves catalog_attach for a writable catalog.
func (w *Worker) handleWritableAttach(req CatalogAttachRequestWire, c *WritableCatalog) (CatalogAttachResultWire, error) {
	c.mu.Lock()
	if len(c.attachID) == 0 {
		c.attachID = []byte("writable:" + c.Name)
	}
	attachID := append([]byte{}, c.attachID...)
	version := c.version
	c.mu.Unlock()

	var serializedSettings [][]byte
	for _, spec := range w.settings {
		data, err := serializeSettingSpec(spec)
		if err == nil {
			serializedSettings = append(serializedSettings, data)
		}
	}
	if serializedSettings == nil {
		serializedSettings = [][]byte{}
	}
	var serializedSecretTypes [][]byte
	for _, spec := range w.secretTypes {
		data, err := serializeSecretTypeSpec(spec)
		if err == nil {
			serializedSecretTypes = append(serializedSecretTypes, data)
		}
	}
	if serializedSecretTypes == nil {
		serializedSecretTypes = [][]byte{}
	}

	return CatalogAttachResultWire{
		AttachID:                 attachID,
		SupportsTransactions:     true,
		SupportsTimeTravel:       false,
		CatalogVersionFrozen:     false,
		CatalogVersion:           version,
		AttachIDRequired:         true,
		DefaultSchema:            "main",
		Settings:                 serializedSettings,
		SecretTypes:              serializedSecretTypes,
		Tags:                     map[string]string{},
		SupportsColumnStatistics: false,
	}, nil
}

// ============================================================================
// Schema-listing handlers reroute to writable catalog when attach_id matches.
// ============================================================================

func (w *Worker) writableSchemas(c *WritableCatalog) ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, 0, len(c.schemas))
	for _, s := range c.schemas {
		info := &SchemaInfo{Name: s.name, Comment: s.comment, AttachID: c.attachID}
		data, err := SerializeSchemaInfo(info)
		if err != nil {
			return nil, err
		}
		out = append(out, data)
	}
	return out, nil
}

func (w *Worker) writableSchemaGet(c *WritableCatalog, name string) ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.schemas[strings.ToLower(name)]
	if !ok {
		return nil, nil
	}
	info := &SchemaInfo{Name: s.name, Comment: s.comment, AttachID: c.attachID}
	data, err := SerializeSchemaInfo(info)
	if err != nil {
		return nil, err
	}
	return [][]byte{data}, nil
}

func (w *Worker) writableSchemaContentsTables(c *WritableCatalog, schemaName string) ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.schemas[strings.ToLower(schemaName)]
	if !ok {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, len(s.tables))
	for _, t := range s.tables {
		info, err := tableInfoFromWritable(t, schemaName)
		if err != nil {
			return nil, err
		}
		data, err := SerializeTableInfo(info)
		if err != nil {
			return nil, err
		}
		out = append(out, data)
	}
	return out, nil
}

func (w *Worker) writableTableGet(c *WritableCatalog, schemaName, tableName string) ([][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.schemas[strings.ToLower(schemaName)]
	if !ok {
		return nil, nil
	}
	t, ok := s.tables[strings.ToLower(tableName)]
	if !ok {
		return nil, nil
	}
	info, err := tableInfoFromWritable(t, schemaName)
	if err != nil {
		return nil, err
	}
	data, err := SerializeTableInfo(info)
	if err != nil {
		return nil, err
	}
	return [][]byte{data}, nil
}

func tableInfoFromWritable(t *writableTable, schemaName string) (*TableInfo, error) {
	cols := t.schema
	var err error
	if len(t.defaults) > 0 {
		cols, err = applyDefaults(cols, t.defaults)
		if err != nil {
			return nil, err
		}
	}
	notNull := resolveColumnIndices(cols, t.notNull)
	unique := resolveColumnGroupIndices(cols, t.unique)
	primaryKey := resolveColumnGroupIndices(cols, t.primaryKey)
	var fkBytes [][]byte
	for i := range t.foreignKey {
		data, err := serializeForeignKey(schemaName, &t.foreignKey[i])
		if err != nil {
			return nil, err
		}
		fkBytes = append(fkBytes, data)
	}
	if fkBytes == nil {
		fkBytes = [][]byte{}
	}
	return &TableInfo{
		Name:                     t.name,
		SchemaName:               schemaName,
		Comment:                  t.comment,
		Columns:                  cols,
		NotNullConstraints:       notNull,
		UniqueConstraints:        unique,
		PrimaryKeyConstraints:    primaryKey,
		ForeignKeyConstraints:    fkBytes,
		CheckConstraints:         t.check,
		SupportsInsert:           true,
		SupportsUpdate:           true,
		SupportsDelete:           true,
		SupportsColumnStatistics: false,
	}, nil
}

// ============================================================================
// Helpers reused by writable-catalog scan/insert functions.
// ============================================================================

// rowsToBatch builds a RecordBatch from a slice of column-name → value maps.
func rowsToBatch(schema *arrow.Schema, rows []map[string]interface{}) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		col, err := buildColumnFromValues(mem, f, rows)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	return array.NewRecordBatch(schema, cols, int64(len(rows))), nil
}

func buildColumnFromValues(mem memory.Allocator, f arrow.Field, rows []map[string]interface{}) (arrow.Array, error) {
	switch f.Type.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[f.Name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(toInt64(v))
		}
		return b.NewArray(), nil
	case arrow.INT32:
		b := array.NewInt32Builder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[f.Name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(int32(toInt64(v)))
		}
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[f.Name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(toFloat64(v))
		}
		return b.NewArray(), nil
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[f.Name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(toString(v))
		}
		return b.NewArray(), nil
	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[f.Name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(v.(bool))
		}
		return b.NewArray(), nil
	case arrow.BINARY:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[f.Name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			switch x := v.(type) {
			case []byte:
				b.Append(x)
			case string:
				b.Append([]byte(x))
			default:
				return nil, fmt.Errorf("buildColumnFromValues binary: unsupported %T", v)
			}
		}
		return b.NewArray(), nil
	}
	return nil, fmt.Errorf("buildColumnFromValues: unsupported type %s", f.Type)
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case uint64:
		return int64(x)
	case uint32:
		return int64(x)
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case int32:
		return float64(x)
	}
	return 0
}

func toString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return fmt.Sprintf("%v", v)
}

// batchToRows converts a RecordBatch into a slice of column-name → Go-value maps.
func batchToRows(batch arrow.RecordBatch) ([]map[string]interface{}, error) {
	n := int(batch.NumRows())
	out := make([]map[string]interface{}, n)
	schema := batch.Schema()
	for i := 0; i < n; i++ {
		out[i] = map[string]interface{}{}
	}
	for c := 0; c < int(batch.NumCols()); c++ {
		name := schema.Field(c).Name
		col := batch.Column(c)
		for i := 0; i < n; i++ {
			if col.IsNull(i) {
				out[i][name] = nil
				continue
			}
			out[i][name] = arrowValueAt(col, i)
		}
	}
	return out, nil
}

func arrowValueAt(col arrow.Array, i int) interface{} {
	switch a := col.(type) {
	case *array.Int64:
		return a.Value(i)
	case *array.Int32:
		return int64(a.Value(i))
	case *array.Int16:
		return int64(a.Value(i))
	case *array.Int8:
		return int64(a.Value(i))
	case *array.Uint64:
		return int64(a.Value(i))
	case *array.Uint32:
		return int64(a.Value(i))
	case *array.Float64:
		return a.Value(i)
	case *array.Float32:
		return float64(a.Value(i))
	case *array.String:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.Binary:
		return append([]byte{}, a.Value(i)...)
	}
	return nil
}

