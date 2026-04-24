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

// rowIDFieldName is the synthesized column name used to expose a stable
// row identifier on writable tables. DuckDB hides columns with the
// `is_row_id` metadata key from SELECT *.
const rowIDFieldName = "__row_id"

var rowIDMetadata = arrow.NewMetadata([]string{"is_row_id"}, []string{""})

// withSynthesizedRowID returns a new schema with __row_id (int64) appended
// if not already present.
func withSynthesizedRowID(s *arrow.Schema) *arrow.Schema {
	for i := 0; i < s.NumFields(); i++ {
		f := s.Field(i)
		if !f.HasMetadata() {
			continue
		}
		md := f.Metadata
		for k := 0; k < md.Len(); k++ {
			if md.Keys()[k] == "is_row_id" {
				return s
			}
		}
	}
	fields := make([]arrow.Field, s.NumFields(), s.NumFields()+1)
	for i := 0; i < s.NumFields(); i++ {
		fields[i] = s.Field(i)
	}
	fields = append(fields, arrow.Field{
		Name:     rowIDFieldName,
		Type:     arrow.PrimitiveTypes.Int64,
		Metadata: rowIDMetadata,
	})
	md := s.Metadata()
	return arrow.NewSchema(fields, &md)
}

// ============================================================================
// DDL handlers (schema/table create/drop/alter).
// ============================================================================

// onConflictAction encodes the SQL ON-CONFLICT semantics carried as a
// dictionary-encoded string by the C++ extension.
type onConflictAction string

const (
	onConflictError   onConflictAction = "ERROR"
	onConflictReplace onConflictAction = "REPLACE"
	onConflictIgnore  onConflictAction = "IGNORE"
)

func parseOnConflict(s string) onConflictAction {
	switch strings.ToUpper(s) {
	case "REPLACE":
		return onConflictReplace
	case "IGNORE":
		return onConflictIgnore
	}
	return onConflictError
}

func (w *Worker) writableSchemaCreate(c *WritableCatalog, name string, onConflict onConflictAction, comment *string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.ToLower(name)
	if existing, ok := c.schemas[key]; ok {
		switch onConflict {
		case onConflictReplace:
			existing.tables = map[string]*writableTable{}
			if comment != nil {
				existing.comment = *comment
			}
			c.version++
			return nil
		case onConflictIgnore:
			return nil
		default:
			return fmt.Errorf("schema %q already exists", name)
		}
	}
	s := &writableSchema{name: name, tables: map[string]*writableTable{}}
	if comment != nil {
		s.comment = *comment
	}
	c.schemas[key] = s
	c.version++
	return nil
}

func (w *Worker) writableSchemaDrop(c *WritableCatalog, name string, ignoreNotFound, cascade bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.ToLower(name)
	s, ok := c.schemas[key]
	if !ok {
		if ignoreNotFound {
			return nil
		}
		return fmt.Errorf("schema %q does not exist", name)
	}
	if !cascade && len(s.tables) > 0 {
		return fmt.Errorf("schema %q is not empty (use CASCADE)", name)
	}
	delete(c.schemas, key)
	c.version++
	return nil
}

func (w *Worker) writableTableCreate(c *WritableCatalog, req TableCreateRequestWire) error {
	if len(req.Columns) == 0 {
		return fmt.Errorf("catalog_table_create: missing columns schema")
	}
	schema, err := DeserializeSchema(req.Columns)
	if err != nil {
		return fmt.Errorf("catalog_table_create: deserialize columns: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	sKey := strings.ToLower(req.SchemaName)
	s, ok := c.schemas[sKey]
	if !ok {
		return fmt.Errorf("schema %q does not exist", req.SchemaName)
	}
	tKey := strings.ToLower(req.Name)
	if _, ok := s.tables[tKey]; ok {
		switch parseOnConflict(req.OnConflict) {
		case onConflictReplace:
			delete(s.tables, tKey)
		case onConflictIgnore:
			return nil
		default:
			return fmt.Errorf("table %q.%q already exists", req.SchemaName, req.Name)
		}
	}

	t := &writableTable{
		name:          req.Name,
		schema:        schema,
		notNull:       columnsByIndex(schema, req.NotNullConstraints),
		primaryKey:    columnGroupsByIndex(schema, req.PrimaryKeyConstraints),
		unique:        columnGroupsByIndex(schema, req.UniqueConstraints),
		check:         req.CheckConstraints,
		defaults:      defaultsFromSchemaMetadata(schema),
		columnComment: map[string]string{},
	}
	s.tables[tKey] = t
	c.version++
	return nil
}

func (w *Worker) writableTableDrop(c *WritableCatalog, schemaName, name string, ignoreNotFound, cascade bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.schemas[strings.ToLower(schemaName)]
	if !ok {
		if ignoreNotFound {
			return nil
		}
		return fmt.Errorf("schema %q does not exist", schemaName)
	}
	tKey := strings.ToLower(name)
	if _, ok := s.tables[tKey]; !ok {
		if ignoreNotFound {
			return nil
		}
		return fmt.Errorf("table %q.%q does not exist", schemaName, name)
	}
	delete(s.tables, tKey)
	c.version++
	return nil
}

func columnsByIndex(schema *arrow.Schema, idx []int32) []string {
	out := make([]string, 0, len(idx))
	for _, i := range idx {
		if int(i) < schema.NumFields() {
			out = append(out, schema.Field(int(i)).Name)
		}
	}
	return out
}

func columnGroupsByIndex(schema *arrow.Schema, groups [][]int32) [][]string {
	out := make([][]string, 0, len(groups))
	for _, g := range groups {
		out = append(out, columnsByIndex(schema, g))
	}
	return out
}

// defaultsFromSchemaMetadata extracts the default-value SQL expression
// stored as Arrow field metadata (key "default") set by DuckDB's column
// definition serializer.
func defaultsFromSchemaMetadata(schema *arrow.Schema) map[string]any {
	out := map[string]any{}
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		if !f.HasMetadata() {
			continue
		}
		md := f.Metadata
		for k := 0; k < md.Len(); k++ {
			if md.Keys()[k] == "default" {
				out[f.Name] = Sql(md.Values()[k])
				break
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// writableByAttachID returns the writable catalog whose attach_id matches.
// Attach IDs are deterministic ("writable:<name>") so DuckDB-spawned worker
// processes resolve to the same catalog without sharing in-memory state.
func (w *Worker) writableByAttachID(attachID []byte) *WritableCatalog {
	if len(attachID) == 0 {
		return nil
	}
	const prefix = "writable:"
	if !bytes.HasPrefix(attachID, []byte(prefix)) {
		return nil
	}
	name := string(attachID[len(prefix):])
	if c, ok := w.extraCatalogs[name]; ok {
		return c
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
	// Inject a synthesized __row_id column at the end so DuckDB has a row
	// reference for UPDATE/DELETE. DuckDB hides is_row_id columns from
	// SELECT *. The generic scan function fills it with the row's index.
	cols = withSynthesizedRowID(cols)
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

