// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sync"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

// debugLog writes debug messages to /tmp/vgi-go.log
func debugLog(format string, args ...interface{}) {
	f, err := os.OpenFile("/tmp/vgi-go.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, format+"\n", args...)
}

// Ensure context is used (for handler signatures).
var _ = context.Background

// SettingSpec describes a DuckDB custom setting registered by the worker.
type SettingSpec struct {
	Name         string
	Description  string
	Type         arrow.DataType
	DefaultValue interface{} // Go value matching the Type (nil = no default)
}

// serializeSettingSpec serializes a SettingSpec to Arrow IPC bytes.
// Format: RecordBatch with schema {name: string, description: string, type: binary, default_value: binary?}
func serializeSettingSpec(spec SettingSpec) ([]byte, error) {
	mem := memory.NewGoAllocator()
	settingSchema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "type", Type: arrow.BinaryTypes.Binary, Nullable: false},
		{Name: "default_value", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	// Serialize the type as a single-field schema
	typeSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: spec.Type},
	}, nil)
	typeBytes, err := SerializeSchema(typeSchema)
	if err != nil {
		return nil, fmt.Errorf("serializing setting type: %w", err)
	}

	// Serialize default value if present
	var defaultBytes []byte
	if spec.DefaultValue != nil {
		defaultBatch, err := buildDefaultValueBatch(mem, typeSchema, spec.Type, spec.DefaultValue)
		if err != nil {
			return nil, fmt.Errorf("serializing setting default: %w", err)
		}
		defer defaultBatch.Release()
		defaultBytes, err = SerializeRecordBatch(defaultBatch)
		if err != nil {
			return nil, fmt.Errorf("serializing setting default batch: %w", err)
		}
	}

	// Build the setting batch
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(spec.Name)
	nameArr := nameBuilder.NewArray()
	defer nameArr.Release()

	descBuilder := array.NewStringBuilder(mem)
	defer descBuilder.Release()
	descBuilder.Append(spec.Description)
	descArr := descBuilder.NewArray()
	defer descArr.Release()

	typeBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer typeBuilder.Release()
	typeBuilder.Append(typeBytes)
	typeArr := typeBuilder.NewArray()
	defer typeArr.Release()

	defaultBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer defaultBuilder.Release()
	if defaultBytes != nil {
		defaultBuilder.Append(defaultBytes)
	} else {
		defaultBuilder.AppendNull()
	}
	defaultArr := defaultBuilder.NewArray()
	defer defaultArr.Release()

	batch := array.NewRecordBatch(settingSchema,
		[]arrow.Array{nameArr, descArr, typeArr, defaultArr}, 1)

	// Serialize to IPC
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(settingSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// buildDefaultValueBatch creates a 1-row batch with the default value.
func buildDefaultValueBatch(mem memory.Allocator, schema *arrow.Schema, dt arrow.DataType, val interface{}) (arrow.RecordBatch, error) {
	b := array.NewBuilder(mem, dt)
	defer b.Release()

	switch v := val.(type) {
	case bool:
		b.(*array.BooleanBuilder).Append(v)
	case int:
		b.(*array.Int64Builder).Append(int64(v))
	case int64:
		b.(*array.Int64Builder).Append(v)
	case float64:
		b.(*array.Float64Builder).Append(v)
	case string:
		b.(*array.StringBuilder).Append(v)
	case []byte:
		b.(*array.BinaryBuilder).Append(v)
	default:
		b.AppendNull()
	}

	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(schema, []arrow.Array{arr}, 1), nil
}

// Worker is the main VGI worker that hosts functions and serves RPC.
type Worker struct {
	scalars     map[string]ScalarFunction
	tables      map[string]TableFunction
	tableInOuts map[string]TableInOutFunction
	catalogName string
	catalog     *DefaultReadOnlyCatalog
	storages    sync.Map // map[hex execution ID string]*ExecutionStorage
	settings    []SettingSpec
}

// WorkerOption configures a Worker.
type WorkerOption func(*Worker)

// WithCatalogName sets the catalog name.
func WithCatalogName(name string) WorkerOption {
	return func(w *Worker) {
		w.catalogName = name
	}
}

// WithSettings adds custom DuckDB settings to the worker.
func WithSettings(settings ...SettingSpec) WorkerOption {
	return func(w *Worker) {
		w.settings = append(w.settings, settings...)
	}
}

// NewWorker creates a new VGI worker.
func NewWorker(opts ...WorkerOption) *Worker {
	w := &Worker{
		scalars:     make(map[string]ScalarFunction),
		tables:      make(map[string]TableFunction),
		tableInOuts: make(map[string]TableInOutFunction),
		catalogName: "example",
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// RegisterScalar registers a scalar function.
func (w *Worker) RegisterScalar(f ScalarFunction) {
	w.scalars[f.Name()] = f
}

// RegisterTable registers a table function.
func (w *Worker) RegisterTable(f TableFunction) {
	w.tables[f.Name()] = f
}

// RegisterTableInOut registers a table-in-out function.
func (w *Worker) RegisterTableInOut(f TableInOutFunction) {
	w.tableInOuts[f.Name()] = f
}

// RunStdio runs the worker serving RPC over stdin/stdout.
func (w *Worker) RunStdio() {
	// Build catalog from registered functions
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)

	s := vgirpc.NewServer()

	// Register bind (unary)
	vgirpc.Unary[BindRequestWire, BindResponseWire](s, "bind", w.handleBind)

	// Register init (dynamic stream with header)
	headerSchema := arrow.NewSchema([]arrow.Field{
		{Name: "execution_id", Type: arrow.BinaryTypes.Binary},
		{Name: "max_workers", Type: arrow.PrimitiveTypes.Int64},
		{Name: "opaque_data", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	vgirpc.DynamicStreamWithHeader[InitRequestWire](s, "init", headerSchema, w.handleInit)

	// Register table_function_cardinality (unary)
	vgirpc.Unary[CardinalityRequestWire, TableCardinality](s, "table_function_cardinality", w.handleCardinality)

	// Register all catalog methods
	w.registerCatalogMethods(s)

	s.RunStdio()
}

// newExecutionID generates a UUID-based execution ID.
func newExecutionID() []byte {
	id := uuid.New()
	return id[:]
}

// getOrCreateStorage returns or creates an ExecutionStorage for the given execution ID.
func (w *Worker) getOrCreateStorage(executionID []byte) *ExecutionStorage {
	key := hex.EncodeToString(executionID)
	if s, ok := w.storages.Load(key); ok {
		return s.(*ExecutionStorage)
	}
	s := NewExecutionStorage()
	s.SetExecutionID(executionID)
	actual, loaded := w.storages.LoadOrStore(key, s)
	if loaded {
		// Another goroutine beat us; close our duplicate
		s.Cleanup()
	}
	return actual.(*ExecutionStorage)
}
