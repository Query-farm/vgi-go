// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

// storageTrackerKeyType is the context key type for the storage tracker.
type storageTrackerKeyType struct{}

// storageTrackerKey is the context key used to store the tracker.
var storageTrackerKey storageTrackerKeyType

// storageTracker records execution ID hex keys used during a single dispatch.
type storageTracker struct {
	keys []string
}

// track records a hex key, deduplicating against previously tracked keys.
func (t *storageTracker) track(key string) {
	for _, k := range t.keys {
		if k == key {
			return
		}
	}
	t.keys = append(t.keys, key)
}

// storageCleanupHook implements vgirpc.DispatchHook to clean up storage
// entries when execution streams complete.
//
// Cleanup is deferred by one dispatch cycle: keys tracked during a dispatch
// become "pending" at its end, and are only cleaned up in the next dispatch's
// OnDispatchEnd — but only if the new dispatch did NOT reuse them. This handles
// multi-worker (primary + secondary inits share an execution ID) and
// table-in-out (INPUT + FINALIZE share an execution ID).
type storageCleanupHook struct {
	worker      *Worker
	pendingKeys []string // keys from previous dispatch, candidates for cleanup
	staleKeys   []string // snapshot of pendingKeys at current dispatch start
}

func (h *storageCleanupHook) OnDispatchStart(ctx context.Context, info vgirpc.DispatchInfo) (context.Context, vgirpc.HookToken) {
	if info.Method != "init" {
		return ctx, nil
	}
	// Snapshot pending keys; they'll be cleaned in OnDispatchEnd if not reused.
	h.staleKeys = h.pendingKeys
	h.pendingKeys = nil

	tracker := &storageTracker{}
	ctx = context.WithValue(ctx, storageTrackerKey, tracker)
	return ctx, tracker
}

func (h *storageCleanupHook) OnDispatchEnd(ctx context.Context, token vgirpc.HookToken, info vgirpc.DispatchInfo, stats *vgirpc.CallStatistics, err error) {
	tracker, ok := token.(*storageTracker)
	if !ok || tracker == nil {
		return
	}

	// Clean up stale keys that were NOT reused in this dispatch.
	for _, key := range h.staleKeys {
		reused := false
		for _, tk := range tracker.keys {
			if key == tk {
				reused = true
				break
			}
		}
		if !reused {
			if val, loaded := h.worker.storages.LoadAndDelete(key); loaded {
				val.(*ExecutionStorage).Cleanup()
				slog.Debug("storage: cleaned up execution", "key", key)
			}
		}
	}
	h.staleKeys = nil

	// Save this dispatch's keys for the next cycle.
	h.pendingKeys = append(h.pendingKeys, tracker.keys...)
}

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
		return nil, fmt.Errorf("unsupported default value type: %T", val)
	}

	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(schema, []arrow.Array{arr}, 1), nil
}

// Worker is the main VGI worker that hosts functions and serves RPC.
type Worker struct {
	scalars                map[string]ScalarFunction
	tables                 map[string]TableFunction
	tableInOuts            map[string]TableInOutFunction
	catalogName            string
	catalog                *DefaultReadOnlyCatalog
	storages               sync.Map // map[hex execution ID string]*ExecutionStorage
	settings               []SettingSpec
	catalogTables          map[string][]CatalogTable // schema_name → tables
	catalogViews           map[string][]CatalogView  // schema_name → views
	catalogMacros          map[string][]CatalogMacro  // schema_name → macros
	scanFunctionGetHandler ScanFunctionGetHandler
	logLevel               slog.Level   // slog.LevelInfo (0) by default — Info level is intentional.
	logHandler             slog.Handler // nil means default TextHandler to stderr
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

// WithLogLevel sets the minimum log level for the default handler.
// The zero value (slog.LevelInfo) logs Info and above.
func WithLogLevel(level slog.Level) WorkerOption {
	return func(w *Worker) {
		w.logLevel = level
	}
}

// WithLogHandler sets a custom slog.Handler for all logging.
// When set, WithLogLevel is ignored (the handler controls its own level).
func WithLogHandler(h slog.Handler) WorkerOption {
	return func(w *Worker) {
		w.logHandler = h
	}
}

// NewWorker creates a new VGI worker.
func NewWorker(opts ...WorkerOption) *Worker {
	w := &Worker{
		scalars:       make(map[string]ScalarFunction),
		tables:        make(map[string]TableFunction),
		tableInOuts:   make(map[string]TableInOutFunction),
		catalogTables: make(map[string][]CatalogTable),
		catalogViews:  make(map[string][]CatalogView),
		catalogMacros: make(map[string][]CatalogMacro),
		catalogName:   "example",
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

// RegisterCatalogTable registers a table in the given schema of the catalog.
func (w *Worker) RegisterCatalogTable(schemaName string, table CatalogTable) {
	w.catalogTables[schemaName] = append(w.catalogTables[schemaName], table)
}

// RegisterCatalogView registers a view in the given schema of the catalog.
func (w *Worker) RegisterCatalogView(schemaName string, view CatalogView) {
	w.catalogViews[schemaName] = append(w.catalogViews[schemaName], view)
}

// RegisterCatalogMacro registers a macro in the given schema of the catalog.
func (w *Worker) RegisterCatalogMacro(schemaName string, macro CatalogMacro) {
	w.catalogMacros[schemaName] = append(w.catalogMacros[schemaName], macro)
}

// SetScanFunctionGetHandler sets a handler for resolving scan functions
// for tables that are not directly backed by a registered Function.
func (w *Worker) SetScanFunctionGetHandler(h ScanFunctionGetHandler) {
	w.scanFunctionGetHandler = h
}

// RunStdio runs the worker serving RPC over stdin/stdout.
func (w *Worker) RunStdio() {
	// Configure structured logging.
	// logLevel zero value is slog.LevelInfo (0) — Info by default is intentional.
	handler := w.logHandler
	if handler == nil {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: w.logLevel})
	}
	slog.SetDefault(slog.New(handler))

	// Build catalog from registered functions
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)

	s := vgirpc.NewServer()
	s.SetDispatchHook(&storageCleanupHook{worker: w})

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
func (w *Worker) getOrCreateStorage(ctx context.Context, executionID []byte) (*ExecutionStorage, error) {
	key := hex.EncodeToString(executionID)
	if tracker, ok := ctx.Value(storageTrackerKey).(*storageTracker); ok {
		tracker.track(key)
	}
	if s, ok := w.storages.Load(key); ok {
		return s.(*ExecutionStorage), nil
	}
	s := NewExecutionStorage()
	if err := s.SetExecutionID(executionID); err != nil {
		return nil, err
	}
	actual, loaded := w.storages.LoadOrStore(key, s)
	if loaded {
		// Another goroutine beat us; close our duplicate
		s.Cleanup()
	}
	return actual.(*ExecutionStorage), nil
}
