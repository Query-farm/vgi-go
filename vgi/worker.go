// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

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
	isHTTP      bool     // when true, skip storage cleanup (no reliable stream-end signal)
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
	if h.isHTTP {
		// In HTTP mode there is no reliable stream-end signal — each request
		// is independent, so we skip the deferred cleanup heuristic.
		return
	}
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

// SecretTypeSpec describes a DuckDB secret type registered by the worker.
type SecretTypeSpec struct {
	Name        string
	Description string
	Schema      *arrow.Schema // parameter schema; use field metadata {"redact":"true"} for sensitive fields
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

// serializeSecretTypeSpec serializes a SecretTypeSpec to Arrow IPC bytes.
// Format: RecordBatch with schema {name: string, description: string, parameters_schema: binary}
func serializeSecretTypeSpec(spec SecretTypeSpec) ([]byte, error) {
	if spec.Schema == nil {
		return nil, fmt.Errorf("secret type %q: Schema must not be nil", spec.Name)
	}
	mem := memory.NewGoAllocator()
	secretTypeSchema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "parameters_schema", Type: arrow.BinaryTypes.Binary, Nullable: false},
	}, nil)

	// Serialize the parameter schema
	paramSchemaBytes, err := SerializeSchema(spec.Schema)
	if err != nil {
		return nil, fmt.Errorf("serializing secret type schema: %w", err)
	}

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

	schemaBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer schemaBuilder.Release()
	schemaBuilder.Append(paramSchemaBytes)
	schemaArr := schemaBuilder.NewArray()
	defer schemaArr.Release()

	batch := array.NewRecordBatch(secretTypeSchema,
		[]arrow.Array{nameArr, descArr, schemaArr}, 1)

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(secretTypeSchema))
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
	scalars                map[string][]ScalarFunction
	tables                 map[string][]TableFunction
	tableInOuts            map[string][]TableInOutFunction
	aggregates             map[string][]AggregateFunction
	aggStorage             *aggregateStorage
	catalogName            string
	catalogComment         string
	catalogTags            map[string]string
	schemaComments         map[string]string
	catalog                *DefaultReadOnlyCatalog
	// extraCatalogs are additional catalog names this worker accepts via
	// catalog_attach. They share the worker's registered functions but
	// have their own (writable) table/schema state. Indexed by name.
	extraCatalogs map[string]*WritableCatalog
	storages               sync.Map // map[hex execution ID string]*ExecutionStorage
	settings               []SettingSpec
	catalogTables          map[string][]CatalogTable // schema_name → tables
	catalogViews           map[string][]CatalogView  // schema_name → views
	catalogMacros          map[string][]CatalogMacro // schema_name → macros
	scanFunctionGetHandler       ScanFunctionGetHandler
	tableGetHandler              TableGetHandler
	catalogInfoOverride          *CatalogInfo
	attachValidator              AttachValidator
	schemaContentsHandler        SchemaContentsHandler
	attachTableGetHandler        AttachTableGetHandler
	attachScanFunctionGetHandler AttachScanFunctionGetHandler
	authenticateFunc       vgirpc.AuthenticateFunc
	oauthMetadata          *vgirpc.OAuthResourceMetadata
	secretTypes            []SecretTypeSpec
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

// WithCatalogComment sets the comment reported by catalog_attach (surfaces in
// duckdb_databases().comment).
func WithCatalogComment(comment string) WorkerOption {
	return func(w *Worker) {
		w.catalogComment = comment
	}
}

// WithCatalogTags sets tags reported by catalog_attach (duckdb_databases().tags).
func WithCatalogTags(tags map[string]string) WorkerOption {
	return func(w *Worker) {
		w.catalogTags = tags
	}
}

// WithCatalogInfo overrides the CatalogInfo record returned by catalog_catalogs
// (used by `vgi_catalogs(location)` for discovery). Set it to advertise
// implementation_version / data_version_spec for versioned workers.
func WithCatalogInfo(info CatalogInfo) WorkerOption {
	return func(w *Worker) {
		copy := info
		w.catalogInfoOverride = &copy
	}
}

// AttachDecision is the custom response returned by an AttachValidator.
// Any non-empty ResolvedDataVersion / ResolvedImplementationVersion value
// is forwarded to the client so it appears as a duckdb_databases().tag.
// If AttachID is nil, the worker falls back to the catalog name.
type AttachDecision struct {
	ResolvedDataVersion           string
	ResolvedImplementationVersion string
	AttachID                      []byte
}

// AttachValidator is invoked by the default catalog_attach handler. It may
// inspect the requested data_version_spec / implementation_version, perform
// validation, and return resolved values. Returning an error causes the
// ATTACH to fail with that message — the test suite treats a "ValueError"
// RPC type as a user error the extension should surface verbatim.
type AttachValidator func(req *CatalogAttachRequestWire, callCtx *vgirpc.CallContext) (*AttachDecision, error)

// WithAttachValidator installs a custom attach validator. Required by the
// versioned/versioned-tables example workers.
func WithAttachValidator(v AttachValidator) WorkerOption {
	return func(w *Worker) {
		w.attachValidator = v
	}
}

// SchemaContentsHandler lets callers override catalog_schema_contents_tables
// on a per-attach-id basis (e.g. return different tables per resolved data
// version). Return nil to fall through to the default registered-tables
// behaviour.
type SchemaContentsHandler func(attachID []byte, schemaName string) ([]SerializedSchemaItem, bool)

// SerializedSchemaItem is a single pre-serialized schema item (TableInfo or
// ViewInfo IPC bytes).
type SerializedSchemaItem []byte

// WithSchemaContentsHandler installs a handler that can replace the tables
// returned for a given (attach_id, schema) pair.
func WithSchemaContentsHandler(h SchemaContentsHandler) WorkerOption {
	return func(w *Worker) {
		w.schemaContentsHandler = h
	}
}

// AttachTableGetHandler is the attach-id-aware version of TableGetHandler.
// Return (data, true) to override the default; return (nil, false) to fall
// through. Used by version-aware workers where the attach_id encodes the
// resolved version.
type AttachTableGetHandler func(attachID []byte, schemaName, name string, atUnit, atValue *string) (data []byte, handled bool, err error)

// WithAttachTableGetHandler installs an attach-id-aware table_get handler.
func WithAttachTableGetHandler(h AttachTableGetHandler) WorkerOption {
	return func(w *Worker) {
		w.attachTableGetHandler = h
	}
}

// AttachScanFunctionGetHandler is the attach-id-aware version of
// ScanFunctionGetHandler. Return (result, true) to override; (nil, false) to
// fall through.
type AttachScanFunctionGetHandler func(attachID []byte, schemaName, name string, atUnit, atValue *string) (result *ScanFunctionResult, handled bool, err error)

// WithAttachScanFunctionGetHandler installs an attach-id-aware
// scan_function_get handler.
func WithAttachScanFunctionGetHandler(h AttachScanFunctionGetHandler) WorkerOption {
	return func(w *Worker) {
		w.attachScanFunctionGetHandler = h
	}
}

// WithSchemaComments overrides the default comment for built-in schemas
// ("main" and "data"). Other schemas retain their auto-generated comments.
func WithSchemaComments(comments map[string]string) WorkerOption {
	return func(w *Worker) {
		if w.schemaComments == nil {
			w.schemaComments = map[string]string{}
		}
		for k, v := range comments {
			w.schemaComments[k] = v
		}
	}
}

// WithSettings adds custom DuckDB settings to the worker.
func WithSettings(settings ...SettingSpec) WorkerOption {
	return func(w *Worker) {
		w.settings = append(w.settings, settings...)
	}
}

// WithSecretTypes registers secret types that will be sent to DuckDB during catalog_attach.
func WithSecretTypes(types ...SecretTypeSpec) WorkerOption {
	return func(w *Worker) {
		w.secretTypes = append(w.secretTypes, types...)
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
		scalars:       make(map[string][]ScalarFunction),
		tables:        make(map[string][]TableFunction),
		tableInOuts:   make(map[string][]TableInOutFunction),
		aggregates:    make(map[string][]AggregateFunction),
		aggStorage:    newAggregateStorage(),
		extraCatalogs: make(map[string]*WritableCatalog),
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
	w.scalars[f.Name()] = append(w.scalars[f.Name()], f)
}

// RegisterTable registers a table function.
func (w *Worker) RegisterTable(f TableFunction) {
	w.tables[f.Name()] = append(w.tables[f.Name()], f)
}

// RegisterTableInOut registers a table-in-out function.
func (w *Worker) RegisterTableInOut(f TableInOutFunction) {
	w.tableInOuts[f.Name()] = append(w.tableInOuts[f.Name()], f)
}

// RegisterAggregate registers an aggregate function. Multiple registrations
// with the same Name() are kept as overloads — distinguished by their
// ArgumentSpecs at catalog-discovery time.
func (w *Worker) RegisterAggregate(f AggregateFunction) {
	w.aggregates[f.Name()] = append(w.aggregates[f.Name()], f)
}

// RegisterWritableCatalog adds a writable catalog this worker handles
// alongside its primary read-only catalog. The catalog accepts ATTACH
// requests by its name and supports DDL/DML operations on user-created
// tables.
func (w *Worker) RegisterWritableCatalog(c *WritableCatalog) {
	w.extraCatalogs[c.Name] = c
	w.registerWritableFunctions()
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

// SetTableGetHandler sets a handler for customizing catalog_table_get responses.
// Return non-nil bytes to override the default lookup; return nil to fall through.
func (w *Worker) SetTableGetHandler(h TableGetHandler) {
	w.tableGetHandler = h
}

// SetAuthenticate sets an authentication callback for HTTP mode.
// When set, every HTTP request is validated and the resulting AuthContext
// is available via ProcessParams.Auth and BindParams.Auth.
func (w *Worker) SetAuthenticate(fn vgirpc.AuthenticateFunc) {
	w.authenticateFunc = fn
}

// SetOAuthResourceMetadata configures OAuth Protected Resource Metadata
// (RFC 9728) for HTTP mode. When set, the server exposes a well-known
// endpoint and includes a WWW-Authenticate header on 401 responses.
func (w *Worker) SetOAuthResourceMetadata(m *vgirpc.OAuthResourceMetadata) {
	w.oauthMetadata = m
}


// buildServer creates and configures the vgirpc.Server with all handlers
// registered. Shared between RunStdio and RunHttp.
func (w *Worker) buildServer(isHTTP bool) *vgirpc.Server {
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
	s.SetDispatchHook(&storageCleanupHook{worker: w, isHTTP: isHTTP})

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

	// Register table_function_statistics (unary). Uses []byte result to avoid
	// vgi-rpc-go's struct-to-IPC double-wrap — the C++ extension parses the
	// IPC payload directly (see catalog_table_column_statistics_get).
	vgirpc.Unary[CardinalityRequestWire, []byte](s, "table_function_statistics", w.handleTableFunctionStatistics)

	// Register all catalog methods
	w.registerCatalogMethods(s)

	// Register aggregate RPC handlers
	w.registerAggregateRPCs(s)

	return s
}

// RunStdio runs the worker serving RPC over stdin/stdout.
func (w *Worker) RunStdio() {
	s := w.buildServer(false)
	s.RunStdio()
}

// RunHttp runs the worker serving RPC over HTTP. It listens on the given
// address (e.g. "127.0.0.1:0" for a random port) and prints "PORT:<n>" to
// stdout so callers can discover the assigned port.
func (w *Worker) RunHttp(addr string) error {
	s := w.buildServer(true)
	hs := vgirpc.NewHttpServer(s)
	hs.SetRehydrateFunc(w.rehydrateState)
	hs.SetProducerBatchLimit(100)
	if w.authenticateFunc != nil {
		hs.SetAuthenticate(w.authenticateFunc)
	}
	if w.oauthMetadata != nil {
		hs.SetOAuthResourceMetadata(w.oauthMetadata)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	port := listener.Addr().(*net.TCPAddr).Port
	fmt.Printf("PORT:%d\n", port)
	os.Stdout.Sync()

	srv := &http.Server{Handler: hs}

	// Handle SIGTERM/SIGINT for clean shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		srv.Shutdown(context.Background())
	}()

	err = srv.Serve(listener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
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
