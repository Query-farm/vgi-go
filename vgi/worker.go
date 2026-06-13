// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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
				LogWorker.Debug("storage: cleaned up execution", "key", key)
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
	scalars         map[string][]ScalarFunction
	tables          map[string][]TableFunction
	tableInOuts     map[string][]TableInOutFunction
	tableBufferings map[string][]TableBufferingFunction
	aggregates      map[string][]AggregateFunction
	aggStorage      *aggregateStorage
	// streamingSessions tracks per-execution_id state for streaming-partitioned
	// aggregates (aggregate_streaming_open/_chunk/_close).
	streamingSessions    streamingSessionStore
	catalogName          string
	catalogComment       string
	catalogTags          map[string]string
	supportsTransactions bool
	schemaComments       map[string]string
	catalog              *DefaultReadOnlyCatalog
	// extraCatalogs are additional catalog names this worker accepts via
	// catalog_attach. They share the worker's registered functions but
	// have their own (writable) table/schema state. Indexed by name.
	extraCatalogs map[string]*WritableCatalog
	// catalogAliases are extra catalog names that ATTACH against the
	// worker's primary read-only catalog (no separate writable state, no
	// distinct catalog implementation). Useful for fixture workers that
	// publish multiple cross-language reproducer catalogs (e.g.
	// projection_repro) sharing one binary.
	catalogAliases map[string]struct{}
	// catalogFunctionScope maps function-name → catalog-name when a
	// function is restricted to a single catalog (e.g. proj_repro_* only
	// appears under the ``projection_repro`` catalog, not ``example``).
	// Functions not present in the map are visible to every catalog.
	catalogFunctionScope map[string]string
	// catalogAliasInfos carries per-alias discovery metadata (data version,
	// etc.) for aliases that should advertise themselves distinctly in
	// catalog_catalogs and mint a random per-ATTACH scope at attach (so two
	// ATTACHes of the same alias are isolated). Keyed by catalog name.
	catalogAliasInfos             map[string]CatalogInfo
	storages                      sync.Map // map[hex execution ID string]*ExecutionStorage
	fsOnce                        sync.Once
	fs                            FunctionStorage
	fsErr                         error
	settings                      []SettingSpec
	catalogTables                 map[string][]CatalogTable // schema_name → tables
	catalogViews                  map[string][]CatalogView  // schema_name → views
	catalogMacros                 map[string][]CatalogMacro // schema_name → macros
	dynamicSchemas                map[string]string         // schema_name → comment (for SchemaContentsHandler-only schemas)
	scanFunctionGetHandler        ScanFunctionGetHandler
	tableGetHandler               TableGetHandler
	catalogInfoOverride           *CatalogInfo
	attachValidator               AttachValidator
	schemaContentsHandler         SchemaContentsHandler
	attachTableGetHandler         AttachTableGetHandler
	attachScanFunctionGetHandler  AttachScanFunctionGetHandler
	attachScanBranchesGetHandler  AttachScanBranchesGetHandler
	attachWriteFunctionGetHandler AttachWriteFunctionGetHandler
	catalogVersionHook            CatalogVersionHook
	authenticateFunc              vgirpc.AuthenticateFunc
	oauthMetadata                 *vgirpc.OAuthResourceMetadata
	oauthPkce                     *vgirpc.OAuthPkceConfig
	secretTypes                   []SecretTypeSpec
	attachOptions                 []AttachOptionSpec
	logLevel                      slog.Level   // slog.LevelInfo (0) by default — Info level is intentional.
	logHandler                    slog.Handler // nil means default TextHandler to stderr
	logFormat                     LogFormat    // empty means text
	logLoggers                    []string     // empty means all known loggers
	logConfigured                 bool         // true once any logging WorkerOption fires
	httpSigningKey                []byte       // optional explicit HMAC key for HTTP state tokens
}

// WorkerOption configures a Worker.
type WorkerOption func(*Worker)

// WithCatalogName sets the catalog name.
func WithCatalogName(name string) WorkerOption {
	return func(w *Worker) {
		w.catalogName = name
	}
}

// WithCatalogAliases adds extra catalog names this worker accepts via
// catalog_attach. Each alias resolves to the same primary read-only
// catalog (same registered functions, no separate state). Useful for
// fixture workers that publish multiple cross-language reproducer
// catalogs out of one binary (projection_repro, schema_reconcile, ...).
func WithCatalogAliases(names ...string) WorkerOption {
	return func(w *Worker) {
		if w.catalogAliases == nil {
			w.catalogAliases = make(map[string]struct{})
		}
		for _, n := range names {
			w.catalogAliases[n] = struct{}{}
		}
	}
}

// WithCatalogAliasInfo registers a catalog alias that advertises itself
// distinctly in catalog discovery and is isolated per ATTACH. Unlike a plain
// WithCatalogAliases entry (which shares the primary catalog's identity and a
// stable name-based attach scope), an alias registered here:
//
//   - appears as its own row in vgi_catalogs() carrying info.DataVersionSpec; and
//   - mints a random per-ATTACH scope at attach time (info.Name + NUL + random),
//     so two ATTACHes of the same alias never share attach-scoped state.
//
// info.Name should equal name. Functions meant to surface only under this alias
// must be registered with Register*ForCatalog(name, ...). Used by the accumulate
// fixture, whose per-ATTACH row collections must be isolated.
func WithCatalogAliasInfo(name string, info CatalogInfo) WorkerOption {
	return func(w *Worker) {
		if w.catalogAliases == nil {
			w.catalogAliases = make(map[string]struct{})
		}
		if w.catalogAliasInfos == nil {
			w.catalogAliasInfos = make(map[string]CatalogInfo)
		}
		if info.Name == "" {
			info.Name = name
		}
		w.catalogAliases[name] = struct{}{}
		w.catalogAliasInfos[name] = info
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

// WithSupportsTransactions makes the catalog report supports_transactions=true
// on attach, so DuckDB threads a transaction_opaque_data through bind/scan
// inside BEGIN/COMMIT (needed by transaction-scoped storage like tx_cached_value).
func WithSupportsTransactions(v bool) WorkerOption {
	return func(w *Worker) {
		w.supportsTransactions = v
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
// If AttachOpaqueData is nil, the worker falls back to the catalog name.
type AttachDecision struct {
	ResolvedDataVersion           string
	ResolvedImplementationVersion string
	AttachOpaqueData              []byte
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
// on a per-attach-opaque-data basis (e.g. return different tables per resolved data
// version). Return nil to fall through to the default registered-tables
// behaviour.
type SchemaContentsHandler func(attachOpaqueData []byte, schemaName string) ([]SerializedSchemaItem, bool)

// SerializedSchemaItem is a single pre-serialized schema item (TableInfo or
// ViewInfo IPC bytes).
type SerializedSchemaItem []byte

// WithSchemaContentsHandler installs a handler that can replace the tables
// returned for a given (attach_opaque_data, schema) pair.
func WithSchemaContentsHandler(h SchemaContentsHandler) WorkerOption {
	return func(w *Worker) {
		w.schemaContentsHandler = h
	}
}

// AttachTableGetHandler is the attach-opaque-data-aware version of TableGetHandler.
// Return (data, true) to override the default; return (nil, false) to fall
// through. Used by version-aware workers where the attach_opaque_data encodes the
// resolved version.
type AttachTableGetHandler func(attachOpaqueData []byte, schemaName, name string, atUnit, atValue *string) (data []byte, handled bool, err error)

// WithAttachTableGetHandler installs an attach-opaque-data-aware table_get handler.
func WithAttachTableGetHandler(h AttachTableGetHandler) WorkerOption {
	return func(w *Worker) {
		w.attachTableGetHandler = h
	}
}

// AttachScanFunctionGetHandler is the attach-opaque-data-aware version of
// ScanFunctionGetHandler. Return (result, true) to override; (nil, false) to
// fall through.
type AttachScanFunctionGetHandler func(attachOpaqueData []byte, schemaName, name string, atUnit, atValue *string) (result *ScanFunctionResult, handled bool, err error)

// WithAttachScanFunctionGetHandler installs an attach-opaque-data-aware
// scan_function_get handler.
func WithAttachScanFunctionGetHandler(h AttachScanFunctionGetHandler) WorkerOption {
	return func(w *Worker) {
		w.attachScanFunctionGetHandler = h
	}
}

// AttachScanBranchesGetHandler is the attach-opaque-data-aware handler for
// catalog_table_scan_branches_get. Return (result, true) to serve a
// multi-branch table; (nil, false) to fall through (the C++ extension then
// falls back to catalog_table_scan_function_get).
type AttachScanBranchesGetHandler func(attachOpaqueData []byte, schemaName, name string, atUnit, atValue *string) (result *ScanBranchesResult, handled bool, err error)

// WithAttachScanBranchesGetHandler installs an attach-opaque-data-aware
// scan_branches_get handler for multi-branch (UNION-of-sources) tables.
func WithAttachScanBranchesGetHandler(h AttachScanBranchesGetHandler) WorkerOption {
	return func(w *Worker) {
		w.attachScanBranchesGetHandler = h
	}
}

// AttachWriteFunctionGetHandler is the attach-opaque-data-aware version of
// catalog_table_{insert,update,delete}_function_get. Op is WriteOpInsert,
// WriteOpUpdate, or WriteOpDelete. Return (result, true) to route; (nil, false)
// to fall through to the built-in writable-catalog path or the read-only
// rejection.
type AttachWriteFunctionGetHandler func(op WriteOp, attachOpaqueData []byte, schemaName, name string) (result *ScanFunctionResult, handled bool, err error)

// WithAttachWriteFunctionGetHandler installs an attach-opaque-data-aware handler
// that resolves the worker function backing INSERT/UPDATE/DELETE on a
// given (attach_opaque_data, schema, table). Use this for fixture catalogs that
// publish writable tables outside of the built-in WritableCatalog path
// (e.g. schema_reconcile, which has its own per-table SQLite store and
// strict-schema validators).
func WithAttachWriteFunctionGetHandler(h AttachWriteFunctionGetHandler) WorkerOption {
	return func(w *Worker) {
		w.attachWriteFunctionGetHandler = h
	}
}

// CatalogVersionHook runs on every catalog_version RPC before the response is
// returned. Returning a non-nil error causes the RPC to fail with that
// message (wrapped as a ValueError). The hook can inspect call-context
// cookies to assert the HTTP cookie jar is round-tripping correctly — a
// useful regression check for versioned workers that set a sticky cookie at
// ATTACH time.
type CatalogVersionHook func(attachOpaqueData []byte, callCtx *vgirpc.CallContext) error

// WithCatalogVersionHook installs a hook that runs on every catalog_version
// RPC. Use it to assert invariants like cookie presence on HTTP transport.
func WithCatalogVersionHook(h CatalogVersionHook) WorkerOption {
	return func(w *Worker) {
		w.catalogVersionHook = h
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

// WithAttachOptions declares ATTACH-time options accepted by the worker.
// These are advertised via catalog_catalogs so DuckDB can validate option
// names/types at ATTACH time. Values passed at ATTACH arrive as the
// CatalogAttachRequestWire.Options RecordBatch.
func WithAttachOptions(opts ...AttachOptionSpec) WorkerOption {
	return func(w *Worker) {
		w.attachOptions = append(w.attachOptions, opts...)
	}
}

// WithLogLevel sets the minimum log level for the default handler.
// The zero value (slog.LevelInfo) logs Info and above.
func WithLogLevel(level slog.Level) WorkerOption {
	return func(w *Worker) {
		w.logLevel = level
		w.logConfigured = true
	}
}

// WithLogFormat selects the stderr log format. Default is text.
// Ignored when WithLogHandler is also set (the custom handler wins).
func WithLogFormat(format LogFormat) WorkerOption {
	return func(w *Worker) {
		w.logFormat = format
		w.logConfigured = true
	}
}

// WithLoggers restricts which named loggers emit records. Use to silence
// noisy subsystems or focus on one (e.g. WithLoggers("vgi.catalog")).
// Empty means all known loggers. Unknown names log a warning but are honored.
func WithLoggers(names ...string) WorkerOption {
	return func(w *Worker) {
		w.logLoggers = append(w.logLoggers, names...)
		w.logConfigured = true
	}
}

// WithHttpSigningKey configures the HMAC key used by the HTTP transport
// to sign opaque state tokens. When unset, the HTTP server generates a
// random key at startup — fine for ephemeral workers, but in-flight
// state tokens become invalid on restart. Pass a stable key (≥16 bytes,
// typically from VGI_SIGNING_KEY) for production deployments.
//
// Has no effect when running in stdio mode.
func WithHttpSigningKey(key []byte) WorkerOption {
	return func(w *Worker) {
		w.httpSigningKey = key
	}
}

// WithLogHandler sets a custom slog.Handler for all logging.
// When set, WithLogLevel/WithLogFormat/WithLoggers are ignored — the handler
// controls its own level and formatting. The package's named loggers are
// re-bound to the supplied handler at worker startup.
func WithLogHandler(h slog.Handler) WorkerOption {
	return func(w *Worker) {
		w.logHandler = h
		w.logConfigured = true
	}
}

// WithFunctionStorage injects an explicit FunctionStorage backend. When
// unset, the worker defaults to a local SQLite backend at the per-user
// state path. Use this to wire a Cloudflare Durable Object client or any
// other backend implementing the FunctionStorage interface; combine with
// vgi/storage/resolve.FromEnv to get vgi-python-style env-driven selection
// (VGI_WORKER_SHARED_STORAGE=sqlite|cloudflare-do).
func WithFunctionStorage(s FunctionStorage) WorkerOption {
	return func(w *Worker) {
		w.fs = s
		// Mark fsOnce as done so functionStorage() returns the injected
		// instance without trying to construct a default.
		w.fsOnce.Do(func() {})
	}
}

// NewWorker creates a new VGI worker.
func NewWorker(opts ...WorkerOption) *Worker {
	w := &Worker{
		scalars:              make(map[string][]ScalarFunction),
		tables:               make(map[string][]TableFunction),
		tableInOuts:          make(map[string][]TableInOutFunction),
		tableBufferings:      make(map[string][]TableBufferingFunction),
		aggregates:           make(map[string][]AggregateFunction),
		aggStorage:           newAggregateStorage(),
		extraCatalogs:        make(map[string]*WritableCatalog),
		catalogAliases:       make(map[string]struct{}),
		catalogFunctionScope: make(map[string]string),
		catalogAliasInfos:    make(map[string]CatalogInfo),
		catalogTables:        make(map[string][]CatalogTable),
		catalogViews:         make(map[string][]CatalogView),
		catalogMacros:        make(map[string][]CatalogMacro),
		dynamicSchemas:       make(map[string]string),
		catalogName:          "example",
	}
	for _, opt := range opts {
		opt(w)
	}
	// Wire aggregateStorage to the same shared FunctionStorage backend that
	// ExecutionStorage uses, so aggregate state, work queues, and worker
	// state all live in one database (or one HTTP endpoint, for cloud
	// backends).
	w.aggStorage.setResolver(w.functionStorage)
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

// RegisterTableForCatalog registers a table function scoped to a single
// catalog name. The function is invokable as normal but only surfaces in
// catalog_schema_contents_functions / duckdb_functions for ATTACH calls
// against the named catalog. Use for fixture functions that should only
// be visible to their own reproducer catalog (e.g. proj_repro_* under
// “projection_repro“).
func (w *Worker) RegisterTableForCatalog(catalogName string, f TableFunction) {
	w.tables[f.Name()] = append(w.tables[f.Name()], f)
	w.catalogFunctionScope[f.Name()] = catalogName
}

// RegisterTableInOut registers a table-in-out function.
func (w *Worker) RegisterTableInOut(f TableInOutFunction) {
	w.tableInOuts[f.Name()] = append(w.tableInOuts[f.Name()], f)
}

// RegisterTableInOutForCatalog is the catalog-scoped sibling of
// RegisterTableInOut; the function is invokable as normal but only
// surfaces under the named catalog's function listing. See
// RegisterTableForCatalog for the rationale (per-catalog function
// inventories that fixture tests assert on).
func (w *Worker) RegisterTableInOutForCatalog(catalogName string, f TableInOutFunction) {
	w.tableInOuts[f.Name()] = append(w.tableInOuts[f.Name()], f)
	w.catalogFunctionScope[f.Name()] = catalogName
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
// RegisterCatalogSchema declares a schema that exists in the catalog but
// whose tables are produced dynamically by a SchemaContentsHandler. Use
// when there are no registered CatalogTable/View/Macro entries to "anchor"
// the schema (otherwise it wouldn't appear in catalog_schemas).
func (w *Worker) RegisterCatalogSchema(name, comment string) {
	w.dynamicSchemas[name] = comment
}

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

// SetOAuthPkce enables the browser-based OAuth PKCE login flow for HTTP mode.
// It requires SetAuthenticate and SetOAuthResourceMetadata (with a ClientID) to
// also be configured; RunHttp applies it after both. When enabled, the server
// serves /_oauth/callback, /_oauth/logout, and the /_oauth/token exchange proxy,
// and redirects unauthenticated browser GETs to the authorization server.
func (w *Worker) SetOAuthPkce(cfg vgirpc.OAuthPkceConfig) {
	c := cfg
	w.oauthPkce = &c
}

// buildServer creates and configures the vgirpc.Server with all handlers
// registered. Shared between RunStdio and RunHttp.
// serverTransport selects how the worker serves and how execution-storage
// cleanup is handled.
type serverTransport int

const (
	transportStdio serverTransport = iota // single serial stream — deferred cleanup
	transportHTTP                         // independent requests — no deferred cleanup (TTL sweep)
	transportUnix                         // concurrent connections — no cleanup hook (TTL sweep)
)

func (w *Worker) buildServer(transport serverTransport) *vgirpc.Server {
	// Configure structured logging.
	// If a custom slog.Handler was supplied, install it directly — the caller
	// owns level/format. Otherwise (re)configure the named-logger registry
	// from the WorkerOptions. When no logging option fired, leave whatever
	// ConfigureLogging (or the caller) installed alone, so main() can drive
	// the configuration via ParseLoggingFlags.
	if w.logHandler != nil {
		loggerRegistryLock.Lock()
		root := slog.New(w.logHandler)
		slog.SetDefault(root)
		Log = root
		LogWorker = root.With("logger", LoggerNameWorker)
		LogCatalog = root.With("logger", LoggerNameCatalog)
		LogRPC = root.With("logger", LoggerNameRPC)
		LogClient = root.With("logger", LoggerNameClient)
		LogFilterPushdown = root.With("logger", LoggerNameFilterPushdown)
		loggerRegistryLock.Unlock()
	} else if w.logConfigured {
		ConfigureLogging(LoggingConfig{
			Level:   w.logLevel,
			Format:  w.logFormat,
			Loggers: w.logLoggers,
		})
	}

	// Build catalog from registered functions
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)

	s := vgirpc.NewServer()
	// Execution-storage cleanup. The deferred-cleanup heuristic assumes a single
	// serial dispatch stream (stdio); HTTP disables the deferred step. The unix
	// (launcher) transport serves concurrent connections, where the hook's
	// per-dispatch state is not safe to share — skip the hook entirely and rely
	// on the framework's explicit per-execution clears at end-of-query.
	switch transport {
	case transportStdio:
		s.SetDispatchHook(&storageCleanupHook{worker: w, isHTTP: false})
	case transportHTTP:
		s.SetDispatchHook(&storageCleanupHook{worker: w, isHTTP: true})
	case transportUnix:
		// no cleanup hook
	}

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
	w.registerAggregateStreamingRPCs(s)

	// Optional table-function profiling hook (EXPLAIN ANALYZE Extra Info).
	w.registerDynamicToStringRPCs(s)

	// Table-buffering sink RPCs (process/combine/destructor).
	w.registerTableBufferingRPCs(s)

	return s
}

// RunStdio runs the worker serving RPC over stdin/stdout.
func (w *Worker) RunStdio() {
	s := w.buildServer(transportStdio)
	s.RunStdio()
}

// RunUnix runs the worker serving RPC over an AF_UNIX socket at path — the
// "launcher" transport. It prints the readiness marker "UNIX:<path>" to stdout
// once the socket is listening (then writes nothing further to stdout), and
// self-shuts-down after idleTimeout with no active connections (<=0 disables
// the timeout). Mutually exclusive with HTTP.
func (w *Worker) RunUnix(path string, idleTimeout time.Duration) error {
	s := w.buildServer(transportUnix)
	return s.RunUnix(path, idleTimeout, func(bound string) {
		fmt.Println("UNIX:" + bound)
		os.Stdout.Sync()
	})
}

// RunHttp runs the worker serving RPC over HTTP. It listens on the given
// address (e.g. "127.0.0.1:0" for a random port) and prints "PORT:<n>" to
// stdout so callers can discover the assigned port.
func (w *Worker) RunHttp(addr string) error {
	s := w.buildServer(transportHTTP)
	// Resolve the signing key once so the worker (catalog opaque-data
	// sealing — see crypto.go) and the HTTP state-token machinery share the
	// same key. Generate an ephemeral per-process key when the operator did
	// not configure one via WithHttpSigningKey: sealed values are then valid
	// for the life of this process, and clients re-ATTACH after a restart.
	if len(w.httpSigningKey) == 0 {
		w.httpSigningKey = make([]byte, 32)
		if _, err := rand.Read(w.httpSigningKey); err != nil {
			return fmt.Errorf("http signing key: %w", err)
		}
	}
	hs, err := vgirpc.NewHttpServerWithKey(s, w.httpSigningKey)
	if err != nil {
		return fmt.Errorf("http signing key: %w", err)
	}
	hs.SetRehydrateFunc(w.rehydrateState)
	hs.SetProducerBatchLimit(100)
	// Honor framework-level HTTP env config. SetPrefix rebuilds the route
	// table, so apply it before the OAuth setup (which derives redirect/proxy
	// URLs and routes from the prefix).
	if prefix := os.Getenv("VGI_HTTP_PREFIX"); prefix != "" {
		hs.SetPrefix(prefix)
	}
	if cors := os.Getenv("VGI_HTTP_CORS_ORIGINS"); cors != "" {
		hs.SetCorsOrigins(cors)
	}
	if w.authenticateFunc != nil {
		hs.SetAuthenticate(w.authenticateFunc)
	}
	if w.oauthMetadata != nil {
		if err := hs.SetOAuthResourceMetadata(w.oauthMetadata); err != nil {
			return fmt.Errorf("oauth resource metadata: %w", err)
		}
	}
	if w.oauthPkce != nil {
		if err := hs.SetOAuthPkce(*w.oauthPkce); err != nil {
			return fmt.Errorf("oauth pkce: %w", err)
		}
		// SetOAuthPkce flips on the /_oauth/* routes, which are only wired
		// during route (re)build. Re-init pages now so they register
		// deterministically rather than lazily on the first request.
		hs.InitPages()
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

// getOrCreateStorage returns or creates an ExecutionStorage for the given
// execution ID. The wrapper binds against the worker's shared FunctionStorage
// backend (initialized lazily on first call) so every execution in the
// worker uses one backend — one SQLite DB shared across all executions and
// across worker subprocesses spawned for parallel scans.
func (w *Worker) getOrCreateStorage(ctx context.Context, executionID []byte, shardKey string) (*ExecutionStorage, error) {
	key := hex.EncodeToString(executionID)
	if tracker, ok := ctx.Value(storageTrackerKey).(*storageTracker); ok {
		tracker.track(key)
	}
	if s, ok := w.storages.Load(key); ok {
		es := s.(*ExecutionStorage)
		es.SetShardKey(shardKey)
		return es, nil
	}
	back, err := w.functionStorage()
	if err != nil {
		return nil, err
	}
	s := NewExecutionStorage()
	s.SetBackend(back)
	if err := s.SetExecutionID(executionID); err != nil {
		return nil, err
	}
	s.SetShardKey(shardKey)
	actual, _ := w.storages.LoadOrStore(key, s)
	es := actual.(*ExecutionStorage)
	es.SetShardKey(shardKey)
	return es, nil
}

// functionStorage returns the worker's shared FunctionStorage backend,
// constructed on first call. Today this is always a SQLite backend at the
// per-user default path; an env-driven selector ("VGI_WORKER_SHARED_STORAGE")
// can be added later to pick alternative backends (Cloudflare DO, etc.).
func (w *Worker) functionStorage() (FunctionStorage, error) {
	w.fsOnce.Do(func() {
		s, err := NewSQLiteStorage(SQLiteStorageOptions{})
		if err != nil {
			w.fsErr = err
			return
		}
		w.fs = s
	})
	return w.fs, w.fsErr
}
