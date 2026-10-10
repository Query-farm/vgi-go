// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

// catalogNameOf returns the catalog name carried in an unwrapped attach
// plaintext: the bytes before the first NUL separator, or the whole value when
// there is none. Alias-info catalogs (WithCatalogAliasInfo) mint
// "<name>\x00<random>" so each ATTACH is unique; plain catalogs/aliases use the
// bare name. Catalog-scoped function visibility compares against this name.
func catalogNameOf(attachOpaqueData []byte) string {
	if i := bytes.IndexByte(attachOpaqueData, 0); i >= 0 {
		return string(attachOpaqueData[:i])
	}
	return string(attachOpaqueData)
}

// catalogOfAttach names the catalog an unwrapped attach plaintext belongs to.
// Every function is homed in exactly one catalog, so both the function listing
// and bind dispatch need one catalog name per request — this is where it comes
// from.
//
// Writable catalogs mint their own "writable:<name>" plaintext, so their name is
// recovered through that mapping; every other catalog carries its name up front.
// A plaintext naming no catalog this worker serves — an attach validator that
// replaced it with its own encoding (a resolved data version, encoded ATTACH
// options), or no attachment at all — resolves to the worker's own catalog,
// which is the only catalog such a worker can mean.
func (w *Worker) catalogOfAttach(attachOpaqueData []byte) string {
	if c := w.writableByAttachOpaqueData(attachOpaqueData); c != nil {
		return c.Name
	}
	// A routed catalog's attach (RegisterSubCatalog / RegisterMemoryCatalog)
	// names it; its functions are homed there.
	if name, _, ok := parseRoutedAttach(attachOpaqueData); ok && w.routedCatalog(name) {
		return name
	}
	name := catalogNameOf(attachOpaqueData)
	if name == w.catalogName {
		return name
	}
	if _, ok := w.catalogAliases[name]; ok {
		return name
	}
	if _, ok := w.extraCatalogs[name]; ok {
		return name
	}
	return w.catalogName
}

// catalogOfAttachPtr names the catalog for a request that carries the attach
// value as an optional wire field (the unary aggregate / table-buffering RPCs).
// It opens the envelope down to the catalog's own plaintext first, then applies
// catalogOfAttach. Returns the worker's own catalog when there is no attachment
// or it cannot be opened — the same single-home rule the bind path uses.
func (w *Worker) catalogOfAttachPtr(attachOpaqueData *[]byte, cc *vgirpc.CallContext) string {
	if attachOpaqueData == nil {
		return w.catalogName
	}
	plain, err := w.openAttach(*attachOpaqueData, cc)
	if err != nil {
		return w.catalogName
	}
	return w.catalogOfAttach(plain)
}

// SerializedItems is a list of Arrow-IPC-encoded items sent over the wire.
type SerializedItems = [][]byte

// ---------------------------------------------------------------------------
// Catalog wire types
// ---------------------------------------------------------------------------

// CatalogsResponseWire wraps the list of serialized CatalogInfo records.
type CatalogsResponseWire struct {
	Items SerializedItems `vgirpc:"items"`
}

// CatalogAttachRequestWire is the wire type for catalog_attach.
type CatalogAttachRequestWire struct {
	Name                  string  `vgirpc:"name"`
	Options               *[]byte `vgirpc:"options"`
	DataVersionSpec       *string `vgirpc:"data_version_spec"`
	ImplementationVersion *string `vgirpc:"implementation_version"`
	// P5 client capabilities: the engine's name, the formats and catalogs it
	// can bind natively, whether it can stream, and which filter encodings it
	// speaks. Absent from this struct until 2026-08-21, so the C++ client sent
	// a 5-column batch that this worker described as 4 — tolerated silently
	// until vgi-rpc-go began validating the parameter contract before
	// dispatch, at which point every ATTACH failed.
	ClientCapabilities *[]byte `vgirpc:"client_capabilities"`
}

// CatalogAttachResultWire is the wire type for the catalog_attach result.
//
// It is generated from vgi-python's CatalogAttachResult (see
// generated/protocol_types.go), so a field the protocol appends lands here in
// the right position with the right nullability instead of being mirrored by
// hand. The alias keeps the established name.
type CatalogAttachResultWire = generated.CatalogAttachResult

// CatalogVersionResponseWire wraps the version number.
type CatalogVersionResponseWire struct {
	Version int64 `vgirpc:"version"`
}

// ItemsResponseWire wraps a list of serialized items (schemas/tables/views/functions).
type ItemsResponseWire struct {
	Items SerializedItems `vgirpc:"items"`
}

// TransactionBeginResponseWire wraps optional transaction ID.
type TransactionBeginResponseWire struct {
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// CatalogCreateRequestWire is for catalog_create.
type CatalogCreateRequestWire struct {
	Name       string  `vgirpc:"name"`
	OnConflict string  `vgirpc:"on_conflict,enum"`
	Options    *[]byte `vgirpc:"options"`
}

// TableScanFunctionGetResponseWire wraps the scan function result.
// Fields are serialized directly (not wrapped in a binary "result" column)
// so the C++ extension's ExtractAndDeserializeResult can find them.
type TableScanFunctionGetResponseWire struct {
	FunctionName       string   `vgirpc:"function_name"`
	Arguments          []byte   `vgirpc:"arguments"`
	RequiredExtensions []string `vgirpc:"required_extensions"`
	// SchemaPath is the catalog schema FunctionName is registered in (protocol
	// 1.5.0). Nullable: nil means "no VGI-side schema to report" and the client
	// falls back to its pre-1.5.0 table-schema/default-schema heuristic.
	SchemaPath *[]string `vgirpc:"schema_path"`
}

// TableScanBranchesGetResponseWire wraps a ScanBranchesResult. Branches holds
// one IPC-serialized ScanBranch per physical source; field names/types match
// generated.ScanBranchesResultSchema.
type TableScanBranchesGetResponseWire struct {
	Branches           [][]byte `vgirpc:"branches"`
	RequiredExtensions []string `vgirpc:"required_extensions"`
}

// MacroCreateRequestWire is for catalog_macro_create.
type MacroCreateRequestWire struct {
	AttachOpaqueData       []byte   `vgirpc:"attach_opaque_data"`
	SchemaPath             []string `vgirpc:"schema_path"`
	Name                   string   `vgirpc:"name"`
	MacroType              string   `vgirpc:"macro_type,enum"`
	Parameters             []string `vgirpc:"parameters"`
	Definition             string   `vgirpc:"definition"`
	OnConflict             string   `vgirpc:"on_conflict,enum"`
	ParameterDefaultValues *[]byte  `vgirpc:"parameter_default_values"`
	// ArgumentsSchema is the optional macro arguments schema (Arrow IPC schema
	// bytes): one nullable field per parameter, in Parameters order, each
	// carrying its description via the vgi_doc field-metadata key. nil when no
	// per-parameter docs are supplied. Decode with MacroParameterDocsFromSchema.
	ArgumentsSchema       *[]byte `vgirpc:"arguments_schema"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableCreateRequestWire is for catalog_table_create.
type TableCreateRequestWire struct {
	AttachOpaqueData      []byte    `vgirpc:"attach_opaque_data"`
	SchemaPath            []string  `vgirpc:"schema_path"`
	Name                  string    `vgirpc:"name"`
	Columns               []byte    `vgirpc:"columns"`
	OnConflict            string    `vgirpc:"on_conflict,enum"`
	NotNullConstraints    []int32   `vgirpc:"not_null_constraints"`
	UniqueConstraints     [][]int32 `vgirpc:"unique_constraints"`
	CheckConstraints      []string  `vgirpc:"check_constraints"`
	PrimaryKeyConstraints [][]int32 `vgirpc:"primary_key_constraints"`
	ForeignKeyConstraints [][]byte  `vgirpc:"foreign_key_constraints"`
	TransactionOpaqueData *[]byte   `vgirpc:"transaction_opaque_data"`
}

// ---------------------------------------------------------------------------
// DefaultReadOnlyCatalog
// ---------------------------------------------------------------------------

// DefaultReadOnlyCatalog auto-generates from registered functions.
type DefaultReadOnlyCatalog struct {
	catalogName      string
	schemas          map[string]*catalogSchemaInfo
	version          int64
	attachOpaqueData []byte
	// functionCache holds the built function listings (see
	// catalog_function_listing.go). The catalog is immutable once built, so
	// they never go stale; a rebuilt catalog starts with an empty cache.
	functionCache functionListingCache
}

type catalogSchemaInfo struct {
	info      *SchemaInfo
	functions []FunctionInfo
	tables    []CatalogTable
	views     []CatalogView
	macros    []CatalogMacro
}

func resolvedFilterSemanticProfiles(meta FunctionMetadata) []string {
	if len(meta.FilterSemanticProfiles) != 0 {
		return meta.FilterSemanticProfiles
	}
	if meta.FilterPushdown {
		return []string{"vgi.duckdb.standard.v1"}
	}
	return nil
}

// NewDefaultReadOnlyCatalog creates a catalog from registered functions.
func NewDefaultReadOnlyCatalog(catalogName string, w *Worker) *DefaultReadOnlyCatalog {
	cat := &DefaultReadOnlyCatalog{
		catalogName: catalogName,
		schemas:     make(map[string]*catalogSchemaInfo),
		version:     1,
	}

	// schemaFor returns the schema entry for name, creating it on first use.
	// Functions are placed in the schema they were *declared* in (see
	// Worker.funcOrigins), so a schema may come into existence purely because a
	// function names it.
	schemaFor := func(name string) *catalogSchemaInfo {
		if si, ok := cat.schemas[name]; ok {
			return si
		}
		comment := name + " schema"
		switch name {
		case "main":
			comment = "Default schema containing all registered functions"
		case "data":
			comment = "Data schema"
		}
		if c, ok := w.schemaComments[name]; ok {
			comment = c
		}
		si := &catalogSchemaInfo{
			info: &SchemaInfo{
				Path:    strings.Split(name, "\x00"),
				Comment: comment,
			},
		}
		cat.schemas[name] = si
		return si
	}

	// "main" always exists, even for a worker that registers nothing.
	schemaFor("main")

	// Helper to build FunctionInfo from any function type
	buildFunctionInfo := func(name string, ft FunctionType, meta FunctionMetadata, specs []ArgSpec) FunctionInfo {
		fi := FunctionInfo{
			Name:                   name,
			SchemaPath:             SchemaPath{"main"},
			FunctionType:           ft,
			Stability:              meta.Stability,
			NullHandling:           meta.NullHandling,
			ArgumentMonotonicity:   meta.ArgumentMonotonicity,
			Description:            meta.Description,
			Categories:             meta.Categories,
			Tags:                   meta.Tags,
			Examples:               meta.Examples,
			ArgSchema:              BuildArgSchema(specs),
			OutputSchema:           arrow.NewSchema(nil, nil), // empty, resolved at bind time
			ParameterDefaultValues: meta.ParameterDefaultValues,
			RequiredSecrets:        meta.RequiredSecrets,
		}
		if meta.ProjectionPushdown {
			v := true
			fi.ProjectionPushdown = &v
		}
		if meta.FilterPushdown {
			v := true
			fi.FilterPushdown = &v
		}
		if meta.SamplingPushdown {
			v := true
			fi.SamplingPushdown = &v
		}
		if meta.LateMaterialization {
			v := true
			fi.LateMaterialization = &v
		}
		fi.FilterSemanticProfiles = resolvedFilterSemanticProfiles(meta)
		fi.AdditionalFilterFunctions = meta.AdditionalFilterFunctions
		fi.RuntimeFilterAlgorithms = meta.RuntimeFilterAlgorithms
		fi.FilterEvaluationContexts = meta.FilterEvaluationContexts
		if meta.OrderPreservation != "" {
			fi.OrderPreservation = meta.OrderPreservation
		}
		fi.SupportsBatchIndex = meta.SupportsBatchIndex
		fi.SupportsSplits = meta.SupportsSplits
		fi.FiltersExactlyApplied = meta.FiltersExactlyApplied
		fi.SupportsPositions = meta.SupportsPositions
		fi.SplitTokenTTLSeconds = meta.SplitTokenTTLSeconds
		if meta.PartitionKind != "" {
			fi.PartitionKind = meta.PartitionKind
		}
		fi.SourceOrderDependent = meta.SourceOrderDependent
		fi.SinkOrderDependent = meta.SinkOrderDependent
		fi.RequiresInputBatchIndex = meta.RequiresInputBatchIndex
		return fi
	}

	// Scalar functions need a 1-field output schema for DuckDB.
	// Use the concrete return type if declared, otherwise null with vgi:any metadata.
	dynamicOutputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.Null, Metadata: arrow.NewMetadata(
			[]string{"vgi:any"}, []string{"true"},
		)},
	}, nil)

	// place files a FunctionInfo under its registration's home — the
	// (catalog, schema) pair recorded in Worker.funcOrigins, index-aligned with
	// the registry slice. The home lives per-registration rather than per-name
	// so a name declared twice — in two schemas, or in two catalogs served by
	// this worker — stays two distinct entries.
	place := func(kind funcKind, name string, idx int, fi FunctionInfo) {
		origin := w.originOf(kind, name, idx)
		// A routed catalog's functions are listed (and their schemas
		// created) by that catalog, not this one.
		if w.routedCatalog(origin.catalog) {
			return
		}
		fi.SchemaPath = strings.Split(origin.schema, "\x00")
		fi.catalogHome = origin.catalog
		fi.unlisted = origin.unlisted
		si := schemaFor(origin.schema)
		si.functions = append(si.functions, fi)
	}

	// Registries are maps; walk them in name order so every listing (and so
	// every catalog_contents snapshot and its content hash) comes out in the
	// same order on every build. Overloads keep their registration order.
	for _, name := range slices.Sorted(maps.Keys(w.scalars)) {
		fns := w.scalars[name]
		for i, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeScalar, meta, fn.ArgumentSpecs())
			if meta.ReturnType != nil {
				fi.OutputSchema = arrow.NewSchema([]arrow.Field{
					{Name: "result", Type: meta.ReturnType},
				}, nil)
			} else {
				fi.OutputSchema = dynamicOutputSchema
			}
			place(kindScalar, name, i, fi)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(w.tables)) {
		fns := w.tables[name]
		for i, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeTable, meta, fn.ArgumentSpecs())
			place(kindTable, name, i, fi)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(w.tableInOuts)) {
		fns := w.tableInOuts[name]
		for i, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeTable, meta, fn.ArgumentSpecs()) // table-in-out registers as "table"
			fi.HasFinalize = meta.HasFinalize
			// Blended ("UNNEST-style"): positional args ARE the per-row input
			// columns — the C++ extension reads this to register the function
			// with real-typed args serving literal / column / LATERAL shapes.
			fi.InputFromArgs = meta.InputFromArgs
			place(kindTableInOut, name, i, fi)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(w.tableBufferings)) {
		fns := w.tableBufferings[name]
		for i, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeTableBuffering, meta, fn.ArgumentSpecs())
			place(kindTableBuffering, name, i, fi)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(w.aggregates)) {
		fns := w.aggregates[name]
		for i, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeAggregate, meta, fn.ArgumentSpecs())
			if meta.ReturnType != nil {
				fi.OutputSchema = arrow.NewSchema([]arrow.Field{
					{Name: "result", Type: meta.ReturnType},
				}, nil)
			} else {
				fi.OutputSchema = dynamicOutputSchema
			}
			fi.SupportsWindow = meta.SupportsWindow
			fi.StreamingPartitioned = meta.StreamingPartitioned
			fi.OrderDependent = meta.OrderDependent
			fi.DistinctDependent = meta.DistinctDependent
			place(kindAggregate, name, i, fi)
		}
	}

	// Add "data" schema only when the worker actually registers tables,
	// views, macros, or an explicit comment for it. Empty workers (e.g. the
	// versioned-example worker) should expose main only. (A function declared
	// into "data" has already created it above.)
	_, hasDataTables := w.catalogTables["data"]
	_, hasDataViews := w.catalogViews["data"]
	_, hasDataMacros := w.catalogMacros["data"]
	_, hasDataComment := w.schemaComments["data"]
	if hasDataTables || hasDataViews || hasDataMacros || hasDataComment {
		schemaFor("data")
	}

	// Populate dynamic-only schemas (those without any registered table/view/macro
	// — they exist solely so the catalog enumeration sees them and lets the
	// SchemaContentsHandler take over).
	for name, comment := range w.dynamicSchemas {
		if _, ok := cat.schemas[name]; ok {
			continue
		}
		cat.schemas[name] = &catalogSchemaInfo{
			info: &SchemaInfo{Path: strings.Split(name, "\x00"), Comment: comment},
		}
	}

	// Populate catalog tables from worker registrations
	for schemaPath, tables := range w.catalogTables {
		si := schemaFor(schemaPath)
		si.tables = append(si.tables, tables...)
	}

	// Populate catalog views from worker registrations
	for schemaPath, views := range w.catalogViews {
		si := schemaFor(schemaPath)
		si.views = append(si.views, views...)
	}

	// Populate catalog macros from worker registrations
	for schemaPath, macros := range w.catalogMacros {
		si := schemaFor(schemaPath)
		si.macros = append(si.macros, macros...)
	}

	// Populate estimated_object_count on every schema so the C++ extension can
	// skip catalog_schema_contents_* / catalog_*_get RPCs for kinds it knows are
	// empty. Counts are exact for this read-only catalog. Keys match the C++
	// extension's set-kind names: table, view, scalar_function, aggregate_function,
	// table_function, macro, index.
	//
	// Workers that supply tables dynamically (tableGetHandler /
	// schemaContentsHandler / attachTableGetHandler / scanFunctionGetHandler)
	// can extend the catalog beyond the statically-registered set. A zero
	// `table` guarantee would make the C++ client skip the lookup RPC
	// entirely, so we omit the `table` key for those workers and let the
	// bulk RPC drive discovery. Other kinds (view, macro, scalar/aggregate/
	// table_function, index) are still safe to count because the SDK has no
	// dynamic-handler hook for them.
	tablesAreDynamic := w.tableGetHandler != nil ||
		w.schemaContentsHandler != nil ||
		w.attachTableGetHandler != nil ||
		w.scanFunctionGetHandler != nil ||
		w.attachScanFunctionGetHandler != nil
	for _, si := range cat.schemas {
		var nScalar, nAggregate, nTable int64
		for _, fi := range si.functions {
			switch fi.FunctionType {
			case FunctionTypeScalar:
				nScalar++
			case FunctionTypeAggregate:
				nAggregate++
			case FunctionTypeTable, FunctionTypeTableBuffering:
				nTable++
			}
		}
		counts := map[string]int64{
			"view":               int64(len(si.views)),
			"macro":              int64(len(si.macros)),
			"scalar_function":    nScalar,
			"aggregate_function": nAggregate,
			"table_function":     nTable,
			"index":              0,
		}
		if !tablesAreDynamic {
			counts["table"] = int64(len(si.tables))
		}
		si.info.EstimatedObjectCount = counts
	}

	// Apply worker-configured schema-level tags (WithSchemaTags), surfaced via
	// SchemaInfo.tags / duckdb_schemas().tags. The metadata-quality linter
	// expects e.g. vgi.description_llm / vgi.description_md per schema.
	for name, tags := range w.schemaTags {
		si, ok := cat.schemas[name]
		if !ok || len(tags) == 0 {
			continue
		}
		if si.info.Tags == nil {
			si.info.Tags = make(map[string]string, len(tags))
		}
		for k, v := range tags {
			si.info.Tags[k] = v
		}
	}

	return cat
}

// ---------------------------------------------------------------------------
// Catalog RPC handlers: the catalog_* methods of workerService. The generated
// registerVgiCatalogMethods hosts them through unaryCatalog.
// ---------------------------------------------------------------------------

// serializedGlobalFunctions returns the IPC-serialized FunctionInfo of every
// function named by WithGlobalFunctions, in declaration order, resolved
// against the catalog's default schema. Names are carried unprefixed — the
// client applies global_function_prefix itself. Unknown or unlisted names
// contribute nothing, so declaring a function that was never registered is
// inert rather than fatal.
func (w *Worker) serializedGlobalFunctions() (SerializedItems, error) {
	items := make(SerializedItems, 0, len(w.globalFunctionNames))
	if w.catalog == nil {
		return items, nil
	}
	si, ok := w.catalog.schemas[defaultFunctionSchema]
	if !ok {
		return items, nil
	}
	for _, name := range w.globalFunctionNames {
		for i := range si.functions {
			fi := &si.functions[i]
			if fi.Name != name || fi.unlisted {
				continue
			}
			data, err := w.catalog.functionCache.encode(fi)
			if err != nil {
				return nil, err
			}
			items = append(items, data)
		}
	}
	return items, nil
}

// readOnlyErr is the answer of a DDL method on a read-only catalog.
func readOnlyErr(op string) error {
	return &vgirpc.RpcError{
		Type:    "NotImplementedError",
		Code:    string(vgirpc.CodeFailedPrecondition),
		Message: fmt.Sprintf("catalog is read-only: %s not supported", op),
	}
}

// CatalogCatalogs serves vgi.v2 catalog_catalogs.
func (w workerService) CatalogCatalogs(ctx context.Context, callCtx *vgirpc.CallContext, _ CatalogCatalogsParams) (CatalogsResponseWire, error) {
	info := &CatalogInfo{Name: w.catalogName}
	if w.catalogInfoOverride != nil {
		c := *w.catalogInfoOverride
		info = &c
		if info.Name == "" {
			info.Name = w.catalogName
		}
	}
	if len(w.attachOptions) > 0 && info.AttachOptionSpecs == nil {
		specs := make([][]byte, 0, len(w.attachOptions))
		for _, opt := range w.attachOptions {
			data, err := serializeAttachOptionSpec(opt)
			if err != nil {
				LogCatalog.Error("failed to serialize attach option", "name", opt.Name, "err", err)
				continue
			}
			specs = append(specs, data)
		}
		info.AttachOptionSpecs = specs
	}
	data, err := SerializeCatalogInfo(info)
	if err != nil {
		return CatalogsResponseWire{}, err
	}
	items := SerializedItems{data}
	// Advertise each alias registered with its own discovery metadata
	// (WithCatalogAliasInfo) as a distinct catalog row, carrying its own
	// data version. Plain WithCatalogAliases entries are intentionally
	// not advertised — they share the primary catalog's identity.
	for aliasName, aliasInfo := range w.catalogAliasInfos {
		ai := aliasInfo
		// An alias with its own declared options advertises those, not
		// the primary catalog's — otherwise a client cannot tell which
		// option a gated catalog actually requires.
		if specs, ok := w.catalogAttachOptions[aliasName]; ok && ai.AttachOptionSpecs == nil {
			serialized := make([][]byte, 0, len(specs))
			for _, opt := range specs {
				data, err := serializeAttachOptionSpec(opt)
				if err != nil {
					LogCatalog.Error("failed to serialize attach option", "catalog", aliasName, "name", opt.Name, "err", err)
					continue
				}
				serialized = append(serialized, data)
			}
			ai.AttachOptionSpecs = serialized
		}
		aData, aErr := SerializeCatalogInfo(&ai)
		if aErr != nil {
			return CatalogsResponseWire{}, aErr
		}
		items = append(items, aData)
	}
	// Routed catalogs (sub-catalogs, memory catalogs) list themselves.
	routed, err := w.routedCatalogInfos(ctx, callCtx)
	if err != nil {
		return CatalogsResponseWire{}, err
	}
	items = append(items, routed...)
	return CatalogsResponseWire{Items: items}, nil
}

// CatalogAttach serves vgi.v2 catalog_attach.
func (w workerService) CatalogAttach(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogAttachRequestWire) (CatalogAttachResultWire, error) {
	// A vgi_attach_ticket is redeemed before anything else -- before
	// routing, so the sealed catalog name picks the catalog, and
	// before any catalog code sees the options.
	if err := w.redeemAttachTicketInPlace(&req, callCtx); err != nil {
		return CatalogAttachResultWire{}, err
	}
	// A routed catalog (sub-catalog, memory catalog) attaches itself.
	if b, ok := w.routes[req.Name]; ok {
		return w.attachRouted(ctx, callCtx, req, b)
	}
	// Writable catalogs are handled separately so they have their
	// own attach_opaque_data and per-catalog table state.
	if wc, ok := w.extraCatalogs[req.Name]; ok {
		res, err := w.handleWritableAttach(req, wc)
		if err != nil {
			return res, err
		}
		// Minted and sealed like every other attach value: the
		// "writable:<name>" bytes travel inside the seal, never as a
		// plaintext the worker would trust on sight.
		if res.AttachOpaqueData, err = w.mintAttach(res.AttachOpaqueData, callCtx); err != nil {
			return CatalogAttachResultWire{}, err
		}
		return res, nil
	}
	// Options declared Required must actually be supplied: fail the
	// attach loudly rather than yielding a catalog that reads as empty.
	var optionsIPC []byte
	if req.Options != nil {
		optionsIPC = *req.Options
	}
	if err := validateRequiredAttachOptions(req.Name, w.attachOptionsFor(req.Name), optionsIPC); err != nil {
		return CatalogAttachResultWire{}, &vgirpc.RpcError{
			Type:    "ValueError",
			Code:    string(vgirpc.CodeInvalidArgument),
			Message: err.Error(),
		}
	}
	// Validate catalog name matches the primary or one of the
	// declared aliases (WithCatalogAliases).
	if req.Name != w.catalogName {
		if _, ok := w.catalogAliases[req.Name]; !ok {
			return CatalogAttachResultWire{}, &vgirpc.RpcError{
				Type:    "ValueError",
				Code:    string(vgirpc.CodeNotFound),
				Message: fmt.Sprintf("No worker handles catalog '%s'", req.Name),
			}
		}
	}
	// Generate a simple attach ID
	attachOpaqueData := []byte(req.Name)
	if w.catalog != nil {
		w.catalog.attachOpaqueData = attachOpaqueData
	}
	version := int64(1)
	if w.catalog != nil {
		version = w.catalog.version
	}
	// Serialize settings
	var serializedSettings [][]byte
	for _, spec := range w.settings {
		data, err := serializeSettingSpec(spec)
		if err != nil {
			LogCatalog.Error("failed to serialize setting", "name", spec.Name, "err", err)
			continue
		}
		serializedSettings = append(serializedSettings, data)
	}
	if serializedSettings == nil {
		serializedSettings = [][]byte{}
	}

	// Serialize secret types
	var serializedSecretTypes [][]byte
	for _, spec := range w.secretTypes {
		data, err := serializeSecretTypeSpec(spec)
		if err != nil {
			LogCatalog.Error("failed to serialize secret type", "name", spec.Name, "err", err)
			continue
		}
		serializedSecretTypes = append(serializedSecretTypes, data)
	}
	if serializedSecretTypes == nil {
		serializedSecretTypes = [][]byte{}
	}

	// Serialize companion catalogs (attach_catalogs)
	var serializedAttachCatalogs [][]byte
	for _, info := range w.attachCatalogs {
		data, err := SerializeAttachCatalogInfo(info)
		if err != nil {
			LogCatalog.Error("failed to serialize attach catalog", "alias", info.Alias, "err", err)
			continue
		}
		serializedAttachCatalogs = append(serializedAttachCatalogs, data)
	}
	if serializedAttachCatalogs == nil {
		serializedAttachCatalogs = [][]byte{}
	}

	// Auto-derive time travel support from registered tables.
	supportsTimeTravel := false
	if w.catalog != nil {
		for _, si := range w.catalog.schemas {
			for _, t := range si.tables {
				if t.SupportsTimeTravel {
					supportsTimeTravel = true
					break
				}
			}
			if supportsTimeTravel {
				break
			}
		}
	}

	// Auto-derive supports_column_statistics from any table having Statistics set.
	supportsColStats := false
	for _, tbls := range w.catalogTables {
		for i := range tbls {
			if len(tbls[i].Statistics) > 0 {
				supportsColStats = true
				break
			}
		}
		if supportsColStats {
			break
		}
	}
	tags := w.catalogTags
	if tags == nil {
		tags = map[string]string{}
	}
	// Invoke the attach validator if installed — the versioned
	// workers use this to resolve data/implementation versions and
	// to embed the chosen version into attach_opaque_data.
	attachOpaqueDataRequired := false
	var resolvedData, resolvedImpl *string
	if w.attachValidator != nil {
		decision, vErr := w.attachValidator(&req, callCtx)
		if vErr != nil {
			return CatalogAttachResultWire{}, rewrapError("ValueError", vErr)
		}
		if decision != nil {
			if decision.AttachOpaqueData != nil {
				attachOpaqueData = decision.AttachOpaqueData
				attachOpaqueDataRequired = true
				if w.catalog != nil {
					w.catalog.attachOpaqueData = attachOpaqueData
				}
			}
			if decision.ResolvedDataVersion != "" {
				v := decision.ResolvedDataVersion
				resolvedData = &v
			}
			if decision.ResolvedImplementationVersion != "" {
				v := decision.ResolvedImplementationVersion
				resolvedImpl = &v
			}
		}
	}
	// Aliases registered with discovery metadata (WithCatalogAliasInfo)
	// get a random per-ATTACH scope so two ATTACHes of the same alias are
	// isolated. The plaintext is "<name>\x00<random>" — the name prefix
	// keeps catalog-scoped function visibility working (catalogNameOf
	// splits on the NUL) while the random suffix makes each attach unique.
	// The client persists and resends it, so it survives a worker restart.
	if aliasInfo, ok := w.catalogAliasInfos[req.Name]; ok {
		scope := make([]byte, 0, len(req.Name)+1+attachUUIDLen)
		scope = append(scope, req.Name...)
		scope = append(scope, 0)
		u := uuid.New()
		scope = append(scope, u[:]...)
		attachOpaqueData = scope
		attachOpaqueDataRequired = true
		if w.catalog != nil {
			w.catalog.attachOpaqueData = attachOpaqueData
		}
		if aliasInfo.DataVersionSpec != nil {
			resolvedData = aliasInfo.DataVersionSpec
		}
		if aliasInfo.ImplementationVersion != nil {
			resolvedImpl = aliasInfo.ImplementationVersion
		}
	}
	// Global functions belong to the catalog that declared them, so an
	// alias ATTACH (projection_repro, twin_a, ...) must not re-advertise
	// them: the client would otherwise be asked to publish the same
	// prefixed name once per attached alias.
	globalFunctions := SerializedItems{}
	globalFunctionPrefix := ""
	if req.Name == w.catalogName {
		var err error
		if globalFunctions, err = w.serializedGlobalFunctions(); err != nil {
			return CatalogAttachResultWire{}, err
		}
		globalFunctionPrefix = w.globalFunctionPrefix
	}
	result := CatalogAttachResultWire{
		AttachOpaqueData:              attachOpaqueData,
		SupportsTransactions:          w.supportsTransactions,
		SupportsTimeTravel:            supportsTimeTravel,
		CatalogVersionFrozen:          true,
		CatalogVersion:                version,
		AttachOpaqueDataRequired:      attachOpaqueDataRequired,
		DefaultSchema:                 "main",
		Settings:                      serializedSettings,
		SecretTypes:                   serializedSecretTypes,
		AttachCatalogs:                serializedAttachCatalogs,
		Tags:                          tags,
		SupportsColumnStatistics:      supportsColStats,
		GlobalFunctions:               globalFunctions,
		GlobalFunctionPrefix:          globalFunctionPrefix,
		ResolvedDataVersion:           resolvedData,
		ResolvedImplementationVersion: resolvedImpl,
		// The default catalog is static and version-frozen, so the
		// whole of it can be served in one catalog_contents call.
		SupportsCatalogContents: !w.catalogContentsDisabled,
	}
	if w.catalogComment != "" {
		c := w.catalogComment
		result.Comment = &c
	}
	// Mint the shard identity: prepend a fresh framework UUID to the
	// catalog's plaintext (uuid(16) || catalog_bytes), then seal. Storage
	// shards on this UUID — stable across re-seals and globally unique,
	// unlike the random-nonce ciphertext or the (possibly non-unique)
	// catalog bytes. openAttach strips the UUID back off, so the catalog
	// only ever sees its own bytes.
	// Seal the attach value into an AEAD envelope bound to the
	// caller's identity before it leaves the worker (HTTP transport;
	// pass-through on subprocess / unix).
	sealed, sErr := w.mintAttach(result.AttachOpaqueData, callCtx)
	if sErr != nil {
		return CatalogAttachResultWire{}, sErr
	}
	result.AttachOpaqueData = sealed
	return result, nil
}

// CatalogDetach serves vgi.v2 catalog_detach.
func (w workerService) CatalogDetach(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogDetachParams) error {
	return nil
}

// CatalogVersion serves vgi.v2 catalog_version.
func (w workerService) CatalogVersion(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogVersionParams) (CatalogVersionResponseWire, error) {
	version, err := w.catalogVersionOf(req.AttachOpaqueData, callCtx)
	if err != nil {
		return CatalogVersionResponseWire{}, err
	}
	return CatalogVersionResponseWire{Version: version}, nil
}

// CatalogContents serves vgi.v2 catalog_contents — every schema and all of its
// contents in one call (protocol 2.1.0). Composed from the same listings the
// per-schema RPCs below serve; see catalog_contents.go.
func (w workerService) CatalogContents(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogContentsParams) (generated.CatalogContentsResponse, error) {
	return w.catalogContents(req, callCtx)
}

// CatalogTransactionBegin serves vgi.v2 catalog_transaction_begin — allocate a
// fresh transaction id when the worker advertises transaction support, so
// DuckDB threads it through bind/scan inside BEGIN/COMMIT (enables
// transaction-scoped storage).
func (w workerService) CatalogTransactionBegin(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTransactionBeginParams) (TransactionBeginResponseWire, error) {
	if !w.supportsTransactions {
		return TransactionBeginResponseWire{}, nil
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return TransactionBeginResponseWire{}, err
	}
	// Sealed for the caller and bound to the sealed attach in the
	// unaryCatalog wrapper (sealTransactionResult), which still holds
	// the sealed attach this handler only sees opened.
	tx := append([]byte(nil), id[:]...)
	return TransactionBeginResponseWire{TransactionOpaqueData: &tx}, nil
}

// CatalogTransactionCommit serves vgi.v2 catalog_transaction_commit — clear
// per-transaction storage.
func (w workerService) CatalogTransactionCommit(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTransactionCommitParams) error {
	w.clearTransactionState(req.TransactionOpaqueData)
	return nil
}

// CatalogTransactionRollback serves vgi.v2 catalog_transaction_rollback — same
// cleanup path as commit.
func (w workerService) CatalogTransactionRollback(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTransactionRollbackParams) error {
	w.clearTransactionState(req.TransactionOpaqueData)
	return nil
}

// CatalogSchemas serves vgi.v2 catalog_schemas.
func (w workerService) CatalogSchemas(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemasParams) (ItemsResponseWire, error) {
	infos, err := w.listSchemaInfos(req.AttachOpaqueData)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	items := make([][]byte, 0, len(infos))
	for _, info := range infos {
		data, err := SerializeSchemaInfo(info)
		if err != nil {
			return ItemsResponseWire{}, err
		}
		items = append(items, data)
	}
	return ItemsResponseWire{Items: items}, nil
}

// CatalogSchemaGet serves vgi.v2 catalog_schema_get.
func (w workerService) CatalogSchemaGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemaGetParams) (ItemsResponseWire, error) {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		items, err := w.writableSchemaGet(wc, req.Path)
		if err != nil {
			return ItemsResponseWire{}, err
		}
		return ItemsResponseWire{Items: items}, nil
	}
	if w.catalog == nil {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	key := schemaPathKey(req.Path)
	if _, aliasOnly := w.catalogAliasInfos[catalogNameOf(req.AttachOpaqueData)]; aliasOnly && key != "main" {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	si, ok := w.catalog.schemas[key]
	if !ok {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	data, err := SerializeSchemaInfo(si.info)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	return ItemsResponseWire{Items: [][]byte{data}}, nil
}

// CatalogSchemaCreate serves vgi.v2 catalog_schema_create.
func (w workerService) CatalogSchemaCreate(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemaCreateParams) error {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		return w.writableSchemaCreate(wc, req.Path, parseOnConflict(req.OnConflict), req.Comment)
	}
	return readOnlyErr("catalog_schema_create")
}

// CatalogSchemaDrop serves vgi.v2 catalog_schema_drop.
func (w workerService) CatalogSchemaDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemaDropParams) error {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		return w.writableSchemaDrop(wc, req.Path, req.IgnoreNotFound, req.Cascade)
	}
	return readOnlyErr("catalog_schema_drop")
}

// CatalogSchemaContentsTables serves vgi.v2 catalog_schema_contents_tables.
func (w workerService) CatalogSchemaContentsTables(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemaContentsTablesParams) (ItemsResponseWire, error) {
	items, err := w.listTableItems(req.AttachOpaqueData, req.Path)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	return ItemsResponseWire{Items: items}, nil
}

// CatalogSchemaContentsViews serves vgi.v2 catalog_schema_contents_views.
func (w workerService) CatalogSchemaContentsViews(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemaContentsViewsParams) (ItemsResponseWire, error) {
	items, err := w.listViewItems(req.AttachOpaqueData, req.Path)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	return ItemsResponseWire{Items: items}, nil
}

// CatalogSchemaContentsFunctions serves vgi.v2
// catalog_schema_contents_functions.
func (w workerService) CatalogSchemaContentsFunctions(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemaContentsFunctionsParams) (ItemsResponseWire, error) {
	LogCatalog.Debug("catalog: listing functions", "schema", schemaPathDisplay(req.Path), "type", req.Type)
	items, err := w.listFunctionItems(req.AttachOpaqueData, req.Path, req.Type)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	return ItemsResponseWire{Items: items}, nil
}

// CatalogCopyFromFormats serves vgi.v2 catalog_copy_from_formats — list custom
// COPY ... FROM formats advertised by this catalog (catalog-level, not
// schema-scoped). Returns an empty list when the worker registers no copy-from
// formats. The VGI extension registers one DuckDB CopyFunction per returned
// entry at ATTACH time.
func (w workerService) CatalogCopyFromFormats(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogCopyFromFormatsParams) (ItemsResponseWire, error) {
	items := make([][]byte, 0, len(w.copyFromFormats))
	for _, rec := range w.copyFromFormats {
		data, err := SerializeCopyFromFormatInfo(rec)
		if err != nil {
			return ItemsResponseWire{}, err
		}
		items = append(items, data)
	}
	return ItemsResponseWire{Items: items}, nil
}

// CatalogTableGet serves vgi.v2 catalog_table_get.
func (w workerService) CatalogTableGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableGetParams) (ItemsResponseWire, error) {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		items, err := w.writableTableGet(wc, req.SchemaPath, req.Name)
		if err != nil {
			return ItemsResponseWire{}, err
		}
		if items == nil {
			return ItemsResponseWire{Items: [][]byte{}}, nil
		}
		return ItemsResponseWire{Items: items}, nil
	}
	// Attach-id-aware handler (e.g. versioned-tables worker).
	if w.attachTableGetHandler != nil {
		data, handled, err := w.attachTableGetHandler(req.AttachOpaqueData, req.SchemaPath, req.Name, req.AtUnit, req.AtValue)
		if err != nil {
			return ItemsResponseWire{}, rewrapError("ValueError", err)
		}
		if handled {
			if data == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			return ItemsResponseWire{Items: [][]byte{data}}, nil
		}
	}
	// Delegate to the custom handler first (e.g. for time-travel version-specific schemas)
	if w.tableGetHandler != nil {
		data, err := w.tableGetHandler(req.SchemaPath, req.Name, req.AtUnit, req.AtValue)
		if err != nil {
			return ItemsResponseWire{}, rewrapError("ValueError", err)
		}
		if data != nil {
			return ItemsResponseWire{Items: [][]byte{data}}, nil
		}
	}

	if w.catalog == nil {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	si, ok := w.catalog.schemas[schemaPathKey(req.SchemaPath)]
	if !ok {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	for i := range si.tables {
		if si.tables[i].Name == req.Name {
			data, err := w.serializeCatalogTable(req.SchemaPath, &si.tables[i])
			if err != nil {
				return ItemsResponseWire{}, err
			}
			return ItemsResponseWire{Items: [][]byte{data}}, nil
		}
	}
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

// CatalogTableCreate serves vgi.v2 catalog_table_create.
func (w workerService) CatalogTableCreate(ctx context.Context, callCtx *vgirpc.CallContext, req TableCreateRequestWire) error {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		return w.writableTableCreate(wc, req)
	}
	return readOnlyErr("catalog_table_create")
}

// CatalogTableDrop serves vgi.v2 catalog_table_drop.
func (w workerService) CatalogTableDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableDropParams) error {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		return w.writableTableDrop(wc, req.SchemaPath, req.Name, req.IgnoreNotFound, req.Cascade)
	}
	return readOnlyErr("catalog_table_drop")
}

// CatalogTableScanFunctionGet serves vgi.v2 catalog_table_scan_function_get.
func (w workerService) CatalogTableScanFunctionGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableScanFunctionGetParams) (TableScanFunctionGetResponseWire, error) {
	result, err := w.resolveScanFunction(req)
	if err != nil {
		return TableScanFunctionGetResponseWire{}, err
	}
	return buildScanFunctionGetResponse(result)
}

// CatalogTableScanBranchesGet serves vgi.v2 catalog_table_scan_branches_get —
// multi-branch (UNION-of-sources) tables. For non-branch tables it wraps the
// single scan_function_get result as a one-branch list, mirroring vgi-python's
// default implementation, so the method is always implemented (never triggers a
// C++ legacy fallback).
func (w workerService) CatalogTableScanBranchesGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableScanBranchesGetParams) (TableScanBranchesGetResponseWire, error) {
	if w.attachScanBranchesGetHandler != nil {
		result, handled, err := w.attachScanBranchesGetHandler(req.AttachOpaqueData, req.SchemaPath, req.Name, req.AtUnit, req.AtValue)
		if err != nil {
			return TableScanBranchesGetResponseWire{}, rewrapError("ValueError", err)
		}
		if handled {
			return buildScanBranchesGetResponse(result)
		}
	}
	// Default: wrap the single scan_function_get result as one branch.
	// The two methods take the same parameters, so the params convert.
	sf, err := w.resolveScanFunction(CatalogTableScanFunctionGetParams(req))
	if err != nil {
		return TableScanBranchesGetResponseWire{}, err
	}
	return buildScanBranchesGetResponse(&ScanBranchesResult{
		Branches: []ScanBranch{{
			FunctionName:        sf.FunctionName,
			PositionalArguments: sf.PositionalArguments,
			NamedArguments:      sf.NamedArguments,
			// Propagate whatever schema the scan_function_get path
			// already resolved; this wrapper has no independent way to
			// know the function's own schema (which is NOT necessarily
			// req.SchemaPath — see ScanBranch.SchemaPath's own doc).
			SchemaPath: sf.SchemaPath,
		}},
		RequiredExtensions: sf.RequiredExtensions,
	})
}

// CatalogTableCommentSet serves vgi.v2 catalog_table_comment_set (read-only).
func (w workerService) CatalogTableCommentSet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableCommentSetParams) error {
	return readOnlyErr("catalog_table_comment_set")
}

// CatalogTableRename serves vgi.v2 catalog_table_rename (read-only).
func (w workerService) CatalogTableRename(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableRenameParams) error {
	return readOnlyErr("catalog_table_rename")
}

// CatalogTableColumnAdd serves vgi.v2 catalog_table_column_add (read-only).
func (w workerService) CatalogTableColumnAdd(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableColumnAddParams) error {
	return readOnlyErr("catalog_table_column_add")
}

// CatalogTableColumnDrop serves vgi.v2 catalog_table_column_drop (read-only).
func (w workerService) CatalogTableColumnDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableColumnDropParams) error {
	return readOnlyErr("catalog_table_column_drop")
}

// CatalogTableColumnRename serves vgi.v2 catalog_table_column_rename
// (read-only).
func (w workerService) CatalogTableColumnRename(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableColumnRenameParams) error {
	return readOnlyErr("catalog_table_column_rename")
}

// CatalogTableColumnDefaultSet serves vgi.v2 catalog_table_column_default_set
// (read-only).
func (w workerService) CatalogTableColumnDefaultSet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableColumnDefaultSetParams) error {
	return readOnlyErr("catalog_table_column_default_set")
}

// CatalogTableColumnDefaultDrop serves vgi.v2 catalog_table_column_default_drop
// (read-only).
func (w workerService) CatalogTableColumnDefaultDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableColumnDefaultDropParams) error {
	return readOnlyErr("catalog_table_column_default_drop")
}

// CatalogTableColumnTypeChange serves vgi.v2 catalog_table_column_type_change
// (read-only).
func (w workerService) CatalogTableColumnTypeChange(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableColumnTypeChangeParams) error {
	return readOnlyErr("catalog_table_column_type_change")
}

// CatalogTableNotNullSet serves vgi.v2 catalog_table_not_null_set (read-only).
func (w workerService) CatalogTableNotNullSet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableNotNullSetParams) error {
	return readOnlyErr("catalog_table_not_null_set")
}

// CatalogTableNotNullDrop serves vgi.v2 catalog_table_not_null_drop
// (read-only).
func (w workerService) CatalogTableNotNullDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableNotNullDropParams) error {
	return readOnlyErr("catalog_table_not_null_drop")
}

// CatalogViewGet serves vgi.v2 catalog_view_get.
func (w workerService) CatalogViewGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogViewGetParams) (ItemsResponseWire, error) {
	if w.catalog == nil {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	si, ok := w.catalog.schemas[schemaPathKey(req.SchemaPath)]
	if !ok {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	for _, cv := range si.views {
		if cv.Name == req.Name {
			info := &ViewInfo{
				Name:       cv.Name,
				SchemaPath: req.SchemaPath,
				Comment:    cv.Comment,
				Tags:       cv.Tags,
				Definition: cv.Definition,
			}
			data, err := SerializeViewInfo(info)
			if err != nil {
				return ItemsResponseWire{}, err
			}
			return ItemsResponseWire{Items: [][]byte{data}}, nil
		}
	}
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

// CatalogViewCreate serves vgi.v2 catalog_view_create (read-only).
func (w workerService) CatalogViewCreate(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogViewCreateParams) error {
	return readOnlyErr("catalog_view_create")
}

// CatalogViewDrop serves vgi.v2 catalog_view_drop (read-only).
func (w workerService) CatalogViewDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogViewDropParams) error {
	return readOnlyErr("catalog_view_drop")
}

// CatalogViewRename serves vgi.v2 catalog_view_rename (read-only).
func (w workerService) CatalogViewRename(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogViewRenameParams) error {
	return readOnlyErr("catalog_view_rename")
}

// CatalogViewCommentSet serves vgi.v2 catalog_view_comment_set (read-only).
func (w workerService) CatalogViewCommentSet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogViewCommentSetParams) error {
	return readOnlyErr("catalog_view_comment_set")
}

// CatalogMacroGet serves vgi.v2 catalog_macro_get.
func (w workerService) CatalogMacroGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogMacroGetParams) (ItemsResponseWire, error) {
	if w.catalog == nil {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	si, ok := w.catalog.schemas[schemaPathKey(req.SchemaPath)]
	if !ok {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	for _, cm := range si.macros {
		if cm.Name == req.Name {
			info, err := macroInfoFromCatalogMacro(cm, req.SchemaPath)
			if err != nil {
				return ItemsResponseWire{}, err
			}
			data, err := SerializeMacroInfo(info)
			if err != nil {
				return ItemsResponseWire{}, err
			}
			return ItemsResponseWire{Items: [][]byte{data}}, nil
		}
	}
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

// CatalogSchemaContentsMacros serves vgi.v2 catalog_schema_contents_macros.
func (w workerService) CatalogSchemaContentsMacros(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogSchemaContentsMacrosParams) (ItemsResponseWire, error) {
	items, err := w.listMacroItems(req.AttachOpaqueData, req.Path, req.Type)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	return ItemsResponseWire{Items: items}, nil
}

// CatalogMacroCreate serves vgi.v2 catalog_macro_create (read-only).
func (w workerService) CatalogMacroCreate(ctx context.Context, callCtx *vgirpc.CallContext, req MacroCreateRequestWire) error {
	return readOnlyErr("catalog_macro_create")
}

// CatalogMacroDrop serves vgi.v2 catalog_macro_drop (read-only).
func (w workerService) CatalogMacroDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogMacroDropParams) error {
	return readOnlyErr("catalog_macro_drop")
}

// CatalogCreate serves vgi.v2 catalog_create (read-only).
func (w workerService) CatalogCreate(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogCreateRequestWire) error {
	return readOnlyErr("catalog_create")
}

// CatalogDrop serves vgi.v2 catalog_drop (read-only).
func (w workerService) CatalogDrop(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogDropParams) error {
	return readOnlyErr("catalog_drop")
}

// CatalogTableInsertFunctionGet serves vgi.v2
// catalog_table_insert_function_get.
func (w workerService) CatalogTableInsertFunctionGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableInsertFunctionGetParams) (TableScanFunctionGetResponseWire, error) {
	if w.attachWriteFunctionGetHandler != nil {
		if result, handled, err := w.attachWriteFunctionGetHandler(WriteOpInsert, req.AttachOpaqueData, req.SchemaPath, req.Name); err != nil {
			return TableScanFunctionGetResponseWire{}, err
		} else if handled {
			return buildScanFunctionGetResponse(result)
		}
	}
	if w.writableByAttachOpaqueData(req.AttachOpaqueData) == nil {
		return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Code: string(vgirpc.CodeFailedPrecondition), Message: fmt.Sprintf("table %s.%s is read-only (attach_opaque_data=%x len=%d, extra_catalogs=%d)", req.SchemaPath, req.Name, req.AttachOpaqueData, len(req.AttachOpaqueData), len(w.extraCatalogs))}
	}
	return buildScanFunctionGetResponse(&ScanFunctionResult{
		FunctionName: writableInsertFunctionName,
		PositionalArguments: []ScanArg{
			{Value: req.SchemaPath, Type: arrow.BinaryTypes.String},
			{Value: req.Name, Type: arrow.BinaryTypes.String},
		},
		SchemaPath: w.resolveFunctionSchema(kindTableInOut, writableInsertFunctionName, req.SchemaPath),
	})
}

// CatalogTableUpdateFunctionGet serves vgi.v2
// catalog_table_update_function_get.
func (w workerService) CatalogTableUpdateFunctionGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableUpdateFunctionGetParams) (TableScanFunctionGetResponseWire, error) {
	if w.attachWriteFunctionGetHandler != nil {
		if result, handled, err := w.attachWriteFunctionGetHandler(WriteOpUpdate, req.AttachOpaqueData, req.SchemaPath, req.Name); err != nil {
			return TableScanFunctionGetResponseWire{}, err
		} else if handled {
			return buildScanFunctionGetResponse(result)
		}
	}
	if w.writableByAttachOpaqueData(req.AttachOpaqueData) == nil {
		return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Code: string(vgirpc.CodeFailedPrecondition), Message: fmt.Sprintf("table %s.%s is read-only (attach_opaque_data=%x len=%d, extra_catalogs=%d)", req.SchemaPath, req.Name, req.AttachOpaqueData, len(req.AttachOpaqueData), len(w.extraCatalogs))}
	}
	return buildScanFunctionGetResponse(&ScanFunctionResult{
		FunctionName: writableUpdateFunctionName,
		PositionalArguments: []ScanArg{
			{Value: req.SchemaPath, Type: arrow.BinaryTypes.String},
			{Value: req.Name, Type: arrow.BinaryTypes.String},
		},
		SchemaPath: w.resolveFunctionSchema(kindTableInOut, writableUpdateFunctionName, req.SchemaPath),
	})
}

// CatalogTableDeleteFunctionGet serves vgi.v2
// catalog_table_delete_function_get.
func (w workerService) CatalogTableDeleteFunctionGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableDeleteFunctionGetParams) (TableScanFunctionGetResponseWire, error) {
	if w.attachWriteFunctionGetHandler != nil {
		if result, handled, err := w.attachWriteFunctionGetHandler(WriteOpDelete, req.AttachOpaqueData, req.SchemaPath, req.Name); err != nil {
			return TableScanFunctionGetResponseWire{}, err
		} else if handled {
			return buildScanFunctionGetResponse(result)
		}
	}
	if w.writableByAttachOpaqueData(req.AttachOpaqueData) == nil {
		return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Code: string(vgirpc.CodeFailedPrecondition), Message: fmt.Sprintf("table %s.%s is read-only (attach_opaque_data=%x len=%d, extra_catalogs=%d)", req.SchemaPath, req.Name, req.AttachOpaqueData, len(req.AttachOpaqueData), len(w.extraCatalogs))}
	}
	return buildScanFunctionGetResponse(&ScanFunctionResult{
		FunctionName: writableDeleteFunctionName,
		PositionalArguments: []ScanArg{
			{Value: req.SchemaPath, Type: arrow.BinaryTypes.String},
			{Value: req.Name, Type: arrow.BinaryTypes.String},
		},
		SchemaPath: w.resolveFunctionSchema(kindTableInOut, writableDeleteFunctionName, req.SchemaPath),
	})
}

// CatalogTableColumnStatisticsGet serves vgi.v2
// catalog_table_column_statistics_get — returns raw IPC bytes in the standard
// "result" binary column. Using []byte directly (not a struct wrapper) avoids
// vgi-rpc-go's struct-to-IPC double-wrap, so the C++ extension's
// DeserializeFromIpcBytesWithMetadata parses the stats batch directly instead
// of a nested {result: binary} envelope. The result is nullable, as the
// reference declares it; null is "none".
func (w workerService) CatalogTableColumnStatisticsGet(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogTableColumnStatisticsGetParams) (*[]byte, error) {
	ct := w.findCatalogTable(req.SchemaPath, req.Name)
	if ct == nil || len(ct.Statistics) == 0 {
		return nil, nil
	}
	cols := ct.Columns
	var ordered []ColumnStatistics
	seen := map[string]bool{}
	if cols != nil {
		for i := 0; i < cols.NumFields(); i++ {
			name := cols.Field(i).Name
			if s, ok := ct.Statistics[name]; ok {
				ordered = append(ordered, *s)
				seen[name] = true
			}
		}
	}
	for name, s := range ct.Statistics {
		if seen[name] {
			continue
		}
		ordered = append(ordered, *s)
	}
	return optionalBytes(SerializeColumnStatistics(ordered, ct.StatisticsCacheMaxAgeSeconds))
}

// findCatalogTable returns the registered CatalogTable for (schema, name) or nil.
func (w *Worker) findCatalogTable(schemaPath SchemaPath, name string) *CatalogTable {
	tables, ok := w.catalogTables[schemaPathKey(schemaPath)]
	if !ok {
		return nil
	}
	for i := range tables {
		if tables[i].Name == name {
			return &tables[i]
		}
	}
	return nil
}

// resolveColumnIndices maps column names to their indices in the schema.
func resolveColumnIndices(columns *arrow.Schema, names []string) []int32 {
	if columns == nil {
		return nil
	}
	var indices []int32
	for _, colName := range names {
		for i := 0; i < columns.NumFields(); i++ {
			if columns.Field(i).Name == colName {
				indices = append(indices, int32(i))
				break
			}
		}
	}
	return indices
}

// resolveColumnGroupIndices maps groups of column names to groups of indices.
func resolveColumnGroupIndices(columns *arrow.Schema, groups [][]string) [][]int32 {
	if columns == nil || len(groups) == 0 {
		return nil
	}
	result := make([][]int32, 0, len(groups))
	for _, group := range groups {
		var indices []int32
		for _, colName := range group {
			for i := 0; i < columns.NumFields(); i++ {
				if columns.Field(i).Name == colName {
					indices = append(indices, int32(i))
					break
				}
			}
		}
		result = append(result, indices)
	}
	return result
}

// validateRequiredFilters checks a table's CNF required-filter groups. Each
// OR-group must be non-empty and contain no empty strings, and the leading
// dotted segment of every path must name a real column on the table. Returns
// nil when groups is empty (the no-enforcement fast path) or columns is nil.
func validateRequiredFilters(tableName string, columns *arrow.Schema, groups [][]string) error {
	if len(groups) == 0 {
		return nil
	}
	columnNames := map[string]struct{}{}
	if columns != nil {
		for i := 0; i < columns.NumFields(); i++ {
			columnNames[columns.Field(i).Name] = struct{}{}
		}
	}
	for _, group := range groups {
		if len(group) == 0 {
			return fmt.Errorf("table %q: required_filters must not contain empty groups", tableName)
		}
		for _, path := range group {
			if path == "" {
				return fmt.Errorf("table %q: required_filters must not contain empty strings", tableName)
			}
			if columns == nil {
				continue
			}
			head := path
			if idx := strings.IndexByte(path, '.'); idx >= 0 {
				head = path[:idx]
			}
			if _, ok := columnNames[head]; !ok {
				return fmt.Errorf("table %q: required_filters path %q references unknown column %q", tableName, path, head)
			}
		}
	}
	return nil
}

// serializeCatalogTable converts a CatalogTable into serialized TableInfo bytes.
func (w *Worker) serializeCatalogTable(schemaPath SchemaPath, ct *CatalogTable) ([]byte, error) {
	// Resolve columns: if Function is set but Columns is nil, derive via OnBind
	columns := ct.Columns
	if columns == nil && ct.Function != nil {
		bindParams := &BindParams{
			FunctionName: ct.Function.Name(),
			FunctionType: FunctionTypeTable,
			Args:         w.buildBindArgs(ct),
		}
		resp, err := ct.Function.OnBind(bindParams)
		if err != nil {
			return nil, fmt.Errorf("resolving columns for table %s via OnBind: %w", ct.Name, err)
		}
		columns = resp.OutputSchema
	}

	// Apply column defaults as Arrow field metadata
	if len(ct.Defaults) > 0 && columns != nil {
		var err error
		columns, err = applyDefaults(columns, ct.Defaults)
		if err != nil {
			return nil, fmt.Errorf("applying defaults for table %s: %w", ct.Name, err)
		}
	}

	// Apply generated-column expressions as Arrow field metadata
	if len(ct.Generated) > 0 && columns != nil {
		var err error
		columns, err = applyGenerated(columns, ct.Generated)
		if err != nil {
			return nil, fmt.Errorf("applying generated columns for table %s: %w", ct.Name, err)
		}
	}

	// Apply per-column comments as Arrow field metadata
	if len(ct.ColumnComments) > 0 && columns != nil {
		var err error
		columns, err = applyColumnComments(columns, ct.ColumnComments)
		if err != nil {
			return nil, fmt.Errorf("applying column comments for table %s: %w", ct.Name, err)
		}
	}

	// Resolve constraint column indices from names
	notNull := resolveColumnIndices(columns, ct.NotNull)
	unique := resolveColumnGroupIndices(columns, ct.Unique)
	primaryKey := resolveColumnGroupIndices(columns, ct.PrimaryKey)

	// Validate required_filters (CNF): each OR-group must be non-empty and
	// contain no empty strings, and the leading dotted segment of each path
	// must be a real column on this table. Mirrors vgi-python's descriptor
	// validation; struct-subfield validity is left to DuckDB's binder.
	if err := validateRequiredFilters(ct.Name, columns, ct.RequiredFilters); err != nil {
		return nil, err
	}

	// Serialize FOREIGN KEY constraints
	var foreignKeys [][]byte
	for _, fk := range ct.ForeignKey {
		fkBytes, err := serializeForeignKey(schemaPath, &fk)
		if err != nil {
			return nil, fmt.Errorf("serializing foreign key for table %s: %w", ct.Name, err)
		}
		foreignKeys = append(foreignKeys, fkBytes)
	}

	info := &TableInfo{
		Name:                     ct.Name,
		SchemaPath:               schemaPath,
		Comment:                  ct.Comment,
		Tags:                     ct.Tags,
		Columns:                  columns,
		NotNullConstraints:       notNull,
		UniqueConstraints:        unique,
		CheckConstraints:         ct.Check,
		PrimaryKeyConstraints:    primaryKey,
		ForeignKeyConstraints:    foreignKeys,
		SupportsColumnStatistics: len(ct.Statistics) > 0,
		CardinalityEstimate:      ct.CardinalityEstimate,
		CardinalityMax:           ct.CardinalityMax,
		RequiredFilters:          ct.RequiredFilters,
	}

	// Inline the scan function for function-backed tables so the C++ extension
	// skips catalog_table_scan_function_get. Explicit-columns tables (Function
	// == nil) keep ScanFunction nil and continue to use the per-bind RPC path.
	if ct.Function != nil {
		sfBytes, err := SerializeScanFunctionResult(w.buildScanResultFromTable(schemaPath, ct))
		if err != nil {
			return nil, fmt.Errorf("inlining scan_function for table %s: %w", ct.Name, err)
		}
		info.ScanFunction = sfBytes
	}

	// Inline the bind result for static-schema tables so the C++ extension
	// skips the per-scan bind RPC. Use the post-metadata columns so the inlined
	// schema matches what a real bind would have produced.
	if ct.InlineBind && columns != nil {
		brBytes, err := serializeInlineBindResult(columns)
		if err != nil {
			return nil, fmt.Errorf("inlining bind_result for table %s: %w", ct.Name, err)
		}
		info.BindResult = brBytes
	}

	return SerializeTableInfo(info)
}

// fkSchema is the generated wire format for a single foreign key constraint.
var fkSchema = generated.ForeignKeyInfoSchema

// serializeForeignKey serializes a ForeignKeyConstraint to IPC bytes.
func serializeForeignKey(schemaPath SchemaPath, fk *ForeignKeyConstraint) ([]byte, error) {
	mem := memory.NewGoAllocator()

	// fk_columns
	fkColBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer fkColBuilder.Release()
	fkColBuilder.Append(true)
	fkVB := fkColBuilder.ValueBuilder().(*array.StringBuilder)
	for _, col := range fk.Columns {
		fkVB.Append(col)
	}

	// pk_columns
	pkColBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer pkColBuilder.Release()
	pkColBuilder.Append(true)
	pkVB := pkColBuilder.ValueBuilder().(*array.StringBuilder)
	for _, col := range fk.ReferencedColumns {
		pkVB.Append(col)
	}

	// referenced_table
	refTableBuilder := array.NewStringBuilder(mem)
	defer refTableBuilder.Release()
	refTableBuilder.Append(fk.ReferencedTable)

	// referenced_schema_path
	refSchemaBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer refSchemaBuilder.Release()
	refSchema := fk.ReferencedSchemaPath
	if len(refSchema) == 0 {
		refSchema = schemaPath
	}
	appendSchemaPath(refSchemaBuilder, refSchema)

	cols := []arrow.Array{
		fkColBuilder.NewArray(),
		pkColBuilder.NewArray(),
		refTableBuilder.NewArray(),
		refSchemaBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(fkSchema, cols, 1)
	defer batch.Release()

	return SerializeRecordBatch(batch)
}

// buildScanResultFromTable creates a ScanFunctionResult from a function-backed
// CatalogTable. tableSchema is the schema the table itself is declared in; it
// only seeds the registry lookup that resolves where the backing function
// actually lives, which is not necessarily the same schema.
func (w *Worker) buildScanResultFromTable(tableSchema SchemaPath, ct *CatalogTable) *ScanFunctionResult {
	result := &ScanFunctionResult{
		FunctionName: ct.Function.Name(),
		SchemaPath:   w.resolveFunctionSchema(kindTable, ct.Function.Name(), tableSchema),
	}

	for _, arg := range ct.FuncArgs {
		sa := ScanArg{Value: arg.Value, Type: arg.Type}
		if arg.Position >= 0 {
			// Grow slice if needed
			for len(result.PositionalArguments) <= arg.Position {
				result.PositionalArguments = append(result.PositionalArguments, ScanArg{})
			}
			result.PositionalArguments[arg.Position] = sa
		} else {
			if result.NamedArguments == nil {
				result.NamedArguments = make(map[string]ScanArg)
			}
			result.NamedArguments[arg.Name] = sa
		}
	}

	return result
}

// buildScanFunctionGetResponse converts a ScanFunctionResult to the wire response.
func buildScanFunctionGetResponse(result *ScanFunctionResult) (TableScanFunctionGetResponseWire, error) {
	mem := memory.NewGoAllocator()
	argBytes, err := serializeScanArgs(mem, result.PositionalArguments, result.NamedArguments)
	if err != nil {
		return TableScanFunctionGetResponseWire{}, fmt.Errorf("serializing scan arguments: %w", err)
	}
	return TableScanFunctionGetResponseWire{
		FunctionName:       result.FunctionName,
		Arguments:          argBytes,
		RequiredExtensions: result.RequiredExtensions,
		SchemaPath:         result.SchemaPath,
	}, nil
}

// clearTransactionState best-effort clears per-transaction K/V storage when a
// transaction commits or rolls back.
func (w *Worker) clearTransactionState(txID []byte) {
	if len(txID) == 0 {
		return
	}
	if back, err := w.functionStorage(); err == nil {
		_ = back.TransactionStateClear(txID)
	}
}

// resolveScanFunction resolves the scan function backing a catalog table,
// following the same precedence as catalog_table_scan_function_get: writable
// catalog, registered function-backed table, attach-aware handler, then plain
// handler. Returns an RpcError when no scan function is available.
func (w *Worker) resolveScanFunction(req CatalogTableScanFunctionGetParams) (*ScanFunctionResult, error) {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		return &ScanFunctionResult{
			FunctionName: writableScanFunctionName,
			PositionalArguments: []ScanArg{
				{Value: req.SchemaPath, Type: arrow.BinaryTypes.String},
				{Value: req.Name, Type: arrow.BinaryTypes.String},
			},
			SchemaPath: w.resolveFunctionSchema(kindTable, writableScanFunctionName, req.SchemaPath),
		}, nil
	}
	LogCatalog.Debug("catalog: scan function get", "schema", req.SchemaPath, "table", req.Name)

	// Registered catalog table with a backing function (skip when AT params
	// are present — let the handler deal with time travel).
	hasAt := req.AtUnit != nil && *req.AtUnit != ""
	if !hasAt && w.catalog != nil {
		if si, ok := w.catalog.schemas[schemaPathKey(req.SchemaPath)]; ok {
			for i := range si.tables {
				if si.tables[i].Name == req.Name && si.tables[i].Function != nil {
					return w.buildScanResultFromTable(req.SchemaPath, &si.tables[i]), nil
				}
			}
		}
	}

	// Attach-id-aware handler takes precedence over the plain one.
	if w.attachScanFunctionGetHandler != nil {
		result, handled, err := w.attachScanFunctionGetHandler(req.AttachOpaqueData, req.SchemaPath, req.Name, req.AtUnit, req.AtValue)
		if err != nil {
			return nil, rewrapError("ValueError", err)
		}
		if handled {
			return result, nil
		}
	}
	if w.scanFunctionGetHandler != nil {
		result, err := w.scanFunctionGetHandler(req.SchemaPath, req.Name, req.AtUnit, req.AtValue)
		if err != nil {
			return nil, rewrapError("ValueError", err)
		}
		return result, nil
	}

	return nil, &vgirpc.RpcError{
		Type:    "NotImplementedError",
		Code:    string(vgirpc.CodeUnimplemented),
		Message: fmt.Sprintf("table_scan_function_get not implemented for %s.%s", req.SchemaPath, req.Name),
	}
}

// buildScanBranchesGetResponse serializes each branch to IPC bytes and packs
// them into the wire response. The branches list must be non-empty per the
// protocol contract; an empty list is sent through as-is so the C++ extension
// can surface the "loud at attach" BinderException.
func buildScanBranchesGetResponse(result *ScanBranchesResult) (TableScanBranchesGetResponseWire, error) {
	branches := make([][]byte, 0, len(result.Branches))
	for i := range result.Branches {
		blob, err := SerializeScanBranch(&result.Branches[i])
		if err != nil {
			return TableScanBranchesGetResponseWire{}, fmt.Errorf("serializing branch %d: %w", i, err)
		}
		branches = append(branches, blob)
	}
	return TableScanBranchesGetResponseWire{
		Branches:           branches,
		RequiredExtensions: result.RequiredExtensions,
	}, nil
}

// buildBindArgs creates an Arguments struct from CatalogTable.FuncArgs
// for use in OnBind calls to derive output schemas.
func (w *Worker) buildBindArgs(ct *CatalogTable) *Arguments {
	mem := memory.NewGoAllocator()
	args := &Arguments{
		Named: make(map[string]arrow.Array),
	}

	for _, arg := range ct.FuncArgs {
		b := array.NewBuilder(mem, arg.Type)
		appendValue(b, arg.Value)
		arr := b.NewArray()
		b.Release()

		if arg.Position >= 0 {
			for len(args.Positional) <= arg.Position {
				args.Positional = append(args.Positional, nil)
			}
			args.Positional[arg.Position] = arr
		} else {
			args.Named[arg.Name] = arr
		}
	}

	return args
}
