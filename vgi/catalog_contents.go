// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// catalog_contents (protocol 2.1.0) returns every schema and all of its
// contents in one result, replacing catalog_schemas plus one
// catalog_schema_contents_* call per schema and kind. It is composed from the
// SAME listings those RPCs serve (listSchemaInfos, listTableItems, ...), so
// every item is byte-for-byte what the per-schema RPC returns and a client
// decodes it with the decoder it already has. It mirrors vgi-python's
// CatalogInterface.catalog_contents default.
//
// The response (generated.CatalogContentsResponse) carries one inline
// SchemaContents struct row per schema (its path, then the items), plus an
// optional etag: a client that holds the etag sends it back as if_none_match
// and gets not_modified (no schemas) while the catalog is unchanged. A catalog
// returns an etag through WithCatalogContentsHandler, or opts in to the
// framework content hash with WithCatalogContentsEtag.

// CatalogContentsRequestWire is the wire type for catalog_contents. It takes no
// transaction: the client caches the answer for the whole attach, so it is the
// committed catalog at catalog_version.
//
// IfNoneMatch is the etag of a snapshot the client already holds; when it
// equals the current etag the answer is not_modified with no schemas.
type CatalogContentsRequestWire struct {
	AttachOpaqueData []byte  `vgirpc:"attach_opaque_data"`
	IfNoneMatch      *string `vgirpc:"if_none_match"`
}

// Function and macro kinds catalog_contents lists, as the per-schema RPCs'
// type filter spells them.
const (
	contentsScalarFunction    = "SCALAR_FUNCTION"
	contentsAggregateFunction = "AGGREGATE_FUNCTION"
	contentsTableFunction     = "TABLE_FUNCTION"
	contentsScalarMacro       = "SCALAR_MACRO"
	contentsTableMacro        = "TABLE_MACRO"
)

// catalogVersionOf answers catalog_version for an attach: the version hook
// (if any) validates the attach first.
func (w *Worker) catalogVersionOf(attachOpaqueData []byte, callCtx *vgirpc.CallContext) (int64, error) {
	if w.catalogVersionHook != nil {
		if err := w.catalogVersionHook(attachOpaqueData, callCtx); err != nil {
			return 0, &vgirpc.RpcError{Type: "ValueError", Message: err.Error()}
		}
	}
	if w.catalog != nil {
		return w.catalog.version, nil
	}
	return 1, nil
}

// listSchemaInfos lists the schemas catalog_schemas serves for an attach,
// parents before children (shallower paths first, then by name).
func (w *Worker) listSchemaInfos(attachOpaqueData []byte) ([]*SchemaInfo, error) {
	var infos []*SchemaInfo
	if wc := w.writableByAttachOpaqueData(attachOpaqueData); wc != nil {
		var err error
		if infos, err = w.writableSchemaInfos(wc); err != nil {
			return nil, err
		}
	} else if w.catalog != nil {
		// Alias-info catalogs (e.g. accumulate) are distinct logical catalogs
		// that share the binary but not the primary catalog's tables: expose
		// only the "main" schema where their catalog-scoped functions live,
		// not the primary's "data"/dynamic schemas.
		_, aliasOnly := w.catalogAliasInfos[catalogNameOf(attachOpaqueData)]
		infos = make([]*SchemaInfo, 0, len(w.catalog.schemas))
		for name, si := range w.catalog.schemas {
			if aliasOnly && name != "main" {
				continue
			}
			infos = append(infos, si.info)
		}
	}
	slices.SortStableFunc(infos, func(a, b *SchemaInfo) int {
		if d := len(a.Path) - len(b.Path); d != 0 {
			return d
		}
		return strings.Compare(schemaPathKey(a.Path), schemaPathKey(b.Path))
	})
	return infos, nil
}

// listTableItems is catalog_schema_contents_tables: the TableInfo items of one
// schema.
func (w *Worker) listTableItems(attachOpaqueData []byte, path SchemaPath) ([][]byte, error) {
	if wc := w.writableByAttachOpaqueData(attachOpaqueData); wc != nil {
		return w.writableSchemaContentsTables(wc, path)
	}
	// Per-attach override (versioned-tables worker etc.): the handler inspects
	// the attach_opaque_data (which can encode the resolved version) and
	// returns the right set of tables.
	if w.schemaContentsHandler != nil {
		if items, ok := w.schemaContentsHandler(attachOpaqueData, path); ok {
			out := make([][]byte, len(items))
			for i, it := range items {
				out[i] = []byte(it)
			}
			return out, nil
		}
	}
	if w.catalog == nil {
		return [][]byte{}, nil
	}
	si, ok := w.catalog.schemas[schemaPathKey(path)]
	if !ok {
		return [][]byte{}, nil
	}
	items := make([][]byte, 0, len(si.tables))
	for i := range si.tables {
		data, err := w.serializeCatalogTable(path, &si.tables[i])
		if err != nil {
			return nil, err
		}
		items = append(items, data)
	}
	return items, nil
}

// listViewItems is catalog_schema_contents_views: the ViewInfo items of one
// schema.
func (w *Worker) listViewItems(_ []byte, path SchemaPath) ([][]byte, error) {
	if w.catalog == nil {
		return [][]byte{}, nil
	}
	si, ok := w.catalog.schemas[schemaPathKey(path)]
	if !ok {
		return [][]byte{}, nil
	}
	items := make([][]byte, 0, len(si.views))
	for _, cv := range si.views {
		data, err := SerializeViewInfo(&ViewInfo{
			Name:           cv.Name,
			SchemaPath:     path,
			Comment:        cv.Comment,
			Tags:           cv.Tags,
			Definition:     cv.Definition,
			ColumnComments: cv.ColumnComments,
		})
		if err != nil {
			return nil, err
		}
		items = append(items, data)
	}
	return items, nil
}

// listFunctionItems is catalog_schema_contents_functions: the FunctionInfo
// items of one schema of the requested type. The returned slice is shared
// (the listing cache) and must not be modified.
func (w *Worker) listFunctionItems(attachOpaqueData []byte, path SchemaPath, functionType string) ([][]byte, error) {
	if w.catalog == nil {
		return [][]byte{}, nil
	}
	// attach_opaque_data has already been unwrapped to the catalog's own
	// plaintext by unwrapReqOpaque (the unaryCatalog wrapper strips the
	// framework UUID and opens any seal). For a plain catalog/alias it is
	// []byte(catalog_name); for an alias-info catalog it is
	// "<catalog_name>\x00<random>", so take the name up to the first NUL.
	// Per-catalog function visibility (e.g. proj_repro_* only surfacing under
	// projection_repro, accumulate_* only under accumulate) compares against
	// it.
	catalogName := w.catalogOfAttach(attachOpaqueData)
	items, ok, err := w.catalog.functionListing(schemaPathKey(path), functionType, catalogName)
	if err != nil {
		return nil, err
	}
	if !ok || items == nil {
		return [][]byte{}, nil
	}
	return items, nil
}

// listMacroItems is catalog_schema_contents_macros: the MacroInfo items of one
// schema, filtered by macro type (an empty or unrecognized type filters
// nothing).
func (w *Worker) listMacroItems(_ []byte, path SchemaPath, macroType string) ([][]byte, error) {
	if w.catalog == nil {
		return [][]byte{}, nil
	}
	si, ok := w.catalog.schemas[schemaPathKey(path)]
	if !ok {
		return [][]byte{}, nil
	}
	items := make([][]byte, 0, len(si.macros))
	for _, cm := range si.macros {
		if macroType != "" {
			switch want := macroKindFilter(macroType); want {
			case MacroTypeScalar, MacroTypeTable:
				if cm.MacroType != want {
					continue
				}
			}
		}
		info, err := macroInfoFromCatalogMacro(cm, path)
		if err != nil {
			return nil, err
		}
		data, err := SerializeMacroInfo(info)
		if err != nil {
			return nil, err
		}
		items = append(items, data)
	}
	return items, nil
}

// ---------------------------------------------------------------------------
// catalog_contents v2: revalidation and the catalog-level API
// ---------------------------------------------------------------------------

// CatalogContentsResult is a catalog_contents answer: a snapshot of every
// schema, or "not modified". It mirrors vgi-python's CatalogContentsResult.
type CatalogContentsResult struct {
	// Schemas is one entry per schema. Each entry's Path must equal the
	// SchemaInfo.path inside its Schema item; paths must be unique and every
	// nested schema's parent must be present. The worker orders them parents
	// first. Must be empty when NotModified is set.
	Schemas []generated.SchemaContents
	// Etag is an opaque validator for this snapshot (a generation counter, a
	// schema version, a git sha, ...) that the client sends back as
	// if_none_match. Nil means the catalog does not revalidate, unless the
	// worker opts in to the framework content hash (WithCatalogContentsEtag).
	Etag *string
	// NotModified reports that the request's if_none_match equals the current
	// etag, so the catalog skipped building the snapshot. It requires Etag
	// (the matching validator) and no Schemas.
	NotModified bool
}

// CatalogContentsCall is one catalog_contents request, as a
// CatalogContentsHandler sees it.
type CatalogContentsCall struct {
	// AttachOpaqueData is the catalog's own attach plaintext (already
	// unwrapped by the framework).
	AttachOpaqueData []byte
	// CatalogName is the catalog the attach belongs to.
	CatalogName string
	// IfNoneMatch is the etag of the snapshot the client already holds, or
	// nil.
	IfNoneMatch *string
	// CatalogVersion is the catalog version the answer is for.
	CatalogVersion int64
	// CallCtx is the RPC call context.
	CallCtx *vgirpc.CallContext

	// build composes the default snapshot of the catalog being asked.
	build func() ([]generated.SchemaContents, error)
}

// Contents builds the snapshot the catalog serves when no handler is set:
// every schema catalog_schemas lists, with each kind composed from the same
// listings the catalog_schema_contents_* RPCs serve.
func (c *CatalogContentsCall) Contents() ([]generated.SchemaContents, error) {
	return c.build()
}

// CatalogContentsHandler answers catalog_contents for a catalog. It receives
// if_none_match, so a cheap validator can answer
// CatalogContentsResult{Etag: &etag, NotModified: true} before building
// anything; otherwise it returns the contents (typically call.Contents()) with
// their etag, or with a nil etag when it does not revalidate.
type CatalogContentsHandler func(call *CatalogContentsCall) (CatalogContentsResult, error)

// WithCatalogContentsHandler replaces how the default catalog answers
// catalog_contents: the catalog-level API, the counterpart of overriding
// vgi-python's CatalogInterface.catalog_contents. The worker still enforces
// the protocol's rules on what the handler returns (see catalogContents). A
// worker with a handler does not cache catalog_contents responses, because the
// handler may answer differently per attach or caller.
func WithCatalogContentsHandler(h CatalogContentsHandler) WorkerOption {
	return func(w *Worker) {
		w.catalogContentsHandler = h
	}
}

// CatalogContentsEtagMode is a framework etag mode for catalog_contents.
type CatalogContentsEtagMode string

// CatalogContentsEtagContentHash is the content-hash mode: when the catalog
// returns no etag of its own, the worker uses the hex SHA-256 of the snapshot
// (CatalogContentsDigest) as the etag and answers a matching if_none_match
// with not_modified. It still builds the snapshot on every call, but saves the
// transfer and the client's decode.
const CatalogContentsEtagContentHash CatalogContentsEtagMode = "content-hash"

// WithCatalogContentsEtag opts in to a framework etag for catalog_contents
// (vgi-python: catalog_contents_etag = "content-hash"). Off by default: for a
// catalog whose version is not frozen, a client that holds an etag
// revalidates with catalog_contents at every transaction start instead of a
// cheap catalog_version poll, so an etag that costs a full build each time
// should be a deliberate choice. Pass "" to turn it off.
func WithCatalogContentsEtag(mode CatalogContentsEtagMode) WorkerOption {
	return func(w *Worker) {
		w.catalogContentsEtag = mode
	}
}

// CatalogContentsDigest is the hex SHA-256 over a catalog_contents snapshot:
// the content-hash etag. It covers every schema's path and the exact item
// bytes of every kind, in wire order, each length-prefixed (8-byte
// little-endian) so no two different snapshots share an input. It is the same
// function as vgi-python's catalog_contents_digest, so a snapshot hashes alike
// in every SDK. Deterministic because the items' encoding is (map columns are
// written in sorted key order).
func CatalogContentsDigest(schemas []generated.SchemaContents) string {
	h := sha256.New()
	var n [8]byte
	length := func(l int) {
		binary.LittleEndian.PutUint64(n[:], uint64(l))
		h.Write(n[:])
	}
	chunk := func(data []byte) {
		length(len(data))
		h.Write(data)
	}
	chunks := func(values [][]byte) {
		length(len(values))
		for _, v := range values {
			chunk(v)
		}
	}
	length(len(schemas))
	for i := range schemas {
		s := &schemas[i]
		length(len(s.Path))
		for _, part := range s.Path {
			chunk([]byte(part))
		}
		chunk(s.Schema)
		for _, kind := range [][][]byte{
			s.Tables, s.Views, s.ScalarFunctions, s.AggregateFunctions,
			s.TableFunctions, s.ScalarMacros, s.TableMacros, s.Indexes,
		} {
			chunks(kind)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// contentsCacheKey identifies one cached catalog_contents response: the
// catalog instance (rebuilt with the server), the catalog name the attach
// resolves to (aliases list different functions), and its version.
type contentsCacheKey struct {
	catalog *DefaultReadOnlyCatalog
	name    string
	version int64
}

// catalogContentsCacheable reports whether catalog_contents for this attach can
// be built once and reused. The default catalog is version-frozen and static
// (built when the server starts), and its contents depend on the attach only
// through the catalog name, so it qualifies — unless the worker plugs in code
// that may answer per attach: a catalog_contents handler, or a schema contents
// handler (which sees the whole attach_opaque_data). Writable catalogs change
// under DDL and are never cached.
func (w *Worker) catalogContentsCacheable(attachOpaqueData []byte) bool {
	return w.catalog != nil &&
		w.catalogContentsHandler == nil &&
		w.schemaContentsHandler == nil &&
		w.writableByAttachOpaqueData(attachOpaqueData) == nil
}

// catalogContents answers catalog_contents.
//
// Revalidation: if_none_match goes to the handler (if any), which may answer
// not_modified without building. A full answer carries the handler's etag or,
// with WithCatalogContentsEtag(CatalogContentsEtagContentHash), the snapshot's
// SHA-256; an etag equal to if_none_match turns it into not_modified. With no
// etag, if_none_match is ignored.
//
// Caching: when catalogContentsCacheable, the response is built once per
// (catalog, name, version) and reused by every later call.
func (w *Worker) catalogContents(req CatalogContentsRequestWire, callCtx *vgirpc.CallContext) (generated.CatalogContentsResponse, error) {
	// Check the version (and the attach, through the version hook) even on a
	// cache hit.
	version, err := w.catalogVersionOf(req.AttachOpaqueData, callCtx)
	if err != nil {
		return generated.CatalogContentsResponse{}, err
	}
	if !w.catalogContentsCacheable(req.AttachOpaqueData) {
		return w.catalogContentsResponse(req.AttachOpaqueData, req.IfNoneMatch, version, callCtx)
	}
	key := contentsCacheKey{catalog: w.catalog, name: w.catalogOfAttach(req.AttachOpaqueData), version: version}
	cached, ok := w.contentsCache.Load(key)
	if !ok {
		built, err := w.catalogContentsResponse(req.AttachOpaqueData, nil, version, callCtx)
		if err != nil {
			return generated.CatalogContentsResponse{}, err
		}
		// Concurrent first calls may each build; they build the same thing,
		// and every caller then serves the one that was stored.
		cached, _ = w.contentsCache.LoadOrStore(key, built)
	}
	full := cached.(generated.CatalogContentsResponse)
	if full.Etag != nil && req.IfNoneMatch != nil && *req.IfNoneMatch == *full.Etag {
		return notModifiedResponse(version, *full.Etag), nil
	}
	return full, nil
}

func notModifiedResponse(version int64, etag string) generated.CatalogContentsResponse {
	return generated.CatalogContentsResponse{
		CatalogVersion: version,
		Etag:           &etag,
		NotModified:    true,
		Schemas:        []generated.SchemaContents{},
	}
}

// catalogContentsResponse asks the catalog (its handler, or the default
// composition) for its contents and shapes the wire response, enforcing the
// protocol's rules: not_modified needs an etag equal to if_none_match and no
// schemas; a catalog with no etag never yields not_modified; schema paths are
// unique, each nested schema's parent is present, and parents come first.
func (w *Worker) catalogContentsResponse(attachOpaqueData []byte, ifNoneMatch *string, version int64, callCtx *vgirpc.CallContext) (generated.CatalogContentsResponse, error) {
	return answerCatalogContents(w.catalogContentsHandler, w.catalogContentsEtag, &CatalogContentsCall{
		AttachOpaqueData: attachOpaqueData,
		CatalogName:      w.catalogOfAttach(attachOpaqueData),
		IfNoneMatch:      ifNoneMatch,
		CatalogVersion:   version,
		CallCtx:          callCtx,
		build:            func() ([]generated.SchemaContents, error) { return w.defaultCatalogContents(attachOpaqueData) },
	})
}

// answerCatalogContents is the catalog-independent half of catalog_contents:
// it asks the handler (or, with none, the call's default composition) for the
// snapshot and applies the protocol's rules and the etag mode. Every catalog
// kind a worker serves — the default static catalog, a sub-catalog, a
// MemoryCatalog — answers through it, so they all revalidate alike.
func answerCatalogContents(handler CatalogContentsHandler, etagMode CatalogContentsEtagMode, call *CatalogContentsCall) (generated.CatalogContentsResponse, error) {
	ifNoneMatch, version := call.IfNoneMatch, call.CatalogVersion
	var result CatalogContentsResult
	fromHandler := handler != nil
	if fromHandler {
		var err error
		result, err = handler(call)
		if err != nil {
			return generated.CatalogContentsResponse{}, err
		}
	} else {
		schemas, err := call.build()
		if err != nil {
			return generated.CatalogContentsResponse{}, err
		}
		result = CatalogContentsResult{Schemas: schemas}
	}

	if result.NotModified {
		if result.Etag == nil || ifNoneMatch == nil || *result.Etag != *ifNoneMatch {
			return generated.CatalogContentsResponse{}, &vgirpc.RpcError{Type: "ValueError",
				Message: "catalog_contents returned not_modified, but only a catalog whose etag equals " +
					"if_none_match may (and it must return that etag)"}
		}
		if len(result.Schemas) != 0 {
			return generated.CatalogContentsResponse{}, &vgirpc.RpcError{Type: "ValueError",
				Message: "catalog_contents returned not_modified with schemas; it must return none"}
		}
		return notModifiedResponse(version, *result.Etag), nil
	}

	schemas := slices.Clone(result.Schemas)
	if schemas == nil {
		schemas = []generated.SchemaContents{}
	}
	if err := checkContentsPaths(schemas, fromHandler); err != nil {
		return generated.CatalogContentsResponse{}, &vgirpc.RpcError{Type: "ValueError", Message: err.Error()}
	}
	// Same parent-before-child order catalog_schemas guarantees.
	slices.SortStableFunc(schemas, func(a, b generated.SchemaContents) int { return len(a.Path) - len(b.Path) })

	etag := result.Etag
	if etag == nil && etagMode == CatalogContentsEtagContentHash {
		digest := CatalogContentsDigest(schemas)
		etag = &digest
	}
	if etag != nil && ifNoneMatch != nil && *etag == *ifNoneMatch {
		return notModifiedResponse(version, *etag), nil
	}
	return generated.CatalogContentsResponse{CatalogVersion: version, Etag: etag, Schemas: schemas}, nil
}

// checkContentsPaths validates a snapshot's schema paths: non-empty, unique,
// and every nested schema's parent present. For a handler's answer it also
// checks that each Path equals the SchemaInfo.path its Schema item carries (the
// default composition sets Path from that same SchemaInfo).
func checkContentsPaths(schemas []generated.SchemaContents, verifyItems bool) error {
	keys := make(map[string]bool, len(schemas))
	for i := range schemas {
		path := schemas[i].Path
		if len(path) == 0 {
			return fmt.Errorf("catalog_contents returned a schema with an empty path")
		}
		key := schemaPathKey(path)
		if keys[key] {
			return fmt.Errorf("catalog_contents returned duplicate schema path %s", schemaPathDisplay(path))
		}
		keys[key] = true
		if verifyItems {
			itemPath, err := schemaInfoItemPath(schemas[i].Schema)
			if err != nil {
				return fmt.Errorf("catalog_contents schema %s: decoding its SchemaInfo item: %w", schemaPathDisplay(path), err)
			}
			if !slices.Equal(itemPath, path) {
				return fmt.Errorf("catalog_contents schema path %s differs from its SchemaInfo.path %s",
					schemaPathDisplay(path), schemaPathDisplay(itemPath))
			}
		}
	}
	for i := range schemas {
		path := schemas[i].Path
		if len(path) > 1 && !keys[schemaPathKey(path[:len(path)-1])] {
			return fmt.Errorf("catalog_contents returned schema path %s without its parent", schemaPathDisplay(path))
		}
	}
	return nil
}

// schemaInfoItemPath reads the path column of a serialized SchemaInfo item.
func schemaInfoItemPath(item []byte) ([]string, error) {
	rec, err := DeserializeRecordBatch(item)
	if err != nil {
		return nil, err
	}
	defer rec.Release()
	idx := rec.Schema().FieldIndices("path")
	if len(idx) != 1 || rec.NumRows() < 1 {
		return nil, fmt.Errorf("not a SchemaInfo record")
	}
	list, ok := rec.Column(idx[0]).(*array.List)
	if !ok {
		return nil, fmt.Errorf("SchemaInfo.path is %T", rec.Column(idx[0]))
	}
	values, ok := list.ListValues().(*array.String)
	if !ok {
		return nil, fmt.Errorf("SchemaInfo.path holds %T", list.ListValues())
	}
	start, end := list.ValueOffsets(0)
	out := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, values.Value(int(i)))
	}
	return out, nil
}

// defaultCatalogContents composes every schema catalog_schemas lists, parents
// first, with each kind from the per-schema listings.
func (w *Worker) defaultCatalogContents(attachOpaqueData []byte) ([]generated.SchemaContents, error) {
	infos, err := w.listSchemaInfos(attachOpaqueData)
	if err != nil {
		return nil, err
	}
	schemas := make([]generated.SchemaContents, 0, len(infos))
	for _, info := range infos {
		contents, err := w.schemaContentsOf(attachOpaqueData, info)
		if err != nil {
			return nil, fmt.Errorf("catalog_contents: schema %s: %w", schemaPathDisplay(info.Path), err)
		}
		schemas = append(schemas, contents)
	}
	return schemas, nil
}

// schemaContentsOf gathers one schema's items, kind by kind. A kind whose
// estimated_object_count is exactly 0 (a hard guarantee) is not computed; it
// is still sent, as an empty list, because each kind is complete.
func (w *Worker) schemaContentsOf(attachOpaqueData []byte, info *SchemaInfo) (generated.SchemaContents, error) {
	schemaItem, err := SerializeSchemaInfo(info)
	if err != nil {
		return generated.SchemaContents{}, err
	}
	var firstErr error
	kind := func(countKey string, list func() ([][]byte, error)) [][]byte {
		if n, ok := info.EstimatedObjectCount[countKey]; (ok && n == 0) || firstErr != nil {
			return [][]byte{}
		}
		items, err := list()
		if err != nil {
			firstErr = err
			return [][]byte{}
		}
		if items == nil {
			return [][]byte{}
		}
		return items
	}
	path := info.Path
	functions := func(t string) func() ([][]byte, error) {
		return func() ([][]byte, error) { return w.listFunctionItems(attachOpaqueData, path, t) }
	}
	macros := func(t string) func() ([][]byte, error) {
		return func() ([][]byte, error) { return w.listMacroItems(attachOpaqueData, path, t) }
	}
	contents := generated.SchemaContents{
		Path:               slices.Clone([]string(path)),
		Schema:             schemaItem,
		Tables:             kind("table", func() ([][]byte, error) { return w.listTableItems(attachOpaqueData, path) }),
		Views:              kind("view", func() ([][]byte, error) { return w.listViewItems(attachOpaqueData, path) }),
		ScalarFunctions:    kind("scalar_function", functions(contentsScalarFunction)),
		AggregateFunctions: kind("aggregate_function", functions(contentsAggregateFunction)),
		TableFunctions:     kind("table_function", functions(contentsTableFunction)),
		ScalarMacros:       kind("macro", macros(contentsScalarMacro)),
		TableMacros:        kind("macro", macros(contentsTableMacro)),
		// This SDK has no index API (catalog_schema_contents_indexes is not
		// served), so every schema has none.
		Indexes: [][]byte{},
	}
	return contents, firstErr
}
