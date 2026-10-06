// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// MemoryCatalog is an in-memory catalog that accepts DDL — CREATE / DROP
// SCHEMA, TABLE (definitions only, no rows) and VIEW — and whose every ATTACH
// is private: each attach starts from an empty "main" schema and never sees
// another attach's objects, even when they share one worker process. Its
// version starts at 1 and is bumped by every DDL; it is not frozen.
//
// It advertises catalog_contents by default, built from its current objects
// (the counterpart of vgi-python's CatalogInterface.catalog_contents default
// over an InMemoryCatalog), and revalidates the way the options say: with a
// handler's etag (WithMemoryCatalogContentsHandler), the framework content
// hash (WithMemoryCatalogContentsEtag), or not at all.
//
// State lives in the worker process. A transport that keeps one process per
// client session (launcher, HTTP, a pooled subprocess) keeps it; a fresh
// process does not, and answers "not attached". Serve it with
// Worker.RegisterMemoryCatalog.
type MemoryCatalog struct {
	name            string
	comment         string
	unversioned     bool
	contentsOff     bool
	contentsHandler CatalogContentsHandler
	contentsEtag    CatalogContentsEtagMode

	mu       sync.Mutex
	attaches map[string]*memoryState
	methods  map[string]routedMethod
}

type memoryState struct {
	version int64
	schemas map[string]*memorySchema // keyed by schemaPathKey
}

type memorySchema struct {
	path    SchemaPath
	comment string
	// Serialized TableInfo / ViewInfo items, keyed by lower-cased name.
	tables map[string]memoryObject
	views  map[string]memoryObject
}

type memoryObject struct {
	name string
	item []byte
}

// MemoryCatalogOption configures a MemoryCatalog.
type MemoryCatalogOption func(*MemoryCatalog)

// WithMemoryCatalogComment sets the catalog's comment.
func WithMemoryCatalogComment(comment string) MemoryCatalogOption {
	return func(m *MemoryCatalog) { m.comment = comment }
}

// WithMemoryCatalogUnversioned makes the catalog report catalog_version 0
// ("unknown") always, in its attach result, catalog_version and
// catalog_contents — a catalog that does not track its version. A client
// cannot tell whether such a catalog changed, so it re-checks at every
// transaction start.
func WithMemoryCatalogUnversioned() MemoryCatalogOption {
	return func(m *MemoryCatalog) { m.unversioned = true }
}

// WithMemoryCatalogContents sets whether the catalog advertises
// catalog_contents (default true).
func WithMemoryCatalogContents(enabled bool) MemoryCatalogOption {
	return func(m *MemoryCatalog) { m.contentsOff = !enabled }
}

// WithMemoryCatalogContentsHandler answers catalog_contents with h, as
// WithCatalogContentsHandler does for a worker's default catalog;
// call.Contents() builds the snapshot of the attach's current objects.
func WithMemoryCatalogContentsHandler(h CatalogContentsHandler) MemoryCatalogOption {
	return func(m *MemoryCatalog) { m.contentsHandler = h }
}

// WithMemoryCatalogContentsEtag opts in to a framework etag mode, as
// WithCatalogContentsEtag does for a worker's default catalog.
func WithMemoryCatalogContentsEtag(mode CatalogContentsEtagMode) MemoryCatalogOption {
	return func(m *MemoryCatalog) { m.contentsEtag = mode }
}

// NewMemoryCatalog builds an in-memory DDL-capable catalog named name.
func NewMemoryCatalog(name string, opts ...MemoryCatalogOption) *MemoryCatalog {
	m := &MemoryCatalog{name: name, attaches: map[string]*memoryState{}}
	for _, opt := range opts {
		opt(m)
	}
	m.methods = map[string]routedMethod{
		"catalog_attach":                    memoryMethod(m.attach),
		"catalog_detach":                    memoryVoid(m.detach),
		"catalog_version":                   memoryMethod(m.versionRPC),
		"catalog_contents":                  memoryMethod(m.contents),
		"catalog_transaction_begin":         memoryMethod(m.transactionBegin),
		"catalog_transaction_commit":        memoryVoid(m.transactionEnd),
		"catalog_transaction_rollback":      memoryVoid(m.transactionEnd),
		"catalog_schemas":                   memoryMethod(m.schemasRPC),
		"catalog_schema_get":                memoryMethod(m.schemaGet),
		"catalog_schema_create":             memoryVoid(m.schemaCreate),
		"catalog_schema_drop":               memoryVoid(m.schemaDrop),
		"catalog_schema_contents_tables":    memoryMethod(m.contentsTables),
		"catalog_schema_contents_views":     memoryMethod(m.contentsViews),
		"catalog_schema_contents_functions": memoryMethod(m.contentsFunctions),
		"catalog_schema_contents_macros":    memoryMethod(m.contentsMacros),
		"catalog_copy_from_formats":         memoryMethod(m.copyFromFormats),
		"catalog_table_get":                 memoryMethod(m.tableGet),
		"catalog_table_create":              memoryVoid(m.tableCreate),
		"catalog_table_drop":                memoryVoid(m.tableDrop),
		"catalog_view_get":                  memoryMethod(m.viewGet),
		"catalog_view_create":               memoryVoid(m.viewCreate),
		"catalog_view_drop":                 memoryVoid(m.viewDrop),
		"catalog_macro_get":                 memoryMethod(m.macroGet),
	}
	return m
}

// Name is the catalog name ATTACH uses.
func (m *MemoryCatalog) Name() string { return m.name }

func memoryMethod[P any, R any](fn func(*vgirpc.CallContext, *P) (R, error)) routedMethod {
	return func(_ context.Context, cc *vgirpc.CallContext, req any) (any, error) {
		return fn(cc, req.(*P))
	}
}

func memoryVoid[P any](fn func(*P) error) routedMethod {
	return func(_ context.Context, _ *vgirpc.CallContext, req any) (any, error) {
		return nil, fn(req.(*P))
	}
}

func (m *MemoryCatalog) method(name string) (routedMethod, bool) {
	fn, ok := m.methods[name]
	return fn, ok
}

func (m *MemoryCatalog) catalogInfoItems(context.Context, *vgirpc.CallContext) ([][]byte, error) {
	data, err := SerializeCatalogInfo(&CatalogInfo{Name: m.name})
	if err != nil {
		return nil, err
	}
	return [][]byte{data}, nil
}

func memoryErr(format string, args ...any) error {
	return &vgirpc.RpcError{Type: "ValueError", Message: fmt.Sprintf(format, args...)}
}

// state returns the attach's state; m.mu must be held.
func (m *MemoryCatalog) state(attach []byte) (*memoryState, error) {
	st, ok := m.attaches[string(attach)]
	if !ok {
		return nil, memoryErr("%s: not attached (its state lives in the worker process that attached it)", m.name)
	}
	return st, nil
}

// reportedVersion is the version the catalog reports for st.
func (m *MemoryCatalog) reportedVersion(st *memoryState) int64 {
	if m.unversioned {
		return 0
	}
	return st.version
}

func (m *MemoryCatalog) attach(_ *vgirpc.CallContext, req *CatalogAttachRequestWire) (CatalogAttachResultWire, error) {
	if req.Name != m.name {
		return CatalogAttachResultWire{}, memoryErr("Unknown catalog: '%s'. Available: %s", req.Name, m.name)
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return CatalogAttachResultWire{}, err
	}
	attach := []byte(m.name + "\x00" + hex.EncodeToString(token[:]))
	st := &memoryState{version: 1, schemas: map[string]*memorySchema{}}
	st.schemas["main"] = newMemorySchema(SchemaPath{"main"}, "")
	m.mu.Lock()
	m.attaches[string(attach)] = st
	version := m.reportedVersion(st)
	m.mu.Unlock()
	res := CatalogAttachResultWire{
		AttachOpaqueData:         attach,
		CatalogVersion:           version,
		AttachOpaqueDataRequired: true,
		DefaultSchema:            "main",
		Settings:                 [][]byte{},
		SecretTypes:              [][]byte{},
		AttachCatalogs:           [][]byte{},
		Tags:                     map[string]string{},
		GlobalFunctions:          SerializedItems{},
		SupportsCatalogContents:  !m.contentsOff,
	}
	if m.comment != "" {
		c := m.comment
		res.Comment = &c
	}
	return res, nil
}

func newMemorySchema(path SchemaPath, comment string) *memorySchema {
	return &memorySchema{path: slices.Clone(path), comment: comment,
		tables: map[string]memoryObject{}, views: map[string]memoryObject{}}
}

func (m *MemoryCatalog) detach(req *DetachRequestWire) error {
	m.mu.Lock()
	delete(m.attaches, string(req.AttachOpaqueData))
	m.mu.Unlock()
	return nil
}

func (m *MemoryCatalog) versionRPC(_ *vgirpc.CallContext, req *CatalogVersionRequestWire) (CatalogVersionResponseWire, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, err := m.state(req.AttachOpaqueData)
	if err != nil {
		return CatalogVersionResponseWire{}, err
	}
	return CatalogVersionResponseWire{Version: m.reportedVersion(st)}, nil
}

func (m *MemoryCatalog) transactionBegin(*vgirpc.CallContext, *TransactionBeginRequestWire) (TransactionBeginResponseWire, error) {
	return TransactionBeginResponseWire{}, nil
}

func (m *MemoryCatalog) transactionEnd(*TransactionRequestWire) error { return nil }

func (m *MemoryCatalog) contents(cc *vgirpc.CallContext, req *CatalogContentsRequestWire) (generated.CatalogContentsResponse, error) {
	m.mu.Lock()
	st, err := m.state(req.AttachOpaqueData)
	var version int64
	if err == nil {
		version = m.reportedVersion(st)
	}
	m.mu.Unlock()
	if err != nil {
		return generated.CatalogContentsResponse{}, err
	}
	attach := req.AttachOpaqueData
	return answerCatalogContents(m.contentsHandler, m.contentsEtag, &CatalogContentsCall{
		AttachOpaqueData: attach,
		CatalogName:      m.name,
		IfNoneMatch:      req.IfNoneMatch,
		CatalogVersion:   version,
		CallCtx:          cc,
		build:            func() ([]generated.SchemaContents, error) { return m.snapshot(attach) },
	})
}

// snapshot is the catalog_contents of an attach: every schema, parents first,
// with its tables and views in name order (so equal contents hash alike).
func (m *MemoryCatalog) snapshot(attach []byte) ([]generated.SchemaContents, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, err := m.state(attach)
	if err != nil {
		return nil, err
	}
	out := make([]generated.SchemaContents, 0, len(st.schemas))
	for _, s := range sortedSchemas(st) {
		item, err := m.schemaItem(attach, s)
		if err != nil {
			return nil, err
		}
		out = append(out, generated.SchemaContents{
			Path:               slices.Clone([]string(s.path)),
			Schema:             item,
			Tables:             objectItems(s.tables),
			Views:              objectItems(s.views),
			ScalarFunctions:    [][]byte{},
			AggregateFunctions: [][]byte{},
			TableFunctions:     [][]byte{},
			ScalarMacros:       [][]byte{},
			TableMacros:        [][]byte{},
			Indexes:            [][]byte{},
		})
	}
	return out, nil
}

// sortedSchemas orders an attach's schemas parents first, then by path.
func sortedSchemas(st *memoryState) []*memorySchema {
	out := slices.Collect(maps.Values(st.schemas))
	slices.SortFunc(out, func(a, b *memorySchema) int {
		if d := len(a.path) - len(b.path); d != 0 {
			return d
		}
		return strings.Compare(schemaPathKey(a.path), schemaPathKey(b.path))
	})
	return out
}

func objectItems(objs map[string]memoryObject) [][]byte {
	out := make([][]byte, 0, len(objs))
	for _, key := range slices.Sorted(maps.Keys(objs)) {
		out = append(out, objs[key].item)
	}
	return out
}

func (m *MemoryCatalog) schemaItem(attach []byte, s *memorySchema) ([]byte, error) {
	return SerializeSchemaInfo(&SchemaInfo{Path: s.path, Comment: s.comment, Tags: map[string]string{}, AttachOpaqueData: attach})
}

// schema looks up one schema of an attach; m.mu must be held.
func (m *MemoryCatalog) schema(attach []byte, path SchemaPath) (*memorySchema, *memoryState, error) {
	st, err := m.state(attach)
	if err != nil {
		return nil, nil, err
	}
	return st.schemas[schemaPathKey(path)], st, nil
}

func (m *MemoryCatalog) schemasRPC(_ *vgirpc.CallContext, req *SchemasRequestWire) (ItemsResponseWire, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, err := m.state(req.AttachOpaqueData)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	items := [][]byte{}
	for _, s := range sortedSchemas(st) {
		item, err := m.schemaItem(req.AttachOpaqueData, s)
		if err != nil {
			return ItemsResponseWire{}, err
		}
		items = append(items, item)
	}
	return ItemsResponseWire{Items: items}, nil
}

func (m *MemoryCatalog) schemaGet(_ *vgirpc.CallContext, req *SchemaGetRequestWire) (ItemsResponseWire, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, _, err := m.schema(req.AttachOpaqueData, req.Path)
	if err != nil || s == nil {
		return ItemsResponseWire{Items: [][]byte{}}, err
	}
	item, err := m.schemaItem(req.AttachOpaqueData, s)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	return ItemsResponseWire{Items: [][]byte{item}}, nil
}

func (m *MemoryCatalog) schemaCreate(req *SchemaCreateRequestWire) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, st, err := m.schema(req.AttachOpaqueData, req.Path)
	if err != nil {
		return err
	}
	if len(req.Path) == 0 {
		return memoryErr("schema path is empty")
	}
	if existing != nil {
		if replace, err := onConflictReplaces("Schema", schemaPathDisplay(req.Path), req.OnConflict); !replace {
			return err
		}
	}
	if len(req.Path) > 1 {
		if _, ok := st.schemas[schemaPathKey(req.Path[:len(req.Path)-1])]; !ok {
			return memoryErr("Schema %s does not exist", schemaPathDisplay(req.Path[:len(req.Path)-1]))
		}
	}
	comment := ""
	if req.Comment != nil {
		comment = *req.Comment
	}
	st.schemas[schemaPathKey(req.Path)] = newMemorySchema(req.Path, comment)
	st.version++
	return nil
}

func (m *MemoryCatalog) schemaDrop(req *SchemaDropRequestWire) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, st, err := m.schema(req.AttachOpaqueData, req.Path)
	if err != nil {
		return err
	}
	if s == nil {
		if req.IgnoreNotFound {
			return nil
		}
		return memoryErr("Schema %s does not exist", schemaPathDisplay(req.Path))
	}
	key := schemaPathKey(req.Path)
	var children []string
	for k := range st.schemas {
		if strings.HasPrefix(k, key+"\x00") {
			children = append(children, k)
		}
	}
	if !req.Cascade && (len(s.tables) > 0 || len(s.views) > 0 || len(children) > 0) {
		return memoryErr("Schema %s is not empty; use CASCADE", schemaPathDisplay(req.Path))
	}
	for _, k := range children {
		delete(st.schemas, k)
	}
	delete(st.schemas, key)
	st.version++
	return nil
}

// onConflictReplaces resolves CREATE's on_conflict for an existing object:
// replace it (true), keep it (false, nil error), or fail.
func onConflictReplaces(kind, name, onConflict string) (bool, error) {
	switch parseOnConflict(onConflict) {
	case onConflictReplace:
		return true, nil
	case onConflictIgnore:
		return false, nil
	default:
		return false, memoryErr("%s with name \"%s\" already exists", kind, name)
	}
}

func (m *MemoryCatalog) listObjects(attach []byte, path SchemaPath, views bool) (ItemsResponseWire, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, _, err := m.schema(attach, path)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	if s == nil {
		return ItemsResponseWire{Items: [][]byte{}}, nil
	}
	if views {
		return ItemsResponseWire{Items: objectItems(s.views)}, nil
	}
	return ItemsResponseWire{Items: objectItems(s.tables)}, nil
}

func (m *MemoryCatalog) contentsTables(_ *vgirpc.CallContext, req *SchemaContentsRequestWire) (ItemsResponseWire, error) {
	return m.listObjects(req.AttachOpaqueData, req.Path, false)
}

func (m *MemoryCatalog) contentsViews(_ *vgirpc.CallContext, req *SchemaContentsRequestWire) (ItemsResponseWire, error) {
	return m.listObjects(req.AttachOpaqueData, req.Path, true)
}

// A MemoryCatalog has no functions, macros or COPY formats.
func (m *MemoryCatalog) contentsFunctions(*vgirpc.CallContext, *SchemaContentsFunctionsRequestWire) (ItemsResponseWire, error) {
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

func (m *MemoryCatalog) contentsMacros(*vgirpc.CallContext, *SchemaContentsMacrosRequestWire) (ItemsResponseWire, error) {
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

func (m *MemoryCatalog) copyFromFormats(*vgirpc.CallContext, *CopyFromFormatsRequestWire) (ItemsResponseWire, error) {
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

func (m *MemoryCatalog) macroGet(*vgirpc.CallContext, *MacroGetRequestWire) (ItemsResponseWire, error) {
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

func (m *MemoryCatalog) getObject(attach []byte, path SchemaPath, name string, views bool) (ItemsResponseWire, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, _, err := m.schema(attach, path)
	if err != nil {
		return ItemsResponseWire{}, err
	}
	if s != nil {
		objs := s.tables
		if views {
			objs = s.views
		}
		if obj, ok := objs[strings.ToLower(name)]; ok {
			return ItemsResponseWire{Items: [][]byte{obj.item}}, nil
		}
	}
	return ItemsResponseWire{Items: [][]byte{}}, nil
}

func (m *MemoryCatalog) tableGet(_ *vgirpc.CallContext, req *TableGetRequestWire) (ItemsResponseWire, error) {
	return m.getObject(req.AttachOpaqueData, req.SchemaPath, req.Name, false)
}

func (m *MemoryCatalog) viewGet(_ *vgirpc.CallContext, req *ViewGetRequestWire) (ItemsResponseWire, error) {
	return m.getObject(req.AttachOpaqueData, req.SchemaPath, req.Name, true)
}

// putObject stores a created table or view, honouring on_conflict, and bumps
// the version.
func (m *MemoryCatalog) putObject(attach []byte, path SchemaPath, name, kind, onConflict string, views bool, item []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, st, err := m.schema(attach, path)
	if err != nil {
		return err
	}
	if s == nil {
		return memoryErr("Schema %s does not exist", schemaPathDisplay(path))
	}
	objs := s.tables
	if views {
		objs = s.views
	}
	key := strings.ToLower(name)
	if _, exists := objs[key]; exists {
		if replace, err := onConflictReplaces(kind, name, onConflict); !replace {
			return err
		}
	}
	objs[key] = memoryObject{name: name, item: item}
	st.version++
	return nil
}

func (m *MemoryCatalog) dropObject(attach []byte, path SchemaPath, name, kind string, ignoreNotFound, views bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, st, err := m.schema(attach, path)
	if err != nil {
		return err
	}
	key := strings.ToLower(name)
	if s != nil {
		objs := s.tables
		if views {
			objs = s.views
		}
		if _, ok := objs[key]; ok {
			delete(objs, key)
			st.version++
			return nil
		}
	}
	if ignoreNotFound {
		return nil
	}
	return memoryErr("%s with name %s does not exist", kind, name)
}

func (m *MemoryCatalog) tableCreate(req *TableCreateRequestWire) error {
	if len(req.Columns) == 0 {
		return memoryErr("catalog_table_create: missing columns schema")
	}
	columns, err := DeserializeSchema(req.Columns)
	if err != nil {
		return fmt.Errorf("catalog_table_create: deserialize columns: %w", err)
	}
	item, err := SerializeTableInfo(&TableInfo{
		Name:                  req.Name,
		SchemaPath:            slices.Clone(req.SchemaPath),
		Columns:               columns,
		NotNullConstraints:    req.NotNullConstraints,
		UniqueConstraints:     req.UniqueConstraints,
		CheckConstraints:      req.CheckConstraints,
		PrimaryKeyConstraints: req.PrimaryKeyConstraints,
		ForeignKeyConstraints: req.ForeignKeyConstraints,
	})
	if err != nil {
		return err
	}
	return m.putObject(req.AttachOpaqueData, req.SchemaPath, req.Name, "Table", req.OnConflict, false, item)
}

func (m *MemoryCatalog) tableDrop(req *TableDropRequestWire) error {
	return m.dropObject(req.AttachOpaqueData, req.SchemaPath, req.Name, "Table", req.IgnoreNotFound, false)
}

func (m *MemoryCatalog) viewCreate(req *ViewCreateRequestWire) error {
	item, err := SerializeViewInfo(&ViewInfo{Name: req.Name, SchemaPath: slices.Clone(req.SchemaPath), Definition: req.Definition})
	if err != nil {
		return err
	}
	return m.putObject(req.AttachOpaqueData, req.SchemaPath, req.Name, "View", req.OnConflict, true, item)
}

func (m *MemoryCatalog) viewDrop(req *ViewDropRequestWire) error {
	return m.dropObject(req.AttachOpaqueData, req.SchemaPath, req.Name, "View", req.IgnoreNotFound, true)
}
