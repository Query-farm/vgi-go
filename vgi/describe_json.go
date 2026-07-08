// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Standardized VGI worker HTTP landing surface.
//
// Every VGI worker serves a small, stable JSON contract that the shared static
// landing page (landing.html, byte-identical across the Python, Go, Rust,
// TypeScript, and Java implementations) fetches same-origin and renders. This
// file is the Go producer for that contract; see the normative spec at
// ~/Development/vgi/docs/http-landing-contract.md and the Python reference
// producer vgi-python/vgi/http/describe_json.py.
//
// Three routes make up the surface, registered by RunHttp:
//
//   - GET {prefix}/                                     — landing.html (browsers)
//     or {"status":"ok",...} (?format=json / Accept: application/json)
//   - GET {prefix}/describe.json                        — the describe contract
//   - GET {prefix}/describe/{catalog}/{schema}/{table}.json — lazy columns
//
// The document is versioned by landing_schema_version independently of the VGI
// wire protocol. Table/view columns are lazy: describe.json carries only a
// column count, and the page fetches per-object detail from the columns route
// on first expand.

package vgi

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// landingHTML is the vendored, self-contained shared landing page. It is
// byte-identical to the copy vendored by every other VGI language worker (see
// vgi-web-frontend/public/landing.html) and carries a
// "<!-- vgi-landing-asset vN -->" marker the conformance harness asserts on.
//
//go:embed landing.html
var landingHTML []byte

const (
	landingSchemaVersion = 1
	cupolaBase           = "https://cupola.query-farm.services"
)

// vgiCatalogTagKeys maps describe.json tag names to the reserved vgi.* keys in
// the catalog's tag map (duckdb_databases().tags). All are optional.
var vgiCatalogStringTags = [...][2]string{
	{"title", "vgi.title"},
	{"doc_md", "vgi.doc_md"},
	{"source_url", "vgi.source_url"},
	{"license", "vgi.license"},
	{"author", "vgi.author"},
	{"copyright", "vgi.copyright"},
	{"support_contact", "vgi.support_contact"},
	{"support_policy_url", "vgi.support_policy_url"},
}

const vgiKeywordsTag = "vgi.keywords" // JSON array encoded as a string in the tags map

// vgiGoVersion returns the build version of the module that embeds this package,
// falling back to "dev". Used only for describe.json's worker.version display
// field (normalized away in cross-language golden comparison).
func vgiGoVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		if v := info.Main.Version; v != "" && v != "(devel)" {
			return v
		}
	}
	return "dev"
}

// buildDescribeJSON builds the full describe.json document. name/serverID/oauth
// are supplied by RunHttp since they depend on runtime config, not the worker's
// registered catalog.
func (w *Worker) buildDescribeJSON(name, serverID string, oauth bool) map[string]any {
	return map[string]any{
		"landing_schema_version": landingSchemaVersion,
		"worker": map[string]any{
			"name":    name,
			"doc":     w.catalogComment,
			"version": vgiGoVersion(),
			"lang":    "go",
		},
		"server_id":   serverID,
		"oauth":       oauth,
		"cupola_base": cupolaBase,
		"catalogs":    w.buildDescribeCatalogs(),
	}
}

// buildDescribeCatalogs enumerates the catalogs this worker advertises: the
// primary read-only catalog plus every alias registered with its own discovery
// metadata (WithCatalogAliasInfo), mirroring catalog_catalogs. Plain aliases
// (WithCatalogAliases) share the primary catalog's identity and are not listed.
func (w *Worker) buildDescribeCatalogs() []map[string]any {
	catalogs := []map[string]any{}

	// Primary catalog.
	primary := descCatalog{
		name:     w.catalogName,
		tags:     w.catalogTags,
		attachOs: w.attachOptions,
	}
	if w.catalogInfoOverride != nil {
		primary.impl = w.catalogInfoOverride.ImplementationVersion
		primary.dvs = w.catalogInfoOverride.DataVersionSpec
		primary.releases = w.catalogInfoOverride.Releases
	}
	catalogs = append(catalogs, w.buildDescribeCatalog(primary, false))

	// Alias-info catalogs (deterministic order).
	aliasNames := make([]string, 0, len(w.catalogAliasInfos))
	for n := range w.catalogAliasInfos {
		aliasNames = append(aliasNames, n)
	}
	sort.Strings(aliasNames)
	for _, n := range aliasNames {
		info := w.catalogAliasInfos[n]
		catalogs = append(catalogs, w.buildDescribeCatalog(descCatalog{
			name:     n,
			impl:     info.ImplementationVersion,
			dvs:      info.DataVersionSpec,
			releases: info.Releases,
		}, true))
	}
	return catalogs
}

// descCatalog carries the per-catalog inputs to buildDescribeCatalog.
type descCatalog struct {
	name     string
	impl     *string
	dvs      *string
	releases []CatalogDataVersionRelease
	tags     map[string]string
	attachOs []AttachOptionSpec
}

// buildDescribeCatalog builds one catalog entry. aliasOnly restricts the catalog
// to the "main" schema and hides functions scoped to a different catalog,
// matching the catalog_schemas / catalog_schema_contents_functions RPC behavior.
func (w *Worker) buildDescribeCatalog(dc descCatalog, aliasOnly bool) map[string]any {
	dataVersions := []map[string]any{}
	for _, r := range dc.releases {
		dv := map[string]any{"spec": r.Version}
		if r.Summary != "" {
			dv["label"] = r.Summary
		}
		dataVersions = append(dataVersions, dv)
	}

	attachOptions := []map[string]any{}
	for _, spec := range dc.attachOs {
		typeStr := ""
		if spec.Type != nil {
			typeStr = spec.Type.String()
		}
		attachOptions = append(attachOptions, map[string]any{
			"name":        spec.Name,
			"type":        typeStr,
			"default":     "",
			"description": spec.Description,
		})
	}

	schemas, counts := w.buildDescribeSchemas(dc.name, aliasOnly)

	catalog := map[string]any{
		"name":                   dc.name,
		"implementation_version": strPtrOrNil(dc.impl),
		"data_version_spec":      strPtrOrNil(dc.dvs),
		"data_versions":          dataVersions,
		"attach_options":         attachOptions,
		"tags":                   vgiCatalogTags(dc.tags),
		"counts":                 counts,
		"schemas":                schemas,
	}
	return catalog
}

// buildDescribeSchemas builds the schema list + aggregate counts for a catalog.
//
// Each catalog reports only the objects that belong to it (mirroring the Python
// reference, which attaches to that catalog and enumerates its own contents):
//
//   - The primary catalog owns the shared tables/views and the unscoped
//     functions (plus any function scoped to itself); functions scoped to a
//     different catalog are hidden.
//   - An alias/secondary catalog (aliasOnly) exposes only its "main" schema and
//     only the functions scoped to it. It does NOT inherit the primary catalog's
//     shared tables/views or its unscoped functions.
func (w *Worker) buildDescribeSchemas(catalogName string, aliasOnly bool) ([]map[string]any, map[string]int) {
	counts := map[string]int{"schemas": 0, "tables": 0, "views": 0, "functions": 0}
	schemas := []map[string]any{}
	if w.catalog == nil {
		return schemas, counts
	}

	names := make([]string, 0, len(w.catalog.schemas))
	for n := range w.catalog.schemas {
		if aliasOnly && n != "main" {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		si := w.catalog.schemas[name]

		// The shared tables/views belong to the primary catalog only; a
		// secondary catalog must not inherit them.
		tables := []map[string]any{}
		views := []map[string]any{}
		if !aliasOnly {
			for i := range si.tables {
				t := &si.tables[i]
				cols := w.resolveTableColumns(t)
				n := 0
				if cols != nil {
					n = len(cols.Fields())
				}
				tables = append(tables, map[string]any{
					"name":    t.Name,
					"cols":    n,
					"comment": t.Comment,
				})
			}
			for i := range si.views {
				v := &si.views[i]
				views = append(views, map[string]any{
					"name":    v.Name,
					"cols":    len(v.ColumnComments),
					"comment": v.Comment,
					"def":     v.Definition,
				})
			}
		}

		functions := w.buildDescribeFunctions(si.functions, catalogName, aliasOnly)

		// Macros fold into the same functions array (a scalar macro is invoked
		// like a scalar function in SQL, a table macro like a table function).
		// Like the shared tables/views and unscoped functions, they belong to
		// the primary catalog only; a secondary (alias) catalog must not inherit
		// them, so skip them under aliasOnly.
		if !aliasOnly {
			functions = append(functions, buildDescribeMacros(si.macros)...)
			// Re-sort the combined functions+macros by (type, name) so the
			// document (and the cross-language golden) is stable.
			sort.SliceStable(functions, func(i, j int) bool {
				ti, _ := functions[i]["type"].(string)
				tj, _ := functions[j]["type"].(string)
				if ti != tj {
					return ti < tj
				}
				ni, _ := functions[i]["name"].(string)
				nj, _ := functions[j]["name"].(string)
				return ni < nj
			})
		}

		// Drop empty inherited schemas from a secondary catalog: if a schema
		// contributes no catalog-local functions (and, for alias catalogs, no
		// tables/views), it isn't part of this catalog's surface.
		if aliasOnly && len(functions) == 0 && len(tables) == 0 && len(views) == 0 {
			continue
		}

		schemas = append(schemas, map[string]any{
			"name":      name,
			"tables":    tables,
			"views":     views,
			"functions": functions,
		})
		counts["schemas"]++
		counts["tables"] += len(tables)
		counts["views"] += len(views)
		counts["functions"] += len(functions)
	}
	return schemas, counts
}

// buildDescribeFunctions builds the catalog-local function list for a schema.
//
// A function pinned to a specific catalog (catalogFunctionScope) belongs only to
// that catalog. An unscoped function belongs to the primary catalog. Therefore:
//
//   - primary catalog (aliasOnly == false): include unscoped functions and
//     functions scoped to this catalog; hide functions scoped elsewhere.
//   - alias catalog (aliasOnly == true): include only functions scoped to this
//     catalog; unscoped functions belong to the primary catalog, not here.
func (w *Worker) buildDescribeFunctions(fns []FunctionInfo, catalogName string, aliasOnly bool) []map[string]any {
	visible := make([]*FunctionInfo, 0, len(fns))
	for i := range fns {
		fi := &fns[i]
		scope, scoped := w.catalogFunctionScope[fi.Name]
		if aliasOnly {
			if !scoped || scope != catalogName {
				continue
			}
		} else if scoped && catalogName != "" && catalogName != scope {
			continue
		}
		visible = append(visible, fi)
	}
	sort.SliceStable(visible, func(i, j int) bool {
		ti, tj := describeFunctionType(visible[i]), describeFunctionType(visible[j])
		if ti != tj {
			return ti < tj
		}
		return visible[i].Name < visible[j].Name
	})

	out := make([]map[string]any, 0, len(visible))
	for _, fi := range visible {
		fn := map[string]any{
			"name": fi.Name,
			"type": describeFunctionType(fi),
			"doc":  fi.Description,
			"args": describeFunctionArgs(fi),
		}
		if r := describeFunctionReturns(fi); r != "" {
			fn["returns"] = r
		}
		out = append(out, fn)
	}
	return out
}

// describeFunctionType maps a FunctionInfo to the contract's function type
// (scalar | table | aggregate | table_in_out). Table-in-out functions register
// as DuckDB table functions but carry HasFinalize.
func describeFunctionType(fi *FunctionInfo) string {
	switch fi.FunctionType {
	case FunctionTypeScalar:
		return "scalar"
	case FunctionTypeAggregate:
		return "aggregate"
	default:
		if fi.HasFinalize {
			return "table_in_out"
		}
		return "table"
	}
}

// describeFunctionReturns renders the return-type string, or "" to omit it.
// Scalar/aggregate output is a single "result" column; table output (resolved
// at bind time) carries no fields here and is omitted.
func describeFunctionReturns(fi *FunctionInfo) string {
	sch := fi.OutputSchema
	if sch == nil || len(sch.Fields()) == 0 {
		return ""
	}
	if fi.FunctionType == FunctionTypeScalar || fi.FunctionType == FunctionTypeAggregate {
		return sch.Field(0).Type.String()
	}
	parts := make([]string, 0, len(sch.Fields()))
	for _, f := range sch.Fields() {
		parts = append(parts, f.Name+" "+f.Type.String())
	}
	return "TABLE(" + strings.Join(parts, ", ") + ")"
}

// describeFunctionArgs renders a function's user-supplied arguments, skipping the
// piped input relation of a table-in-out function (vgi_type=table).
func describeFunctionArgs(fi *FunctionInfo) []map[string]any {
	args := []map[string]any{}
	if fi.ArgSchema == nil {
		return args
	}
	for _, f := range fi.ArgSchema.Fields() {
		if fieldMeta(f, "vgi_type") == "table" {
			continue
		}
		arg := map[string]any{
			"name": f.Name,
			"type": f.Type.String(),
		}
		if fieldMeta(f, "vgi_arg") == "named" {
			arg["named"] = true
		}
		if doc := fieldMeta(f, "vgi_doc"); doc != "" {
			arg["desc"] = doc
		}
		if def, ok := fieldMetaOK(f, "vgi_default"); ok {
			arg["default"] = def
		}
		args = append(args, arg)
	}
	return args
}

// buildDescribeMacros renders a schema's macros as function-shaped entries for
// the describe.json functions array. A scalar macro is invoked exactly like a
// scalar function in SQL, and a table macro like a table function; VGI catalogs
// commonly expose their callable "functions" as declarative macros, so they
// must appear alongside functions on the landing page. Macros carry no returns
// field (their output type is resolved by DuckDB when the macro expands).
func buildDescribeMacros(macros []CatalogMacro) []map[string]any {
	out := make([]map[string]any, 0, len(macros))
	for i := range macros {
		cm := &macros[i]
		out = append(out, map[string]any{
			"name": cm.Name,
			"type": describeMacroType(cm),
			"doc":  cm.Comment,
			"args": describeMacroArgs(cm),
		})
	}
	return out
}

// describeMacroType maps a macro to the contract's function type: a table macro
// lists as "table", every other macro (scalar) as "scalar".
func describeMacroType(cm *CatalogMacro) string {
	if cm.MacroType == MacroTypeTable {
		return "table"
	}
	return "scalar"
}

// describeMacroArgs renders a macro's parameters, in declaration order. Each
// parameter's type comes from the macro arguments_schema (the type of its typed
// default when known, else the Arrow null placeholder, which we surface as the
// literal "ANY"); its description rides as vgi_doc field metadata (the same
// channel functions use). A defaulted parameter is optional and callable by
// name in DuckDB, so it is presented as a named arg carrying its JSON-encoded
// default value.
func describeMacroArgs(cm *CatalogMacro) []map[string]any {
	args := []map[string]any{}

	// Default values keyed by parameter name (a 1-row RecordBatch of typed
	// defaults). Presence marks a parameter as optional/named.
	defaults := map[string]any{}
	if len(cm.ParameterDefaultValues) > 0 {
		if batch, err := DeserializeRecordBatch(cm.ParameterDefaultValues); err == nil {
			for i, f := range batch.Schema().Fields() {
				defaults[f.Name] = arrowValueAt(batch.Column(i), 0)
			}
			batch.Release()
		} else {
			LogRPC.Debug("describe: macro default-values deserialize failed", "macro", cm.Name, "err", err)
		}
	}

	// Per-parameter typed fields (+ vgi_doc metadata) from the arguments schema.
	fieldByName := map[string]arrow.Field{}
	if raw, err := BuildMacroArgumentsSchema(cm.Parameters, cm.ParameterDefaultValues, cm.ParameterDocs); err == nil && len(raw) > 0 {
		if sch, err := DeserializeSchema(raw); err == nil {
			for _, f := range sch.Fields() {
				fieldByName[f.Name] = f
			}
		} else {
			LogRPC.Debug("describe: macro arguments schema deserialize failed", "macro", cm.Name, "err", err)
		}
	}

	for _, name := range cm.Parameters {
		field, hasField := fieldByName[name]
		arg := map[string]any{"name": name}
		// Macro parameters are untyped unless a typed default pins them; show
		// ANY rather than the Arrow null placeholder.
		if hasField && field.Type != nil && field.Type.ID() != arrow.NULL {
			arg["type"] = field.Type.String()
		} else {
			arg["type"] = "ANY"
		}
		if hasField {
			if doc := fieldMeta(field, "vgi_doc"); doc != "" {
				arg["desc"] = doc
			}
		}
		if val, ok := defaults[name]; ok {
			arg["named"] = true
			if b, err := json.Marshal(val); err == nil {
				arg["default"] = string(b)
			} else {
				arg["default"] = fmt.Sprintf("%v", val)
			}
		}
		args = append(args, arg)
	}
	return args
}

// vgiCatalogTags projects the reserved vgi.* keys out of a catalog's tag map
// into the describe.json tags object. Returns an empty (non-nil) map when none
// are present.
func vgiCatalogTags(tags map[string]string) map[string]any {
	out := map[string]any{}
	if len(tags) == 0 {
		return out
	}
	for _, kv := range vgiCatalogStringTags {
		if v := tags[kv[1]]; v != "" {
			out[kv[0]] = v
		}
	}
	if kw := tags[vgiKeywordsTag]; kw != "" {
		var parsed []string
		if err := json.Unmarshal([]byte(kw), &parsed); err == nil {
			out["keywords"] = parsed
		}
	}
	return out
}

// buildColumnsJSON builds the lazy per-object column payload for one table or
// view: {"columns":[{"name","type","comment"?}]}. Returns (nil, false) when the
// object can't be found. Tables resolve their Arrow schema (deriving it via the
// backing function's bind for function-backed tables); views expose their
// declared column comments (types are only known after binding the SQL, which
// is not done here).
func (w *Worker) buildColumnsJSON(schema, table string) (map[string]any, bool) {
	if w.catalog == nil {
		return nil, false
	}
	si, ok := w.catalog.schemas[schema]
	if !ok {
		return nil, false
	}
	for i := range si.tables {
		if si.tables[i].Name == table {
			cols := w.resolveTableColumns(&si.tables[i])
			return map[string]any{"columns": columnsFromSchema(cols)}, true
		}
	}
	for i := range si.views {
		if si.views[i].Name == table {
			columns := []map[string]any{}
			for name, comment := range si.views[i].ColumnComments {
				columns = append(columns, map[string]any{
					"name":    name,
					"type":    "",
					"comment": comment,
				})
			}
			return map[string]any{"columns": columns}, true
		}
	}
	return nil, false
}

// columnsFromSchema renders an Arrow schema's fields as column objects.
func columnsFromSchema(sch *arrow.Schema) []map[string]any {
	columns := []map[string]any{}
	if sch == nil {
		return columns
	}
	for _, f := range sch.Fields() {
		col := map[string]any{
			"name": f.Name,
			"type": f.Type.String(),
		}
		if c := fieldMeta(f, "comment"); c != "" {
			col["comment"] = c
		} else if c := fieldMeta(f, "vgi_doc"); c != "" {
			col["comment"] = c
		}
		columns = append(columns, col)
	}
	return columns
}

// resolveTableColumns resolves a catalog table's columns: the explicit Columns
// schema when set, otherwise (function-backed tables) the schema produced by the
// backing function's OnBind. Best-effort: returns nil on bind failure.
func (w *Worker) resolveTableColumns(ct *CatalogTable) *arrow.Schema {
	if ct.Columns != nil {
		return ct.Columns
	}
	if ct.Function != nil {
		bindParams := &BindParams{
			FunctionName: ct.Function.Name(),
			FunctionType: FunctionTypeTable,
			Args:         w.buildBindArgs(ct),
		}
		if resp, err := ct.Function.OnBind(bindParams); err == nil && resp != nil {
			return resp.OutputSchema
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

// makeLandingHandler returns the GET {prefix}/ handler: the shared landing.html
// for browsers, a small JSON status for ?format=json / Accept: application/json.
func makeLandingHandler(serverID string) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		wantJSON := r.URL.Query().Get("format") == "json" ||
			(strings.Contains(accept, "application/json") && !strings.Contains(accept, "text/html"))
		if wantJSON {
			rw.Header().Set("Content-Type", "application/json")
			writeJSON(rw, map[string]any{"status": "ok", "server_id": serverID, "protocol": "vgi"})
			return
		}
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write(landingHTML)
	}
}

// makeDescribeJSONHandler returns the GET {prefix}/describe.json handler.
func (w *Worker) makeDescribeJSONHandler(name, serverID string, oauth bool) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		writeJSON(rw, w.buildDescribeJSON(name, serverID, oauth))
	}
}

// makeColumnsHandler returns the GET {prefix}/describe/{catalog}/{schema}/{table}
// handler (the ".json" suffix is stripped from the final path segment).
func (w *Worker) makeColumnsHandler() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		schema := r.PathValue("schema")
		table := strings.TrimSuffix(r.PathValue("table"), ".json")
		rw.Header().Set("Content-Type", "application/json")
		cols, ok := w.buildColumnsJSON(schema, table)
		if !ok {
			rw.WriteHeader(http.StatusNotFound)
			writeJSON(rw, map[string]any{"error": "object not found"})
			return
		}
		writeJSON(rw, cols)
	}
}

func writeJSON(rw http.ResponseWriter, v any) {
	enc := json.NewEncoder(rw)
	if err := enc.Encode(v); err != nil {
		LogRPC.Debug("describe: json encode failed", "err", err)
	}
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func fieldMeta(f arrow.Field, key string) string {
	v, _ := fieldMetaOK(f, key)
	return v
}

func fieldMetaOK(f arrow.Field, key string) (string, bool) {
	idx := f.Metadata.FindKey(key)
	if idx < 0 {
		return "", false
	}
	return f.Metadata.Values()[idx], true
}

// strPtrOrNil returns the pointed-to string, or nil (JSON null) when p is nil,
// preserving the contract's distinction between "" and null for version fields.
func strPtrOrNil(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}
