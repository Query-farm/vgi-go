// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package simple_writable is a Go port of vgi-python's simple_writable test
// fixture: a minimal in-memory writable catalog that drives the C++ extension's
// INSERT/UPDATE/DELETE/RETURNING wire path. It exists so the shared
// test/sql/integration/simple_writable/*.test files (which run against every VGI
// implementation) also exercise the Go worker's write-function plumbing.
//
// The catalog exposes four pre-defined tables under the "main" schema with
// distinct capabilities, so the tests can exercise each rejection/validation
// path:
//
//   - items                  — INSERT/UPDATE/DELETE with RETURNING.
//   - items_no_returning     — INSERT/UPDATE/DELETE, RETURNING unsupported.
//   - items_insert_only      — INSERT only (UPDATE/DELETE not exposed).
//   - items_broken_returning — advertises RETURNING but its insert function
//     always emits a (count) batch, exercising the extension's runtime
//     RETURNING-schema validation (clean error, not a crash).
//
// Tables are virtual (served by the handlers below, not RegisterCatalogTable);
// rows live in per-attach AttachStore (see store.go). Wire it onto a Worker via
// Options() + Register().
package simple_writable

import (
	"bytes"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
)

// CatalogName is the SQL-visible catalog (tests ATTACH 'simple_writable').
const CatalogName = "simple_writable"

// SchemaPath is the single schema the catalog exposes.
const SchemaPath = "main"

func isSchema(path vgi.SchemaPath) bool { return len(path) == 1 && path[0] == SchemaPath }

// rowIDMeta marks the worker-declared rowid pseudocolumn so the C++ extension
// tracks it for UPDATE/DELETE row addressing.
var rowIDMeta = arrow.NewMetadata([]string{"is_row_id"}, []string{""})

func rowIDField() arrow.Field {
	return arrow.Field{Name: "rowid", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: rowIDMeta}
}

// countSchema is the affected-row-count result emitted for result_mode=count.
var countSchema = arrow.NewSchema([]arrow.Field{
	{Name: "count", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// userSchemas holds the user-visible columns (no rowid) of each table.
var userSchemas = map[string]*arrow.Schema{
	"items": arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "qty", Type: arrow.PrimitiveTypes.Int64},
	}, nil),
	"items_no_returning": arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "qty", Type: arrow.PrimitiveTypes.Int64},
	}, nil),
	"items_insert_only": arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil),
	"items_broken_returning": arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil),
}

// tableOrder is the stable listing order for schema_contents (count(*) etc.).
var tableOrder = []string{"items", "items_broken_returning", "items_insert_only", "items_no_returning"}

func userSchema(table string) (*arrow.Schema, bool) {
	s, ok := userSchemas[table]
	return s, ok
}

func maximumResultMode(table string) string {
	if table == "items_no_returning" {
		return "count"
	}
	if table == "items_broken_returning" {
		return "rows"
	}
	return "changes"
}

func supportsUpdateDelete(table string) bool {
	return table != "items_insert_only" && table != "items_broken_returning"
}

// catalogNameOf extracts the catalog name from attach_opaque_data. The validator
// below mints "simple_writable\x00<uuid>", so take the bytes up to the first NUL
// (mirrors the SDK's internal catalogNameOf, which examples can't call).
func catalogNameOf(attachOpaqueData []byte) string {
	s := attachOpaqueData
	if i := bytes.IndexByte(s, 0); i >= 0 {
		s = s[:i]
	}
	return string(s)
}

func isOurs(attachOpaqueData []byte) bool { return catalogNameOf(attachOpaqueData) == CatalogName }

// tableInfo builds the wire TableInfo for one table (user columns + rowid, with
// the per-table capability flags).
func tableInfo(table string) (*vgi.TableInfo, bool) {
	us, ok := userSchema(table)
	if !ok {
		return nil, false
	}
	fields := append(append([]arrow.Field{}, us.Fields()...), rowIDField())
	ud := supportsUpdateDelete(table)
	modes := map[string]string{"insert": maximumResultMode(table)}
	if ud {
		modes["update"] = maximumResultMode(table)
		modes["delete"] = maximumResultMode(table)
	}
	return &vgi.TableInfo{
		Name:             table,
		SchemaPath:       vgi.SchemaPath{SchemaPath},
		Columns:          arrow.NewSchema(fields, nil),
		WriteResultModes: modes,
	}, true
}

func serializeTableInfo(table string) ([]byte, error) {
	info, ok := tableInfo(table)
	if !ok {
		return nil, nil
	}
	return vgi.SerializeTableInfo(info)
}

// ---------------------------------------------------------------------------
// Catalog handlers (wired via the vgi.WithAttach*Handler worker options)
// ---------------------------------------------------------------------------

// SchemaContentsHandler lists the four tables for the main schema.
func SchemaContentsHandler(attachOpaqueData []byte, schemaPath vgi.SchemaPath) ([]vgi.SerializedSchemaItem, bool) {
	if !isOurs(attachOpaqueData) || !isSchema(schemaPath) {
		return nil, false
	}
	items := make([]vgi.SerializedSchemaItem, 0, len(tableOrder))
	for _, t := range tableOrder {
		data, err := serializeTableInfo(t)
		if err != nil || data == nil {
			continue
		}
		items = append(items, data)
	}
	return items, true
}

// AttachTableGetHandler answers single-table catalog_table_get RPCs.
func AttachTableGetHandler(attachOpaqueData []byte, schemaPath vgi.SchemaPath, name string, atUnit, atValue *string) ([]byte, bool, error) {
	if !isOurs(attachOpaqueData) || !isSchema(schemaPath) {
		return nil, false, nil
	}
	if _, ok := userSchema(name); !ok {
		return nil, false, nil
	}
	data, err := serializeTableInfo(name)
	if err != nil {
		return nil, true, err
	}
	return data, true, nil
}

// AttachScanFunctionGetHandler routes SELECT to simple_writable_scan(<table>).
func AttachScanFunctionGetHandler(attachOpaqueData []byte, schemaPath vgi.SchemaPath, name string, atUnit, atValue *string) (*vgi.ScanFunctionResult, bool, error) {
	if !isOurs(attachOpaqueData) || !isSchema(schemaPath) {
		return nil, false, nil
	}
	if _, ok := userSchema(name); !ok {
		return nil, false, nil
	}
	return scanResult("simple_writable_scan", name), true, nil
}

// AttachWriteFunctionGetHandler routes INSERT/UPDATE/DELETE. UPDATE/DELETE are
// only offered for tables that support them; items_broken_returning routes its
// INSERT to the misbehaving function.
func AttachWriteFunctionGetHandler(op vgi.WriteOp, attachOpaqueData []byte, schemaPath vgi.SchemaPath, name string) (*vgi.ScanFunctionResult, bool, error) {
	if !isOurs(attachOpaqueData) || !isSchema(schemaPath) {
		return nil, false, nil
	}
	if _, ok := userSchema(name); !ok {
		return nil, false, nil
	}
	switch op {
	case vgi.WriteOpInsert:
		if name == "items_broken_returning" {
			return scanResult("simple_writable_broken_returning_insert", name), true, nil
		}
		return scanResult("simple_writable_insert", name), true, nil
	case vgi.WriteOpUpdate:
		if !supportsUpdateDelete(name) {
			return nil, false, nil
		}
		return scanResult("simple_writable_update", name), true, nil
	case vgi.WriteOpDelete:
		if !supportsUpdateDelete(name) {
			return nil, false, nil
		}
		return scanResult("simple_writable_delete", name), true, nil
	default:
		return nil, false, nil
	}
}

func scanResult(fn, table string) *vgi.ScanFunctionResult {
	return &vgi.ScanFunctionResult{
		FunctionName: fn,
		PositionalArguments: []vgi.ScanArg{
			{Value: table, Type: arrow.BinaryTypes.String},
		},
	}
}

// ---------------------------------------------------------------------------
// Worker wiring
// ---------------------------------------------------------------------------

// Options returns the WorkerOptions that publish the simple_writable catalog.
// The AttachValidator mints a random per-ATTACH scope ("simple_writable\x00<uuid>")
// so each ATTACH gets isolated row storage (matching vgi-python's per-attach
// SQLite file); the handlers serve the virtual tables.
func Options() []vgi.WorkerOption {
	return []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithSupportsTransactions(false),
		vgi.WithAttachValidator(func(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
			if req.Name != CatalogName {
				return nil, nil
			}
			scope := make([]byte, 0, len(CatalogName)+1+16)
			scope = append(scope, CatalogName...)
			scope = append(scope, 0)
			u := uuid.New()
			scope = append(scope, u[:]...)
			return &vgi.AttachDecision{AttachOpaqueData: scope}, nil
		}),
		vgi.WithSchemaContentsHandler(SchemaContentsHandler),
		vgi.WithAttachTableGetHandler(AttachTableGetHandler),
		vgi.WithAttachScanFunctionGetHandler(AttachScanFunctionGetHandler),
		vgi.WithAttachWriteFunctionGetHandler(AttachWriteFunctionGetHandler),
	}
}

// Register wires the five worker functions onto w, scoped to the catalog.
func Register(w *vgi.Worker) {
	w.RegisterTableForCatalog(CatalogName, vgi.AsTableFunction[scanState](scanFn{}))
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[noState](insertFn{}))
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[noState](updateFn{}))
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[noState](deleteFn{}))
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[noState](brokenInsertFn{}))
}
