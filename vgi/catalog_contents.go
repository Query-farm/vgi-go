// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// catalog_contents (protocol 2.1.0) returns every schema and all of its
// contents in one result, replacing catalog_schemas plus one
// catalog_schema_contents_* call per schema and kind. It is composed from the
// SAME listings those RPCs serve (listSchemaInfos, listTableItems, ...), so
// every item is byte-for-byte what the per-schema RPC returns and a client
// decodes it with the decoder it already has. It mirrors vgi-python's
// CatalogInterface.catalog_contents default.

// CatalogContentsRequestWire is the wire type for catalog_contents. It takes no
// transaction: the client caches the answer for the whole attach, so it is the
// committed catalog at catalog_version.
type CatalogContentsRequestWire struct {
	AttachOpaqueData []byte `vgirpc:"attach_opaque_data"`
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

// catalogContents answers catalog_contents: the catalog version and one
// IPC-serialized SchemaContents per schema, parents before children.
func (w *Worker) catalogContents(attachOpaqueData []byte, callCtx *vgirpc.CallContext) (generated.CatalogContentsResponse, error) {
	version, err := w.catalogVersionOf(attachOpaqueData, callCtx)
	if err != nil {
		return generated.CatalogContentsResponse{}, err
	}
	infos, err := w.listSchemaInfos(attachOpaqueData)
	if err != nil {
		return generated.CatalogContentsResponse{}, err
	}
	schemas := make([][]byte, 0, len(infos))
	for _, info := range infos {
		contents, err := w.schemaContentsOf(attachOpaqueData, info)
		if err != nil {
			return generated.CatalogContentsResponse{}, fmt.Errorf("catalog_contents: schema %s: %w", schemaPathDisplay(info.Path), err)
		}
		data, err := SerializeSchemaContents(&contents)
		if err != nil {
			return generated.CatalogContentsResponse{}, err
		}
		schemas = append(schemas, data)
	}
	return generated.CatalogContentsResponse{CatalogVersion: version, Schemas: schemas}, nil
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

// SerializeSchemaContents encodes one catalog_contents schema entry as Arrow
// IPC bytes matching generated.SchemaContentsSchema.
func SerializeSchemaContents(contents *generated.SchemaContents) ([]byte, error) {
	return encodeWireRecord(generated.SchemaContentsSchema, contents)
}

// DeserializeSchemaContents decodes one catalog_contents schema entry.
func DeserializeSchemaContents(data []byte) (generated.SchemaContents, error) {
	var out generated.SchemaContents
	err := decodeWireRecord(data, &out)
	return out, err
}

// ---------------------------------------------------------------------------
// Generated-record codec
// ---------------------------------------------------------------------------

// The generated record types (generated/protocol_types.go) are vgirpc-tagged
// structs. vgi-rpc-go serializes one itself when it is a method's params or
// result, but a record nested as an opaque binary blob (SchemaContents inside
// CatalogContentsResponse.schemas) is this SDK's to encode. These two do that
// for any generated record, column by column against the schema codegen owns,
// matching each column to the field whose vgirpc tag names it. They cover the
// Go types the record generator emits for the records it lists; anything else
// is an error, not a guess.

// encodeWireRecord encodes v (a pointer to a vgirpc-tagged struct) as a
// one-row Arrow IPC stream with the given schema.
func encodeWireRecord(schema *arrow.Schema, v any) ([]byte, error) {
	rv := reflect.Indirect(reflect.ValueOf(v))
	fields := wireFieldIndex(rv.Type())
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, 0, schema.NumFields())
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	for _, f := range schema.Fields() {
		idx, ok := fields[f.Name]
		if !ok {
			return nil, fmt.Errorf("%s: no field tagged %q", rv.Type(), f.Name)
		}
		b := array.NewBuilder(mem, f.Type)
		err := appendWireValue(b, rv.Field(idx))
		if err == nil && b.Len() != 1 {
			err = fmt.Errorf("built %d values", b.Len())
		}
		if err != nil {
			b.Release()
			return nil, fmt.Errorf("%s.%s: %w", rv.Type(), f.Name, err)
		}
		cols = append(cols, b.NewArray())
		b.Release()
	}
	batch := array.NewRecordBatch(schema, cols, 1)
	defer batch.Release()
	var buf bytes.Buffer
	wtr := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := wtr.Write(batch); err != nil {
		return nil, err
	}
	if err := wtr.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeWireRecord decodes the first row of an IPC stream into out (a pointer
// to a vgirpc-tagged struct). Columns with no matching field are ignored, so a
// record a newer peer appended to still decodes.
func decodeWireRecord(data []byte, out any) error {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return err
		}
		return fmt.Errorf("record carries no batch")
	}
	batch := reader.RecordBatch()
	if batch.NumRows() < 1 {
		return fmt.Errorf("record batch has no rows")
	}
	rv := reflect.ValueOf(out).Elem()
	fields := wireFieldIndex(rv.Type())
	for i, f := range batch.Schema().Fields() {
		idx, ok := fields[f.Name]
		if !ok {
			continue
		}
		if err := setWireValue(rv.Field(idx), batch.Column(i), 0); err != nil {
			return fmt.Errorf("%s.%s: %w", rv.Type(), f.Name, err)
		}
	}
	return nil
}

// wireFieldIndex maps each vgirpc tag name of t to its field index.
func wireFieldIndex(t reflect.Type) map[string]int {
	out := make(map[string]int, t.NumField())
	for i := range t.NumField() {
		tag := t.Field(i).Tag.Get("vgirpc")
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		out[name] = i
	}
	return out
}

// appendWireValue appends one Go value to an Arrow builder of the matching
// type. A nil pointer is a null.
func appendWireValue(b array.Builder, v reflect.Value) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			b.AppendNull()
			return nil
		}
		v = v.Elem()
	}
	switch b := b.(type) {
	case *array.BooleanBuilder:
		b.Append(v.Bool())
	case *array.Int32Builder:
		b.Append(int32(v.Int()))
	case *array.Int64Builder:
		b.Append(v.Int())
	case *array.StringBuilder:
		b.Append(v.String())
	case *array.BinaryBuilder:
		if v.Kind() == reflect.String {
			b.AppendString(v.String())
		} else {
			b.Append(v.Bytes())
		}
	case *array.ListBuilder:
		b.Append(true)
		for i := range v.Len() {
			if err := appendWireValue(b.ValueBuilder(), v.Index(i)); err != nil {
				return err
			}
		}
	case *array.MapBuilder:
		b.Append(true)
		keys := v.MapKeys()
		// Sorted, so the same record always encodes to the same bytes.
		slices.SortFunc(keys, func(x, y reflect.Value) int { return strings.Compare(fmt.Sprint(x), fmt.Sprint(y)) })
		for _, k := range keys {
			if err := appendWireValue(b.KeyBuilder(), k); err != nil {
				return err
			}
			if err := appendWireValue(b.ItemBuilder(), v.MapIndex(k)); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported Arrow builder %T for Go %s", b, v.Type())
	}
	return nil
}

// setWireValue stores row i of an Arrow array into a Go field of the
// matching type. A null leaves a pointer field nil.
func setWireValue(field reflect.Value, arr arrow.Array, i int) error {
	if field.Kind() == reflect.Pointer {
		if arr.IsNull(i) {
			field.SetZero()
			return nil
		}
		field.Set(reflect.New(field.Type().Elem()))
		field = field.Elem()
	}
	switch arr := arr.(type) {
	case *array.Boolean:
		field.SetBool(arr.Value(i))
	case *array.Int32:
		field.SetInt(int64(arr.Value(i)))
	case *array.Int64:
		field.SetInt(arr.Value(i))
	case *array.String:
		field.SetString(arr.Value(i))
	case *array.Binary:
		field.SetBytes(bytes.Clone(arr.Value(i)))
	case *array.List:
		start, end := arr.ValueOffsets(i)
		out := reflect.MakeSlice(field.Type(), int(end-start), int(end-start))
		for j := start; j < end; j++ {
			if err := setWireValue(out.Index(int(j-start)), arr.ListValues(), int(j)); err != nil {
				return err
			}
		}
		field.Set(out)
	case *array.Map:
		start, end := arr.ValueOffsets(i)
		out := reflect.MakeMapWithSize(field.Type(), int(end-start))
		for j := start; j < end; j++ {
			k := reflect.New(field.Type().Key()).Elem()
			val := reflect.New(field.Type().Elem()).Elem()
			if err := setWireValue(k, arr.Keys(), int(j)); err != nil {
				return err
			}
			if err := setWireValue(val, arr.Items(), int(j)); err != nil {
				return err
			}
			out.SetMapIndex(k, val)
		}
		field.Set(out)
	default:
		return fmt.Errorf("unsupported Arrow array %T for Go %s", arr, field.Type())
	}
	return nil
}
