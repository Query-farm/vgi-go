// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SerializedItems is a list of Arrow-IPC-encoded items sent over the wire.
type SerializedItems = [][]byte

// ---------------------------------------------------------------------------
// Catalog wire types
// ---------------------------------------------------------------------------

// CatalogsResponseWire wraps the list of catalog names.
type CatalogsResponseWire struct {
	Items []string `vgirpc:"items"`
}

// CatalogAttachRequestWire is the wire type for catalog_attach.
type CatalogAttachRequestWire struct {
	Name    string  `vgirpc:"name"`
	Options *[]byte `vgirpc:"options"`
}

// CatalogAttachResultWire is the wire type for catalog_attach result.
type CatalogAttachResultWire struct {
	AttachID             []byte          `vgirpc:"attach_id"`
	SupportsTransactions bool            `vgirpc:"supports_transactions"`
	SupportsTimeTravel   bool            `vgirpc:"supports_time_travel"`
	CatalogVersionFrozen bool            `vgirpc:"catalog_version_frozen"`
	CatalogVersion       int64           `vgirpc:"catalog_version"`
	AttachIDRequired     bool            `vgirpc:"attach_id_required"`
	DefaultSchema        string          `vgirpc:"default_schema"`
	Settings             SerializedItems `vgirpc:"settings"`
}

// CatalogVersionRequestWire is the wire type for catalog_version.
type CatalogVersionRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// CatalogVersionResponseWire wraps the version number.
type CatalogVersionResponseWire struct {
	Version int64 `vgirpc:"version"`
}

// SchemasRequestWire is the wire type for catalog_schemas and related.
type SchemasRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// SchemaGetRequestWire is the wire type for catalog_schema_get.
type SchemaGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// SchemaContentsRequestWire is for schema_contents_tables/views.
type SchemaContentsRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// SchemaContentsFunctionsRequestWire is for schema_contents_functions.
type SchemaContentsFunctionsRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	Name          string  `vgirpc:"name"`
	Type          string  `vgirpc:"type,enum"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// ItemsResponseWire wraps a list of serialized items (schemas/tables/views/functions).
type ItemsResponseWire struct {
	Items SerializedItems `vgirpc:"items"`
}

// DetachRequestWire is the wire type for catalog_detach.
type DetachRequestWire struct {
	AttachID []byte `vgirpc:"attach_id"`
}

// TransactionBeginRequestWire is the wire type for catalog_transaction_begin.
type TransactionBeginRequestWire struct {
	AttachID []byte `vgirpc:"attach_id"`
}

// TransactionBeginResponseWire wraps optional transaction ID.
type TransactionBeginResponseWire struct {
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TransactionRequestWire is the wire type for commit/rollback.
type TransactionRequestWire struct {
	AttachID      []byte `vgirpc:"attach_id"`
	TransactionID []byte `vgirpc:"transaction_id"`
}

// CatalogDropRequestWire is for catalog_drop.
type CatalogDropRequestWire struct {
	Name string `vgirpc:"name"`
}

// CatalogCreateRequestWire is for catalog_create.
type CatalogCreateRequestWire struct {
	Name       string  `vgirpc:"name"`
	OnConflict string  `vgirpc:"on_conflict,enum"`
	Options    *[]byte `vgirpc:"options"`
}

// SchemaCreateRequestWire is for catalog_schema_create.
type SchemaCreateRequestWire struct {
	AttachID      []byte             `vgirpc:"attach_id"`
	Name          string             `vgirpc:"name"`
	Comment       *string            `vgirpc:"comment"`
	Tags          *map[string]string `vgirpc:"tags"`
	TransactionID *[]byte            `vgirpc:"transaction_id"`
}

// SchemaDropRequestWire is for catalog_schema_drop.
type SchemaDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	Name           string  `vgirpc:"name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	Cascade        *bool   `vgirpc:"cascade"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableGetRequestWire is for catalog_table_get.
type TableGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TableDropRequestWire is for catalog_table_drop.
type TableDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableScanFunctionGetRequestWire is for catalog_table_scan_function_get.
type TableScanFunctionGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	AtUnit        *string `vgirpc:"at_unit"`
	AtValue       *string `vgirpc:"at_value"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TableScanFunctionGetResponseWire wraps the scan function result.
// Fields are serialized directly (not wrapped in a binary "result" column)
// so the C++ extension's ExtractAndDeserializeResult can find them.
type TableScanFunctionGetResponseWire struct {
	FunctionName       string   `vgirpc:"function_name"`
	Arguments          []byte   `vgirpc:"arguments"`
	RequiredExtensions []string `vgirpc:"required_extensions"`
}

// TableCommentSetRequestWire is for catalog_table_comment_set.
type TableCommentSetRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	Comment        *string `vgirpc:"comment"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableRenameRequestWire is for catalog_table_rename.
type TableRenameRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	NewName        string  `vgirpc:"new_name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableColumnAddRequestWire is for catalog_table_column_add.
type TableColumnAddRequestWire struct {
	AttachID          []byte  `vgirpc:"attach_id"`
	SchemaName        string  `vgirpc:"schema_name"`
	Name              string  `vgirpc:"name"`
	ColumnDefinition  []byte  `vgirpc:"column_definition"`
	IgnoreNotFound    *bool   `vgirpc:"ignore_not_found"`
	IfColumnNotExists *bool   `vgirpc:"if_column_not_exists"`
	TransactionID     *[]byte `vgirpc:"transaction_id"`
}

// TableColumnDropRequestWire is for catalog_table_column_drop.
type TableColumnDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	ColumnName     string  `vgirpc:"column_name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	IfColumnExists *bool   `vgirpc:"if_column_exists"`
	Cascade        *bool   `vgirpc:"cascade"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableColumnRenameRequestWire is for catalog_table_column_rename.
type TableColumnRenameRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	ColumnName     string  `vgirpc:"column_name"`
	NewColumnName  string  `vgirpc:"new_column_name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableColumnDefaultSetRequestWire is for catalog_table_column_default_set.
type TableColumnDefaultSetRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	ColumnName     string  `vgirpc:"column_name"`
	Expression     string  `vgirpc:"expression"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableColumnDefaultDropRequestWire is for catalog_table_column_default_drop.
type TableColumnDefaultDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	ColumnName     string  `vgirpc:"column_name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableColumnTypeChangeRequestWire is for catalog_table_column_type_change.
type TableColumnTypeChangeRequestWire struct {
	AttachID         []byte  `vgirpc:"attach_id"`
	SchemaName       string  `vgirpc:"schema_name"`
	Name             string  `vgirpc:"name"`
	ColumnDefinition []byte  `vgirpc:"column_definition"`
	Expression       *string `vgirpc:"expression"`
	IgnoreNotFound   *bool   `vgirpc:"ignore_not_found"`
	TransactionID    *[]byte `vgirpc:"transaction_id"`
}

// TableNotNullRequestWire is for catalog_table_not_null_set/drop.
type TableNotNullRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	ColumnName     string  `vgirpc:"column_name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// ViewGetRequestWire is for catalog_view_get.
type ViewGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// ViewCreateRequestWire is for catalog_view_create.
type ViewCreateRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	Definition    string  `vgirpc:"definition"`
	OnConflict    string  `vgirpc:"on_conflict,enum"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// ViewDropRequestWire is for catalog_view_drop.
type ViewDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// ViewRenameRequestWire is for catalog_view_rename.
type ViewRenameRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	NewName        string  `vgirpc:"new_name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// ViewCommentSetRequestWire is for catalog_view_comment_set.
type ViewCommentSetRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	Comment        *string `vgirpc:"comment"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// MacroGetRequestWire is for catalog_macro_get.
type MacroGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// MacroCreateRequestWire is for catalog_macro_create.
type MacroCreateRequestWire struct {
	AttachID               []byte  `vgirpc:"attach_id"`
	SchemaName             string  `vgirpc:"schema_name"`
	Name                   string  `vgirpc:"name"`
	MacroType              string  `vgirpc:"macro_type,enum"`
	Parameters             []string `vgirpc:"parameters"`
	Definition             string  `vgirpc:"definition"`
	OnConflict             string  `vgirpc:"on_conflict,enum"`
	ParameterDefaultValues *[]byte `vgirpc:"parameter_default_values"`
	TransactionID          *[]byte `vgirpc:"transaction_id"`
}

// MacroDropRequestWire is for catalog_macro_drop.
type MacroDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	IgnoreNotFound *bool   `vgirpc:"ignore_not_found"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// SchemaContentsMacrosRequestWire is for schema_contents_macros.
type SchemaContentsMacrosRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	Name          string  `vgirpc:"name"`
	Type          string  `vgirpc:"type,enum"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TableCreateRequestWire is for catalog_table_create.
type TableCreateRequestWire struct {
	AttachID           []byte    `vgirpc:"attach_id"`
	SchemaName         string    `vgirpc:"schema_name"`
	Name               string    `vgirpc:"name"`
	Columns            []byte    `vgirpc:"columns"`
	OnConflict         string    `vgirpc:"on_conflict,enum"`
	NotNullConstraints *[]int32  `vgirpc:"not_null_constraints"`
	CheckConstraints   *[]string `vgirpc:"check_constraints"`
	TransactionID      *[]byte   `vgirpc:"transaction_id"`
}

// ---------------------------------------------------------------------------
// DefaultReadOnlyCatalog
// ---------------------------------------------------------------------------

// DefaultReadOnlyCatalog auto-generates from registered functions.
type DefaultReadOnlyCatalog struct {
	catalogName string
	schemas     map[string]*catalogSchemaInfo
	version     int64
	attachID    []byte
}

type catalogSchemaInfo struct {
	info      *SchemaInfo
	functions []FunctionInfo
	tables    []CatalogTable
	views     []CatalogView
	macros    []CatalogMacro
}

// NewDefaultReadOnlyCatalog creates a catalog from registered functions.
func NewDefaultReadOnlyCatalog(catalogName string, w *Worker) *DefaultReadOnlyCatalog {
	cat := &DefaultReadOnlyCatalog{
		catalogName: catalogName,
		schemas:     make(map[string]*catalogSchemaInfo),
		version:     1,
	}

	// Create "main" schema with all functions
	mainSchema := &catalogSchemaInfo{
		info: &SchemaInfo{
			Name:    "main",
			Comment: "Default schema containing all registered functions",
		},
	}

	// Helper to build FunctionInfo from any function type
	buildFunctionInfo := func(name string, ft FunctionType, meta FunctionMetadata, specs []ArgSpec) FunctionInfo {
		fi := FunctionInfo{
			Name:         name,
			SchemaName:   "main",
			FunctionType: ft,
			Stability:    meta.Stability,
			NullHandling: meta.NullHandling,
			Description:  meta.Description,
			Categories:   meta.Categories,
			ArgSchema:    BuildArgSchema(specs),
			OutputSchema: arrow.NewSchema(nil, nil), // empty, resolved at bind time
		}
		if meta.ProjectionPushdown {
			v := true
			fi.ProjectionPushdown = &v
		}
		if meta.FilterPushdown {
			v := true
			fi.FilterPushdown = &v
		}
		return fi
	}

	// Scalar functions need a 1-field output schema for DuckDB.
	// Use the concrete return type if declared, otherwise null with vgi:any metadata.
	dynamicOutputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.Null, Metadata: arrow.NewMetadata(
			[]string{"vgi:any"}, []string{"true"},
		)},
	}, nil)

	for name, fn := range w.scalars {
		meta := fn.Metadata()
		fi := buildFunctionInfo(name, FunctionTypeScalar, meta, fn.ArgumentSpecs())
		if meta.ReturnType != nil {
			fi.OutputSchema = arrow.NewSchema([]arrow.Field{
				{Name: "result", Type: meta.ReturnType},
			}, nil)
		} else {
			fi.OutputSchema = dynamicOutputSchema
		}
		mainSchema.functions = append(mainSchema.functions, fi)
	}

	for name, fn := range w.tables {
		meta := fn.Metadata()
		fi := buildFunctionInfo(name, FunctionTypeTable, meta, fn.ArgumentSpecs())
		mainSchema.functions = append(mainSchema.functions, fi)
	}

	for name, fn := range w.tableInOuts {
		meta := fn.Metadata()
		fi := buildFunctionInfo(name, FunctionTypeTable, meta, fn.ArgumentSpecs()) // table-in-out registers as "table"
		mainSchema.functions = append(mainSchema.functions, fi)
	}

	cat.schemas["main"] = mainSchema

	// Add "data" schema (empty, for catalog compatibility)
	dataSchema := &catalogSchemaInfo{
		info: &SchemaInfo{
			Name:    "data",
			Comment: "Data schema",
		},
	}
	cat.schemas["data"] = dataSchema

	// Populate catalog tables from worker registrations
	for schemaName, tables := range w.catalogTables {
		si, ok := cat.schemas[schemaName]
		if !ok {
			si = &catalogSchemaInfo{
				info: &SchemaInfo{
					Name:    schemaName,
					Comment: schemaName + " schema",
				},
			}
			cat.schemas[schemaName] = si
		}
		si.tables = append(si.tables, tables...)
	}

	// Populate catalog views from worker registrations
	for schemaName, views := range w.catalogViews {
		si, ok := cat.schemas[schemaName]
		if !ok {
			si = &catalogSchemaInfo{
				info: &SchemaInfo{
					Name:    schemaName,
					Comment: schemaName + " schema",
				},
			}
			cat.schemas[schemaName] = si
		}
		si.views = append(si.views, views...)
	}

	// Populate catalog macros from worker registrations
	for schemaName, macros := range w.catalogMacros {
		si, ok := cat.schemas[schemaName]
		if !ok {
			si = &catalogSchemaInfo{
				info: &SchemaInfo{
					Name:    schemaName,
					Comment: schemaName + " schema",
				},
			}
			cat.schemas[schemaName] = si
		}
		si.macros = append(si.macros, macros...)
	}

	return cat
}

// ---------------------------------------------------------------------------
// Catalog RPC handler registration
// ---------------------------------------------------------------------------

// registerCatalogMethods registers all catalog RPC methods on the server.
func (w *Worker) registerCatalogMethods(s *vgirpc.Server) {
	readOnlyErr := func(op string) error {
		return &vgirpc.RpcError{
			Type:    "NotImplementedError",
			Message: fmt.Sprintf("Catalog is read-only: %s not supported", op),
		}
	}

	// catalog_catalogs
	vgirpc.Unary[struct{}, CatalogsResponseWire](s, "catalog_catalogs",
		func(ctx context.Context, callCtx *vgirpc.CallContext, _ struct{}) (CatalogsResponseWire, error) {
			return CatalogsResponseWire{Items: []string{w.catalogName}}, nil
		})

	// catalog_attach
	vgirpc.Unary[CatalogAttachRequestWire, CatalogAttachResultWire](s, "catalog_attach",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogAttachRequestWire) (CatalogAttachResultWire, error) {
			// Validate catalog name matches
			if req.Name != w.catalogName {
				return CatalogAttachResultWire{}, &vgirpc.RpcError{
					Type:    "ValueError",
					Message: fmt.Sprintf("Unknown catalog: '%s'", req.Name),
				}
			}
			// Generate a simple attach ID
			attachID := []byte(req.Name)
			if w.catalog != nil {
				w.catalog.attachID = attachID
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
					slog.Error("failed to serialize setting", "name", spec.Name, "err", err)
					continue
				}
				serializedSettings = append(serializedSettings, data)
			}
			if serializedSettings == nil {
				serializedSettings = [][]byte{}
			}

			return CatalogAttachResultWire{
				AttachID:             attachID,
				SupportsTransactions: false,
				SupportsTimeTravel:   false,
				CatalogVersionFrozen: true,
				CatalogVersion:       version,
				AttachIDRequired:     false,
				DefaultSchema:        "main",
				Settings:             serializedSettings,
			}, nil
		})

	// catalog_detach
	vgirpc.UnaryVoid[DetachRequestWire](s, "catalog_detach",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req DetachRequestWire) error {
			return nil
		})

	// catalog_version
	vgirpc.Unary[CatalogVersionRequestWire, CatalogVersionResponseWire](s, "catalog_version",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogVersionRequestWire) (CatalogVersionResponseWire, error) {
			version := int64(1)
			if w.catalog != nil {
				version = w.catalog.version
			}
			return CatalogVersionResponseWire{Version: version}, nil
		})

	// catalog_transaction_begin
	vgirpc.Unary[TransactionBeginRequestWire, TransactionBeginResponseWire](s, "catalog_transaction_begin",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TransactionBeginRequestWire) (TransactionBeginResponseWire, error) {
			return TransactionBeginResponseWire{}, nil
		})

	// catalog_transaction_commit
	vgirpc.UnaryVoid[TransactionRequestWire](s, "catalog_transaction_commit",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TransactionRequestWire) error {
			return nil
		})

	// catalog_transaction_rollback
	vgirpc.UnaryVoid[TransactionRequestWire](s, "catalog_transaction_rollback",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TransactionRequestWire) error {
			return nil
		})

	// catalog_schemas
	vgirpc.Unary[SchemasRequestWire, ItemsResponseWire](s, "catalog_schemas",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemasRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			var items [][]byte
			for _, si := range w.catalog.schemas {
				data, err := SerializeSchemaInfo(si.info)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				items = append(items, data)
			}
			return ItemsResponseWire{Items: items}, nil
		})

	// catalog_schema_get
	vgirpc.Unary[SchemaGetRequestWire, ItemsResponseWire](s, "catalog_schema_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaGetRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.Name]
			if !ok {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			data, err := SerializeSchemaInfo(si.info)
			if err != nil {
				return ItemsResponseWire{}, err
			}
			return ItemsResponseWire{Items: [][]byte{data}}, nil
		})

	// catalog_schema_create (read-only)
	vgirpc.UnaryVoid[SchemaCreateRequestWire](s, "catalog_schema_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaCreateRequestWire) error {
			return readOnlyErr("catalog_schema_create")
		})

	// catalog_schema_drop (read-only)
	vgirpc.UnaryVoid[SchemaDropRequestWire](s, "catalog_schema_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaDropRequestWire) error {
			return readOnlyErr("catalog_schema_drop")
		})

	// catalog_schema_contents_tables
	vgirpc.Unary[SchemaContentsRequestWire, ItemsResponseWire](s, "catalog_schema_contents_tables",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaContentsRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.Name]
			if !ok || len(si.tables) == 0 {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			var items [][]byte
			for i := range si.tables {
				data, err := w.serializeCatalogTable(req.Name, &si.tables[i])
				if err != nil {
					return ItemsResponseWire{}, err
				}
				items = append(items, data)
			}
			return ItemsResponseWire{Items: items}, nil
		})

	// catalog_schema_contents_views
	vgirpc.Unary[SchemaContentsRequestWire, ItemsResponseWire](s, "catalog_schema_contents_views",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaContentsRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.Name]
			if !ok || len(si.views) == 0 {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			var items [][]byte
			for _, cv := range si.views {
				info := &ViewInfo{
					Name:       cv.Name,
					SchemaName: req.Name,
					Comment:    cv.Comment,
					Definition: cv.Definition,
				}
				data, err := SerializeViewInfo(info)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				items = append(items, data)
			}
			return ItemsResponseWire{Items: items}, nil
		})

	// catalog_schema_contents_functions
	vgirpc.Unary[SchemaContentsFunctionsRequestWire, ItemsResponseWire](s, "catalog_schema_contents_functions",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaContentsFunctionsRequestWire) (ItemsResponseWire, error) {
			slog.Debug("catalog: listing functions", "schema", req.Name, "type", req.Type)
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.Name]
			if !ok {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}

			var items [][]byte
			for i := range si.functions {
				fi := &si.functions[i]
				// Filter by type if requested
				// DuckDB sends "SCALAR_FUNCTION", "TABLE_FUNCTION", etc.
				if req.Type != "" {
					wantScalar := req.Type == "scalar" || req.Type == "SCALAR_FUNCTION"
					wantTable := req.Type == "table" || req.Type == "TABLE_FUNCTION"
					if wantScalar && fi.FunctionType != FunctionTypeScalar {
						continue
					}
					if wantTable && fi.FunctionType != FunctionTypeTable {
						continue
					}
				}
				slog.Debug("catalog: returning function", "name", fi.Name, "type", fi.FunctionType)
				data, err := SerializeFunctionInfo(fi)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				items = append(items, data)
			}
			return ItemsResponseWire{Items: items}, nil
		})

	// catalog_table_get
	vgirpc.Unary[TableGetRequestWire, ItemsResponseWire](s, "catalog_table_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableGetRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.SchemaName]
			if !ok {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			for i := range si.tables {
				if si.tables[i].Name == req.Name {
					data, err := w.serializeCatalogTable(req.SchemaName, &si.tables[i])
					if err != nil {
						return ItemsResponseWire{}, err
					}
					return ItemsResponseWire{Items: [][]byte{data}}, nil
				}
			}
			return ItemsResponseWire{Items: [][]byte{}}, nil
		})

	// catalog_table_create (read-only)
	vgirpc.UnaryVoid[TableCreateRequestWire](s, "catalog_table_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableCreateRequestWire) error {
			return readOnlyErr("catalog_table_create")
		})

	// catalog_table_drop (read-only)
	vgirpc.UnaryVoid[TableDropRequestWire](s, "catalog_table_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableDropRequestWire) error {
			return readOnlyErr("catalog_table_drop")
		})

	// catalog_table_scan_function_get
	vgirpc.Unary[TableScanFunctionGetRequestWire, TableScanFunctionGetResponseWire](s, "catalog_table_scan_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableScanFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			slog.Debug("catalog: scan function get", "schema", req.SchemaName, "table", req.Name)

			// Check for a registered catalog table with a backing function
			if w.catalog != nil {
				if si, ok := w.catalog.schemas[req.SchemaName]; ok {
					for i := range si.tables {
						if si.tables[i].Name == req.Name && si.tables[i].Function != nil {
							result := w.buildScanResultFromTable(&si.tables[i])
							return buildScanFunctionGetResponse(result)
						}
					}
				}
			}

			// Delegate to the handler if set
			if w.scanFunctionGetHandler != nil {
				result, err := w.scanFunctionGetHandler(req.SchemaName, req.Name)
				if err != nil {
					return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: err.Error(),
					}
				}
				return buildScanFunctionGetResponse(result)
			}

			return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{
				Type:    "NotImplementedError",
				Message: fmt.Sprintf("table_scan_function_get not implemented for %s.%s", req.SchemaName, req.Name),
			}
		})

	// catalog_table_comment_set (read-only)
	vgirpc.UnaryVoid[TableCommentSetRequestWire](s, "catalog_table_comment_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableCommentSetRequestWire) error {
			return readOnlyErr("catalog_table_comment_set")
		})

	// catalog_table_rename (read-only)
	vgirpc.UnaryVoid[TableRenameRequestWire](s, "catalog_table_rename",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableRenameRequestWire) error {
			return readOnlyErr("catalog_table_rename")
		})

	// catalog_table_column_add (read-only)
	vgirpc.UnaryVoid[TableColumnAddRequestWire](s, "catalog_table_column_add",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnAddRequestWire) error {
			return readOnlyErr("catalog_table_column_add")
		})

	// catalog_table_column_drop (read-only)
	vgirpc.UnaryVoid[TableColumnDropRequestWire](s, "catalog_table_column_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnDropRequestWire) error {
			return readOnlyErr("catalog_table_column_drop")
		})

	// catalog_table_column_rename (read-only)
	vgirpc.UnaryVoid[TableColumnRenameRequestWire](s, "catalog_table_column_rename",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnRenameRequestWire) error {
			return readOnlyErr("catalog_table_column_rename")
		})

	// catalog_table_column_default_set (read-only)
	vgirpc.UnaryVoid[TableColumnDefaultSetRequestWire](s, "catalog_table_column_default_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnDefaultSetRequestWire) error {
			return readOnlyErr("catalog_table_column_default_set")
		})

	// catalog_table_column_default_drop (read-only)
	vgirpc.UnaryVoid[TableColumnDefaultDropRequestWire](s, "catalog_table_column_default_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnDefaultDropRequestWire) error {
			return readOnlyErr("catalog_table_column_default_drop")
		})

	// catalog_table_column_type_change (read-only)
	vgirpc.UnaryVoid[TableColumnTypeChangeRequestWire](s, "catalog_table_column_type_change",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnTypeChangeRequestWire) error {
			return readOnlyErr("catalog_table_column_type_change")
		})

	// catalog_table_not_null_set (read-only)
	vgirpc.UnaryVoid[TableNotNullRequestWire](s, "catalog_table_not_null_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableNotNullRequestWire) error {
			return readOnlyErr("catalog_table_not_null_set")
		})

	// catalog_table_not_null_drop (read-only)
	vgirpc.UnaryVoid[TableNotNullRequestWire](s, "catalog_table_not_null_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableNotNullRequestWire) error {
			return readOnlyErr("catalog_table_not_null_drop")
		})

	// catalog_view_get
	vgirpc.Unary[ViewGetRequestWire, ItemsResponseWire](s, "catalog_view_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewGetRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.SchemaName]
			if !ok {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			for _, cv := range si.views {
				if cv.Name == req.Name {
					info := &ViewInfo{
						Name:       cv.Name,
						SchemaName: req.SchemaName,
						Comment:    cv.Comment,
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
		})

	// catalog_view_create (read-only)
	vgirpc.UnaryVoid[ViewCreateRequestWire](s, "catalog_view_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewCreateRequestWire) error {
			return readOnlyErr("catalog_view_create")
		})

	// catalog_view_drop (read-only)
	vgirpc.UnaryVoid[ViewDropRequestWire](s, "catalog_view_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewDropRequestWire) error {
			return readOnlyErr("catalog_view_drop")
		})

	// catalog_view_rename (read-only)
	vgirpc.UnaryVoid[ViewRenameRequestWire](s, "catalog_view_rename",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewRenameRequestWire) error {
			return readOnlyErr("catalog_view_rename")
		})

	// catalog_view_comment_set (read-only)
	vgirpc.UnaryVoid[ViewCommentSetRequestWire](s, "catalog_view_comment_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewCommentSetRequestWire) error {
			return readOnlyErr("catalog_view_comment_set")
		})

	// catalog_macro_get
	vgirpc.Unary[MacroGetRequestWire, ItemsResponseWire](s, "catalog_macro_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req MacroGetRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.SchemaName]
			if !ok {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			for _, cm := range si.macros {
				if cm.Name == req.Name {
					info := &MacroInfo{
						Name:                   cm.Name,
						SchemaName:             req.SchemaName,
						Comment:                cm.Comment,
						MacroType:              cm.MacroType,
						Parameters:             cm.Parameters,
						ParameterDefaultValues: cm.ParameterDefaultValues,
						Definition:             cm.Definition,
					}
					data, err := SerializeMacroInfo(info)
					if err != nil {
						return ItemsResponseWire{}, err
					}
					return ItemsResponseWire{Items: [][]byte{data}}, nil
				}
			}
			return ItemsResponseWire{Items: [][]byte{}}, nil
		})

	// catalog_schema_contents_macros
	vgirpc.Unary[SchemaContentsMacrosRequestWire, ItemsResponseWire](s, "catalog_schema_contents_macros",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaContentsMacrosRequestWire) (ItemsResponseWire, error) {
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.Name]
			if !ok {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}

			var items [][]byte
			for _, cm := range si.macros {
				// Filter by type if requested
				if req.Type != "" {
					wantScalar := req.Type == "scalar_macro" || req.Type == "SCALAR_MACRO"
					wantTable := req.Type == "table_macro" || req.Type == "TABLE_MACRO"
					if wantScalar && cm.MacroType != MacroTypeScalar {
						continue
					}
					if wantTable && cm.MacroType != MacroTypeTable {
						continue
					}
				}
				info := &MacroInfo{
					Name:                   cm.Name,
					SchemaName:             req.Name,
					Comment:                cm.Comment,
					MacroType:              cm.MacroType,
					Parameters:             cm.Parameters,
					ParameterDefaultValues: cm.ParameterDefaultValues,
					Definition:             cm.Definition,
				}
				data, err := SerializeMacroInfo(info)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				items = append(items, data)
			}
			return ItemsResponseWire{Items: items}, nil
		})

	// catalog_macro_create (read-only)
	vgirpc.UnaryVoid[MacroCreateRequestWire](s, "catalog_macro_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req MacroCreateRequestWire) error {
			return readOnlyErr("catalog_macro_create")
		})

	// catalog_macro_drop (read-only)
	vgirpc.UnaryVoid[MacroDropRequestWire](s, "catalog_macro_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req MacroDropRequestWire) error {
			return readOnlyErr("catalog_macro_drop")
		})

	// catalog_create (read-only)
	vgirpc.UnaryVoid[CatalogCreateRequestWire](s, "catalog_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogCreateRequestWire) error {
			return readOnlyErr("catalog_create")
		})

	// catalog_drop (read-only)
	vgirpc.UnaryVoid[CatalogDropRequestWire](s, "catalog_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogDropRequestWire) error {
			return readOnlyErr("catalog_drop")
		})
}

// serializeCatalogTable converts a CatalogTable into serialized TableInfo bytes.
func (w *Worker) serializeCatalogTable(schemaName string, ct *CatalogTable) ([]byte, error) {
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

	// Resolve NOT NULL constraint indices from column names
	var notNull []int32
	if columns != nil {
		for _, colName := range ct.NotNull {
			for i := 0; i < columns.NumFields(); i++ {
				if columns.Field(i).Name == colName {
					notNull = append(notNull, int32(i))
					break
				}
			}
		}
	}

	// Resolve UNIQUE constraint indices from column name groups
	var unique [][]int32
	if columns != nil {
		for _, group := range ct.Unique {
			var indices []int32
			for _, colName := range group {
				for i := 0; i < columns.NumFields(); i++ {
					if columns.Field(i).Name == colName {
						indices = append(indices, int32(i))
						break
					}
				}
			}
			unique = append(unique, indices)
		}
	}

	info := &TableInfo{
		Name:               ct.Name,
		SchemaName:         schemaName,
		Comment:            ct.Comment,
		Columns:            columns,
		NotNullConstraints: notNull,
		UniqueConstraints:  unique,
		CheckConstraints:   ct.Check,
	}

	return SerializeTableInfo(info)
}

// buildScanResultFromTable creates a ScanFunctionResult from a function-backed CatalogTable.
func (w *Worker) buildScanResultFromTable(ct *CatalogTable) *ScanFunctionResult {
	result := &ScanFunctionResult{
		FunctionName: ct.Function.Name(),
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
