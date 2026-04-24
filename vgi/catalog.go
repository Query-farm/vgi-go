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
}

// CatalogAttachResultWire is the wire type for catalog_attach result.
type CatalogAttachResultWire struct {
	AttachID                      []byte            `vgirpc:"attach_id"`
	SupportsTransactions          bool              `vgirpc:"supports_transactions"`
	SupportsTimeTravel            bool              `vgirpc:"supports_time_travel"`
	CatalogVersionFrozen          bool              `vgirpc:"catalog_version_frozen"`
	CatalogVersion                int64             `vgirpc:"catalog_version"`
	AttachIDRequired              bool              `vgirpc:"attach_id_required"`
	DefaultSchema                 string            `vgirpc:"default_schema"`
	Settings                      SerializedItems   `vgirpc:"settings"`
	SecretTypes                   SerializedItems   `vgirpc:"secret_types"`
	Comment                       *string           `vgirpc:"comment"`
	Tags                          map[string]string `vgirpc:"tags"`
	SupportsColumnStatistics      bool              `vgirpc:"supports_column_statistics"`
	ResolvedDataVersion           *string           `vgirpc:"resolved_data_version"`
	ResolvedImplementationVersion *string           `vgirpc:"resolved_implementation_version"`
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
	OnConflict    string             `vgirpc:"on_conflict,enum"`
	Comment       *string            `vgirpc:"comment"`
	Tags          *map[string]string `vgirpc:"tags"`
	TransactionID *[]byte            `vgirpc:"transaction_id"`
}

// SchemaDropRequestWire is for catalog_schema_drop.
type SchemaDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	Name           string  `vgirpc:"name"`
	IgnoreNotFound bool    `vgirpc:"ignore_not_found"`
	Cascade        bool    `vgirpc:"cascade"`
	TransactionID  *[]byte `vgirpc:"transaction_id"`
}

// TableGetRequestWire is for catalog_table_get.
type TableGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	AtUnit        *string `vgirpc:"at_unit"`
	AtValue       *string `vgirpc:"at_value"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TableDropRequestWire is for catalog_table_drop.
type TableDropRequestWire struct {
	AttachID       []byte  `vgirpc:"attach_id"`
	SchemaName     string  `vgirpc:"schema_name"`
	Name           string  `vgirpc:"name"`
	IgnoreNotFound bool    `vgirpc:"ignore_not_found"`
	Cascade        bool    `vgirpc:"cascade"`
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
	AttachID               []byte   `vgirpc:"attach_id"`
	SchemaName             string   `vgirpc:"schema_name"`
	Name                   string   `vgirpc:"name"`
	MacroType              string   `vgirpc:"macro_type,enum"`
	Parameters             []string `vgirpc:"parameters"`
	Definition             string   `vgirpc:"definition"`
	OnConflict             string   `vgirpc:"on_conflict,enum"`
	ParameterDefaultValues *[]byte  `vgirpc:"parameter_default_values"`
	TransactionID          *[]byte  `vgirpc:"transaction_id"`
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

// TableColumnStatisticsGetRequestWire is for catalog_table_column_statistics_get.
type TableColumnStatisticsGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}


// TableInsertFunctionGetRequestWire is for catalog_table_insert_function_get.
type TableInsertFunctionGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TableUpdateFunctionGetRequestWire is for catalog_table_update_function_get.
type TableUpdateFunctionGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TableDeleteFunctionGetRequestWire is for catalog_table_delete_function_get.
type TableDeleteFunctionGetRequestWire struct {
	AttachID      []byte  `vgirpc:"attach_id"`
	SchemaName    string  `vgirpc:"schema_name"`
	Name          string  `vgirpc:"name"`
	TransactionID *[]byte `vgirpc:"transaction_id"`
}

// TableCreateRequestWire is for catalog_table_create.
type TableCreateRequestWire struct {
	AttachID               []byte    `vgirpc:"attach_id"`
	SchemaName             string    `vgirpc:"schema_name"`
	Name                   string    `vgirpc:"name"`
	Columns                []byte    `vgirpc:"columns"`
	OnConflict             string    `vgirpc:"on_conflict,enum"`
	NotNullConstraints     []int32   `vgirpc:"not_null_constraints"`
	UniqueConstraints      [][]int32 `vgirpc:"unique_constraints"`
	CheckConstraints       []string  `vgirpc:"check_constraints"`
	PrimaryKeyConstraints  [][]int32 `vgirpc:"primary_key_constraints"`
	ForeignKeyConstraints  [][]byte  `vgirpc:"foreign_key_constraints"`
	TransactionID          *[]byte   `vgirpc:"transaction_id"`
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
	mainComment := "Default schema containing all registered functions"
	if c, ok := w.schemaComments["main"]; ok {
		mainComment = c
	}
	mainSchema := &catalogSchemaInfo{
		info: &SchemaInfo{
			Name:    "main",
			Comment: mainComment,
		},
	}

	// Helper to build FunctionInfo from any function type
	buildFunctionInfo := func(name string, ft FunctionType, meta FunctionMetadata, specs []ArgSpec) FunctionInfo {
		fi := FunctionInfo{
			Name:            name,
			SchemaName:      "main",
			FunctionType:    ft,
			Stability:       meta.Stability,
			NullHandling:    meta.NullHandling,
			Description:     meta.Description,
			Categories:      meta.Categories,
			ArgSchema:       BuildArgSchema(specs),
			OutputSchema:    arrow.NewSchema(nil, nil), // empty, resolved at bind time
			RequiredSecrets: meta.RequiredSecrets,
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
		if len(meta.SupportedExpressionFilters) > 0 {
			fi.SupportedExpressionFilters = meta.SupportedExpressionFilters
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

	for name, fns := range w.scalars {
		for _, fn := range fns {
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
	}

	for name, fns := range w.tables {
		for _, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeTable, meta, fn.ArgumentSpecs())
			mainSchema.functions = append(mainSchema.functions, fi)
		}
	}

	for name, fns := range w.tableInOuts {
		for _, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeTable, meta, fn.ArgumentSpecs()) // table-in-out registers as "table"
			fi.HasFinalize = meta.HasFinalize
			mainSchema.functions = append(mainSchema.functions, fi)
		}
	}

	for name, fns := range w.aggregates {
		for _, fn := range fns {
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
			fi.OrderDependent = meta.OrderDependent
			fi.DistinctDependent = meta.DistinctDependent
			mainSchema.functions = append(mainSchema.functions, fi)
		}
	}

	cat.schemas["main"] = mainSchema

	// Add "data" schema only when the worker actually registers tables,
	// views, macros, or an explicit comment for it. Empty workers (e.g. the
	// versioned-example worker) should expose main only.
	_, hasDataTables := w.catalogTables["data"]
	_, hasDataViews := w.catalogViews["data"]
	_, hasDataMacros := w.catalogMacros["data"]
	_, hasDataComment := w.schemaComments["data"]
	if hasDataTables || hasDataViews || hasDataMacros || hasDataComment {
		dataComment := "Data schema"
		if c, ok := w.schemaComments["data"]; ok {
			dataComment = c
		}
		dataSchema := &catalogSchemaInfo{
			info: &SchemaInfo{
				Name:    "data",
				Comment: dataComment,
			},
		}
		cat.schemas["data"] = dataSchema
	}

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
			info := &CatalogInfo{Name: w.catalogName}
			if w.catalogInfoOverride != nil {
				c := *w.catalogInfoOverride
				info = &c
				if info.Name == "" {
					info.Name = w.catalogName
				}
			}
			data, err := SerializeCatalogInfo(info)
			if err != nil {
				return CatalogsResponseWire{}, err
			}
			return CatalogsResponseWire{Items: SerializedItems{data}}, nil
		})

	// catalog_attach
	vgirpc.Unary[CatalogAttachRequestWire, CatalogAttachResultWire](s, "catalog_attach",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogAttachRequestWire) (CatalogAttachResultWire, error) {
			// Writable catalogs are handled separately so they have their
			// own attach_id and per-catalog table state.
			if wc, ok := w.extraCatalogs[req.Name]; ok {
				return w.handleWritableAttach(req, wc)
			}
			// Validate catalog name matches
			if req.Name != w.catalogName {
				return CatalogAttachResultWire{}, &vgirpc.RpcError{
					Type:    "ValueError",
					Message: fmt.Sprintf("No worker handles catalog '%s'", req.Name),
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

			// Serialize secret types
			var serializedSecretTypes [][]byte
			for _, spec := range w.secretTypes {
				data, err := serializeSecretTypeSpec(spec)
				if err != nil {
					slog.Error("failed to serialize secret type", "name", spec.Name, "err", err)
					continue
				}
				serializedSecretTypes = append(serializedSecretTypes, data)
			}
			if serializedSecretTypes == nil {
				serializedSecretTypes = [][]byte{}
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
			// to embed the chosen version into attach_id.
			attachIDRequired := false
			var resolvedData, resolvedImpl *string
			if w.attachValidator != nil {
				decision, vErr := w.attachValidator(&req, callCtx)
				if vErr != nil {
					return CatalogAttachResultWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: vErr.Error(),
					}
				}
				if decision != nil {
					if decision.AttachID != nil {
						attachID = decision.AttachID
						attachIDRequired = true
						if w.catalog != nil {
							w.catalog.attachID = attachID
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
			result := CatalogAttachResultWire{
				AttachID:                      attachID,
				SupportsTransactions:          false,
				SupportsTimeTravel:            supportsTimeTravel,
				CatalogVersionFrozen:          true,
				CatalogVersion:                version,
				AttachIDRequired:              attachIDRequired,
				DefaultSchema:                 "main",
				Settings:                      serializedSettings,
				SecretTypes:                   serializedSecretTypes,
				Tags:                          tags,
				SupportsColumnStatistics:      supportsColStats,
				ResolvedDataVersion:           resolvedData,
				ResolvedImplementationVersion: resolvedImpl,
			}
			if w.catalogComment != "" {
				c := w.catalogComment
				result.Comment = &c
			}
			return result, nil
		})

	// catalog_detach
	vgirpc.UnaryVoid[DetachRequestWire](s, "catalog_detach",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req DetachRequestWire) error {
			return nil
		})

	// catalog_version
	vgirpc.Unary[CatalogVersionRequestWire, CatalogVersionResponseWire](s, "catalog_version",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogVersionRequestWire) (CatalogVersionResponseWire, error) {
			if w.catalogVersionHook != nil {
				if err := w.catalogVersionHook(req.AttachID, callCtx); err != nil {
					return CatalogVersionResponseWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: err.Error(),
					}
				}
			}
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
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				items, err := w.writableSchemas(wc)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				return ItemsResponseWire{Items: items}, nil
			}
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
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				items, err := w.writableSchemaGet(wc, req.Name)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				return ItemsResponseWire{Items: items}, nil
			}
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

	// catalog_schema_create
	vgirpc.UnaryVoid[SchemaCreateRequestWire](s, "catalog_schema_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaCreateRequestWire) error {
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				return w.writableSchemaCreate(wc, req.Name, parseOnConflict(req.OnConflict), req.Comment)
			}
			return readOnlyErr("catalog_schema_create")
		})

	// catalog_schema_drop
	vgirpc.UnaryVoid[SchemaDropRequestWire](s, "catalog_schema_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaDropRequestWire) error {
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				return w.writableSchemaDrop(wc, req.Name, req.IgnoreNotFound, req.Cascade)
			}
			return readOnlyErr("catalog_schema_drop")
		})

	// catalog_schema_contents_tables
	vgirpc.Unary[SchemaContentsRequestWire, ItemsResponseWire](s, "catalog_schema_contents_tables",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaContentsRequestWire) (ItemsResponseWire, error) {
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				items, err := w.writableSchemaContentsTables(wc, req.Name)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				return ItemsResponseWire{Items: items}, nil
			}
			// Per-attach override (versioned-tables worker etc.): the handler
			// inspects the attach_id (which can encode the resolved version)
			// and returns the right set of tables.
			if w.schemaContentsHandler != nil {
				if items, ok := w.schemaContentsHandler(req.AttachID, req.Name); ok {
					out := make([][]byte, len(items))
					for i, it := range items {
						out[i] = []byte(it)
					}
					return ItemsResponseWire{Items: out}, nil
				}
			}
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
					wantAggregate := req.Type == "aggregate" || req.Type == "AGGREGATE_FUNCTION"
					if wantScalar && fi.FunctionType != FunctionTypeScalar {
						continue
					}
					if wantTable && fi.FunctionType != FunctionTypeTable {
						continue
					}
					if wantAggregate && fi.FunctionType != FunctionTypeAggregate {
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
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				items, err := w.writableTableGet(wc, req.SchemaName, req.Name)
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
				data, handled, err := w.attachTableGetHandler(req.AttachID, req.SchemaName, req.Name, req.AtUnit, req.AtValue)
				if err != nil {
					return ItemsResponseWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: err.Error(),
					}
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
				data, err := w.tableGetHandler(req.SchemaName, req.Name, req.AtUnit, req.AtValue)
				if err != nil {
					return ItemsResponseWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: err.Error(),
					}
				}
				if data != nil {
					return ItemsResponseWire{Items: [][]byte{data}}, nil
				}
			}

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

	// catalog_table_create
	vgirpc.UnaryVoid[TableCreateRequestWire](s, "catalog_table_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableCreateRequestWire) error {
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				return w.writableTableCreate(wc, req)
			}
			return readOnlyErr("catalog_table_create")
		})

	// catalog_table_drop
	vgirpc.UnaryVoid[TableDropRequestWire](s, "catalog_table_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableDropRequestWire) error {
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				return w.writableTableDrop(wc, req.SchemaName, req.Name, req.IgnoreNotFound, req.Cascade)
			}
			return readOnlyErr("catalog_table_drop")
		})

	// catalog_table_scan_function_get
	vgirpc.Unary[TableScanFunctionGetRequestWire, TableScanFunctionGetResponseWire](s, "catalog_table_scan_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableScanFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			if wc := w.writableByAttachID(req.AttachID); wc != nil {
				return buildScanFunctionGetResponse(&ScanFunctionResult{
					FunctionName: writableScanFunctionName,
					PositionalArguments: []ScanArg{
						{Value: req.SchemaName, Type: arrow.BinaryTypes.String},
						{Value: req.Name, Type: arrow.BinaryTypes.String},
					},
				})
			}
			slog.Debug("catalog: scan function get", "schema", req.SchemaName, "table", req.Name)

			// Check for a registered catalog table with a backing function
			// (skip if AT params are present — let the handler deal with time travel)
			hasAt := req.AtUnit != nil && *req.AtUnit != ""
			if !hasAt && w.catalog != nil {
				if si, ok := w.catalog.schemas[req.SchemaName]; ok {
					for i := range si.tables {
						if si.tables[i].Name == req.Name && si.tables[i].Function != nil {
							result := w.buildScanResultFromTable(&si.tables[i])
							return buildScanFunctionGetResponse(result)
						}
					}
				}
			}

			// Attach-id-aware handler takes precedence over the plain one.
			if w.attachScanFunctionGetHandler != nil {
				result, handled, err := w.attachScanFunctionGetHandler(req.AttachID, req.SchemaName, req.Name, req.AtUnit, req.AtValue)
				if err != nil {
					return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: err.Error(),
					}
				}
				if handled {
					return buildScanFunctionGetResponse(result)
				}
			}
			// Delegate to the handler if set
			if w.scanFunctionGetHandler != nil {
				result, err := w.scanFunctionGetHandler(req.SchemaName, req.Name, req.AtUnit, req.AtValue)
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

	// catalog_table_insert_function_get
	vgirpc.Unary[TableInsertFunctionGetRequestWire, TableScanFunctionGetResponseWire](s, "catalog_table_insert_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableInsertFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			if w.writableByAttachID(req.AttachID) == nil {
				return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Message: fmt.Sprintf("table %s.%s is read-only (attach_id=%x len=%d, extra_catalogs=%d)", req.SchemaName, req.Name, req.AttachID, len(req.AttachID), len(w.extraCatalogs))}
			}
			return buildScanFunctionGetResponse(&ScanFunctionResult{
				FunctionName: writableInsertFunctionName,
				PositionalArguments: []ScanArg{
					{Value: req.SchemaName, Type: arrow.BinaryTypes.String},
					{Value: req.Name, Type: arrow.BinaryTypes.String},
				},
			})
		})

	// catalog_table_update_function_get
	vgirpc.Unary[TableUpdateFunctionGetRequestWire, TableScanFunctionGetResponseWire](s, "catalog_table_update_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableUpdateFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			if w.writableByAttachID(req.AttachID) == nil {
				return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Message: fmt.Sprintf("table %s.%s is read-only (attach_id=%x len=%d, extra_catalogs=%d)", req.SchemaName, req.Name, req.AttachID, len(req.AttachID), len(w.extraCatalogs))}
			}
			return buildScanFunctionGetResponse(&ScanFunctionResult{
				FunctionName: writableUpdateFunctionName,
				PositionalArguments: []ScanArg{
					{Value: req.SchemaName, Type: arrow.BinaryTypes.String},
					{Value: req.Name, Type: arrow.BinaryTypes.String},
				},
			})
		})

	// catalog_table_delete_function_get
	vgirpc.Unary[TableDeleteFunctionGetRequestWire, TableScanFunctionGetResponseWire](s, "catalog_table_delete_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableDeleteFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			if w.writableByAttachID(req.AttachID) == nil {
				return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Message: fmt.Sprintf("table %s.%s is read-only (attach_id=%x len=%d, extra_catalogs=%d)", req.SchemaName, req.Name, req.AttachID, len(req.AttachID), len(w.extraCatalogs))}
			}
			return buildScanFunctionGetResponse(&ScanFunctionResult{
				FunctionName: writableDeleteFunctionName,
				PositionalArguments: []ScanArg{
					{Value: req.SchemaName, Type: arrow.BinaryTypes.String},
					{Value: req.Name, Type: arrow.BinaryTypes.String},
				},
			})
		})

	// catalog_table_column_statistics_get — returns raw IPC bytes in the
	// standard "result" binary column. Using []byte directly (not a struct
	// wrapper) avoids vgi-rpc-go's struct-to-IPC double-wrap, so the C++
	// extension's DeserializeFromIpcBytesWithMetadata parses the stats
	// batch directly instead of a nested {result: binary} envelope.
	vgirpc.Unary[TableColumnStatisticsGetRequestWire, []byte](s, "catalog_table_column_statistics_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnStatisticsGetRequestWire) ([]byte, error) {
			ct := w.findCatalogTable(req.SchemaName, req.Name)
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
			return SerializeColumnStatistics(ordered, ct.StatisticsCacheMaxAgeSeconds)
		})
}

// findCatalogTable returns the registered CatalogTable for (schema, name) or nil.
func (w *Worker) findCatalogTable(schemaName, name string) *CatalogTable {
	tables, ok := w.catalogTables[schemaName]
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

	// Serialize FOREIGN KEY constraints
	var foreignKeys [][]byte
	for _, fk := range ct.ForeignKey {
		fkBytes, err := serializeForeignKey(schemaName, &fk)
		if err != nil {
			return nil, fmt.Errorf("serializing foreign key for table %s: %w", ct.Name, err)
		}
		foreignKeys = append(foreignKeys, fkBytes)
	}

	info := &TableInfo{
		Name:                     ct.Name,
		SchemaName:               schemaName,
		Comment:                  ct.Comment,
		Columns:                  columns,
		NotNullConstraints:       notNull,
		UniqueConstraints:        unique,
		CheckConstraints:         ct.Check,
		PrimaryKeyConstraints:    primaryKey,
		ForeignKeyConstraints:    foreignKeys,
		SupportsColumnStatistics: len(ct.Statistics) > 0,
	}

	return SerializeTableInfo(info)
}

// fkSchema is the wire format for a single foreign key constraint.
var fkSchema = arrow.NewSchema([]arrow.Field{
	{Name: "fk_columns", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	{Name: "pk_columns", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	{Name: "referenced_table", Type: arrow.BinaryTypes.String},
	{Name: "referenced_schema", Type: arrow.BinaryTypes.String},
}, nil)

// serializeForeignKey serializes a ForeignKeyConstraint to IPC bytes.
func serializeForeignKey(schemaName string, fk *ForeignKeyConstraint) ([]byte, error) {
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

	// referenced_schema
	refSchemaBuilder := array.NewStringBuilder(mem)
	defer refSchemaBuilder.Release()
	refSchema := fk.ReferencedSchema
	if refSchema == "" {
		refSchema = schemaName
	}
	refSchemaBuilder.Append(refSchema)

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
