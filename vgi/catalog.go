// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
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
	AttachOpaqueData              []byte            `vgirpc:"attach_opaque_data"`
	SupportsTransactions          bool              `vgirpc:"supports_transactions"`
	SupportsTimeTravel            bool              `vgirpc:"supports_time_travel"`
	CatalogVersionFrozen          bool              `vgirpc:"catalog_version_frozen"`
	CatalogVersion                int64             `vgirpc:"catalog_version"`
	AttachOpaqueDataRequired      bool              `vgirpc:"attach_opaque_data_required"`
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
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// CatalogVersionResponseWire wraps the version number.
type CatalogVersionResponseWire struct {
	Version int64 `vgirpc:"version"`
}

// SchemasRequestWire is the wire type for catalog_schemas and related.
type SchemasRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// SchemaGetRequestWire is the wire type for catalog_schema_get.
type SchemaGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// SchemaContentsRequestWire is for schema_contents_tables/views.
type SchemaContentsRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// SchemaContentsFunctionsRequestWire is for schema_contents_functions.
type SchemaContentsFunctionsRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	Name                  string  `vgirpc:"name"`
	Type                  string  `vgirpc:"type,enum"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// ItemsResponseWire wraps a list of serialized items (schemas/tables/views/functions).
type ItemsResponseWire struct {
	Items SerializedItems `vgirpc:"items"`
}

// DetachRequestWire is the wire type for catalog_detach.
type DetachRequestWire struct {
	AttachOpaqueData []byte `vgirpc:"attach_opaque_data"`
}

// TransactionBeginRequestWire is the wire type for catalog_transaction_begin.
type TransactionBeginRequestWire struct {
	AttachOpaqueData []byte `vgirpc:"attach_opaque_data"`
}

// TransactionBeginResponseWire wraps optional transaction ID.
type TransactionBeginResponseWire struct {
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TransactionRequestWire is the wire type for commit/rollback.
type TransactionRequestWire struct {
	AttachOpaqueData      []byte `vgirpc:"attach_opaque_data"`
	TransactionOpaqueData []byte `vgirpc:"transaction_opaque_data"`
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
	AttachOpaqueData      []byte             `vgirpc:"attach_opaque_data"`
	Name                  string             `vgirpc:"name"`
	OnConflict            string             `vgirpc:"on_conflict,enum"`
	Comment               *string            `vgirpc:"comment"`
	Tags                  *map[string]string `vgirpc:"tags"`
	TransactionOpaqueData *[]byte            `vgirpc:"transaction_opaque_data"`
}

// SchemaDropRequestWire is for catalog_schema_drop.
type SchemaDropRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	Name                  string  `vgirpc:"name"`
	IgnoreNotFound        bool    `vgirpc:"ignore_not_found"`
	Cascade               bool    `vgirpc:"cascade"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableGetRequestWire is for catalog_table_get.
type TableGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	AtUnit                *string `vgirpc:"at_unit"`
	AtValue               *string `vgirpc:"at_value"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableDropRequestWire is for catalog_table_drop.
type TableDropRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	IgnoreNotFound        bool    `vgirpc:"ignore_not_found"`
	Cascade               bool    `vgirpc:"cascade"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableScanFunctionGetRequestWire is for catalog_table_scan_function_get.
type TableScanFunctionGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	AtUnit                *string `vgirpc:"at_unit"`
	AtValue               *string `vgirpc:"at_value"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableScanFunctionGetResponseWire wraps the scan function result.
// Fields are serialized directly (not wrapped in a binary "result" column)
// so the C++ extension's ExtractAndDeserializeResult can find them.
type TableScanFunctionGetResponseWire struct {
	FunctionName       string   `vgirpc:"function_name"`
	Arguments          []byte   `vgirpc:"arguments"`
	RequiredExtensions []string `vgirpc:"required_extensions"`
}

// TableScanBranchesGetRequestWire is for catalog_table_scan_branches_get.
type TableScanBranchesGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	AtUnit                *string `vgirpc:"at_unit"`
	AtValue               *string `vgirpc:"at_value"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableScanBranchesGetResponseWire wraps a ScanBranchesResult. Branches holds
// one IPC-serialized ScanBranch per physical source; field names/types match
// generated.ScanBranchesResultSchema.
type TableScanBranchesGetResponseWire struct {
	Branches           [][]byte `vgirpc:"branches"`
	RequiredExtensions []string `vgirpc:"required_extensions"`
}

// TableCommentSetRequestWire is for catalog_table_comment_set.
type TableCommentSetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	Comment               *string `vgirpc:"comment"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableRenameRequestWire is for catalog_table_rename.
type TableRenameRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	NewName               string  `vgirpc:"new_name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableColumnAddRequestWire is for catalog_table_column_add.
type TableColumnAddRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	ColumnDefinition      []byte  `vgirpc:"column_definition"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	IfColumnNotExists     *bool   `vgirpc:"if_column_not_exists"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableColumnDropRequestWire is for catalog_table_column_drop.
type TableColumnDropRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	ColumnName            string  `vgirpc:"column_name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	IfColumnExists        *bool   `vgirpc:"if_column_exists"`
	Cascade               *bool   `vgirpc:"cascade"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableColumnRenameRequestWire is for catalog_table_column_rename.
type TableColumnRenameRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	ColumnName            string  `vgirpc:"column_name"`
	NewColumnName         string  `vgirpc:"new_column_name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableColumnDefaultSetRequestWire is for catalog_table_column_default_set.
type TableColumnDefaultSetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	ColumnName            string  `vgirpc:"column_name"`
	Expression            string  `vgirpc:"expression"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableColumnDefaultDropRequestWire is for catalog_table_column_default_drop.
type TableColumnDefaultDropRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	ColumnName            string  `vgirpc:"column_name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableColumnTypeChangeRequestWire is for catalog_table_column_type_change.
type TableColumnTypeChangeRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	ColumnDefinition      []byte  `vgirpc:"column_definition"`
	Expression            *string `vgirpc:"expression"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableNotNullRequestWire is for catalog_table_not_null_set/drop.
type TableNotNullRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	ColumnName            string  `vgirpc:"column_name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// ViewGetRequestWire is for catalog_view_get.
type ViewGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// ViewCreateRequestWire is for catalog_view_create.
type ViewCreateRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	Definition            string  `vgirpc:"definition"`
	OnConflict            string  `vgirpc:"on_conflict,enum"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// ViewDropRequestWire is for catalog_view_drop.
type ViewDropRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// ViewRenameRequestWire is for catalog_view_rename.
type ViewRenameRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	NewName               string  `vgirpc:"new_name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// ViewCommentSetRequestWire is for catalog_view_comment_set.
type ViewCommentSetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	Comment               *string `vgirpc:"comment"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// MacroGetRequestWire is for catalog_macro_get.
type MacroGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// MacroCreateRequestWire is for catalog_macro_create.
type MacroCreateRequestWire struct {
	AttachOpaqueData       []byte   `vgirpc:"attach_opaque_data"`
	SchemaName             string   `vgirpc:"schema_name"`
	Name                   string   `vgirpc:"name"`
	MacroType              string   `vgirpc:"macro_type,enum"`
	Parameters             []string `vgirpc:"parameters"`
	Definition             string   `vgirpc:"definition"`
	OnConflict             string   `vgirpc:"on_conflict,enum"`
	ParameterDefaultValues *[]byte  `vgirpc:"parameter_default_values"`
	TransactionOpaqueData  *[]byte  `vgirpc:"transaction_opaque_data"`
}

// MacroDropRequestWire is for catalog_macro_drop.
type MacroDropRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	IgnoreNotFound        *bool   `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// SchemaContentsMacrosRequestWire is for schema_contents_macros.
type SchemaContentsMacrosRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	Name                  string  `vgirpc:"name"`
	Type                  string  `vgirpc:"type,enum"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableColumnStatisticsGetRequestWire is for catalog_table_column_statistics_get.
type TableColumnStatisticsGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableInsertFunctionGetRequestWire is for catalog_table_insert_function_get.
type TableInsertFunctionGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
	// WritableBranchFunctionName is set by the C++ extension for a multi-branch
	// table's writable arm, naming the branch the INSERT routes to.
	WritableBranchFunctionName *string `vgirpc:"writable_branch_function_name"`
}

// TableUpdateFunctionGetRequestWire is for catalog_table_update_function_get.
type TableUpdateFunctionGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableDeleteFunctionGetRequestWire is for catalog_table_delete_function_get.
type TableDeleteFunctionGetRequestWire struct {
	AttachOpaqueData      []byte  `vgirpc:"attach_opaque_data"`
	SchemaName            string  `vgirpc:"schema_name"`
	Name                  string  `vgirpc:"name"`
	TransactionOpaqueData *[]byte `vgirpc:"transaction_opaque_data"`
}

// TableCreateRequestWire is for catalog_table_create.
type TableCreateRequestWire struct {
	AttachOpaqueData      []byte    `vgirpc:"attach_opaque_data"`
	SchemaName            string    `vgirpc:"schema_name"`
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
		if meta.LateMaterialization {
			v := true
			fi.LateMaterialization = &v
		}
		if len(meta.SupportedExpressionFilters) > 0 {
			fi.SupportedExpressionFilters = meta.SupportedExpressionFilters
		}
		if meta.OrderPreservation != "" {
			fi.OrderPreservation = meta.OrderPreservation
		}
		fi.SupportsBatchIndex = meta.SupportsBatchIndex
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

	for name, fns := range w.tableBufferings {
		for _, fn := range fns {
			meta := fn.Metadata()
			fi := buildFunctionInfo(name, FunctionTypeTableBuffering, meta, fn.ArgumentSpecs())
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
			fi.StreamingPartitioned = meta.StreamingPartitioned
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

	// Populate dynamic-only schemas (those without any registered table/view/macro
	// — they exist solely so the catalog enumeration sees them and lets the
	// SchemaContentsHandler take over).
	for name, comment := range w.dynamicSchemas {
		if _, ok := cat.schemas[name]; ok {
			continue
		}
		cat.schemas[name] = &catalogSchemaInfo{
			info: &SchemaInfo{Name: name, Comment: comment},
		}
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
			Message: fmt.Sprintf("catalog is read-only: %s not supported", op),
		}
	}

	// catalog_catalogs
	unaryCatalog[struct{}, CatalogsResponseWire](w, s, "catalog_catalogs",
		func(ctx context.Context, callCtx *vgirpc.CallContext, _ struct{}) (CatalogsResponseWire, error) {
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
			return CatalogsResponseWire{Items: SerializedItems{data}}, nil
		})

	// catalog_attach
	unaryCatalog[CatalogAttachRequestWire, CatalogAttachResultWire](w, s, "catalog_attach",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogAttachRequestWire) (CatalogAttachResultWire, error) {
			// Writable catalogs are handled separately so they have their
			// own attach_opaque_data and per-catalog table state.
			if wc, ok := w.extraCatalogs[req.Name]; ok {
				res, err := w.handleWritableAttach(req, wc)
				if err != nil {
					return res, err
				}
				if res.AttachOpaqueData, err = w.sealAttach(res.AttachOpaqueData, callCtx); err != nil {
					return CatalogAttachResultWire{}, err
				}
				return res, nil
			}
			// Validate catalog name matches the primary or one of the
			// declared aliases (WithCatalogAliases).
			if req.Name != w.catalogName {
				if _, ok := w.catalogAliases[req.Name]; !ok {
					return CatalogAttachResultWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
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
					return CatalogAttachResultWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: vErr.Error(),
					}
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
				Tags:                          tags,
				SupportsColumnStatistics:      supportsColStats,
				ResolvedDataVersion:           resolvedData,
				ResolvedImplementationVersion: resolvedImpl,
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
			u := uuid.New()
			minted := make([]byte, attachUUIDLen+len(result.AttachOpaqueData))
			copy(minted, u[:])
			copy(minted[attachUUIDLen:], result.AttachOpaqueData)
			// Seal the attach value into an AEAD envelope bound to the
			// caller's identity before it leaves the worker (HTTP transport;
			// pass-through on subprocess / unix).
			sealed, sErr := w.sealAttach(minted, callCtx)
			if sErr != nil {
				return CatalogAttachResultWire{}, sErr
			}
			result.AttachOpaqueData = sealed
			return result, nil
		})

	// catalog_detach
	unaryVoidCatalog[DetachRequestWire](w, s, "catalog_detach",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req DetachRequestWire) error {
			return nil
		})

	// catalog_version
	unaryCatalog[CatalogVersionRequestWire, CatalogVersionResponseWire](w, s, "catalog_version",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogVersionRequestWire) (CatalogVersionResponseWire, error) {
			if w.catalogVersionHook != nil {
				if err := w.catalogVersionHook(req.AttachOpaqueData, callCtx); err != nil {
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

	// catalog_transaction_begin — allocate a fresh transaction id when the
	// worker advertises transaction support, so DuckDB threads it through
	// bind/scan inside BEGIN/COMMIT (enables transaction-scoped storage).
	unaryCatalog[TransactionBeginRequestWire, TransactionBeginResponseWire](w, s, "catalog_transaction_begin",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TransactionBeginRequestWire) (TransactionBeginResponseWire, error) {
			if !w.supportsTransactions {
				return TransactionBeginResponseWire{}, nil
			}
			id := uuid.New()
			tx := append([]byte(nil), id[:]...)
			return TransactionBeginResponseWire{TransactionOpaqueData: &tx}, nil
		})

	// catalog_transaction_commit — clear per-transaction storage.
	unaryVoidCatalog[TransactionRequestWire](w, s, "catalog_transaction_commit",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TransactionRequestWire) error {
			w.clearTransactionState(req.TransactionOpaqueData)
			return nil
		})

	// catalog_transaction_rollback — same cleanup path as commit.
	unaryVoidCatalog[TransactionRequestWire](w, s, "catalog_transaction_rollback",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TransactionRequestWire) error {
			w.clearTransactionState(req.TransactionOpaqueData)
			return nil
		})

	// catalog_schemas
	unaryCatalog[SchemasRequestWire, ItemsResponseWire](w, s, "catalog_schemas",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemasRequestWire) (ItemsResponseWire, error) {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
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
	unaryCatalog[SchemaGetRequestWire, ItemsResponseWire](w, s, "catalog_schema_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaGetRequestWire) (ItemsResponseWire, error) {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
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
	unaryVoidCatalog[SchemaCreateRequestWire](w, s, "catalog_schema_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaCreateRequestWire) error {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
				return w.writableSchemaCreate(wc, req.Name, parseOnConflict(req.OnConflict), req.Comment)
			}
			return readOnlyErr("catalog_schema_create")
		})

	// catalog_schema_drop
	unaryVoidCatalog[SchemaDropRequestWire](w, s, "catalog_schema_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaDropRequestWire) error {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
				return w.writableSchemaDrop(wc, req.Name, req.IgnoreNotFound, req.Cascade)
			}
			return readOnlyErr("catalog_schema_drop")
		})

	// catalog_schema_contents_tables
	unaryCatalog[SchemaContentsRequestWire, ItemsResponseWire](w, s, "catalog_schema_contents_tables",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaContentsRequestWire) (ItemsResponseWire, error) {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
				items, err := w.writableSchemaContentsTables(wc, req.Name)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				return ItemsResponseWire{Items: items}, nil
			}
			// Per-attach override (versioned-tables worker etc.): the handler
			// inspects the attach_opaque_data (which can encode the resolved version)
			// and returns the right set of tables.
			if w.schemaContentsHandler != nil {
				if items, ok := w.schemaContentsHandler(req.AttachOpaqueData, req.Name); ok {
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
	unaryCatalog[SchemaContentsRequestWire, ItemsResponseWire](w, s, "catalog_schema_contents_views",
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
					Name:           cv.Name,
					SchemaName:     req.Name,
					Comment:        cv.Comment,
					Tags:           cv.Tags,
					Definition:     cv.Definition,
					ColumnComments: cv.ColumnComments,
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
	unaryCatalog[SchemaContentsFunctionsRequestWire, ItemsResponseWire](w, s, "catalog_schema_contents_functions",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req SchemaContentsFunctionsRequestWire) (ItemsResponseWire, error) {
			LogCatalog.Debug("catalog: listing functions", "schema", req.Name, "type", req.Type)
			if w.catalog == nil {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}
			si, ok := w.catalog.schemas[req.Name]
			if !ok {
				return ItemsResponseWire{Items: [][]byte{}}, nil
			}

			// attach_opaque_data has already been unwrapped to the catalog's own
			// plaintext by unwrapReqOpaque (the unaryCatalog wrapper strips the
			// framework UUID and opens any seal), so it is []byte(catalog_name)
			// here. Per-catalog function visibility (e.g. proj_repro_* only
			// surfacing under the projection_repro attach) compares against it.
			catalogName := string(req.AttachOpaqueData)

			var items [][]byte
			for i := range si.functions {
				fi := &si.functions[i]
				// Filter by type if requested. DuckDB sends "SCALAR_FUNCTION",
				// "TABLE_FUNCTION", etc.; normalizeFunctionType also accepts the
				// short forms. An unrecognized type filters nothing.
				if req.Type != "" {
					switch want := normalizeFunctionType(FunctionType(req.Type)); want {
					case FunctionTypeTable:
						// Table-buffering functions register as DuckDB table
						// functions, so they match a TABLE_FUNCTION request.
						if fi.FunctionType != FunctionTypeTable && fi.FunctionType != FunctionTypeTableBuffering {
							continue
						}
					case FunctionTypeScalar, FunctionTypeAggregate:
						if fi.FunctionType != want {
							continue
						}
					}
				}
				// Catalog-scoped function visibility: if this function is
				// pinned to a specific catalog and the current attach is for
				// a different catalog, hide it.
				if scope, ok := w.catalogFunctionScope[fi.Name]; ok {
					if catalogName != "" && catalogName != scope {
						continue
					}
				}
				LogCatalog.Debug("catalog: returning function", "name", fi.Name, "type", fi.FunctionType)
				data, err := SerializeFunctionInfo(fi)
				if err != nil {
					return ItemsResponseWire{}, err
				}
				items = append(items, data)
			}
			return ItemsResponseWire{Items: items}, nil
		})

	// catalog_table_get
	unaryCatalog[TableGetRequestWire, ItemsResponseWire](w, s, "catalog_table_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableGetRequestWire) (ItemsResponseWire, error) {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
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
				data, handled, err := w.attachTableGetHandler(req.AttachOpaqueData, req.SchemaName, req.Name, req.AtUnit, req.AtValue)
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
	unaryVoidCatalog[TableCreateRequestWire](w, s, "catalog_table_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableCreateRequestWire) error {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
				return w.writableTableCreate(wc, req)
			}
			return readOnlyErr("catalog_table_create")
		})

	// catalog_table_drop
	unaryVoidCatalog[TableDropRequestWire](w, s, "catalog_table_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableDropRequestWire) error {
			if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
				return w.writableTableDrop(wc, req.SchemaName, req.Name, req.IgnoreNotFound, req.Cascade)
			}
			return readOnlyErr("catalog_table_drop")
		})

	// catalog_table_scan_function_get
	unaryCatalog[TableScanFunctionGetRequestWire, TableScanFunctionGetResponseWire](w, s, "catalog_table_scan_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableScanFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			result, err := w.resolveScanFunction(req)
			if err != nil {
				return TableScanFunctionGetResponseWire{}, err
			}
			return buildScanFunctionGetResponse(result)
		})

	// catalog_table_scan_branches_get — multi-branch (UNION-of-sources) tables.
	// For non-branch tables it wraps the single scan_function_get result as a
	// one-branch list, mirroring vgi-python's default implementation, so the
	// method is always implemented (never triggers a C++ legacy fallback).
	unaryCatalog[TableScanBranchesGetRequestWire, TableScanBranchesGetResponseWire](w, s, "catalog_table_scan_branches_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableScanBranchesGetRequestWire) (TableScanBranchesGetResponseWire, error) {
			if w.attachScanBranchesGetHandler != nil {
				result, handled, err := w.attachScanBranchesGetHandler(req.AttachOpaqueData, req.SchemaName, req.Name, req.AtUnit, req.AtValue)
				if err != nil {
					return TableScanBranchesGetResponseWire{}, &vgirpc.RpcError{
						Type:    "ValueError",
						Message: err.Error(),
					}
				}
				if handled {
					return buildScanBranchesGetResponse(result)
				}
			}
			// Default: wrap the single scan_function_get result as one branch.
			sf, err := w.resolveScanFunction(TableScanFunctionGetRequestWire{
				AttachOpaqueData:      req.AttachOpaqueData,
				SchemaName:            req.SchemaName,
				Name:                  req.Name,
				AtUnit:                req.AtUnit,
				AtValue:               req.AtValue,
				TransactionOpaqueData: req.TransactionOpaqueData,
			})
			if err != nil {
				return TableScanBranchesGetResponseWire{}, err
			}
			return buildScanBranchesGetResponse(&ScanBranchesResult{
				Branches: []ScanBranch{{
					FunctionName:        sf.FunctionName,
					PositionalArguments: sf.PositionalArguments,
					NamedArguments:      sf.NamedArguments,
				}},
				RequiredExtensions: sf.RequiredExtensions,
			})
		})

	// catalog_table_comment_set (read-only)
	unaryVoidCatalog[TableCommentSetRequestWire](w, s, "catalog_table_comment_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableCommentSetRequestWire) error {
			return readOnlyErr("catalog_table_comment_set")
		})

	// catalog_table_rename (read-only)
	unaryVoidCatalog[TableRenameRequestWire](w, s, "catalog_table_rename",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableRenameRequestWire) error {
			return readOnlyErr("catalog_table_rename")
		})

	// catalog_table_column_add (read-only)
	unaryVoidCatalog[TableColumnAddRequestWire](w, s, "catalog_table_column_add",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnAddRequestWire) error {
			return readOnlyErr("catalog_table_column_add")
		})

	// catalog_table_column_drop (read-only)
	unaryVoidCatalog[TableColumnDropRequestWire](w, s, "catalog_table_column_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnDropRequestWire) error {
			return readOnlyErr("catalog_table_column_drop")
		})

	// catalog_table_column_rename (read-only)
	unaryVoidCatalog[TableColumnRenameRequestWire](w, s, "catalog_table_column_rename",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnRenameRequestWire) error {
			return readOnlyErr("catalog_table_column_rename")
		})

	// catalog_table_column_default_set (read-only)
	unaryVoidCatalog[TableColumnDefaultSetRequestWire](w, s, "catalog_table_column_default_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnDefaultSetRequestWire) error {
			return readOnlyErr("catalog_table_column_default_set")
		})

	// catalog_table_column_default_drop (read-only)
	unaryVoidCatalog[TableColumnDefaultDropRequestWire](w, s, "catalog_table_column_default_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnDefaultDropRequestWire) error {
			return readOnlyErr("catalog_table_column_default_drop")
		})

	// catalog_table_column_type_change (read-only)
	unaryVoidCatalog[TableColumnTypeChangeRequestWire](w, s, "catalog_table_column_type_change",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableColumnTypeChangeRequestWire) error {
			return readOnlyErr("catalog_table_column_type_change")
		})

	// catalog_table_not_null_set (read-only)
	unaryVoidCatalog[TableNotNullRequestWire](w, s, "catalog_table_not_null_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableNotNullRequestWire) error {
			return readOnlyErr("catalog_table_not_null_set")
		})

	// catalog_table_not_null_drop (read-only)
	unaryVoidCatalog[TableNotNullRequestWire](w, s, "catalog_table_not_null_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableNotNullRequestWire) error {
			return readOnlyErr("catalog_table_not_null_drop")
		})

	// catalog_view_get
	unaryCatalog[ViewGetRequestWire, ItemsResponseWire](w, s, "catalog_view_get",
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
		})

	// catalog_view_create (read-only)
	unaryVoidCatalog[ViewCreateRequestWire](w, s, "catalog_view_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewCreateRequestWire) error {
			return readOnlyErr("catalog_view_create")
		})

	// catalog_view_drop (read-only)
	unaryVoidCatalog[ViewDropRequestWire](w, s, "catalog_view_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewDropRequestWire) error {
			return readOnlyErr("catalog_view_drop")
		})

	// catalog_view_rename (read-only)
	unaryVoidCatalog[ViewRenameRequestWire](w, s, "catalog_view_rename",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewRenameRequestWire) error {
			return readOnlyErr("catalog_view_rename")
		})

	// catalog_view_comment_set (read-only)
	unaryVoidCatalog[ViewCommentSetRequestWire](w, s, "catalog_view_comment_set",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req ViewCommentSetRequestWire) error {
			return readOnlyErr("catalog_view_comment_set")
		})

	// catalog_macro_get
	unaryCatalog[MacroGetRequestWire, ItemsResponseWire](w, s, "catalog_macro_get",
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
						Tags:                   cm.Tags,
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
	unaryCatalog[SchemaContentsMacrosRequestWire, ItemsResponseWire](w, s, "catalog_schema_contents_macros",
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
				// Filter by type if requested. An unrecognized type filters
				// nothing.
				if req.Type != "" {
					switch want := macroKindFilter(req.Type); want {
					case MacroTypeScalar, MacroTypeTable:
						if cm.MacroType != want {
							continue
						}
					}
				}
				info := &MacroInfo{
					Name:                   cm.Name,
					SchemaName:             req.Name,
					Comment:                cm.Comment,
					Tags:                   cm.Tags,
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
	unaryVoidCatalog[MacroCreateRequestWire](w, s, "catalog_macro_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req MacroCreateRequestWire) error {
			return readOnlyErr("catalog_macro_create")
		})

	// catalog_macro_drop (read-only)
	unaryVoidCatalog[MacroDropRequestWire](w, s, "catalog_macro_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req MacroDropRequestWire) error {
			return readOnlyErr("catalog_macro_drop")
		})

	// catalog_create (read-only)
	unaryVoidCatalog[CatalogCreateRequestWire](w, s, "catalog_create",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogCreateRequestWire) error {
			return readOnlyErr("catalog_create")
		})

	// catalog_drop (read-only)
	unaryVoidCatalog[CatalogDropRequestWire](w, s, "catalog_drop",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req CatalogDropRequestWire) error {
			return readOnlyErr("catalog_drop")
		})

	// catalog_table_insert_function_get
	unaryCatalog[TableInsertFunctionGetRequestWire, TableScanFunctionGetResponseWire](w, s, "catalog_table_insert_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableInsertFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			if w.attachWriteFunctionGetHandler != nil {
				if result, handled, err := w.attachWriteFunctionGetHandler(WriteOpInsert, req.AttachOpaqueData, req.SchemaName, req.Name); err != nil {
					return TableScanFunctionGetResponseWire{}, err
				} else if handled {
					return buildScanFunctionGetResponse(result)
				}
			}
			if w.writableByAttachOpaqueData(req.AttachOpaqueData) == nil {
				return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Message: fmt.Sprintf("table %s.%s is read-only (attach_opaque_data=%x len=%d, extra_catalogs=%d)", req.SchemaName, req.Name, req.AttachOpaqueData, len(req.AttachOpaqueData), len(w.extraCatalogs))}
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
	unaryCatalog[TableUpdateFunctionGetRequestWire, TableScanFunctionGetResponseWire](w, s, "catalog_table_update_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableUpdateFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			if w.attachWriteFunctionGetHandler != nil {
				if result, handled, err := w.attachWriteFunctionGetHandler(WriteOpUpdate, req.AttachOpaqueData, req.SchemaName, req.Name); err != nil {
					return TableScanFunctionGetResponseWire{}, err
				} else if handled {
					return buildScanFunctionGetResponse(result)
				}
			}
			if w.writableByAttachOpaqueData(req.AttachOpaqueData) == nil {
				return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Message: fmt.Sprintf("table %s.%s is read-only (attach_opaque_data=%x len=%d, extra_catalogs=%d)", req.SchemaName, req.Name, req.AttachOpaqueData, len(req.AttachOpaqueData), len(w.extraCatalogs))}
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
	unaryCatalog[TableDeleteFunctionGetRequestWire, TableScanFunctionGetResponseWire](w, s, "catalog_table_delete_function_get",
		func(ctx context.Context, callCtx *vgirpc.CallContext, req TableDeleteFunctionGetRequestWire) (TableScanFunctionGetResponseWire, error) {
			if w.attachWriteFunctionGetHandler != nil {
				if result, handled, err := w.attachWriteFunctionGetHandler(WriteOpDelete, req.AttachOpaqueData, req.SchemaName, req.Name); err != nil {
					return TableScanFunctionGetResponseWire{}, err
				} else if handled {
					return buildScanFunctionGetResponse(result)
				}
			}
			if w.writableByAttachOpaqueData(req.AttachOpaqueData) == nil {
				return TableScanFunctionGetResponseWire{}, &vgirpc.RpcError{Type: "NotImplementedError", Message: fmt.Sprintf("table %s.%s is read-only (attach_opaque_data=%x len=%d, extra_catalogs=%d)", req.SchemaName, req.Name, req.AttachOpaqueData, len(req.AttachOpaqueData), len(w.extraCatalogs))}
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
	unaryCatalog[TableColumnStatisticsGetRequestWire, []byte](w, s, "catalog_table_column_statistics_get",
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
		RequiredFieldFilterPaths: ct.RequiredFieldFilterPaths,
	}

	// Inline the scan function for function-backed tables so the C++ extension
	// skips catalog_table_scan_function_get. Explicit-columns tables (Function
	// == nil) keep ScanFunction nil and continue to use the per-bind RPC path.
	if ct.Function != nil {
		sfBytes, err := SerializeScanFunctionResult(w.buildScanResultFromTable(ct))
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
func (w *Worker) resolveScanFunction(req TableScanFunctionGetRequestWire) (*ScanFunctionResult, error) {
	if wc := w.writableByAttachOpaqueData(req.AttachOpaqueData); wc != nil {
		return &ScanFunctionResult{
			FunctionName: writableScanFunctionName,
			PositionalArguments: []ScanArg{
				{Value: req.SchemaName, Type: arrow.BinaryTypes.String},
				{Value: req.Name, Type: arrow.BinaryTypes.String},
			},
		}, nil
	}
	LogCatalog.Debug("catalog: scan function get", "schema", req.SchemaName, "table", req.Name)

	// Registered catalog table with a backing function (skip when AT params
	// are present — let the handler deal with time travel).
	hasAt := req.AtUnit != nil && *req.AtUnit != ""
	if !hasAt && w.catalog != nil {
		if si, ok := w.catalog.schemas[req.SchemaName]; ok {
			for i := range si.tables {
				if si.tables[i].Name == req.Name && si.tables[i].Function != nil {
					return w.buildScanResultFromTable(&si.tables[i]), nil
				}
			}
		}
	}

	// Attach-id-aware handler takes precedence over the plain one.
	if w.attachScanFunctionGetHandler != nil {
		result, handled, err := w.attachScanFunctionGetHandler(req.AttachOpaqueData, req.SchemaName, req.Name, req.AtUnit, req.AtValue)
		if err != nil {
			return nil, &vgirpc.RpcError{Type: "ValueError", Message: err.Error()}
		}
		if handled {
			return result, nil
		}
	}
	if w.scanFunctionGetHandler != nil {
		result, err := w.scanFunctionGetHandler(req.SchemaName, req.Name, req.AtUnit, req.AtValue)
		if err != nil {
			return nil, &vgirpc.RpcError{Type: "ValueError", Message: err.Error()}
		}
		return result, nil
	}

	return nil, &vgirpc.RpcError{
		Type:    "NotImplementedError",
		Message: fmt.Sprintf("table_scan_function_get not implemented for %s.%s", req.SchemaName, req.Name),
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
