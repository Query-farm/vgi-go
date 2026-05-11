// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"

	"github.com/Query-farm/vgi-go/examples/aggregate"
	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/examples/schema_reconcile"
	"github.com/Query-farm/vgi-go/examples/table"
	table_in_out "github.com/Query-farm/vgi-go/examples/table_in_out"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/Query-farm/vgi-rpc/vgirpc/jwtauth"
	"github.com/apache/arrow-go/v18/arrow"
)

func main() {
	httpMode := flag.Bool("http", false, "Run as HTTP server instead of stdio")
	flag.Parse()

	w := vgi.NewWorker(
		vgi.WithCatalogName("example"),
		vgi.WithCatalogComment("Example VGI catalog for testing"),
		vgi.WithCatalogTags(map[string]string{
			"source":  "vgi-fixture-worker",
			"version": "1",
		}),
		vgi.WithSchemaComments(map[string]string{
			"main": "Example functions for testing VGI",
			"data": "Example tables backed by functions",
		}),
		// Cross-language reproducer catalogs share this binary; ATTACH
		// against any of these names succeeds (functions are catalog-agnostic).
		vgi.WithCatalogAliases("projection_repro", "schema_reconcile"),
		// schema_reconcile fixture: tables/scan-fn/insert-fn/update-fn/delete-fn
		// resolution lives outside the static catalog because the tables are
		// served by handlers (not declared via RegisterCatalogTable). The four
		// handlers below are wired in registration order; they short-circuit on
		// any other catalog name.
		vgi.WithSchemaContentsHandler(schema_reconcile.SchemaContentsHandler),
		vgi.WithAttachTableGetHandler(schema_reconcile.AttachTableGetHandler),
		vgi.WithAttachScanFunctionGetHandler(schema_reconcile.AttachScanFunctionGetHandler),
		vgi.WithAttachWriteFunctionGetHandler(schema_reconcile.AttachWriteFunctionGetHandler),
		vgi.WithSecretTypes(
			vgi.SecretTypeSpec{
				Name:        "vgi_example",
				Description: "Example VGI secret for testing",
				Schema: arrow.NewSchema([]arrow.Field{
					{Name: "secret_string", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{"redact"}, []string{"true"})},
					{Name: "api_key", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{"redact"}, []string{"true"})},
					{Name: "port", Type: arrow.PrimitiveTypes.Int32},
					{Name: "use_ssl", Type: &arrow.BooleanType{}},
					{Name: "timeout", Type: arrow.PrimitiveTypes.Float64},
				}, nil),
			},
		),
		vgi.WithSettings(
			vgi.SettingSpec{
				Name:         "vgi_verbose_mode",
				Description:  "Enable verbose output",
				Type:         &arrow.BooleanType{},
				DefaultValue: false,
			},
			vgi.SettingSpec{
				Name:         "greeting",
				Description:  "Custom greeting message",
				Type:         arrow.BinaryTypes.String,
				DefaultValue: "Hello",
			},
			vgi.SettingSpec{
				Name:         "multiplier",
				Description:  "Value multiplier",
				Type:         arrow.PrimitiveTypes.Int64,
				DefaultValue: int64(1),
			},
			vgi.SettingSpec{
				Name:         "threshold",
				Description:  "Filter threshold",
				Type:         arrow.PrimitiveTypes.Int64,
				DefaultValue: int64(0),
			},
			vgi.SettingSpec{
				Name:        "config",
				Description: "Sequence configuration struct",
				Type: arrow.StructOf(
					arrow.Field{Name: "start", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "step", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
				),
				DefaultValue: nil,
			},
		),
	)

	// Scalar functions
	w.RegisterScalar(&scalar.AddValuesFunction{})
	w.RegisterScalar(&scalar.AnyMixedIntFunction{})
	w.RegisterScalar(&scalar.AnyMixedStrFunction{})
	w.RegisterScalar(&scalar.BernoulliFunction{})
	w.RegisterScalar(&scalar.BinaryPacketFunction{})
	w.RegisterScalar(&scalar.ConcatValuesIntFunction{})
	w.RegisterScalar(&scalar.ConcatValuesStrFunction{})
	w.RegisterScalar(&scalar.ConditionalMessageFunction{})
	w.RegisterScalar(&scalar.DoubleFunction{})
	w.RegisterScalar(&scalar.FormatNumberDefaultFunction{})
	w.RegisterScalar(&scalar.FormatNumberPrecisionFunction{})
	w.RegisterScalar(&scalar.FormatNumberFullFunction{})
	w.RegisterScalar(&scalar.GeoCentroidFixedFunction{})
	w.RegisterScalar(&scalar.GeoCentroidListFunction{})
	w.RegisterScalar(&scalar.GeoCentroidStructFunction{})
	w.RegisterScalar(&scalar.GeoDistanceFixedFunction{})
	w.RegisterScalar(&scalar.GeoDistanceListFunction{})
	w.RegisterScalar(&scalar.GeoDistanceStructFunction{})
	w.RegisterScalar(&scalar.HashSeedFunction{})
	w.RegisterScalar(&scalar.MultiplyBySettingFunction{})
	w.RegisterScalar(&scalar.MultiplyFunction{})
	w.RegisterScalar(&scalar.NullHandlingFunction{})
	w.RegisterScalar(scalar.NewTypeInfoInt32Function())
	w.RegisterScalar(scalar.NewTypeInfoInt64Function())
	w.RegisterScalar(scalar.NewTypeInfoUint32Function())
	w.RegisterScalar(scalar.NewTypeInfoUint64Function())
	w.RegisterScalar(scalar.NewTypeInfoVarcharFunction())
	w.RegisterScalar(&scalar.PairTypeIntIntFunction{})
	w.RegisterScalar(&scalar.PairTypeStrStrFunction{})
	w.RegisterScalar(&scalar.PairTypeIntStrFunction{})
	w.RegisterScalar(&scalar.RandomBytesFunction{})
	w.RegisterScalar(&scalar.RandomIntFunction{})
	w.RegisterScalar(&scalar.ReturnSecretValueFunction{})
	w.RegisterScalar(&scalar.SmartFormatWidthFunction{})
	w.RegisterScalar(&scalar.SmartFormatPrefixFunction{})
	w.RegisterScalar(&scalar.SumValuesFunction{})
	w.RegisterScalar(&scalar.UnnestTensorFunction{})
	w.RegisterScalar(&scalar.UpperCaseFunction{})
	w.RegisterScalar(&scalar.WhoAmIFunction{})

	// Table functions
	w.RegisterTable(table.NewConstantColumnsFunction())
	w.RegisterTable(table.NewFilterEchoFunction())
	w.RegisterTable(table.NewFilterEchoPartitionedFunction())
	w.RegisterTable(table.NewOrderEchoFunction())
	w.RegisterTable(table.NewSampleEchoFunction())
	w.RegisterTable(table.NewSpatialFilterExampleFunction())
	w.RegisterTable(table.NewColorsScanFunction())
	w.RegisterTable(table.NewExpressionFilterTestFunction())
	// Note: geo_points is introspected via vgi_table_statistics only; no
	// separate scan function is registered (matches the vgi-python example
	// worker inventory). Direct `SELECT * FROM data.geo_points` will error
	// because the backing scan function is not exposed.

	// Aggregate functions
	aggregate.RegisterAll(w)

	// Writable catalog (in-memory, per-process state). Gated off by default so
	// the example worker's function inventory matches the reference vgi-python
	// worker; set VGI_WORKER_ENABLE_WRITABLE=1 to exercise the writable tests.
	if os.Getenv("VGI_WORKER_ENABLE_WRITABLE") != "" {
		w.RegisterWritableCatalog(vgi.NewWritableCatalog("writable"))
	}
	w.RegisterTable(table.NewDoubleSequenceFunction())
	w.RegisterTable(table.NewDynamicFilterEchoFunction())
	w.RegisterTable(table.NewGeneratorExceptionFunction())
	w.RegisterTable(table.NewLoggingGeneratorFunction())
	w.RegisterTable(table.NewMakePairsIntFunction())
	w.RegisterTable(table.NewMakePairsMixedFunction())
	w.RegisterTable(table.NewMakePairsStrFunction())
	w.RegisterTable(table.NewMakeSeriesCountFunction())
	w.RegisterTable(table.NewMakeSeriesRangeFunction())
	w.RegisterTable(table.NewMakeSeriesStepFunction())
	w.RegisterTable(table.NewMakeSeriesCsvFunction())
	w.RegisterTable(table.NewMakeSeriesFloatStepFunction())
	w.RegisterTable(table.NewNamedParamsEchoFunction())
	w.RegisterTable(table.NewNestedSequenceFunction())
	w.RegisterTable(table.NewPartitionedSequenceFunction())
	w.RegisterTable(table.NewPartitionedFixedOrderFunction())
	w.RegisterTable(table.NewPartitionedPreservesOrderFunction())
	w.RegisterTable(table.NewPartitionedNoOrderGuaranteeFunction())
	w.RegisterTable(table.NewProfilingDemoFunction())
	w.RegisterTable(table.NewSlowCancellableFunction())
	// Scope projection-pushdown reproducer functions to the
	// ``projection_repro`` catalog only — they're invisible to the
	// ``example`` catalog's function listing (function_registration.test
	// asserts an exact 54-function count there).
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproStrictFunction())
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproFullSchemaFunction())
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproChunkedFunction())
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproMultiWorkerFunction())
	w.RegisterTable(table.NewProjectedDataFunction())
	w.RegisterTable(table.NewRepeatValueIntFunction())
	w.RegisterTable(table.NewRowIdSequenceFunction())
	w.RegisterTable(table.NewRepeatValueStrFunction())
	w.RegisterTable(table.NewScopedSecretDemoFunction())
	w.RegisterTable(table.NewSecretDemoFunction())
	w.RegisterTable(table.NewSequenceFunction())
	w.RegisterTable(table.NewSettingsAwareFunction())
	w.RegisterTable(table.NewStructSettingsFunction())
	w.RegisterTable(table.NewTenThousandFunction())
	w.RegisterTable(table.NewVersionedDataFunction())
	w.RegisterTable(table.NewDepartmentsScanFunction())
	w.RegisterTable(table.NewEmployeesScanFunction())
	w.RegisterTable(table.NewProductsScanFunction())
	w.RegisterTable(table.NewProjectsScanFunction())
	w.RegisterTable(table.NewVersionedConstraintsScanFunction())

	// Table-in-out functions
	w.RegisterTableInOut(table_in_out.NewBufferInputFunction())
	w.RegisterTableInOut(table_in_out.NewDistributedSumFunction())
	w.RegisterTableInOut(table_in_out.NewEchoFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionFinalizeFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionProcessFunction())
	w.RegisterTableInOut(table_in_out.NewFilterBySettingFunction())
	w.RegisterTableInOut(table_in_out.NewRepeatInputsFunction())
	w.RegisterTableInOut(table_in_out.NewSlowCancellableInOutFunction())
	w.RegisterTableInOut(table_in_out.NewSumAllColumnsFunction())

	// schema_reconcile fixture: 3 table-in-outs (insert/update/delete) + 1
	// table function (scan), all scoped to the schema_reconcile catalog so
	// they don't surface in the example catalog's function listing.
	schema_reconcile.RegisterAll(w)
	w.RegisterTableInOut(table_in_out.NewUnnestTensorRowsFunction())

	// Catalog tables

	// Function-backed table: columns derived from sequence's OnBind
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:     "large_sequence",
		Comment:  "A large sequence of integers from 0 to 1,000,000",
		Function: table.NewSequenceFunction(),
		FuncArgs: []vgi.CatalogTableArg{
			{Position: 0, Value: int64(1_000_000), Type: arrow.PrimitiveTypes.Int64},
		},
	})

	// Function-backed table over the no-arg ten_thousand function. Used by
	// inlined_scan_function.test / inlined_cardinality.test / catalog/zero_count_bypass.test
	// / catalog/eager_load_threshold.test to verify catalog inlining.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:     "ten_thousand_table",
		Comment:  "Function-backed table over the no-arg ten_thousand function",
		Function: table.NewTenThousandFunction(),
	})

	// Same backing function, but with inlined cardinality on TableInfo so the
	// per-bind table_function_cardinality RPC is skipped.
	{
		card := int64(10000)
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name:                "cardinality_inlined_table",
			Comment:             "Function-backed table with inlined cardinality (10000 rows)",
			Function:            table.NewTenThousandFunction(),
			CardinalityEstimate: &card,
			CardinalityMax:      &card,
		})
	}

	// Time-travel table: version-specific schema
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name: "versioned_data",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		}, nil),
		SupportsTimeTravel: true,
		Comment:            "Versioned data table demonstrating time travel with schema evolution",
	})

	statsTTL3600 := int64(3600)

	// Explicit-columns table: uses scan function handler below
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "numbers",
		Comment: "First 100 integers (demonstrates explicit columns)",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		Statistics: map[string]*vgi.ColumnStatistics{
			"value": {ColumnName: "value", Type: arrow.PrimitiveTypes.Int64, Min: int64(0), Max: int64(99), HasNotNull: true, DistinctCount: 100},
		},
	})

	// ENUM-derived statistics demo (colors).
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "colors",
		Comment: "Colors table with ENUM-derived statistics",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "color", Type: arrow.BinaryTypes.String},
			{Name: "hex_code", Type: arrow.BinaryTypes.String},
		}, nil),
		Statistics: map[string]*vgi.ColumnStatistics{
			"id":       {ColumnName: "id", Type: arrow.PrimitiveTypes.Int64, Min: int64(1), Max: int64(3), HasNotNull: true, DistinctCount: 3},
			"color":    {ColumnName: "color", Type: arrow.BinaryTypes.String, Min: "blue", Max: "red", HasNotNull: true, DistinctCount: 3, ContainsUnicode: boolPtr(false), MaxStringLength: int64Ptr(5)},
			"hex_code": {ColumnName: "hex_code", Type: arrow.BinaryTypes.String, Min: "#0000FF", Max: "#FF0000", HasNotNull: true, DistinctCount: 3, ContainsUnicode: boolPtr(false), MaxStringLength: int64Ptr(7)},
		},
		StatisticsCacheMaxAgeSeconds: &statsTTL3600,
	})

	// Geometry stats: bounding box via BOX(min,max).
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "geo_points",
		Comment: "5x5 grid of points with spatial bounding-box statistics",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata(
				[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
				[]string{"geoarrow.wkb", "{}"},
			)},
		}, nil),
		Statistics: map[string]*vgi.ColumnStatistics{
			"id": {ColumnName: "id", Type: arrow.PrimitiveTypes.Int64, Min: int64(1), Max: int64(25), HasNotNull: true, DistinctCount: 25},
			// WKB corner points: ST_Point(0,0) and ST_Point(4,4).
			// DuckDB renders GEOMETRY stats as BOX(min, max) in
			// vgi_table_statistics().
			"geom": {ColumnName: "geom", Type: arrow.BinaryTypes.Binary, Min: wkbPoint(0, 0), Max: wkbPoint(4, 4), HasNotNull: true, DistinctCount: 25},
		},
	})

	// Volatile stats (TTL=0): re-fetched per query.
	statsTTL0 := int64(0)
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "volatile_numbers",
		Comment: "Numbers with volatile stats (TTL=0, always re-fetched)",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		Statistics: map[string]*vgi.ColumnStatistics{
			"value": {ColumnName: "value", Type: arrow.PrimitiveTypes.Int64, Min: int64(0), Max: int64(99), HasNotNull: true, DistinctCount: 100},
		},
		StatisticsCacheMaxAgeSeconds: &statsTTL0,
	})

	// Table with NO declared statistics — stats must come from the underlying
	// scan function (SequenceFunction.Statistics) via table_function_statistics RPC.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "funny_numbers",
		Comment: "123456 integers; stats served by the sequence function, not the table",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	})

	// Generated-column example: physical column `n` from sequence(10),
	// with `doubled` and `label` materialized by DuckDB from SQL expressions.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "generated_sequence",
		Comment: "Table with generated columns backed by sequence(10)",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
			{Name: "doubled", Type: arrow.PrimitiveTypes.Int64},
			{Name: "label", Type: arrow.BinaryTypes.String},
		}, nil),
		Generated: map[string]string{
			"doubled": "n * 2",
			"label":   "'item_' || CAST(n AS VARCHAR)",
		},
	})

	// Row ID tables: row_id column at different positions and with different types
	rowIDMeta := arrow.NewMetadata([]string{"is_row_id"}, []string{""})
	rowIDStructType := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
	)
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "rowid_first",
		Comment: "Table with row_id at column index 0",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "row_id", Type: arrow.PrimitiveTypes.Int64, Metadata: rowIDMeta},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "rowid_middle",
		Comment: "Table with row_id at column index 1",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "row_id", Type: arrow.PrimitiveTypes.Int64, Metadata: rowIDMeta},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "rowid_last",
		Comment: "Table with row_id at column index 2",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.BinaryTypes.String},
			{Name: "row_id", Type: arrow.PrimitiveTypes.Int64, Metadata: rowIDMeta},
		}, nil),
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "rowid_string",
		Comment: "Table with string row_id",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "row_id", Type: arrow.BinaryTypes.String, Metadata: rowIDMeta},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "rowid_struct",
		Comment: "Table with struct row_id",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "row_id", Type: rowIDStructType, Metadata: rowIDMeta},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
	})

	// Constraint example tables
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:       "departments",
		Comment:    "Department reference table",
		Columns:    table.DepartmentsSchema,
		NotNull:    []string{"id", "name"},
		PrimaryKey: [][]string{{"id"}},
		Unique:     [][]string{{"name"}},
		Check:      []string{"budget >= 0"},
		Defaults:   map[string]any{"budget": int64(0)},
		Statistics: map[string]*vgi.ColumnStatistics{
			"id":     {ColumnName: "id", Type: arrow.PrimitiveTypes.Int64, Min: int64(1), Max: int64(10), HasNotNull: true, DistinctCount: 10},
			"name":   {ColumnName: "name", Type: arrow.BinaryTypes.String, Min: "Accounting", Max: "Sales", HasNotNull: true, DistinctCount: 10, ContainsUnicode: boolPtr(false), MaxStringLength: int64Ptr(20)},
			"budget": {ColumnName: "budget", Type: arrow.PrimitiveTypes.Float64, Min: float64(50000), Max: float64(500000), HasNotNull: true, DistinctCount: 10},
		},
		StatisticsCacheMaxAgeSeconds: &statsTTL3600,
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:       "employees",
		Comment:    "Employee table with FK to departments",
		Columns:    table.EmployeesSchema,
		NotNull:    []string{"id", "name", "email"},
		PrimaryKey: [][]string{{"id"}},
		Unique:     [][]string{{"email"}},
		ForeignKey: []vgi.ForeignKeyConstraint{
			{Columns: []string{"department_id"}, ReferencedTable: "departments", ReferencedColumns: []string{"id"}},
		},
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:       "projects",
		Comment:    "Projects with composite PK and FK to departments",
		Columns:    table.ProjectsSchema,
		NotNull:    []string{"department_id", "project_code", "title"},
		PrimaryKey: [][]string{{"department_id", "project_code"}},
		ForeignKey: []vgi.ForeignKeyConstraint{
			{Columns: []string{"department_id"}, ReferencedTable: "departments", ReferencedColumns: []string{"id"}},
		},
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:       "products",
		Comment:    "Product table with column defaults",
		Columns:    table.ProductsSchema,
		NotNull:    []string{"id"},
		PrimaryKey: [][]string{{"id"}},
		Defaults:   map[string]any{"quantity": int64(0), "name": "unknown", "price": float64(9.99)},
		ColumnComments: map[string]string{
			"id":    "Unique product identifier",
			"name":  "Product display name",
			"price": "Unit price in USD",
		},
		Statistics: map[string]*vgi.ColumnStatistics{
			"id":       {ColumnName: "id", Type: arrow.PrimitiveTypes.Int64, Min: int64(1), Max: int64(100), HasNotNull: true, DistinctCount: 100},
			"name":     {ColumnName: "name", Type: arrow.BinaryTypes.String, Min: "Anvil", Max: "Zebra Tape", HasNotNull: true, DistinctCount: 100, ContainsUnicode: boolPtr(false), MaxStringLength: int64Ptr(30)},
			"quantity": {ColumnName: "quantity", Type: arrow.PrimitiveTypes.Int64, Min: int64(0), Max: int64(10000), HasNull: true, HasNotNull: true, DistinctCount: 50},
			"price":    {ColumnName: "price", Type: arrow.PrimitiveTypes.Float64, Min: float64(0.99), Max: float64(999.99), HasNotNull: true, DistinctCount: 80},
		},
		StatisticsCacheMaxAgeSeconds: &statsTTL3600,
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:               "versioned_constraints",
		Columns:            table.VersionedConstraintsSchemas[3],
		Comment:            "Table with constraints that evolve across versions",
		SupportsTimeTravel: true,
		NotNull:            []string{"id", "name"},
		PrimaryKey:         [][]string{{"id"}},
		Unique:             [][]string{{"email"}},
		ForeignKey: []vgi.ForeignKeyConstraint{
			{Columns: []string{"department_id"}, ReferencedTable: "departments", ReferencedColumns: []string{"id"}},
		},
	})

	// Views
	w.RegisterCatalogView("main", vgi.CatalogView{
		Name:       "first_ten",
		Definition: "SELECT * FROM sequence(10)",
		Comment:    "First 10 integers",
	})
	w.RegisterCatalogView("main", vgi.CatalogView{
		Name:       "even_numbers",
		Definition: "SELECT * FROM sequence(100) WHERE n % 2 = 0",
		Comment:    "Even numbers from 0 to 98",
	})
	w.RegisterCatalogView("data", vgi.CatalogView{
		Name:       "small_numbers",
		Definition: "SELECT * FROM numbers WHERE value < 10",
	})

	// Macros
	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:       "vgi_multiply",
		MacroType:  vgi.MacroTypeScalar,
		Parameters: []string{"x", "y"},
		Definition: "x * y",
	})

	clampDefaults, err := vgi.BuildMacroDefaultValues([]vgi.MacroDefault{
		{Name: "lo", Value: int64(0), Type: arrow.PrimitiveTypes.Int64},
		{Name: "hi", Value: int64(100), Type: arrow.PrimitiveTypes.Int64},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to build macro defaults: %v", err))
	}
	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:                   "vgi_clamp",
		MacroType:              vgi.MacroTypeScalar,
		Parameters:             []string{"val", "lo", "hi"},
		ParameterDefaultValues: clampDefaults,
		Definition:             "GREATEST(lo, LEAST(hi, val))",
	})

	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:       "vgi_range_table",
		MacroType:  vgi.MacroTypeTable,
		Parameters: []string{"n"},
		Definition: "SELECT * FROM range(n)",
	})

	// Handler for time-travel table_get (returns version-specific schemas)
	w.SetTableGetHandler(func(schemaName, tableName string, atUnit, atValue *string) ([]byte, error) {
		if schemaName == "data" && tableName == "versioned_data" && atUnit != nil && *atUnit != "" {
			version, err := table.ResolveVersion(atUnit, atValue)
			if err != nil {
				return nil, err
			}
			info := &vgi.TableInfo{
				Name:       tableName,
				SchemaName: schemaName,
				Comment:    "Versioned data table demonstrating time travel with schema evolution",
				Columns:    table.VersionedSchema(version),
			}
			return vgi.SerializeTableInfo(info)
		}
		if schemaName == "data" && tableName == "versioned_constraints" && atUnit != nil && *atUnit != "" {
			version, err := table.ResolveVersionedConstraintsVersion(atUnit, atValue)
			if err != nil {
				return nil, err
			}
			info := &vgi.TableInfo{
				Name:       tableName,
				SchemaName: schemaName,
				Comment:    "Table with constraints that evolve across versions",
				Columns:    table.VersionedConstraintsSchema(version),
			}
			return vgi.SerializeTableInfo(info)
		}
		return nil, nil // fall through to default
	})

	// Handler for tables without a backing Function
	rowIDTables := map[string]map[string]string{
		"rowid_first":  {"layout": "first", "row_id_type": "int64"},
		"rowid_middle": {"layout": "middle", "row_id_type": "int64"},
		"rowid_last":   {"layout": "last", "row_id_type": "int64"},
		"rowid_string": {"layout": "first", "row_id_type": "string"},
		"rowid_struct": {"layout": "first", "row_id_type": "struct"},
	}
	w.SetScanFunctionGetHandler(func(schemaName, tableName string, atUnit, atValue *string) (*vgi.ScanFunctionResult, error) {
		// Handle versioned_data time travel
		if schemaName == "data" && tableName == "versioned_data" {
			version, err := table.ResolveVersion(atUnit, atValue)
			if err != nil {
				return nil, err
			}
			return &vgi.ScanFunctionResult{
				FunctionName: "versioned_data_scan",
				PositionalArguments: []vgi.ScanArg{
					{Value: version, Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}

		// Handle versioned_constraints time travel
		if schemaName == "data" && tableName == "versioned_constraints" {
			version, err := table.ResolveVersionedConstraintsVersion(atUnit, atValue)
			if err != nil {
				return nil, err
			}
			return &vgi.ScanFunctionResult{
				FunctionName: "versioned_constraints_scan",
				PositionalArguments: []vgi.ScanArg{
					{Value: version, Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}

		// Reject AT clause on tables that don't support time travel
		if atUnit != nil && *atUnit != "" {
			return nil, fmt.Errorf("Table '%s.%s' does not support time travel queries", schemaName, tableName)
		}

		// Handle static constraint tables
		if schemaName == "data" {
			switch tableName {
			case "departments":
				return &vgi.ScanFunctionResult{FunctionName: "departments_scan"}, nil
			case "employees":
				return &vgi.ScanFunctionResult{FunctionName: "employees_scan"}, nil
			case "products":
				return &vgi.ScanFunctionResult{FunctionName: "products_scan"}, nil
			case "projects":
				return &vgi.ScanFunctionResult{FunctionName: "projects_scan"}, nil
			}
		}

		if schemaName == "data" && tableName == "numbers" {
			return &vgi.ScanFunctionResult{
				FunctionName: "sequence",
				PositionalArguments: []vgi.ScanArg{
					{Value: int64(100), Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}
		if schemaName == "data" && tableName == "volatile_numbers" {
			return &vgi.ScanFunctionResult{
				FunctionName: "sequence",
				PositionalArguments: []vgi.ScanArg{
					{Value: int64(100), Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}
		if schemaName == "data" && tableName == "funny_numbers" {
			return &vgi.ScanFunctionResult{
				FunctionName: "sequence",
				PositionalArguments: []vgi.ScanArg{
					{Value: int64(123456), Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}
		if schemaName == "data" && tableName == "colors" {
			return &vgi.ScanFunctionResult{FunctionName: "colors_scan"}, nil
		}
		if schemaName == "data" && tableName == "geo_points" {
			return &vgi.ScanFunctionResult{FunctionName: "geo_points_scan"}, nil
		}
		if schemaName == "data" && tableName == "generated_sequence" {
			return &vgi.ScanFunctionResult{
				FunctionName: "sequence",
				PositionalArguments: []vgi.ScanArg{
					{Value: int64(10), Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}
		if schemaName == "data" {
			if opts, ok := rowIDTables[tableName]; ok {
				return &vgi.ScanFunctionResult{
					FunctionName: "rowid_sequence",
					PositionalArguments: []vgi.ScanArg{
						{Value: int64(20), Type: arrow.PrimitiveTypes.Int64},
					},
					NamedArguments: map[string]vgi.ScanArg{
						"layout":      {Value: opts["layout"], Type: arrow.BinaryTypes.String},
						"row_id_type": {Value: opts["row_id_type"], Type: arrow.BinaryTypes.String},
					},
				}, nil
			}
		}
		return nil, fmt.Errorf("no scan function for %s.%s", schemaName, tableName)
	})

	if *httpMode {
		authFn, jwtCleanup := resolveAuthenticate()
		if jwtCleanup != nil {
			defer jwtCleanup()
		}
		if authFn != nil {
			w.SetAuthenticate(authFn)
		}
		if m := resolveOAuthResourceMetadata(); m != nil {
			w.SetOAuthResourceMetadata(m)
		}
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
	} else {
		w.RunStdio()
	}
}

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(n int64) *int64 { return &n }

// wkbPoint encodes a 2D WKB point (little-endian) for stats min/max values
// on geoarrow.wkb columns.
func wkbPoint(x, y float64) []byte {
	buf := make([]byte, 21)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], 1)
	binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(x))
	binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(y))
	return buf
}

// ---------------------------------------------------------------------------
// Auth environment variable resolution (matches vgi-python serve.py)
// ---------------------------------------------------------------------------

// resolveAuthenticate builds an AuthenticateFunc from environment variables.
// Returns nil if no auth env vars are set. When both bearer and JWT are
// configured, they are chained (JWT first, bearer fallback).
func resolveAuthenticate() (vgirpc.AuthenticateFunc, func()) {
	bearerAuth := resolveBearerAuthenticate()
	jwtAuth, jwtCleanup := resolveJWTAuthenticate()

	if bearerAuth != nil && jwtAuth != nil {
		return vgirpc.ChainAuthenticate(jwtAuth, bearerAuth), jwtCleanup
	}
	if jwtAuth != nil {
		return jwtAuth, jwtCleanup
	}
	return bearerAuth, nil
}

// resolveBearerAuthenticate parses VGI_BEARER_TOKENS into a static bearer
// token authenticator. Format: "token=principal,token2=principal2".
func resolveBearerAuthenticate() vgirpc.AuthenticateFunc {
	raw := os.Getenv("VGI_BEARER_TOKENS")
	if raw == "" {
		return nil
	}

	tokens := make(map[string]*vgirpc.AuthContext)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if !strings.Contains(entry, "=") {
			log.Fatalf("Error: malformed VGI_BEARER_TOKENS entry: %q\nExpected format: token=principal (e.g. 'mytoken=alice')", entry)
		}
		// Split on first = only — principals may contain =
		token, principal, _ := strings.Cut(entry, "=")
		tokens[token] = &vgirpc.AuthContext{
			Principal:     principal,
			Authenticated: true,
			Domain:        "bearer",
		}
	}

	if len(tokens) == 0 {
		return nil
	}
	return vgirpc.BearerAuthenticateStatic(tokens)
}

// resolveJWTAuthenticate parses VGI_JWT_ISSUER, VGI_JWT_AUDIENCE, and
// optional VGI_JWT_JWKS_URI into a JWT authenticator.
func resolveJWTAuthenticate() (vgirpc.AuthenticateFunc, func()) {
	issuer := os.Getenv("VGI_JWT_ISSUER")
	if issuer == "" {
		return nil, nil
	}

	audienceRaw := os.Getenv("VGI_JWT_AUDIENCE")
	if audienceRaw == "" {
		log.Fatal("Error: VGI_JWT_ISSUER is set but VGI_JWT_AUDIENCE is missing")
	}

	var audiences []string
	for _, s := range strings.Split(audienceRaw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			audiences = append(audiences, s)
		}
	}
	if len(audiences) == 0 {
		log.Fatal("Error: VGI_JWT_AUDIENCE is set but contains no valid values")
	}

	jwksURI := os.Getenv("VGI_JWT_JWKS_URI")
	if jwksURI == "" {
		// Derive from issuer (matching Python behavior where jwks_uri is optional)
		jwksURI = strings.TrimSuffix(issuer, "/") + "/.well-known/jwks.json"
	}

	authFn, cleanup, err := jwtauth.NewAuthenticateFunc(jwtauth.JWTAuthConfig{
		Issuer:   issuer,
		Audience: audiences,
		JWKSURI:  jwksURI,
	})
	if err != nil {
		log.Fatalf("Error: failed to initialize JWT auth: %v", err)
	}
	return authFn, cleanup
}

// resolveOAuthResourceMetadata parses VGI_OAUTH_* env vars into an
// OAuthResourceMetadata for RFC 9728 discovery.
func resolveOAuthResourceMetadata() *vgirpc.OAuthResourceMetadata {
	resource := os.Getenv("VGI_OAUTH_RESOURCE")
	if resource == "" {
		return nil
	}

	authServersRaw := os.Getenv("VGI_OAUTH_AUTH_SERVERS")
	if authServersRaw == "" {
		log.Fatal("Error: VGI_OAUTH_RESOURCE is set but VGI_OAUTH_AUTH_SERVERS is missing")
	}

	var authServers []string
	for _, s := range strings.Split(authServersRaw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			authServers = append(authServers, s)
		}
	}

	var scopes []string
	if scopesRaw := os.Getenv("VGI_OAUTH_SCOPES"); scopesRaw != "" {
		for _, s := range strings.Split(scopesRaw, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				scopes = append(scopes, s)
			}
		}
	}

	useIDToken := false
	if v := strings.ToLower(os.Getenv("VGI_OAUTH_USE_ID_TOKEN")); v == "1" || v == "true" || v == "yes" {
		useIDToken = true
	}

	m := &vgirpc.OAuthResourceMetadata{
		Resource:               resource,
		AuthorizationServers:   authServers,
		ScopesSupported:        scopes,
		ResourceName:           os.Getenv("VGI_OAUTH_RESOURCE_NAME"),
		ClientID:               os.Getenv("VGI_OAUTH_CLIENT_ID"),
		ClientSecret:           os.Getenv("VGI_OAUTH_CLIENT_SECRET"),
		DeviceCodeClientID:     os.Getenv("VGI_OAUTH_DEVICE_CODE_CLIENT_ID"),
		DeviceCodeClientSecret: os.Getenv("VGI_OAUTH_DEVICE_CODE_CLIENT_SECRET"),
		UseIDTokenAsBearer:     useIDToken,
	}

	if err := m.Validate(); err != nil {
		log.Fatalf("Error: invalid OAuth config: %v", err)
	}
	return m
}
