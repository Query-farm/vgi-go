// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/examples/accumulate"
	"github.com/Query-farm/vgi-go/examples/all"
	"github.com/Query-farm/vgi-go/examples/narrow_bind"
	"github.com/Query-farm/vgi-go/examples/schema_reconcile"
	"github.com/Query-farm/vgi-go/examples/table"
	"github.com/Query-farm/vgi-go/internal/covflush"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/resolve"
	"github.com/apache/arrow-go/v18/arrow"
)

func main() {
	httpMode := flag.Bool("http", false, "Run as HTTP server instead of stdio")
	unixPath := flag.String("unix", "", "Bind to this AF_UNIX socket path (launcher transport); mutually exclusive with --http")
	idleTimeout := flag.Float64("idle-timeout", 300, "Self-shutdown after N seconds idle when serving --unix (0 = never)")
	// --describe / --no-describe: accepted for launcher compatibility (the VGI
	// extension passes it through). Description pages aren't served over the
	// socket/stdio transports, so it is currently a no-op here.
	flag.Bool("describe", true, "Enable description pages (accepted for launcher compatibility)")
	flag.Bool("no-describe", false, "Disable description pages (accepted for launcher compatibility)")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	// The launcher varies a worker's argv (e.g. --threaded, --quiet) to
	// produce distinct cache keys for the same binary; tolerate unknown flags
	// rather than failing to start. Flags named here consume a value token.
	flag.CommandLine.Parse(filterKnownFlags(os.Args[1:], map[string]bool{
		"unix":         true,
		"idle-timeout": true,
		"log-level":    true,
		"log-format":   true,
		"log-logger":   true,
	}))

	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	// Flush coverage on SIGTERM (+ periodic) during integration coverage runs
	// (no-op otherwise); the harness kills pooled/long-lived workers with SIGTERM.
	covflush.Start()

	if *unixPath != "" && *httpMode {
		log.Fatal("--unix and --http are mutually exclusive")
	}

	// Pick the FunctionStorage backend from VGI_WORKER_SHARED_STORAGE.
	// Defaults to local SQLite when the env var is unset — preserves the
	// behavior for tests and existing deployments.
	storage, err := resolve.FromEnv()
	if err != nil {
		log.Fatalf("resolve storage backend: %v", err)
	}

	exampleSourceURL := "https://github.com/Query-farm/vgi-go"
	w := vgi.NewWorker(
		vgi.WithFunctionStorage(storage),
		vgi.WithSupportsTransactions(true),
		vgi.WithCatalogName("example"),
		vgi.WithCatalogComment("Example VGI catalog for testing"),
		vgi.WithCatalogTags(map[string]string{
			"source":  "vgi-fixture-worker",
			"version": "1",
		}),
		// Advertise where this worker's code lives (catalog_catalogs().source_url,
		// duckdb_databases()). Satisfies the metadata linter's VGI004 rule.
		vgi.WithCatalogInfo(vgi.CatalogInfo{
			SourceURL: &exampleSourceURL,
		}),
		vgi.WithSchemaComments(map[string]string{
			"main": "Example functions for testing VGI",
			"data": "Example tables backed by functions",
		}),
		// Schema-level description tags (duckdb_schemas().tags). Satisfies the
		// metadata linter's VGI116/VGI118 rules.
		vgi.WithSchemaTags(map[string]map[string]string{
			"main": {
				"vgi.description_llm": "Example functions for testing the VGI protocol.",
				"vgi.description_md":  "Example functions for testing **VGI**.",
			},
			"data": {
				"vgi.description_llm": "Example tables backed by VGI table functions.",
				"vgi.description_md":  "Example tables backed by VGI table functions.",
			},
		}),
		// Cross-language reproducer catalogs share this binary; ATTACH
		// against any of these names succeeds (functions are catalog-agnostic).
		vgi.WithCatalogAliases("projection_repro", "schema_reconcile", narrow_bind.CatalogName),
		// The accumulate fixture catalog is discoverable (data version 2.0.0)
		// and isolated per ATTACH (random scope); its functions are registered
		// catalog-scoped below so they don't leak into the example catalog.
		vgi.WithCatalogAliasInfo(accumulate.CatalogName, accumulate.CatalogAliasInfo()),
		// schema_reconcile fixture: tables/scan-fn/insert-fn/update-fn/delete-fn
		// resolution lives outside the static catalog because the tables are
		// served by handlers (not declared via RegisterCatalogTable). The four
		// handlers below are wired in registration order; they short-circuit on
		// any other catalog name.
		// Each option is single-valued, so the schema_reconcile and narrow_bind
		// fixtures are composed: try the first, fall through to the second when
		// it declines (returns false).
		vgi.WithSchemaContentsHandler(func(attach []byte, schema string) ([]vgi.SerializedSchemaItem, bool) {
			if items, ok := schema_reconcile.SchemaContentsHandler(attach, schema); ok {
				return items, true
			}
			return narrow_bind.SchemaContentsHandler(attach, schema)
		}),
		vgi.WithAttachTableGetHandler(func(attach []byte, schema, name string, atUnit, atValue *string) ([]byte, bool, error) {
			if data, ok, err := schema_reconcile.AttachTableGetHandler(attach, schema, name, atUnit, atValue); ok || err != nil {
				return data, ok, err
			}
			return narrow_bind.AttachTableGetHandler(attach, schema, name, atUnit, atValue)
		}),
		vgi.WithAttachScanFunctionGetHandler(func(attach []byte, schema, name string, atUnit, atValue *string) (*vgi.ScanFunctionResult, bool, error) {
			if res, ok, err := schema_reconcile.AttachScanFunctionGetHandler(attach, schema, name, atUnit, atValue); ok || err != nil {
				return res, ok, err
			}
			return narrow_bind.AttachScanFunctionGetHandler(attach, schema, name, atUnit, atValue)
		}),
		vgi.WithAttachScanBranchesGetHandler(multiBranchScanBranchesGet),
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
				Name:         "scale_factor",
				Description:  "Float scale factor",
				Type:         arrow.PrimitiveTypes.Float64,
				DefaultValue: float64(1),
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

	// Register every example function (scalars, tables, table-in-outs,
	// table-bufferings, aggregates, schema_reconcile). Catalog tables, secret
	// types, and settings remain wired below — they're fixture-specific.
	all.RegisterAll(w)

	// Register the accumulate fixture (catalog-scoped, so it is invisible under
	// the example catalog and preserves its function inventory).
	accumulate.Register(w)

	// Register the narrow_bind reproducer fixture (catalog-scoped). Its
	// scan functions back the mismatch/consistent tables advertised by the
	// composed catalog handlers above.
	narrow_bind.RegisterAll(w)

	// Writable catalog (in-memory, per-process state). Gated off by default so
	// the example worker's function inventory matches the reference vgi-python
	// worker; set VGI_WORKER_ENABLE_WRITABLE=1 to exercise the writable tests.
	if os.Getenv("VGI_WORKER_ENABLE_WRITABLE") != "" {
		w.RegisterWritableCatalog(vgi.NewWritableCatalog("writable"))
	}
	// Note: geo_points is introspected via vgi_table_statistics only; no
	// separate scan function is registered (matches the vgi-python example
	// worker inventory). Direct `SELECT * FROM data.geo_points` will error
	// because the backing scan function is not exposed.

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

	// Multi-branch (scan-branches) tables. Their physical sources are resolved
	// by multiBranchScanBranchesGet via catalog_table_scan_branches_get.
	{
		nCol := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name:    "multi_branch_numbers",
			Columns: nCol,
			Comment: "Multi-branch: UNION of sequence(50) + sequence(50) — used by multi_branch_scan.test",
		})
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name:    "multi_branch_filtered_numbers",
			Columns: nCol,
			Comment: "Multi-branch with complementary branch_filters — exercises pruning",
		})
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name:    "multi_branch_hetero",
			Columns: nCol,
			Comment: "Multi-branch: sequence(50) + read_parquet — used by multi_branch_heterogeneous.test",
		})
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name:    "multi_branch_nopushdown",
			Columns: nCol,
			Comment: "Multi-branch: VGI + read_csv — used by multi_branch_pushdown_incapable.test",
		})
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name:    "multi_branch_empty",
			Columns: nCol,
			Comment: "Multi-branch: empty branches list — used by multi_branch_empty_branches.test",
		})
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name:    "multi_branch_two_writable",
			Columns: nCol,
			Comment: "Multi-branch with two writable=True arms — used by multi_branch_two_writable.test",
		})
		w.RegisterCatalogTable("data", vgi.CatalogTable{
			Name: "multi_branch_recon",
			Columns: arrow.NewSchema([]arrow.Field{
				{Name: "a", Type: arrow.PrimitiveTypes.Int64},
				{Name: "b", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
			Comment: "Multi-branch: column reconciliation — used by multi_branch_reconciliation.test",
		})
	}

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

	// Late-materialization tables (rowid + scrambled ord), all backed by the
	// late_materialization scan function which advertises LateMaterialization.
	// See late_materialization.test.
	lateMatColumns := func() *arrow.Schema {
		return arrow.NewSchema([]arrow.Field{
			{Name: "row_id", Type: arrow.PrimitiveTypes.Int64, Metadata: rowIDMeta},
			{Name: "ord", Type: arrow.PrimitiveTypes.Int64},
			{Name: "payload", Type: arrow.BinaryTypes.String},
			{Name: "pushed", Type: arrow.BinaryTypes.String},
		}, nil)
	}
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "late_mat",
		Comment: "Late-materialization table (1000 rows, unique rowid)",
		Columns: lateMatColumns(),
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "late_mat_dup",
		Comment: "Late-materialization table with deliberately non-unique rowid (contract violation)",
		Columns: lateMatColumns(),
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "late_mat_nulls",
		Comment: "Late-materialization table with NULLs in the ord column",
		Columns: lateMatColumns(),
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
	// required_field_filter_paths fixtures — back the
	// required_field_filter_paths_*.test sqllogictest matrix. The C++ optimizer
	// extension enforces the declared paths at bind time.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:                     "rff_simple",
		Comment:                  "rff_simple — requires a filter referencing column 'a'.",
		Columns:                  table.RffSimpleSchema,
		RequiredFieldFilterPaths: []string{"a"},
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:                     "rff_struct",
		Comment:                  "rff_struct — requires filters on both struct subfields s.a and s.b.",
		Columns:                  table.RffStructSchema,
		RequiredFieldFilterPaths: []string{"s.a", "s.b"},
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:                     "rff_nested",
		Comment:                  "rff_nested — requires a filter on the 3-deep nested path wrapper.mid.leaf.",
		Columns:                  table.RffNestedSchema,
		RequiredFieldFilterPaths: []string{"wrapper.mid.leaf"},
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:                     "rff_multi",
		Comment:                  "rff_multi — mixed top-level + struct subfield requirements.",
		Columns:                  table.RffMultiSchema,
		RequiredFieldFilterPaths: []string{"top", "s.a"},
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "rff_none",
		Comment: "rff_none — control table with no required_field_filter_paths (opt-out fast path).",
		Columns: table.RffNoneSchema,
	})
	// rff_rowid — row_id virtual column + required bbox.* filters. The rowid
	// table_filter is keyed by a sentinel >> column count, which the optimizer
	// must skip. See required_field_filter_paths_rowid.test.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:                     "rff_rowid",
		Comment:                  "rff_rowid — row_id virtual column + required bbox.* filters.",
		Columns:                  table.RffRowidSchema,
		RequiredFieldFilterPaths: []string{"bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"},
	})
	// rff_parquet / rff_hive / rff_hive_mixed — native read_parquet delegation
	// with bbox.* required filters (mirrors Overture transportation.segment).
	// See required_field_filter_paths_native.test.
	rffBboxType := arrow.StructOf(
		arrow.Field{Name: "xmin", Type: arrow.PrimitiveTypes.Float32},
		arrow.Field{Name: "ymin", Type: arrow.PrimitiveTypes.Float32},
		arrow.Field{Name: "xmax", Type: arrow.PrimitiveTypes.Float32},
		arrow.Field{Name: "ymax", Type: arrow.PrimitiveTypes.Float32},
	)
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "rff_parquet",
		Comment: "rff_parquet — native read_parquet delegation with bbox.* required filters.",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "bbox", Type: rffBboxType},
			{Name: "other", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		RequiredFieldFilterPaths: []string{"bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"},
	})
	rffHiveColumns := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "bbox", Type: rffBboxType},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "num", Type: arrow.PrimitiveTypes.Int64},
		{Name: "theme", Type: arrow.BinaryTypes.String},
		{Name: "type", Type: arrow.BinaryTypes.String},
	}, nil)
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:                     "rff_hive",
		Comment:                  "rff_hive — native read_parquet over Hive glob with bbox.* required filters.",
		Columns:                  rffHiveColumns,
		RequiredFieldFilterPaths: []string{"bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"},
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:                     "rff_hive_mixed",
		Comment:                  "rff_hive_mixed — native read_parquet, top-level 'id' + bbox.* required filters.",
		Columns:                  rffHiveColumns,
		RequiredFieldFilterPaths: []string{"id", "bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"},
	})
	// filter_echo_table — catalog table echoing pushed-down filters, backs
	// filter_pushdown_through_view.test.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "filter_echo_table",
		Comment: "Catalog table echoing pushed-down filters (filter-pushdown-through-view tests).",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
			{Name: "s", Type: arrow.BinaryTypes.String},
			{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
		}, nil),
	})
	// Time travel + filter pushdown together (time_travel_pushdown.test).
	// tt_pushdown_fn is function-backed (reads AT at init via the bind request
	// threaded onto init); tt_pushdown_cols is columns-based (AT → version arg
	// via the scan function handler).
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:               "tt_pushdown_fn",
		Comment:            "Function-backed: prunes by filter AND time-travels (AT read at init).",
		Function:           table.NewTimeTravelPushdownFunction(),
		SupportsTimeTravel: true,
	})
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:               "tt_pushdown_cols",
		Comment:            "Columns-based: prunes by filter AND time-travels (AT → version arg).",
		Columns:            table.TtPushdownOutputSchema,
		SupportsTimeTravel: true,
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
		Name:           "first_ten",
		Definition:     "SELECT * FROM sequence(10)",
		Comment:        "First 10 integers",
		ColumnComments: map[string]string{"n": "Sequence index 0..9"},
		Tags:           map[string]string{"layer": "demo", "origin": "sequence"},
	})
	w.RegisterCatalogView("main", vgi.CatalogView{
		Name:       "even_numbers",
		Definition: "SELECT * FROM sequence(100) WHERE n % 2 = 0",
		Comment:    "Even numbers from 0 to 98",
	})
	w.RegisterCatalogView("data", vgi.CatalogView{
		Name:           "small_numbers",
		Definition:     "SELECT * FROM numbers WHERE value < 10",
		Comment:        "Numbers less than 10",
		ColumnComments: map[string]string{"value": "Single-digit value 0..9"},
	})

	// Macros
	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:       "vgi_multiply",
		MacroType:  vgi.MacroTypeScalar,
		Parameters: []string{"x", "y"},
		Definition: "x * y",
		ParameterDocs: map[string]string{
			"x": "First factor",
			"y": "Second factor",
		},
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
		// Per-parameter documentation rides over the wire via the macro
		// arguments_schema's vgi_doc field metadata (the same channel functions
		// use for per-argument docs). Optional; undocumented params carry no doc.
		ParameterDocs: map[string]string{
			"val": "Value to clamp",
			"lo":  "Lower bound (inclusive)",
			"hi":  "Upper bound (inclusive)",
		},
	})

	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:       "vgi_range_table",
		MacroType:  vgi.MacroTypeTable,
		Parameters: []string{"n"},
		Definition: "SELECT * FROM range(n)",
		ParameterDocs: map[string]string{
			"n": "Number of rows to generate",
		},
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
	// Named arguments to late_materialization for each late-mat table.
	lateMatScanArgs := map[string]map[string]vgi.ScanArg{
		"late_mat":     nil,
		"late_mat_dup": {"dup_row_id": {Value: true, Type: arrow.FixedWidthTypes.Boolean}},
		"late_mat_nulls": {
			"null_ord_stride": {Value: int64(7), Type: arrow.PrimitiveTypes.Int64},
		},
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

		// Columns-based time-travel + pushdown: resolve AT → version and pass it
		// as a scan-function argument (the native columns-based AT mechanism).
		if schemaName == "data" && tableName == "tt_pushdown_cols" {
			version, err := table.ResolveTtVersion(atUnit, atValue)
			if err != nil {
				return nil, err
			}
			return &vgi.ScanFunctionResult{
				FunctionName: "tt_pushdown_cols_scan",
				PositionalArguments: []vgi.ScanArg{
					{Value: version, Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}

		// rff_parquet — single-branch native read_parquet delegation.
		if schemaName == "data" && tableName == "rff_parquet" {
			return &vgi.ScanFunctionResult{
				FunctionName: "read_parquet",
				PositionalArguments: []vgi.ScanArg{
					{Value: "/tmp/rff_seg.parquet", Type: arrow.BinaryTypes.String},
				},
			}, nil
		}

		// rff_hive / rff_hive_mixed — native read_parquet over a Hive glob.
		if schemaName == "data" && (tableName == "rff_hive" || tableName == "rff_hive_mixed") {
			return &vgi.ScanFunctionResult{
				FunctionName: "read_parquet",
				PositionalArguments: []vgi.ScanArg{
					{Value: "/tmp/rff_hive/*/*/*.parquet", Type: arrow.BinaryTypes.String},
				},
				NamedArguments: map[string]vgi.ScanArg{
					"hive_partitioning": {Value: true, Type: arrow.FixedWidthTypes.Boolean},
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
			case "rff_simple":
				return &vgi.ScanFunctionResult{FunctionName: "rff_simple_scan"}, nil
			case "rff_struct":
				return &vgi.ScanFunctionResult{FunctionName: "rff_struct_scan"}, nil
			case "rff_nested":
				return &vgi.ScanFunctionResult{FunctionName: "rff_nested_scan"}, nil
			case "rff_multi":
				return &vgi.ScanFunctionResult{FunctionName: "rff_multi_scan"}, nil
			case "rff_none":
				return &vgi.ScanFunctionResult{FunctionName: "rff_none_scan"}, nil
			case "rff_rowid":
				return &vgi.ScanFunctionResult{FunctionName: "rff_rowid_scan"}, nil
			case "filter_echo_table":
				return &vgi.ScanFunctionResult{FunctionName: "filter_echo_table_scan"}, nil
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
		// Late-materialization tables → late_materialization scan function.
		// 1000 rows is large enough that LIMIT k << count makes the rewrite a
		// real win and that LIMIT 200 exceeds dynamic_or_filter_threshold (50).
		if schemaName == "data" {
			if named, ok := lateMatScanArgs[tableName]; ok {
				return &vgi.ScanFunctionResult{
					FunctionName: "late_materialization",
					PositionalArguments: []vgi.ScanArg{
						{Value: int64(1000), Type: arrow.PrimitiveTypes.Int64},
					},
					NamedArguments: named,
				}, nil
			}
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

	switch {
	case *unixPath != "":
		if err := w.RunUnix(*unixPath, time.Duration(*idleTimeout*float64(time.Second))); err != nil {
			log.Fatal(err)
		}
	case *httpMode:
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
	default:
		w.RunStdio()
	}
}

// filterKnownFlags drops command-line tokens for flags this binary doesn't
// define, so launcher-injected argv-differentiation flags (e.g. --threaded,
// --quiet, --debug) don't abort flag parsing. Flags named in valueFlags consume
// the following token as their value (when not given in --flag=value form);
// all other recognized flags are treated as valueless. Unknown flags and stray
// positionals are dropped.
func filterKnownFlags(args []string, valueFlags map[string]bool) []string {
	defined := map[string]bool{}
	flag.CommandLine.VisitAll(func(f *flag.Flag) { defined[f.Name] = true })
	var out []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			continue // stray positional
		}
		name := strings.TrimLeft(a, "-")
		hasInlineValue := strings.ContainsRune(name, '=')
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			name = name[:eq]
		}
		if !defined[name] {
			continue // unknown flag — ignore
		}
		out = append(out, a)
		if valueFlags[name] && !hasInlineValue && i+1 < len(args) {
			i++
			out = append(out, args[i])
		}
	}
	return out
}

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(n int64) *int64 { return &n }

func strPtr(s string) *string { return &s }

// multiBranchScanBranchesGet resolves the physical sources for the
// multi_branch_* fixture tables. It mirrors vgi-python's
// ExampleCatalog.table_scan_branches_get. Returns (nil, false) for any other
// table so the C++ extension falls back to catalog_table_scan_function_get.
func multiBranchScanBranchesGet(_ []byte, schemaName, name string, _, _ *string) (*vgi.ScanBranchesResult, bool, error) {
	if !strings.EqualFold(schemaName, "data") {
		return nil, false, nil
	}
	seq := func(n int64) vgi.ScanBranch {
		return vgi.ScanBranch{
			FunctionName:        "sequence",
			PositionalArguments: []vgi.ScanArg{{Value: n, Type: arrow.PrimitiveTypes.Int64}},
		}
	}
	readParquet := func(path string) vgi.ScanBranch {
		return vgi.ScanBranch{
			FunctionName:        "read_parquet",
			PositionalArguments: []vgi.ScanArg{{Value: path, Type: arrow.BinaryTypes.String}},
		}
	}
	switch strings.ToLower(name) {
	case "multi_branch_numbers":
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{seq(50), seq(50)}}, true, nil
	case "multi_branch_filtered_numbers":
		a, b := seq(100), seq(100)
		a.BranchFilter = strPtr("n < 50")
		b.BranchFilter = strPtr("n >= 50")
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{a, b}}, true, nil
	case "multi_branch_hetero":
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{
			seq(50), readParquet("/tmp/vgi_hetero_branch.parquet"),
		}}, true, nil
	case "multi_branch_nopushdown":
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{
			seq(50),
			{FunctionName: "read_csv_auto", PositionalArguments: []vgi.ScanArg{{Value: "/tmp/vgi_nopushdown_branch.csv", Type: arrow.BinaryTypes.String}}},
		}}, true, nil
	case "multi_branch_recon":
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{
			readParquet("/tmp/vgi_recon_a_b.parquet"),
			readParquet("/tmp/vgi_recon_b_a.parquet"),
			readParquet("/tmp/vgi_recon_a_only.parquet"),
		}}, true, nil
	case "multi_branch_empty":
		// Intentionally empty — exercises the C++ "loud at attach" rejection.
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{}}, true, nil
	case "multi_branch_two_writable":
		a, b := seq(10), seq(10)
		a.Writable = true
		b.Writable = true
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{a, b}}, true, nil
	}
	return nil, false, nil
}

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

// Auth + OAuth env-var wiring lives in auth.go.
