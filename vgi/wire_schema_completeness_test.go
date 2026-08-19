// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// Every wire record below is assembled column-by-column by hand, positionally,
// against a schema that codegen owns. Nothing in the type system ties the two
// together, so a field added to the protocol lands in the generated schema and
// silently misses the builder — and the resulting failure is remote from its
// cause. The sibling SDKs have all been bitten:
//
//   - TypeScript's ScanBranch builder supplied 7 of the schema's 9 columns; the
//     missing one was a LIST, and Arrow dereferences a list's children while
//     writing, so EVERY multi-branch table in that worker died with
//     "Cannot read properties of undefined (reading 'slice')" — including
//     tables that predated the two new fields entirely.
//   - Java marked two PlanResponse fields nullable where the protocol says
//     non-null, and the client rejected the whole response as an
//     "out-of-date Apache Arrow schema".
//
// Go's failure mode for a *count or type* mismatch is at least loud —
// array.NewRecordBatch panics — but only on the code path that builds that one
// record, which is why smoke tests that never construct a fully-populated
// record miss it. The subtler drift is a field that IS wired but always writes
// null; that produces a valid batch and a client that reads back a default.
// So each case here builds a record with EVERY field populated and asserts
// three things: the emitted schema is the generated one field-for-field, the
// batch has a row, and no column came back null.

// wireRecordCase is one hand-built wire record.
//
// `origin` is the dataclass name in the generated file's "// Origin: X"
// comment. It is the join key for the coverage guard below, which reads that
// file so a record type added to the protocol cannot be silently left untested.
type wireRecordCase struct {
	origin string
	schema *arrow.Schema
	// build returns the IPC bytes for a single record with every field of the
	// generated schema populated with a non-null value.
	build func() ([]byte, error)
}

func strPtr(s string) *string { return &s }
func i64Ptr(v int64) *int64   { return &v }
func boolPtr(v bool) *bool    { return &v }

func wireRecordCases(t *testing.T) []wireRecordCase {
	t.Helper()

	columns := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	columnsIPC, err := SerializeSchema(columns)
	if err != nil {
		t.Fatalf("SerializeSchema: %v", err)
	}
	releasedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	scanArgs := []ScanArg{{Value: "s3://bucket/x.parquet", Type: arrow.BinaryTypes.String}}

	return []wireRecordCase{
		{
			origin: "CatalogInfo",
			schema: generated.CatalogInfoSchema,
			build: func() ([]byte, error) {
				return SerializeCatalogInfo(&CatalogInfo{
					Name:                  "acme",
					ImplementationVersion: strPtr("1.2.3"),
					DataVersionSpec:       strPtr(">=1.0.0"),
					AttachOptionSpecs:     [][]byte{{0x01}},
					Releases: []CatalogDataVersionRelease{{
						Version:    "1.0.0",
						ReleasedAt: &releasedAt,
						Summary:    "first release",
						NotesURL:   strPtr("https://query.farm/notes"),
					}},
					SourceURL: strPtr("https://github.com/Query-farm/vgi-go"),
				})
			},
		},
		{
			origin: "SchemaInfo",
			schema: generated.SchemaInfoSchema,
			build: func() ([]byte, error) {
				return SerializeSchemaInfo(&SchemaInfo{
					Name:                 "main",
					Comment:              "the main schema",
					Tags:                 map[string]string{"owner": "data-eng"},
					AttachOpaqueData:     []byte("attach"),
					EstimatedObjectCount: map[string]int64{"table": 7},
				})
			},
		},
		{
			origin: "TableInfo",
			schema: generated.TableInfoSchema,
			build: func() ([]byte, error) {
				return SerializeTableInfo(&TableInfo{
					Name:                     "events",
					SchemaName:               "main",
					Comment:                  "event log",
					Tags:                     map[string]string{"tier": "hot"},
					Columns:                  columns,
					NotNullConstraints:       []int32{0},
					UniqueConstraints:        [][]int32{{0}},
					CheckConstraints:         []string{"id > 0"},
					PrimaryKeyConstraints:    [][]int32{{0}},
					ForeignKeyConstraints:    [][]byte{{0x02}},
					SupportsInsert:           true,
					SupportsUpdate:           true,
					SupportsDelete:           true,
					SupportsReturning:        true,
					SupportsColumnStatistics: true,
					ScanFunction:             []byte{0x03},
					InsertFunction:           []byte{0x04},
					UpdateFunction:           []byte{0x05},
					DeleteFunction:           []byte{0x06},
					CardinalityEstimate:      i64Ptr(100),
					CardinalityMax:           i64Ptr(200),
					ColumnStatistics:         []byte{0x07},
					BindResult:               []byte{0x08},
					RequiredFilters:          [][]string{{"id"}},
				})
			},
		},
		{
			origin: "ViewInfo",
			schema: generated.ViewInfoSchema,
			build: func() ([]byte, error) {
				return SerializeViewInfo(&ViewInfo{
					Name:           "recent_events",
					SchemaName:     "main",
					Comment:        "last 7 days",
					Tags:           map[string]string{"tier": "hot"},
					Definition:     "SELECT * FROM events",
					ColumnComments: map[string]string{"id": "primary key"},
				})
			},
		},
		{
			origin: "FunctionInfo",
			schema: generated.FunctionInfoSchema,
			build: func() ([]byte, error) {
				return SerializeFunctionInfo(&FunctionInfo{
					Name:                       "scan_events",
					SchemaName:                 "main",
					FunctionType:               FunctionTypeTable,
					ArgSchema:                  columns,
					OutputSchema:               columns,
					Stability:                  StabilityVolatile,
					NullHandling:               NullHandlingDefault,
					Description:                "scan the event log",
					Comment:                    "internal",
					Tags:                       map[string]string{"tier": "hot"},
					Examples:                   []CatalogExample{{SQL: "SELECT 1", Description: "trivial", ExpectedOutput: strPtr("1")}},
					Categories:                 []string{"scan"},
					ProjectionPushdown:         boolPtr(true),
					FilterPushdown:             boolPtr(true),
					SamplingPushdown:           boolPtr(true),
					LateMaterialization:        boolPtr(true),
					SupportedExpressionFilters: []string{"="},
					OrderPreservation:          OrderPreservationPreservesOrder,
					MaxWorkers:                 4,
					SupportsBatchIndex:         true,
					SupportsSplits:             true,
					FiltersExactlyApplied:      true,
					SupportsPositions:          true,
					SplitTokenTTLSeconds:       i64Ptr(300),
					PartitionKind:              PartitionKindSingleValuePartitions,
					OrderDependent:             OrderDependenceDependent,
					DistinctDependent:          DistinctDependenceDependent,
					SupportsWindow:             true,
					StreamingPartitioned:       true,
					HasFinalize:                true,
					SourceOrderDependent:       true,
					SinkOrderDependent:         true,
					RequiresInputBatchIndex:    true,
					InputFromArgs:              true,
					RequiredSettings:           []string{"s3_region"},
					RequiredSecrets:            []SecretRequirement{{SecretType: "s3", SecretName: "prod", Scope: "s3://bucket"}},
				})
			},
		},
		{
			origin: "MacroInfo",
			schema: generated.MacroInfoSchema,
			build: func() ([]byte, error) {
				return SerializeMacroInfo(&MacroInfo{
					Name:                   "add_one",
					SchemaName:             "main",
					Comment:                "adds one",
					Tags:                   map[string]string{"tier": "hot"},
					MacroType:              MacroTypeScalar,
					Parameters:             []string{"x"},
					ParameterDefaultValues: []byte{0x09},
					Definition:             "x + 1",
					ArgumentsSchema:        columnsIPC,
				})
			},
		},
		{
			origin: "CopyFromFormatInfo",
			schema: generated.CopyFromFormatInfoSchema,
			build: func() ([]byte, error) {
				return SerializeCopyFromFormatInfo(copyFromFormatRecord{
					formatName:  "acme_csv",
					handler:     "read_acme_csv",
					comment:     "internal",
					direction:   "from",
					description: "Acme's CSV dialect",
					tags:        map[string]string{"tier": "hot"},
					argSpecs:    []ArgSpec{{Name: "delimiter", Position: -1, ArrowType: "varchar", Doc: "field separator"}},
					ordered:     true,
				})
			},
		},
		{
			origin: "ScanFunctionResult",
			schema: generated.ScanFunctionResultSchema,
			build: func() ([]byte, error) {
				return SerializeScanFunctionResult(&ScanFunctionResult{
					FunctionName:        "read_parquet",
					PositionalArguments: scanArgs,
					RequiredExtensions:  []string{"parquet"},
				})
			},
		},
		{
			origin: "ScanBranch",
			schema: generated.ScanBranchSchema,
			build: func() ([]byte, error) {
				return SerializeScanBranch(&ScanBranch{
					FunctionName:        "read_parquet",
					PositionalArguments: scanArgs,
					BranchFilter:        strPtr("ts >= '2026-01-01'"),
					Writable:            true,
					SourceCatalog:       strPtr("lake"),
					SourceSchema:        strPtr("main"),
					SourceTable:         strPtr("events"),
					FormatName:          strPtr("acme_csv"),
					FormatLocations:     []string{"s3://bucket/a.csv"},
				})
			},
		},
		{
			origin: "ScanSplit",
			schema: generated.ScanSplitSchema,
			build: func() ([]byte, error) {
				bounds := []byte{0x0a}
				stats := []byte{0x0b}
				start := []byte{0x0c}
				end := []byte{0x0d}
				ids := []int64{1, 2}
				return SerializeScanSplit(&ScanSplit{
					Payload:          []byte("payload"),
					Token:            []byte("token"),
					EstimatedRows:    i64Ptr(10),
					RowsExact:        true,
					EstimatedBytes:   i64Ptr(1024),
					PartitionBounds:  &bounds,
					ColumnStatistics: &stats,
					LocationIDs:      &ids,
					StartPosition:    &start,
					EndPosition:      &end,
				})
			},
		},
		{
			origin: "AttachCatalogInfo",
			schema: generated.AttachCatalogInfoSchema,
			build: func() ([]byte, error) {
				return SerializeAttachCatalogInfo(AttachCatalogInfo{
					Alias:     "acme_lake",
					Target:    "ducklake:sqlite:/data/meta.sqlite",
					DBType:    "ducklake",
					Options:   map[string]string{"DATA_PATH": "/data/"},
					Hidden:    true,
					Required:  true,
					SecretRef: "pg",
				})
			},
		},
	}
}

// notBuiltByGo lists generated record schemas this SDK never assembles, with
// the reason. Keeping them here rather than ignoring unknown origins is the
// point of the coverage guard: adding a record to the protocol forces a
// deliberate choice between covering it and writing down why not.
var notBuiltByGo = map[string]string{
	"IndexInfo": "vgi-go exposes no index API; nothing constructs an IndexInfo record",
	"ScanBranchesResult": "assembled by vgi-rpc-go's struct-tag reflection over " +
		"TableScanBranchesGetResponseWire, not hand-built here",
}

func TestWireRecordSchemasMatchGenerated(t *testing.T) {
	for _, tc := range wireRecordCases(t) {
		t.Run(tc.origin, func(t *testing.T) {
			data, err := tc.build()
			if err != nil {
				t.Fatalf("%s: building the record failed: %v", tc.origin, err)
			}

			reader, err := ipc.NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("%s: reading back the IPC stream failed: %v", tc.origin, err)
			}
			defer reader.Release()

			assertSchemaMatches(t, tc.origin, reader.Schema(), tc.schema)

			if !reader.Next() {
				t.Fatalf("%s: the IPC stream carried no record batch", tc.origin)
			}
			batch := reader.RecordBatch()
			if batch.NumRows() != 1 {
				t.Fatalf("%s: expected a single-row record, got %d rows", tc.origin, batch.NumRows())
			}
			for i, col := range batch.Columns() {
				if col.IsNull(0) {
					// The builder has a column for this field but never wrote
					// the value into it. On the wire that is indistinguishable
					// from "the worker has nothing to say", so the client reads
					// a default and the feature quietly does not exist.
					t.Errorf("%s: column %q is null even though the test record populates every field — "+
						"the builder is not wiring this field",
						tc.origin, tc.schema.Field(i).Name)
				}
			}
		})
	}
}

// assertSchemaMatches reports the differing field by name rather than dumping
// two whole schemas: a 39-field FunctionInfo diff is unreadable, and the only
// thing the reader needs is which field moved. It also catches a builder that
// stops handing the generated schema to the IPC writer and spells out a local
// literal instead — the drift that let four SDKs disagree about which ScanSplit
// columns were binary and which were large_binary.
func assertSchemaMatches(t *testing.T, origin string, got, want *arrow.Schema) {
	t.Helper()

	gotNames := fieldNames(got)
	wantNames := fieldNames(want)
	if len(gotNames) != len(wantNames) {
		t.Fatalf("%s: emitted %d columns, generated schema declares %d\n emitted: %v\ngenerated: %v",
			origin, len(gotNames), len(wantNames), gotNames, wantNames)
	}
	for i := range wantNames {
		gf, wf := got.Field(i), want.Field(i)
		if gf.Name != wf.Name {
			t.Fatalf("%s: column %d is %q, generated schema declares %q (fields are positional on the wire)\n"+
				" emitted: %v\ngenerated: %v", origin, i, gf.Name, wf.Name, gotNames, wantNames)
		}
		if !arrow.TypeEqual(gf.Type, wf.Type) {
			t.Errorf("%s: column %q has type %v, generated schema declares %v",
				origin, wf.Name, gf.Type, wf.Type)
		}
		if gf.Nullable != wf.Nullable {
			t.Errorf("%s: column %q is nullable=%v, generated schema declares nullable=%v",
				origin, wf.Name, gf.Nullable, wf.Nullable)
		}
	}
}

func fieldNames(s *arrow.Schema) []string {
	names := make([]string, 0, s.NumFields())
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	return names
}

// generatedOriginPattern matches the "// Origin: CatalogInfo" comments the
// codegen emits above each dataclass-derived schema. Method-derived schemas
// read "// Origin: method 'x' result" and are excluded — those are request and
// response envelopes that vgi-rpc-go reflects from struct tags, not records
// this package builds by hand.
var generatedOriginPattern = regexp.MustCompile(`(?m)^// Origin: ([A-Za-z0-9_]+)$`)

// A new record type FAILS here as unlisted rather than being picked up
// automatically. Auto-coverage would need a generic fully-populated builder per
// record, and there isn't one — the builders take Go structs, not schemas — so
// "automatic" would in practice mean "skipped", which recreates the gap this
// test exists to close.
func TestEveryGeneratedRecordSchemaIsCovered(t *testing.T) {
	const generatedPath = "generated/protocol_schemas.go"

	src, err := os.ReadFile(generatedPath)
	if err != nil {
		t.Fatalf("reading %s: %v", generatedPath, err)
	}
	matches := generatedOriginPattern.FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatalf("found no \"// Origin: X\" markers in %s — the generator's comment format changed "+
			"and this coverage guard is now silently vacuous", generatedPath)
	}

	covered := map[string]bool{}
	for _, tc := range wireRecordCases(t) {
		covered[tc.origin] = true
	}

	var missing []string
	for _, m := range matches {
		origin := m[1]
		if covered[origin] {
			continue
		}
		if _, excused := notBuiltByGo[origin]; excused {
			continue
		}
		missing = append(missing, origin)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("%v appear in %s but are not exercised by TestWireRecordSchemasMatchGenerated. "+
			"Add a wireRecordCase that builds each one with every field populated, or add it to "+
			"notBuiltByGo with the reason this SDK never constructs it.", missing, generatedPath)
	}

	// The excuse list has to stay honest too: an entry for a record the
	// generator no longer emits reads as coverage that does not exist.
	declared := map[string]bool{}
	for _, m := range matches {
		declared[m[1]] = true
	}
	for origin := range notBuiltByGo {
		if !declared[origin] {
			t.Errorf("notBuiltByGo lists %q, which %s no longer declares — drop the entry",
				origin, generatedPath)
		}
	}
	for origin := range notBuiltByGo {
		if covered[origin] {
			t.Errorf("%q is both covered by a wireRecordCase and excused in notBuiltByGo", origin)
		}
	}
}
