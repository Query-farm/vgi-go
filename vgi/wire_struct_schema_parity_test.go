//go:build vgirpc_schema_parity

// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// BUILD-TAGGED until vgi-rpc-go ships vgirpc.SchemaForStruct.
//
// The check needs the SAME schema derivation the serializer uses, and that was
// unexported — which is exactly why this drift went unmeasured. The export
// exists in the sibling repo now; this file compiles once vgi-rpc-go releases it
// and go.mod's `require` moves to that version. Until then it is tagged out so
// `go build ./...` and `go test ./...` stay green rather than committing a file
// that does not compile.
//
// To run against a local sibling checkout:
//   go test ./vgi/ -tags vgirpc_schema_parity
// (with a `replace github.com/Query-farm/vgi-rpc-go => ../vgi-rpc-go` in go.mod)
//
// Verified passing that way when written; drop the tag at the version bump.

package vgi

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// Every response this worker sends is described TWICE and neither description
// knows about the other: once by the `vgirpc:` struct tags vgi-rpc-go reflects
// at serialize time, and once by the protocol codegen that emits
// generated/protocol_schemas.go. When they disagree, the worker sends a shape
// no client expects.
//
// That is not hypothetical, and the failure is remote from its cause. Java's
// PlanResponse marked two fields nullable where the protocol says non-null; the
// C++ client rejected the ENTIRE response as an "out-of-date Apache Arrow
// schema" and named the field only because it happened to print a diff. The
// same class of drift on this side would be a struct tag typo, a *T where the
// protocol says non-null, or an int64 where the wire says int32 — none of which
// any existing test would notice, because wire_schema_completeness_test.go
// explicitly excludes method-derived schemas as "reflected from struct tags".
//
// This closes that exclusion. vgirpc.SchemaForStruct is the same derivation the
// serializer uses, so a row passing here means the bytes on the wire match what
// codegen says they should be.

type wireStructCase struct {
	// The RPC method whose result record this struct carries.
	method string
	// A zero value of the response struct; only its type is used.
	value any
	// The schema codegen emits for that method's result.
	schema *arrow.Schema
}

var wireStructCases = []wireStructCase{
	{method: "aggregate_bind", value: AggregateBindResponseWire{}, schema: generated.AggregateBindResultSchema},
	{method: "aggregate_combine", value: AggregateCombineResponseWire{}, schema: generated.AggregateCombineResultSchema},
	{method: "aggregate_destructor", value: AggregateDestructorResponseWire{}, schema: generated.AggregateDestructorResultSchema},
	{method: "aggregate_finalize", value: AggregateFinalizeResponseWire{}, schema: generated.AggregateFinalizeResultSchema},
	{method: "aggregate_streaming_chunk", value: AggregateStreamingChunkResponseWire{}, schema: generated.AggregateStreamingChunkResultSchema},
	{method: "aggregate_streaming_close", value: AggregateStreamingCloseResponseWire{}, schema: generated.AggregateStreamingCloseResultSchema},
	{method: "aggregate_streaming_open", value: AggregateStreamingOpenResponseWire{}, schema: generated.AggregateStreamingOpenResultSchema},
	{method: "aggregate_update", value: AggregateUpdateResponseWire{}, schema: generated.AggregateUpdateResultSchema},
	{method: "aggregate_window", value: AggregateWindowResponseWire{}, schema: generated.AggregateWindowResultSchema},
	{method: "aggregate_window_batch", value: AggregateWindowBatchResponseWire{}, schema: generated.AggregateWindowBatchResultSchema},
	{method: "aggregate_window_destructor", value: AggregateWindowDestructorResponseWire{}, schema: generated.AggregateWindowDestructorResultSchema},
	{method: "aggregate_window_init", value: AggregateWindowInitResponseWire{}, schema: generated.AggregateWindowInitResultSchema},
	{method: "bind", value: BindResponseWire{}, schema: generated.BindResultSchema},
	{method: "catalog_attach", value: CatalogAttachResultWire{}, schema: generated.CatalogAttachResultSchema},
	{method: "catalog_catalogs", value: CatalogsResponseWire{}, schema: generated.CatalogCatalogsResultSchema},
	{method: "catalog_copy_from_formats", value: ItemsResponseWire{}, schema: generated.CatalogCopyFromFormatsResultSchema},
	{method: "catalog_macro_get", value: ItemsResponseWire{}, schema: generated.CatalogMacroGetResultSchema},
	{method: "catalog_schema_contents_functions", value: ItemsResponseWire{}, schema: generated.CatalogSchemaContentsFunctionsResultSchema},
	{method: "catalog_schema_contents_macros", value: ItemsResponseWire{}, schema: generated.CatalogSchemaContentsMacrosResultSchema},
	{method: "catalog_schema_contents_tables", value: ItemsResponseWire{}, schema: generated.CatalogSchemaContentsTablesResultSchema},
	{method: "catalog_schema_contents_views", value: ItemsResponseWire{}, schema: generated.CatalogSchemaContentsViewsResultSchema},
	{method: "catalog_schema_get", value: ItemsResponseWire{}, schema: generated.CatalogSchemaGetResultSchema},
	{method: "catalog_schemas", value: ItemsResponseWire{}, schema: generated.CatalogSchemasResultSchema},
	{method: "catalog_table_get", value: ItemsResponseWire{}, schema: generated.CatalogTableGetResultSchema},
	{method: "catalog_transaction_begin", value: TransactionBeginResponseWire{}, schema: generated.CatalogTransactionBeginResultSchema},
	{method: "catalog_version", value: CatalogVersionResponseWire{}, schema: generated.CatalogVersionResultSchema},
	{method: "catalog_view_get", value: ItemsResponseWire{}, schema: generated.CatalogViewGetResultSchema},
	{method: "table_buffering_combine", value: TableBufferingCombineResponseWire{}, schema: generated.TableBufferingCombineResultSchema},
	{method: "table_buffering_destructor", value: TableBufferingDestructorResponseWire{}, schema: generated.TableBufferingDestructorResultSchema},
	{method: "table_buffering_process", value: TableBufferingProcessResponseWire{}, schema: generated.TableBufferingProcessResultSchema},
	{method: "table_function_cardinality", value: TableCardinality{}, schema: generated.TableFunctionCardinalityResultSchema},
	{method: "table_function_dynamic_to_string", value: TableFunctionDynamicToStringResponseWire{}, schema: generated.TableFunctionDynamicToStringResultSchema},
	{method: "table_function_plan", value: PlanResponseWire{}, schema: generated.TableFunctionPlanResultSchema},
}

// Methods whose result has a generated schema but no Go response struct to
// compare, with the reason. Being explicit is the point: an unexplained gap
// here is indistinguishable from an oversight.
var wireStructExcused = map[string]string{
	"catalog_index_get":               "not implemented by this SDK — no handler is registered",
	"catalog_schema_contents_indexes": "not implemented by this SDK — no handler is registered",
}

func TestWireStructSchemasMatchGenerated(t *testing.T) {
	for _, c := range wireStructCases {
		t.Run(c.method, func(t *testing.T) {
			got, err := vgirpc.SchemaForStruct(reflect.TypeOf(c.value))
			if err != nil {
				t.Fatalf("%s: deriving schema from struct tags: %v", c.method, err)
			}
			compareSchemas(t, c.method, got, c.schema)
		})
	}
}

// compareSchemas reports the FIRST difference in terms of the field, rather
// than dumping two schemas and leaving the reader to diff them — the whole
// value of this test is naming the field that drifted.
func compareSchemas(t *testing.T, method string, got, want *arrow.Schema) {
	t.Helper()
	gotNames, wantNames := wireStructFieldNames(got), wireStructFieldNames(want)
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("%s: column names/order: struct tags give %v but codegen says %v",
			method, gotNames, wantNames)
	}
	for i, wf := range want.Fields() {
		gf := got.Field(i)
		if !arrow.TypeEqual(gf.Type, wf.Type) {
			t.Errorf("%s.%s: struct tags give type %s but codegen says %s",
				method, wf.Name, gf.Type, wf.Type)
		}
		if gf.Nullable != wf.Nullable {
			// The Java bug in miniature: nullable-vs-not is invisible until a
			// client validates the schema and refuses the whole response.
			t.Errorf("%s.%s: struct tags say nullable=%v but codegen says nullable=%v",
				method, wf.Name, gf.Nullable, wf.Nullable)
		}
	}
}

func wireStructFieldNames(s *arrow.Schema) []string {
	names := make([]string, 0, len(s.Fields()))
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	return names
}

// methodResultOriginPattern matches the "// Origin: method 'x' result" comments
// codegen emits. Reading the generated file is what makes the guard below
// self-maintaining: a method added to the protocol appears here without anyone
// remembering to update this test.
var methodResultOriginPattern = regexp.MustCompile(`(?m)^// Origin: method '([a-z0-9_]+)' result$`)

func TestEveryMethodResultIsClassified(t *testing.T) {
	f, err := os.Open("generated/protocol_schemas.go")
	if err != nil {
		t.Fatalf("open generated schemas: %v", err)
	}
	defer f.Close()
	src, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read generated schemas: %v", err)
	}

	covered := map[string]bool{}
	for _, c := range wireStructCases {
		covered[c.method] = true
	}

	var unclassified []string
	seen := map[string]bool{}
	for _, m := range methodResultOriginPattern.FindAllStringSubmatch(string(src), -1) {
		method := m[1]
		if seen[method] {
			continue
		}
		seen[method] = true
		if covered[method] {
			if _, excused := wireStructExcused[method]; excused {
				t.Errorf("%s is both covered and excused — remove the excuse", method)
			}
			continue
		}
		if _, excused := wireStructExcused[method]; excused {
			continue
		}
		unclassified = append(unclassified, method)
	}
	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Fatalf("method result(s) with a generated schema and no parity check: %s\n"+
			"add each to wireStructCases, or to wireStructExcused with a reason",
			strings.Join(unclassified, ", "))
	}

	for method := range wireStructExcused {
		if !seen[method] {
			t.Errorf("%s is excused but codegen emits no result schema for it — stale entry",
				method)
		}
	}
	_ = fmt.Sprint()
}
