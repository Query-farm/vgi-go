// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func filterV2Batch(t *testing.T, document string, fields []arrow.Field, columns []arrow.Array) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	strings := array.NewStringBuilder(mem)
	strings.Append(document)
	filterSpec := strings.NewArray()
	strings.Release()
	allFields := append([]arrow.Field{{Name: "filter_spec", Type: arrow.BinaryTypes.String}}, fields...)
	allColumns := append([]arrow.Array{filterSpec}, columns...)
	metadata := arrow.NewMetadata(
		[]string{"vgi_filter_encoding", "vgi_filter_version", "vgi_evaluation_context"},
		[]string{"vgi.filters.v2", "2", "vgi.none.v1"},
	)
	batch := array.NewRecordBatch(arrow.NewSchema(allFields, &metadata), allColumns, 1)
	filterSpec.Release()
	return batch
}

func int64OutputSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
}

func TestFilterV2RequiredComparison(t *testing.T) {
	mem := memory.NewGoAllocator()
	values := array.NewInt64Builder(mem)
	values.Append(2)
	literal := values.NewArray()
	values.Release()
	defer literal.Release()
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"comparison","op":"gt","left":{"node":"column_ref","column_index":0,"column_name":"n"},"right":{"node":"literal","value_ref":0}}}]}`
	encoded := filterV2Batch(t, document, []arrow.Field{{Name: "value_0", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, []arrow.Array{literal})
	defer encoded.Release()
	filters, err := DeserializeFiltersWithSchema(encoded, int64OutputSchema(), nil)
	if err != nil {
		t.Fatal(err)
	}
	dataBuilder := array.NewInt64Builder(mem)
	dataBuilder.AppendValues([]int64{1, 3, 2}, nil)
	data := dataBuilder.NewArray()
	dataBuilder.Release()
	defer data.Release()
	input := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{data}, 3)
	defer input.Release()
	filtered, err := filters.Apply(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer filtered.Release()
	if filtered.NumRows() != 1 {
		t.Fatalf("got %d rows", filtered.NumRows())
	}
}

func TestFilterV2EmptyInRendersBooleanConstants(t *testing.T) {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	input := array.NewRecordBatch(int64OutputSchema(), []arrow.Array{values}, 0)
	defer input.Release()

	for _, test := range []struct {
		negated bool
		want    string
	}{
		{negated: false, want: "FALSE"},
		{negated: true, want: "TRUE"},
	} {
		expression := &v2Expr{
			Node:       "in",
			Expression: &v2Expr{Node: "column_ref", ColumnIndex: 0, ColumnName: "n"},
			Set:        &v2Set{Values: values},
			Negated:    test.negated,
		}
		got, err := expression.sql(input)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("empty IN negated=%t: got %q, want %q", test.negated, got, test.want)
		}
	}
}

func TestFilterV2ExternalInAndDeltaTombstone(t *testing.T) {
	mem := memory.NewGoAllocator()
	keysBuilder := array.NewInt64Builder(mem)
	keysBuilder.AppendValues([]int64{2, 4}, nil)
	keys := keysBuilder.NewArray()
	keysBuilder.Release()
	defer keys.Release()
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"advisory","source":"join","expression":{"node":"in","expression":{"node":"column_ref","column_index":0,"column_name":"n"},"set":{"kind":"external","batch_index":0,"column_index":0,"column_name":"n"},"negated":false}}]}`
	encoded := filterV2Batch(t, document, nil, nil)
	defer encoded.Release()
	keyBatch := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{keys}, int64(keys.Len()))
	defer keyBatch.Release()
	filters, err := DeserializeFiltersWithSchema(encoded, int64OutputSchema(), []arrow.RecordBatch{keyBatch})
	if err != nil {
		t.Fatal(err)
	}
	remove := filterV2Batch(t, `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{"operation":"remove","id":"p","revision":1}]}`, nil, nil)
	defer remove.Release()
	if err := filters.ApplyDelta(remove); err != nil {
		t.Fatal(err)
	}
	stale := filterV2Batch(t, `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{"operation":"upsert","id":"p","revision":1,"mode":"advisory","source":"join","expression":{"node":"literal","value_ref":999}}]}`, nil, nil)
	defer stale.Release()
	if err := filters.ApplyDelta(stale); err != nil {
		t.Fatal(err)
	}
	if len(filters.Filters) != 0 {
		t.Fatalf("stale update resurrected predicate")
	}
	malformed := filterV2Batch(t, `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{"operation":"upsert","id":"p","revision":1,"mode":"required","source":"join","expression":{},"extra":true}]}`, nil, nil)
	defer malformed.Release()
	if err := filters.ApplyDelta(malformed); err == nil {
		t.Fatal("accepted malformed stale update")
	}
}

func TestFilterV2ExternalInUsesAuthoritativeBatchAndColumn(t *testing.T) {
	mem := memory.NewGoAllocator()
	aBuilder := array.NewInt64Builder(mem)
	aBuilder.AppendValues([]int64{9, 10}, nil)
	a := aBuilder.NewArray()
	aBuilder.Release()
	defer a.Release()
	bBuilder := array.NewInt64Builder(mem)
	bBuilder.AppendValues([]int64{2, 4}, nil)
	b := bBuilder.NewArray()
	bBuilder.Release()
	defer b.Release()
	first := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "ignored", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{a}, 2)
	defer first.Release()
	second := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "also_ignored", Type: arrow.PrimitiveTypes.Int64}, {Name: "keys", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{a, b}, 2)
	defer second.Release()
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"join","expression":{"node":"in","expression":{"node":"column_ref","column_index":0,"column_name":"n"},"set":{"kind":"external","batch_index":1,"column_index":1,"column_name":"keys"},"negated":false}}]}`
	encoded := filterV2Batch(t, document, nil, nil)
	defer encoded.Release()
	filters, err := DeserializeFiltersWithSchema(encoded, int64OutputSchema(), []arrow.RecordBatch{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if filters.v2Predicates[0].Expr.Set.Values.Len() != 2 {
		t.Fatal("wrong external set")
	}
}

func TestFilterV2RejectsV1AndNestedRuntime(t *testing.T) {
	v1 := filterV2Batch(t, `[]`, nil, nil)
	defer v1.Release()
	if _, err := DeserializeFilters(v1); err == nil {
		t.Fatal("accepted v1 document")
	}
	mem := memory.NewGoAllocator()
	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	builder.Append([]byte("x"))
	artifact := builder.NewArray()
	builder.Release()
	defer artifact.Release()
	nested := filterV2Batch(t, `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"advisory","source":"join","expression":{"node":"not","expression":{"node":"runtime_filter","algorithm":{"Namespace":"duckdb.runtime_filter","Name":"bloom","Version":1},"input":{"node":"column_ref","column_index":0,"column_name":"n"},"artifact_ref":0,"null_handling":"pass"}}}]}`, []arrow.Field{{Name: "artifact_0", Type: arrow.BinaryTypes.Binary, Nullable: true}}, []arrow.Array{artifact})
	defer nested.Release()
	if _, err := DeserializeFiltersWithSchema(nested, int64OutputSchema(), nil); err == nil {
		t.Fatal("accepted nested runtime filter")
	}
	badNullHandling := filterV2Batch(t, `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"advisory","source":"join","expression":{"node":"runtime_filter","algorithm":{"namespace":"duckdb.runtime_filter","name":"bloom","version":1},"input":{"node":"column_ref","column_index":0,"column_name":"n"},"artifact_ref":0,"null_handling":"unknown"}}]}`, []arrow.Field{{Name: "artifact_0", Type: arrow.BinaryTypes.Binary, Nullable: true}}, []arrow.Array{artifact})
	defer badNullHandling.Release()
	if _, err := DeserializeFiltersWithSchema(badNullHandling, int64OutputSchema(), nil); err == nil || !strings.Contains(err.Error(), "null_handling") {
		t.Fatalf("invalid runtime null handling was not rejected: %v", err)
	}
}

func TestFilterCapabilityAdvertisementDefaultsAndGuards(t *testing.T) {
	got := resolvedFilterSemanticProfiles(FunctionMetadata{FilterPushdown: true})
	if len(got) != 1 || got[0] != "vgi.duckdb.standard.v1" {
		t.Fatalf("unexpected default profiles: %v", got)
	}
	if got := resolvedFilterSemanticProfiles(FunctionMetadata{}); len(got) != 0 {
		t.Fatalf("non-filtering function advertised profiles: %v", got)
	}
	fingerprint := "unregistered"
	_, err := SerializeFunctionInfo(&FunctionInfo{
		FilterEvaluationContexts: []EvaluationContextCapability{{
			Profile:             "vgi.duckdb.session.v1",
			ProviderFingerprint: &fingerprint,
		}},
	})
	if err == nil {
		t.Fatal("accepted unsupported evaluation-context advertisement")
	}
}

func TestFilterV2BindsAuthoritativeNestedSchema(t *testing.T) {
	inner := arrow.StructOf(arrow.Field{Name: "leaf", Type: arrow.PrimitiveTypes.Int64})
	outer := arrow.StructOf(arrow.Field{Name: "inner", Type: inner})
	schema := arrow.NewSchema([]arrow.Field{{Name: "root", Type: outer}}, nil)
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"is_null","expression":{"node":"field_ref","expression":{"node":"field_ref","expression":{"node":"column_ref","column_index":0,"column_name":"root"},"field_index":0,"field_name":"inner"},"field_index":0,"field_name":"leaf"},"negated":false}}]}`
	encoded := filterV2Batch(t, document, nil, nil)
	defer encoded.Release()
	if _, err := DeserializeFiltersWithSchema(encoded, schema, nil); err != nil {
		t.Fatalf("valid nested path rejected: %v", err)
	}
	bad := strings.Replace(document, `"field_name":"leaf"`, `"field_name":"wrong"`, 1)
	badBatch := filterV2Batch(t, bad, nil, nil)
	defer badBatch.Release()
	if _, err := DeserializeFiltersWithSchema(badBatch, schema, nil); err == nil {
		t.Fatal("accepted mismatched authoritative nested field")
	}
	if _, err := DeserializeFilters(encoded); err == nil {
		t.Fatal("accepted nonempty v2 snapshot without authoritative schema")
	}
}

func TestFilterV2ProcessValidationUsesUnprojectedBindSchema(t *testing.T) {
	mem := memory.NewGoAllocator()
	values := array.NewStringBuilder(mem)
	values.Append("row_5")
	literal := values.NewArray()
	values.Release()
	defer literal.Release()
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"comparison","op":"eq","left":{"node":"column_ref","column_index":1,"column_name":"s"},"right":{"node":"literal","value_ref":0}}}]}`
	encoded := filterV2Batch(t, document, []arrow.Field{{Name: "value_0", Type: arrow.BinaryTypes.String, Nullable: true}}, []arrow.Array{literal})
	defer encoded.Release()
	bindSchema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
	}, nil)
	projectedSchema := ProjectSchema([]int32{1, 0, 2}, bindSchema)
	if _, err := DeserializeFiltersWithSchema(encoded, projectedSchema, nil); err == nil {
		t.Fatal("projected schema unexpectedly accepted a bind-schema column_ref")
	}
	params := &ProcessParams{
		OutputSchema:     projectedSchema,
		BindOutputSchema: bindSchema,
		PushdownFilters:  encoded,
	}
	if _, err := deserializeProcessFiltersForMetadata(params, FunctionMetadata{FilterPushdown: true}); err != nil {
		t.Fatalf("bind-schema column_ref rejected after output projection reordered fields: %v", err)
	}
}

func TestFilterV2ValidatesRootTypesArityAndContext(t *testing.T) {
	mem := memory.NewGoAllocator()
	ints := array.NewInt64Builder(mem)
	ints.Append(2)
	intValue := ints.NewArray()
	ints.Release()
	defer intValue.Release()

	nonBoolean := filterV2Batch(t,
		`{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"literal","value_ref":0}}]}`,
		[]arrow.Field{{Name: "value_0", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, []arrow.Array{intValue})
	defer nonBoolean.Release()
	if _, err := DeserializeFiltersWithSchema(nonBoolean, int64OutputSchema(), nil); err == nil || !strings.Contains(err.Error(), "BOOLEAN") {
		t.Fatalf("non-BOOLEAN predicate root was not rejected: %v", err)
	}

	divide := filterV2Batch(t,
		`{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"comparison","op":"gt","left":{"node":"arithmetic","op":"divide","left":{"node":"column_ref","column_index":0,"column_name":"n"},"right":{"node":"literal","value_ref":0}},"right":{"node":"literal","value_ref":0}}}]}`,
		[]arrow.Field{{Name: "value_0", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, []arrow.Array{intValue})
	defer divide.Release()
	if _, err := DeserializeFiltersWithSchema(divide, int64OutputSchema(), nil); err == nil || !strings.Contains(err.Error(), "session") {
		t.Fatalf("context-dependent division under vgi.none.v1 was not rejected: %v", err)
	}

	badCall := filterV2Batch(t,
		`{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"call","function":"starts_with","arguments":[{"node":"column_ref","column_index":0,"column_name":"n"}]}}]}`,
		nil, nil)
	defer badCall.Release()
	if _, err := DeserializeFiltersWithSchema(badCall, int64OutputSchema(), nil); err == nil || !strings.Contains(err.Error(), "exactly two") {
		t.Fatalf("bad call arity was not rejected: %v", err)
	}
}

func TestFilterV2NoneAllowsBinaryStringsButRejectsContextualCast(t *testing.T) {
	mem := memory.NewGoAllocator()
	stringsBuilder := array.NewStringBuilder(mem)
	stringsBuilder.Append("a")
	stringValue := stringsBuilder.NewArray()
	stringsBuilder.Release()
	defer stringValue.Release()
	stringSchema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil)

	binaryComparison := filterV2Batch(t,
		`{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"comparison","op":"eq","left":{"node":"column_ref","column_index":0,"column_name":"s"},"right":{"node":"literal","value_ref":0}}}]}`,
		[]arrow.Field{{Name: "value_0", Type: arrow.BinaryTypes.String, Nullable: true}}, []arrow.Array{stringValue})
	defer binaryComparison.Release()
	if _, err := DeserializeFiltersWithSchema(binaryComparison, stringSchema, nil); err != nil {
		t.Fatalf("binary string comparison under vgi.none.v1 was rejected: %v", err)
	}

	types := array.NewDate32Builder(mem)
	types.AppendNull()
	typeValue := types.NewArray()
	types.Release()
	defer typeValue.Release()
	contextualCast := filterV2Batch(t,
		`{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"is_null","expression":{"node":"cast","expression":{"node":"column_ref","column_index":0,"column_name":"s"},"type_ref":0},"negated":false}}]}`,
		[]arrow.Field{{Name: "type_0", Type: arrow.FixedWidthTypes.Date32, Nullable: true}}, []arrow.Array{typeValue})
	defer contextualCast.Release()
	if _, err := DeserializeFiltersWithSchema(contextualCast, stringSchema, nil); err == nil || !strings.Contains(err.Error(), "session") {
		t.Fatalf("context-dependent cast under vgi.none.v1 was not rejected: %v", err)
	}
}

func TestFilterV2SpatialIdentityIsCapabilityGated(t *testing.T) {
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"advisory","source":"query","expression":{"node":"call","function":{"namespace":"duckdb.spatial","name":"intersects_extent","version":1},"arguments":[{"node":"column_ref","column_index":0,"column_name":"n"},{"node":"column_ref","column_index":0,"column_name":"n"}]}}]}`
	encoded := filterV2Batch(t, document, nil, nil)
	defer encoded.Release()
	if _, err := DeserializeFiltersWithSchema(encoded, int64OutputSchema(), nil); err == nil || !strings.Contains(err.Error(), "not advertised") {
		t.Fatalf("unadvertised spatial identity was accepted: %v", err)
	}
	meta := FunctionMetadata{AdditionalFilterFunctions: []FilterFunctionCapability{{Namespace: "duckdb.spatial", Name: "intersects_extent", Version: 1}}}
	if _, err := deserializeFiltersForMetadata(encoded, nil, nil, meta, int64OutputSchema()); err != nil {
		t.Fatalf("advertised canonical spatial identity was rejected: %v", err)
	}
	unknown := strings.Replace(document, `"name":"intersects_extent"`, `"name":"unknown"`, 1)
	unknownBatch := filterV2Batch(t, unknown, nil, nil)
	defer unknownBatch.Release()
	if _, err := deserializeFiltersForMetadata(unknownBatch, nil, nil, meta, int64OutputSchema()); err == nil || !strings.Contains(err.Error(), "unknown extension") {
		t.Fatalf("unknown extension identity was accepted: %v", err)
	}
}

// ---------------------------------------------------------------------------
// A BOOLEAN column is a predicate on its own
//
// `WHERE flag` / `WHERE NOT flag` is idiomatic SQL, and DuckDB pushes it down
// as a bare `column_ref` rather than rewriting it to `flag = true`. The
// decoder accepts the shape already — its boolean gate asks for the resolved
// type — but without a projection onto the equality leaf it reaches the
// ergonomic helpers as an opaque v2FilterView, whose ToSQL falls through to
// "1=1": a silently dropped predicate, which is a wrong answer rather than a
// slow one because DuckDB does not re-apply what it pushed into a table
// function. See vgi-python 0.36.2.
// ---------------------------------------------------------------------------

func boolOutputSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
}

// boolInputBatch is flag = TRUE, FALSE, NULL, TRUE with n = 1..4.
func boolInputBatch(t *testing.T) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	flags := array.NewBooleanBuilder(mem)
	flags.AppendValues([]bool{true, false, false, true}, []bool{true, true, false, true})
	flagArray := flags.NewArray()
	flags.Release()
	ints := array.NewInt64Builder(mem)
	ints.AppendValues([]int64{1, 2, 3, 4}, nil)
	intArray := ints.NewArray()
	ints.Release()
	batch := array.NewRecordBatch(boolOutputSchema(), []arrow.Array{flagArray, intArray}, 4)
	flagArray.Release()
	intArray.Release()
	return batch
}

const boolFlagRef = `{"node":"column_ref","column_index":0,"column_name":"flag"}`

func boolPredicateFilters(t *testing.T, expression string) *PushdownFilters {
	t.Helper()
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":` + expression + `}]}`
	encoded := filterV2Batch(t, document, nil, nil)
	defer encoded.Release()
	filters, err := DeserializeFiltersWithSchema(encoded, boolOutputSchema(), nil)
	if err != nil {
		t.Fatalf("deserialize %s: %v", expression, err)
	}
	return filters
}

// remainingInts returns the n column of whatever survived Apply.
func remainingInts(t *testing.T, filters *PushdownFilters, input arrow.RecordBatch) []int64 {
	t.Helper()
	filtered, err := filters.Apply(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer filtered.Release()
	column := filtered.Column(1).(*array.Int64)
	out := make([]int64, column.Len())
	for i := range out {
		out[i] = column.Value(i)
	}
	return out
}

func TestFilterV2BareBooleanColumnIsAPredicateRoot(t *testing.T) {
	input := boolInputBatch(t)
	defer input.Release()

	// A NULL predicate is not satisfied, so the NULL row drops — exactly the
	// rows `flag = true` keeps, which is what makes the rewrite below a
	// projection rather than a change of meaning.
	positive := boolPredicateFilters(t, boolFlagRef)
	if got := remainingInts(t, positive, input); len(got) != 2 || got[0] != 1 || got[1] != 4 {
		t.Fatalf("WHERE flag kept %v, want [1 4]", got)
	}

	// NOT NULL is NULL, so the NULL row drops here too — exactly `flag = false`.
	negative := boolPredicateFilters(t, `{"node":"not","expression":`+boolFlagRef+`}`)
	if got := remainingInts(t, negative, input); len(got) != 1 || got[0] != 2 {
		t.Fatalf("WHERE NOT flag kept %v, want [2]", got)
	}
}

func TestFilterV2BooleanColumnPredicatesRenderAsSQL(t *testing.T) {
	// The half a row count cannot see. Left unprojected these render "1=1",
	// and a worker that builds a WHERE clause from that returns rows the
	// predicate excludes.
	identity := func(s string) string { return s }

	positive := boolPredicateFilters(t, boolFlagRef)
	sql, params := positive.ToSQL(identity, "?")
	if sql != "flag = ?" || len(params) != 1 || params[0] != true {
		t.Fatalf("WHERE flag rendered %q with %v, want \"flag = ?\" with [true]", sql, params)
	}

	negative := boolPredicateFilters(t, `{"node":"not","expression":`+boolFlagRef+`}`)
	sql, params = negative.ToSQL(identity, "?")
	if sql != "flag = ?" || len(params) != 1 || params[0] != false {
		t.Fatalf("WHERE NOT flag rendered %q with %v, want \"flag = ?\" with [false]", sql, params)
	}
}

func TestFilterV2BooleanColumnInsideConjunctionStillPushesDown(t *testing.T) {
	// The shape that actually turns up: one child the worker cannot render
	// used to cost the whole conjunction its pushdown.
	mem := memory.NewGoAllocator()
	values := array.NewInt64Builder(mem)
	values.Append(2)
	literal := values.NewArray()
	values.Release()
	defer literal.Release()

	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"and","children":[{"node":"comparison","op":"gt","left":{"node":"column_ref","column_index":1,"column_name":"n"},"right":{"node":"literal","value_ref":0}},{"node":"not","expression":` + boolFlagRef + `}]}}]}`
	encoded := filterV2Batch(t, document, []arrow.Field{{Name: "value_0", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, []arrow.Array{literal})
	defer encoded.Release()
	filters, err := DeserializeFiltersWithSchema(encoded, boolOutputSchema(), nil)
	if err != nil {
		t.Fatal(err)
	}
	sql, params := filters.ToSQL(func(s string) string { return s }, "?")
	if sql != "(n > ? AND flag = ?)" || len(params) != 2 || params[1] != false {
		t.Fatalf("conjunction rendered %q with %v, want \"(n > ? AND flag = ?)\" with [2 false]", sql, params)
	}
}

func TestFilterV2NonBooleanColumnIsStillRefusedAsPredicateRoot(t *testing.T) {
	// `WHERE n` where n is BIGINT is not a predicate, and the projection must
	// not make it look like one.
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"column_ref","column_index":1,"column_name":"n"}}]}`
	encoded := filterV2Batch(t, document, nil, nil)
	defer encoded.Release()
	if _, err := DeserializeFiltersWithSchema(encoded, boolOutputSchema(), nil); err == nil {
		t.Fatal("a BIGINT column was accepted as a predicate root")
	} else if !strings.Contains(err.Error(), "predicate root") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// NOT IN, negated IN, and the exact SQL rendering
//
// DuckDB sends `region NOT IN ('west','east')` as not(in(...)) and may send a
// negated IN directly. Neither projects onto an ergonomic Filter type, so both
// reached ToSQL as an opaque v2FilterView and rendered "1=1": the worker
// returned every row, and DuckDB — which removed the predicate from its own
// plan when it pushed it — returned them to the user. ToSQLExact renders them
// exactly and refuses, with an error, anything it cannot render.
// ---------------------------------------------------------------------------

func regionOutputSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
}

// regionInputBatch is region = west, east, north, NULL with n = 1..4.
func regionInputBatch(t *testing.T) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	regions := array.NewStringBuilder(mem)
	regions.AppendValues([]string{"west", "east", "north", ""}, []bool{true, true, true, false})
	regionArray := regions.NewArray()
	regions.Release()
	ints := array.NewInt64Builder(mem)
	ints.AppendValues([]int64{1, 2, 3, 4}, nil)
	intArray := ints.NewArray()
	ints.Release()
	batch := array.NewRecordBatch(regionOutputSchema(), []arrow.Array{regionArray, intArray}, 4)
	regionArray.Release()
	intArray.Release()
	return batch
}

const regionRef = `{"node":"column_ref","column_index":0,"column_name":"region"}`
const regionNRef = `{"node":"column_ref","column_index":1,"column_name":"n"}`

// regionFilters decodes one required predicate against regionOutputSchema.
// value_0 is a list<string> literal set (a nil element is a NULL); value_1 is
// the int64 literal 2, for comparisons.
func regionFilters(t *testing.T, expression string, set []*string) *PushdownFilters {
	t.Helper()
	mem := memory.NewGoAllocator()
	lb := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	vb := lb.ValueBuilder().(*array.StringBuilder)
	lb.Append(true)
	for _, v := range set {
		if v == nil {
			vb.AppendNull()
		} else {
			vb.Append(*v)
		}
	}
	// The decoded filters reference the payload arrays, so they live as long
	// as the test.
	list := lb.NewArray()
	lb.Release()
	t.Cleanup(list.Release)
	ib := array.NewInt64Builder(mem)
	ib.Append(2)
	two := ib.NewArray()
	ib.Release()
	t.Cleanup(two.Release)

	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":` + expression + `}]}`
	encoded := filterV2Batch(t, document,
		[]arrow.Field{
			{Name: "value_0", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
			{Name: "value_1", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		},
		[]arrow.Array{list, two})
	t.Cleanup(encoded.Release)
	filters, err := DeserializeFiltersWithSchema(encoded, regionOutputSchema(), nil)
	if err != nil {
		t.Fatalf("deserialize %s: %v", expression, err)
	}
	return filters
}

func strp(s string) *string { return &s }

func regionIn(negated bool) string {
	neg := "false"
	if negated {
		neg = "true"
	}
	return `{"node":"in","expression":` + regionRef + `,"set":{"kind":"literal","value_ref":0},"negated":` + neg + `}`
}

func notOf(expression string) string { return `{"node":"not","expression":` + expression + `}` }

// assertExactSQL checks ToSQLExact renders want/params, and that the
// deprecated ToSQL agrees with it on every filter ToSQLExact accepts.
func assertExactSQL(t *testing.T, label string, filters *PushdownFilters, want string, wantParams []any) {
	t.Helper()
	identity := func(s string) string { return s }
	sql, params, err := filters.ToSQLExact(identity, "?")
	if err != nil {
		t.Fatalf("%s: ToSQLExact error: %v", label, err)
	}
	if sql != want || len(params) != len(wantParams) {
		t.Fatalf("%s: ToSQLExact rendered %q with %v, want %q with %v", label, sql, params, want, wantParams)
	}
	for i := range params {
		if params[i] != wantParams[i] {
			t.Fatalf("%s: param %d = %#v, want %#v", label, i, params[i], wantParams[i])
		}
	}
	legacy, legacyParams := filters.ToSQL(identity, "?")
	if legacy != sql || len(legacyParams) != len(params) {
		t.Fatalf("%s: ToSQL rendered %q with %v, ToSQLExact %q with %v", label, legacy, legacyParams, sql, params)
	}
}

func TestFilterV2NotInRendersAsSQL(t *testing.T) {
	input := regionInputBatch(t)
	defer input.Release()
	westEast := []*string{strp("west"), strp("east")}

	// DuckDB's form: not(in(...)). Only north survives — NULL NOT IN (...) is
	// NULL, which is not satisfied, in DuckDB and in standard SQL alike.
	notIn := regionFilters(t, notOf(regionIn(false)), westEast)
	if got := remainingInts(t, notIn, input); len(got) != 1 || got[0] != 3 {
		t.Fatalf("NOT IN kept %v, want [3]", got)
	}
	assertExactSQL(t, "not(in)", notIn, "region NOT IN (?, ?)", []any{"west", "east"})

	// The same predicate as a negated IN node.
	negatedIn := regionFilters(t, regionIn(true), westEast)
	if got := remainingInts(t, negatedIn, input); len(got) != 1 || got[0] != 3 {
		t.Fatalf("negated IN kept %v, want [3]", got)
	}
	assertExactSQL(t, "in negated", negatedIn, "region NOT IN (?, ?)", []any{"west", "east"})

	// A double negation is the positive IN.
	assertExactSQL(t, "not(in negated)", regionFilters(t, notOf(regionIn(true)), westEast),
		"region IN (?, ?)", []any{"west", "east"})

	// Quoting goes through the caller's quoter.
	sql, _, err := notIn.ToSQLExact(func(s string) string { return `"` + s + `"` }, "?")
	if err != nil || sql != `"region" NOT IN (?, ?)` {
		t.Fatalf("quoted NOT IN rendered %q, %v", sql, err)
	}
}

func TestFilterV2NotInWithNullInSetKeepsNothing(t *testing.T) {
	// x NOT IN ('west', NULL) is never TRUE: it is FALSE for west and NULL for
	// everything else. The NULL travels as a nil parameter, which binds as SQL
	// NULL, so the rendered clause has the same meaning.
	input := regionInputBatch(t)
	defer input.Release()
	filters := regionFilters(t, notOf(regionIn(false)), []*string{strp("west"), nil})
	if got := remainingInts(t, filters, input); len(got) != 0 {
		t.Fatalf("NOT IN with a NULL element kept %v, want []", got)
	}
	assertExactSQL(t, "not(in) with NULL", filters, "region NOT IN (?, ?)", []any{"west", nil})
}

func TestFilterV2NotOverTranslatableChildRendersAsSQL(t *testing.T) {
	gtTwo := `{"node":"comparison","op":"gt","left":` + regionNRef + `,"right":{"node":"literal","value_ref":1}}`
	isNull := `{"node":"is_null","expression":` + regionRef + `,"negated":false}`
	and := `{"node":"and","children":[` + gtTwo + `,` + isNull + `]}`
	assertExactSQL(t, "not(and)", regionFilters(t, notOf(and), nil),
		"NOT ((n > ? AND region IS NULL))", []any{int64(2)})
}

func TestFilterV2UntranslatableFilterIsAnErrorNotAWidening(t *testing.T) {
	identity := func(s string) string { return s }
	// A column-to-column comparison has no ergonomic projection.
	selfCompare := `{"node":"comparison","op":"eq","left":` + regionRef + `,"right":` + regionRef + `}`
	gtTwo := `{"node":"comparison","op":"gt","left":` + regionNRef + `,"right":{"node":"literal","value_ref":1}}`

	for _, test := range []struct {
		label, expression, legacy string
	}{
		{"bare", selfCompare, "1=1"},
		{"not over untranslatable", notOf(selfCompare), "1=1"},
		// Under NOT, widening the child would narrow the result; ToSQL widens
		// the whole negation instead.
		{"untranslatable inside not(and)", notOf(`{"node":"and","children":[` + gtTwo + `,` + selfCompare + `]}`), "1=1"},
		{"untranslatable inside and", `{"node":"and","children":[` + gtTwo + `,` + notOf(selfCompare) + `]}`, "(n > ? AND 1=1)"},
	} {
		filters := regionFilters(t, test.expression, nil)
		sql, params, err := filters.ToSQLExact(identity, "?")
		if err == nil {
			t.Fatalf("%s: ToSQLExact rendered %q with %v, want an error", test.label, sql, params)
		}
		if !errors.Is(err, ErrFilterNotTranslatable) || !strings.Contains(err.Error(), "comparison(eq, column_ref(region), column_ref(region))") {
			t.Fatalf("%s: unexpected error: %v", test.label, err)
		}
		if sql != "" || params != nil {
			t.Fatalf("%s: ToSQLExact returned %q / %v alongside its error", test.label, sql, params)
		}
		// The deprecated ToSQL keeps its widening behaviour.
		if legacy, _ := filters.ToSQL(identity, "?"); legacy != test.legacy {
			t.Fatalf("%s: ToSQL rendered %q, want %q", test.label, legacy, test.legacy)
		}
	}
}

func TestFilterV2ExistingShapesRenderUnchanged(t *testing.T) {
	westEast := []*string{strp("west"), strp("east")}
	gtTwo := `{"node":"comparison","op":"gt","left":` + regionNRef + `,"right":{"node":"literal","value_ref":1}}`
	ltTwoFlipped := `{"node":"comparison","op":"gt","left":{"node":"literal","value_ref":1},"right":` + regionNRef + `}`
	isNotNull := `{"node":"is_null","expression":` + regionRef + `,"negated":true}`

	assertExactSQL(t, "comparison", regionFilters(t, gtTwo, nil), "n > ?", []any{int64(2)})
	assertExactSQL(t, "flipped comparison", regionFilters(t, ltTwoFlipped, nil), "n < ?", []any{int64(2)})
	assertExactSQL(t, "is not null", regionFilters(t, isNotNull, nil), "region IS NOT NULL", nil)
	assertExactSQL(t, "in", regionFilters(t, regionIn(false), westEast), "region IN (?, ?)", []any{"west", "east"})
	assertExactSQL(t, "or", regionFilters(t, `{"node":"or","children":[`+gtTwo+`,`+regionIn(false)+`]}`, westEast),
		"(n > ? OR region IN (?, ?))", []any{int64(2), "west", "east"})
	assertExactSQL(t, "empty", &PushdownFilters{}, "", nil)

	// An empty set is a constant in DuckDB; `col IN ()` is not portable SQL.
	assertExactSQL(t, "empty in", regionFilters(t, regionIn(false), nil), "1=0", nil)
	assertExactSQL(t, "empty not in", regionFilters(t, notOf(regionIn(false)), nil), "1=1", nil)
}
