// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package generated_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func catalogFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("../../conformance/catalog-contracts.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Records map[string]string `json:"records"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	encoded, ok := corpus.Records[name]
	if !ok {
		t.Fatalf("missing corpus record %s", name)
	}
	payload, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func catalogBatch(t *testing.T, payload []byte) arrow.RecordBatch {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatalf("missing fixture batch: %v", reader.Err())
	}
	batch := reader.RecordBatch()
	batch.Retain()
	t.Cleanup(batch.Release)
	return batch
}

// Exercise the public HTTP/RPC path because vgi-rpc-go keeps the reflection
// codec private. The handler receives a typed record, then serializes it again.
func echoCatalogRecord[T any](t *testing.T, name string) {
	t.Helper()
	server := vgirpc.NewServer()
	server.SetServiceName("catalog.contracts.test")
	vgirpc.Unary(server, "echo", func(_ context.Context, _ *vgirpc.CallContext, value T) (T, error) {
		return value, nil
	})
	httpServer := httptest.NewServer(vgirpc.NewHttpServer(server))
	t.Cleanup(httpServer.Close)
	client, err := vgirpc.NewHttpClient(httpServer.URL, vgirpc.WithClientProtocol("catalog.contracts.test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	want := catalogBatch(t, catalogFixture(t, name))
	response, err := client.CallUnary(context.Background(), "echo", want, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Release()
	got := catalogBatch(t, response.Batch.Column(0).(*array.Binary).Value(0))
	// Arrow maps are unordered. Go emits canonical key order; preserve all
	// keys/values while comparing the other patch columns exactly.
	if name == "metadata_patch" {
		if !want.Schema().Equal(got.Schema()) {
			t.Fatal("metadata schema changed")
		}
		for i := range want.NumCols() {
			if want.ColumnName(int(i)) == "set_tags" {
				continue
			}
			if !array.Equal(want.Column(int(i)), got.Column(int(i))) {
				t.Fatalf("patch field %s changed", want.ColumnName(int(i)))
			}
		}
		tags := got.Column(2).(*array.Map)
		begin, end := tags.ValueOffsets(0)
		if end-begin != 3 {
			t.Fatal("tag keys lost or duplicated")
		}
		keys := tags.Keys().(*array.String)
		values := tags.Items().(*array.String)
		decoded := map[string]string{}
		for i := begin; i < end; i++ {
			decoded[keys.Value(int(i))] = values.Value(int(i))
		}
		if !maps.Equal(decoded, map[string]string{"a.b": "", "__proto__": "literal", "": "empty-key"}) {
			t.Fatalf("tag metadata changed: %v", decoded)
		}
		return
	}
	if !array.RecordEqual(want, got) {
		t.Fatalf("%s changed across the typed codec:\nwant %s\ngot %s", name, want, got)
	}
}

func TestColumnExpressionMetadataRetainsIPCBytes(t *testing.T) {
	source := catalogBatch(t, catalogFixture(t, "column_expression_metadata"))
	var encoded bytes.Buffer
	writer := ipc.NewWriter(&encoded, ipc.WithSchema(source.Schema()))
	if err := writer.Write(source); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	restored := catalogBatch(t, encoded.Bytes())
	for index, entry := range []struct{ key, name string }{
		{"vgi.column.default.v1", "column_default_expression"},
		{"vgi.column.generated.v1", "column_generated_expression"},
	} {
		value, ok := restored.Schema().Field(index).Metadata.GetValue(entry.key)
		if !ok {
			t.Fatal("expression metadata missing")
		}
		payload, err := base64.StdEncoding.Strict().DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(payload, catalogFixture(t, entry.name)) {
			t.Fatal("expression IPC changed")
		}
	}
	generation, _ := restored.Schema().Field(1).Metadata.GetValue("vgi.column.generation.v1")
	if generation != "virtual" {
		t.Fatal("generation category changed")
	}
}

func TestCanonicalConstraintPythonCorpus(t *testing.T) {
	t.Run("sql_statement", func(t *testing.T) { echoCatalogRecord[generated.SqlStatement](t, "sql_statement") })
	t.Run("constraint_ref", func(t *testing.T) { echoCatalogRecord[generated.CatalogObjectRef](t, "constraint_ref") })
	t.Run("foreign_key_definition", func(t *testing.T) { echoCatalogRecord[generated.ForeignKeyDefinition](t, "foreign_key_definition") })
	t.Run("constraint_definition_not_null", func(t *testing.T) {
		echoCatalogRecord[generated.ConstraintDefinition](t, "constraint_definition_not_null")
	})
	t.Run("constraint_definition_unique", func(t *testing.T) {
		echoCatalogRecord[generated.ConstraintDefinition](t, "constraint_definition_unique")
	})
	t.Run("constraint_definition_primary", func(t *testing.T) {
		echoCatalogRecord[generated.ConstraintDefinition](t, "constraint_definition_primary")
	})
	t.Run("constraint_definition_check", func(t *testing.T) {
		echoCatalogRecord[generated.ConstraintDefinition](t, "constraint_definition_check")
	})
	t.Run("constraint_definition_foreign", func(t *testing.T) {
		echoCatalogRecord[generated.ConstraintDefinition](t, "constraint_definition_foreign")
	})
	t.Run("constraint_info", func(t *testing.T) { echoCatalogRecord[generated.ConstraintInfo](t, "constraint_info") })
	t.Run("constraint_info_untrusted", func(t *testing.T) { echoCatalogRecord[generated.ConstraintInfo](t, "constraint_info_untrusted") })
	t.Run("constraint_change_add", func(t *testing.T) { echoCatalogRecord[generated.ConstraintChange](t, "constraint_change_add") })
	t.Run("constraint_change_drop", func(t *testing.T) { echoCatalogRecord[generated.ConstraintChange](t, "constraint_change_drop") })
	t.Run("constraint_change_rename", func(t *testing.T) { echoCatalogRecord[generated.ConstraintChange](t, "constraint_change_rename") })
	t.Run("constraint_change_unname", func(t *testing.T) { echoCatalogRecord[generated.ConstraintChange](t, "constraint_change_unname") })
	t.Run("constraint_change_replace", func(t *testing.T) { echoCatalogRecord[generated.ConstraintChange](t, "constraint_change_replace") })
	t.Run("constraint_snapshot", func(t *testing.T) { echoCatalogRecord[generated.ConstraintSnapshot](t, "constraint_snapshot") })
	t.Run("constraint_snapshot_empty", func(t *testing.T) { echoCatalogRecord[generated.ConstraintSnapshot](t, "constraint_snapshot_empty") })
	t.Run("constraints_apply", func(t *testing.T) { echoCatalogRecord[generated.ConstraintsApplyRequest](t, "constraints_apply") })
	t.Run("constraints_apply_autocommit", func(t *testing.T) {
		echoCatalogRecord[generated.ConstraintsApplyRequest](t, "constraints_apply_autocommit")
	})
}

func TestTableConstraintParentFields(t *testing.T) {
	column := func(batch arrow.RecordBatch, name string) arrow.Array {
		indices := batch.Schema().FieldIndices(name)
		if len(indices) != 1 {
			t.Fatalf("missing or duplicate %s", name)
		}
		return batch.Column(indices[0])
	}
	binaryList := func(batch arrow.RecordBatch, name string) [][]byte {
		list := column(batch, name).(*array.List)
		if list.IsNull(0) {
			t.Fatalf("%s must be present", name)
		}
		start, end := list.ValueOffsets(0)
		values := list.ListValues().(*array.Binary)
		out := make([][]byte, 0, end-start)
		for i := start; i < end; i++ {
			out = append(out, append([]byte(nil), values.Value(int(i))...))
		}
		return out
	}
	create := catalogBatch(t, catalogFixture(t, "table_create"))
	info := catalogBatch(t, catalogFixture(t, "table_info"))
	definitions := binaryList(create, "constraint_definitions")
	constraints := binaryList(info, "constraints")
	if len(definitions) != 1 || len(constraints) != 1 {
		t.Fatal("canonical constraints missing")
	}
	if len(binaryList(catalogBatch(t, catalogFixture(t, "table_create_legacy")), "constraint_definitions")) != 0 {
		t.Fatal("empty create gained constraints")
	}
	revision := column(info, "definition_revision").(*array.Binary).Value(0)
	if !bytes.Equal(revision, []byte("revision\x00\xff")) {
		t.Fatal("revision bytes changed")
	}
	constraint := catalogBatch(t, constraints[0])
	if !bytes.Equal(column(constraint, "definition").(*array.Binary).Value(0), definitions[0]) {
		t.Fatal("parent definition differs")
	}
	reference := catalogBatch(t, column(constraint, "ref").(*array.Binary).Value(0))
	if column(reference, "name").(*array.String).Value(0) != "fixture-stable-key" {
		t.Fatal("stable key changed")
	}
	definition := catalogBatch(t, definitions[0])
	paths := column(definition, "column_paths").(*array.List)
	path := paths.ListValues().(*array.List)
	components := path.ListValues().(*array.String)
	if paths.Len() != 1 || path.Len() != 1 || components.Len() != 2 || components.Value(0) != "record" || components.Value(1) != "a.b" {
		t.Fatal("nested column path changed")
	}
	encoded, err := vgi.SerializeTableInfo(&vgi.TableInfo{Name: "t", SchemaPath: []string{"main"}, Constraints: constraints, DefinitionRevision: revision})
	if err != nil {
		t.Fatal(err)
	}
	restored := catalogBatch(t, encoded)
	if !array.Equal(column(info, "constraints"), column(restored, "constraints")) || !array.Equal(column(info, "definition_revision"), column(restored, "definition_revision")) {
		t.Fatal("handwritten TableInfo serializer changed canonical fields")
	}
}

func TestCatalogContractPythonCorpus(t *testing.T) {
	for _, name := range []string{"macro_parameter", "macro_parameter_expression", "macro_parameter_untyped"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.MacroParameter](t, name) })
	}
	t.Run("macro_overload", func(t *testing.T) { echoCatalogRecord[generated.MacroOverload](t, "macro_overload") })
	for _, name := range []string{"macro_create", "macro_create_legacy"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.MacroCreateRequestWire](t, name) })
	}
	t.Run("query_offer", func(t *testing.T) { echoCatalogRecord[generated.ClientQueryCapabilities](t, "query_offer") })
	t.Run("query_capabilities", func(t *testing.T) { echoCatalogRecord[generated.QueryCapabilities](t, "query_capabilities") })
	t.Run("query_context", func(t *testing.T) { echoCatalogRecord[generated.QueryEvaluationContext](t, "query_context") })
	t.Run("query_object_reference", func(t *testing.T) { echoCatalogRecord[generated.QueryObjectReference](t, "query_object_reference") })
	t.Run("query_prepare", func(t *testing.T) { echoCatalogRecord[generated.CatalogQueryPrepareRequest](t, "query_prepare") })
	t.Run("query_accepted", func(t *testing.T) { echoCatalogRecord[generated.CatalogQueryPrepareResult](t, "query_accepted") })
	t.Run("query_declined", func(t *testing.T) { echoCatalogRecord[generated.CatalogQueryPrepareResult](t, "query_declined") })
	for _, name := range []string{"object_ref", "trigger_ref"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.CatalogObjectRef](t, name) })
	}
	for _, name := range []string{"object_dependency", "dependency_membership", "dependency_column_literal",
		"dependency_column_nested", "dependency_generated", "dependency_generated_chain"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.ObjectDependency](t, name) })
	}
	for _, name := range []string{"dependency_snapshot", "empty_dependency_snapshot", "dependency_column_snapshot"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.DependencySnapshot](t, name) })
	}
	for _, name := range []string{"metadata_patch", "metadata_clear_comment", "metadata_noop"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.CatalogMetadataChanges](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "column_add_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableColumnAddParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "column_drop_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableColumnDropParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "column_rename_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableColumnRenameParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "column_type_change_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableColumnTypeChangeParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "column_default_set_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableColumnDefaultSetParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "column_default_drop_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableColumnDefaultDropParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "column_comment_set_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableColumnCommentSetParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "not_null_set_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableNotNullSetParams](t, name) })
	}
	for _, variant := range []string{"literal", "nested"} {
		name := "not_null_drop_" + variant
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.CatalogTableNotNullDropParams](t, name) })
	}
	t.Run("expression", func(t *testing.T) { echoCatalogRecord[generated.SqlExpression](t, "expression") })
	t.Run("sort_key", func(t *testing.T) { echoCatalogRecord[generated.SortKey](t, "sort_key") })
	for _, name := range []string{"layout", "empty_layout"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.TableLayout](t, name) })
	}
	for _, name := range []string{"layout_change", "layout_clear"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.TableLayoutChange](t, name) })
	}
	for _, name := range []string{"table_create", "table_create_legacy"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[vgi.TableCreateRequestWire](t, name) })
	}
	for _, name := range []string{"capabilities", "empty_capabilities"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.CatalogCapabilities](t, name) })
	}
	t.Run("definitions", func(t *testing.T) { echoCatalogRecord[generated.OptionDefinitions](t, "definitions") })
	for _, name := range []string{"changes", "reset_only"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.OptionChanges](t, name) })
	}
	for _, name := range []string{"state", "empty_state"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.OptionState](t, name) })
	}
	for _, name := range []string{"paths", "empty_paths"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.RequiredFilterPaths](t, name) })
	}
}

func TestQueryParameterBatchPreservesMappingAndMetadata(t *testing.T) {
	for _, name := range []string{"query_parameters", "query_empty_parameters"} {
		t.Run(name, func(t *testing.T) {
			source := catalogBatch(t, catalogFixture(t, name))
			var encoded bytes.Buffer
			writer := ipc.NewWriter(&encoded, ipc.WithSchema(source.Schema()))
			if err := writer.Write(source); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			restored := catalogBatch(t, encoded.Bytes())
			if !array.RecordEqual(source, restored) || !source.Schema().Metadata().Equal(restored.Schema().Metadata()) || restored.NumRows() != 1 {
				t.Fatal("parameter batch changed across Arrow IPC")
			}
			wantMode := "named"
			if name == "query_empty_parameters" {
				wantMode = "positional"
			}
			if mode, ok := restored.Schema().Metadata().GetValue("vgi.query.parameters.v1"); !ok || mode != wantMode {
				t.Fatal("parameter mapping metadata lost")
			}
			if name == "query_empty_parameters" {
				if restored.NumCols() != 0 {
					t.Fatal("empty parameter set gained columns")
				}
			} else {
				if !restored.Column(0).IsNull(0) {
					t.Fatal("typed NULL lost")
				}
				field := restored.Schema().Field(1).Type.(*arrow.StructType).Field(0)
				if value, ok := field.Metadata.GetValue("nested"); !ok || value != "retained" {
					t.Fatal("nested metadata lost")
				}
			}
		})
	}
}

func TestViewAliasesMatchPythonCorpus(t *testing.T) {
	comment := "view comment"
	encoded, err := vgi.SerializeViewInfo(&vgi.ViewInfo{
		Name: "view.with.dot", SchemaPath: []string{"one.two", "nested"},
		Definition: "SELECT 1 AS original, 2 AS second", Aliases: []string{"a.b", "with\"quote"},
		ColumnComments: map[string]string{"a.b": "kept"}, Comment: &comment, Tags: map[string]string{"team": "one"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !array.RecordEqual(catalogBatch(t, encoded), catalogBatch(t, catalogFixture(t, "view_info"))) {
		t.Fatal("view metadata changed across the Go writer")
	}
}

func TestViewSecurityMatchesPythonCorpus(t *testing.T) {
	encoded, err := vgi.SerializeViewInfo(&vgi.ViewInfo{
		Name: "barrier", SchemaPath: []string{"main"}, Definition: "SELECT 1 AS original", Security: "barrier",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !array.RecordEqual(catalogBatch(t, encoded), catalogBatch(t, catalogFixture(t, "view_info_barrier"))) {
		t.Fatal("barrier mode changed across the Go writer")
	}
	if _, err := vgi.SerializeViewInfo(&vgi.ViewInfo{Security: "unknown"}); err == nil {
		t.Fatal("unknown security mode accepted")
	}
}

func TestLogicalTypeAndOpaqueMacroCorpus(t *testing.T) {
	for _, name := range []string{"logical_tuple", "logical_struct", "logical_variant_sql_null", "logical_variant_null", "logical_variant_absent", "logical_variant_null_member", "macro_info", "macro_info_legacy"} {
		t.Run(name, func(t *testing.T) {
			original := catalogBatch(t, catalogFixture(t, name))
			var encoded bytes.Buffer
			writer := ipc.NewWriter(&encoded, ipc.WithSchema(original.Schema()))
			if err := writer.Write(original); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			restored := catalogBatch(t, encoded.Bytes())
			if !array.RecordEqual(original, restored) {
				t.Fatal("logical storage changed across IPC")
			}
			kind, annotated := restored.Schema().Field(0).Metadata.GetValue("vgi.logical_type.v1")
			if name == "logical_tuple" && (!annotated || kind != "tuple") {
				t.Fatal("tuple identity lost")
			}
			if name == "logical_struct" && annotated {
				t.Fatal("plain struct became a tuple")
			}
			if strings.HasPrefix(name, "logical_variant_") {
				field := restored.Schema().Field(0)
				extension, present := field.Metadata.GetValue("ARROW:extension:name")
				metadata, metadataPresent := field.Metadata.GetValue("ARROW:extension:metadata")
				if !present || extension != "arrow.parquet.variant" || !metadataPresent || metadata != "" || annotated {
					t.Fatal("canonical variant extension metadata changed")
				}
				fields := field.Type.(*arrow.StructType).Fields()
				if len(fields) != 2 || fields[0].Name != "metadata" || fields[0].Nullable || fields[1].Name != "value" || !fields[1].Nullable {
					t.Fatal("canonical variant storage shape changed")
				}
				if restored.Column(0).IsNull(0) != (name == "logical_variant_sql_null") {
					t.Fatal("SQL NULL confused with encoded variant null")
				}
				if name != "logical_variant_sql_null" {
					column := restored.Column(0).(*array.Struct)
					expectedMetadata, expectedValue := []byte{1, 0, 0}, []byte{0}
					if name == "logical_variant_absent" {
						expectedValue = []byte{2, 0, 0}
					} else if name == "logical_variant_null_member" {
						expectedMetadata = []byte{1, 1, 0, 7, 'p', 'r', 'e', 's', 'e', 'n', 't'}
						expectedValue = []byte{2, 1, 0, 0, 1, 0}
					}
					if !bytes.Equal(column.Field(0).(*array.Binary).Value(0), expectedMetadata) || !bytes.Equal(column.Field(1).(*array.Binary).Value(0), expectedValue) {
						t.Fatal("opaque canonical variant metadata/value bytes changed")
					}
				}
			}
		})
	}
}

func TestTypedIndexContractsPreserveParentPayloads(t *testing.T) {
	for _, name := range []string{"index_key_column", "index_key_nested", "index_key_expression"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.IndexKey](t, name) })
	}
	for _, name := range []string{"index_definition", "index_definition_primary"} {
		t.Run(name, func(t *testing.T) { echoCatalogRecord[generated.IndexDefinition](t, name) })
	}
	getBinary := func(batch arrow.RecordBatch, name string) []byte {
		indices := batch.Schema().FieldIndices(name)
		if len(indices) != 1 {
			t.Fatalf("missing field %s", name)
		}
		return batch.Column(indices[0]).(*array.Binary).Value(0)
	}
	for _, entry := range []struct {
		name   string
		schema *arrow.Schema
	}{
		{"index_info", generated.IndexInfoSchema},
		{"index_create", generated.IndexCreateRequestSchema},
		{"index_create_cross_schema", generated.IndexCreateRequestSchema},
	} {
		source := catalogBatch(t, catalogFixture(t, entry.name))
		if !source.Schema().Equal(entry.schema) {
			t.Fatalf("%s schema drift", entry.name)
		}
		var encoded bytes.Buffer
		writer := ipc.NewWriter(&encoded, ipc.WithSchema(source.Schema()))
		if err := writer.Write(source); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		restored := catalogBatch(t, encoded.Bytes())
		if !array.RecordEqual(source, restored) {
			t.Fatalf("%s parent changed", entry.name)
		}
		if !bytes.Equal(getBinary(restored, "definition"), catalogFixture(t, "index_definition")) {
			t.Fatal("definition changed")
		}
		if entry.name == "index_info" && !bytes.Equal(getBinary(restored, "options"), catalogFixture(t, "state")) {
			t.Fatal("option state changed")
		}
	}
	definition := catalogBatch(t, catalogFixture(t, "index_definition"))
	if !bytes.Equal(getBinary(definition, "options"), catalogFixture(t, "values")) {
		t.Fatal("typed options changed")
	}
	for _, entry := range []struct {
		name string
		want []string
	}{
		{"index_key_column", []string{"a.b"}}, {"index_key_nested", []string{"record", "a.b"}},
	} {
		key := catalogBatch(t, catalogFixture(t, entry.name))
		path := key.Column(key.Schema().FieldIndices("column_path")[0]).(*array.List)
		values := path.ListValues().(*array.String)
		if values.Len() != len(entry.want) {
			t.Fatal("raw path length changed")
		}
		for i, want := range entry.want {
			if values.Value(i) != want {
				t.Fatal("raw path changed")
			}
		}
	}
}

func TestDependencyAlterSubjectsPreserveRawColumnPaths(t *testing.T) {
	col := func(batch arrow.RecordBatch, name string) arrow.Array {
		positions := batch.Schema().FieldIndices(name)
		if len(positions) != 1 {
			t.Fatalf("missing field %s", name)
		}
		return batch.Column(positions[0])
	}
	for _, entry := range []struct {
		name   string
		blocks bool
	}{{"dependency_membership", false}, {"object_dependency", true}} {
		edge := catalogBatch(t, catalogFixture(t, entry.name))
		if col(edge, "blocks_alter").(*array.Boolean).Value(0) != entry.blocks {
			t.Fatal("blocks_alter changed")
		}
		if !col(edge, "subject_column_paths").IsNull(0) || !col(edge, "dependent_column_path").IsNull(0) {
			t.Fatal("whole-object edge lost NULL scope")
		}
	}
	for _, entry := range []struct {
		name string
		want []string
	}{
		{"dependency_column_literal", []string{"a.b"}}, {"dependency_column_nested", []string{"record", "a.b"}},
	} {
		edge := catalogBatch(t, catalogFixture(t, entry.name))
		paths := col(edge, "subject_column_paths").(*array.List)
		path := paths.ListValues().(*array.List)
		parts := path.ListValues().(*array.String)
		if paths.IsNull(0) || path.Len() != 1 || parts.Len() != len(entry.want) {
			t.Fatal("column scope changed")
		}
		for i, want := range entry.want {
			if parts.Value(i) != want {
				t.Fatal("raw column path changed")
			}
		}
	}
	generated := catalogBatch(t, catalogFixture(t, "dependency_generated"))
	if !bytes.Equal(col(generated, "dependent").(*array.Binary).Value(0), col(generated, "subject").(*array.Binary).Value(0)) {
		t.Fatal("same-table column dependency changed object identity")
	}
	for _, entry := range []struct{ name, target string }{{"dependency_generated", "generated"}, {"dependency_generated_chain", "second.generated"}} {
		edge := catalogBatch(t, catalogFixture(t, entry.name))
		path := col(edge, "dependent_column_path").(*array.List)
		parts := path.ListValues().(*array.String)
		if path.IsNull(0) || parts.Len() != 1 || parts.Value(0) != entry.target {
			t.Fatal("dependent column target changed")
		}
	}
}
