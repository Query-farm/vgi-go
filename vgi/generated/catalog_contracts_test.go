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

func TestCatalogContractPythonCorpus(t *testing.T) {
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
	t.Run("object_dependency", func(t *testing.T) { echoCatalogRecord[generated.ObjectDependency](t, "object_dependency") })
	for _, name := range []string{"dependency_snapshot", "empty_dependency_snapshot"} {
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
