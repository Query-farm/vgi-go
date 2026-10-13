// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package generated_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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
	if !array.RecordEqual(want, got) {
		t.Fatalf("%s changed across the typed codec:\nwant %s\ngot %s", name, want, got)
	}
}

func TestCatalogContractPythonCorpus(t *testing.T) {
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
