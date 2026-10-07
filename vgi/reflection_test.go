// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Browser clients first list protocols and describe vgi.v2; serving a current
// bundle is insufficient when the worker never registered Reflection.v1.
func TestWorkerReflectionDiscovery(t *testing.T) {
	w := vgi.NewWorker(vgi.WithCatalogName("example"))
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	defer ts.Close()
	client, err := vgirpc.NewHttpClient(ts.URL, vgirpc.WithClientProtocol(vgirpc.ReflectionProtocolName))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	empty := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 1)
	defer empty.Release()
	listed, err := client.CallUnary(context.Background(), "list_protocols", empty, nil)
	if err != nil {
		t.Fatalf("list protocols: %v", err)
	}
	listed.Release()
	params := jsonRow(t, arrow.NewSchema([]arrow.Field{{Name: "protocol", Type: arrow.BinaryTypes.String}}, nil), map[string]any{"protocol": vgi.ProtocolName})
	defer params.Release()
	described, err := client.CallUnary(context.Background(), "describe", params, nil)
	if err != nil {
		t.Fatalf("describe VGI: %v", err)
	}
	defer described.Release()
	result := resultStruct(t, described)
	defer result.Release()
	protocol := result.Column(result.Schema().FieldIndices("protocol")[0]).(*array.String).Value(0)
	if protocol != vgi.ProtocolName {
		t.Fatalf("described %q, want %q", protocol, vgi.ProtocolName)
	}
}

// vgiV2ReferenceProtocolHash is the vgi.v2 protocol hash vgi-python 0.43.0 (the
// reference) reports through vgi_rpc.Reflection.v1: SHA-256 over every vgi.v2
// method's name, type and params/result/header schemas (WIRE_PROTOCOL.md §14).
// Every SDK hosts the whole of vgi.v2 with exactly the reference's schemas, so
// every SDK reports this hash. It changes only with vgi.v2's protocol version
// (2.1.0): a mismatch here is schema drift, not a constant to update.
const vgiV2ReferenceProtocolHash = "774cb80090d71ea76d09aa311b9cda4ca4c33c3bf72c43242eb6dc87b6f79ce5"

// describeVgiV2 starts an HTTP worker and returns vgi.v2's Reflection.v1
// description.
func describeVgiV2(t *testing.T) arrow.RecordBatch {
	t.Helper()
	w := vgi.NewWorker(vgi.WithCatalogName("example"))
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	client, err := vgirpc.NewHttpClient(ts.URL, vgirpc.WithClientProtocol(vgirpc.ReflectionProtocolName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	params := jsonRow(t, arrow.NewSchema([]arrow.Field{{Name: "protocol", Type: arrow.BinaryTypes.String}}, nil), map[string]any{"protocol": vgi.ProtocolName})
	defer params.Release()
	described, err := client.CallUnary(context.Background(), "describe", params, nil)
	if err != nil {
		t.Fatalf("describe VGI: %v", err)
	}
	defer described.Release()
	result := resultStruct(t, described)
	t.Cleanup(result.Release)
	return result
}

// The hosted vgi.v2 surface is byte-identical to the reference's.
func TestVgiV2ProtocolHash(t *testing.T) {
	result := describeVgiV2(t)
	got := result.Column(result.Schema().FieldIndices("protocol_hash")[0]).(*array.String).Value(0)
	if got != vgiV2ReferenceProtocolHash {
		t.Fatalf("vgi.v2 protocol hash = %s, want the reference's %s: a method or schema drifted from vgi-python's vgi.v2", got, vgiV2ReferenceProtocolHash)
	}
}

// vgi.v2 methods this SDK hosts but does not implement refuse every call with
// UNIMPLEMENTED / method_not_implemented, never a silent success -- for this
// worker's own catalog and for a routed sub-catalog alike. The catalog_*
// stubs are registered through the same routing as every catalog method, so
// they are called with a real attach.
func TestUnimplementedVgiV2MethodsRefuse(t *testing.T) {
	parent := vgi.NewWorker(vgi.WithCatalogName("example"))
	parent.RegisterSubCatalog(staticChild("kid"))
	c := newContentsClient(t, parent)
	for _, catalog := range []string{"example", "kid"} {
		attach := c.attach(catalog).AttachOpaqueData
		indexCreate := jsonRow(t, generated.IndexCreateRequestSchema, map[string]any{
			"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": "i",
			"table_name": "t", "index_type": "ART", "constraint_type": "none",
			"expressions": []string{"x"}, "on_conflict": "error", "options": []any{},
		})
		calls := map[string]arrow.RecordBatch{
			"catalog_index_create": wrapRequest(t, indexCreate),
			"catalog_index_drop": jsonRow(t, generated.CatalogIndexDropParamsSchema, map[string]any{
				"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": "i",
				"ignore_not_found": false, "cascade": false,
			}),
			"catalog_index_get": jsonRow(t, generated.CatalogIndexGetParamsSchema, map[string]any{
				"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": "i",
			}),
			"catalog_schema_contents_indexes": jsonRow(t, generated.CatalogSchemaContentsIndexesParamsSchema, map[string]any{
				"attach_opaque_data": attach, "path": []string{"main"},
			}),
			"catalog_table_column_comment_set": jsonRow(t, generated.CatalogTableColumnCommentSetParamsSchema, map[string]any{
				"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": "t",
				"column_name": "x", "comment": "c", "ignore_not_found": false,
			}),
		}
		for method, params := range calls {
			t.Run(catalog+"/"+method, func(t *testing.T) {
				defer params.Release()
				out, err := c.client.CallUnary(context.Background(), method, params, nil)
				if err == nil {
					out.Release()
					t.Fatalf("%s succeeded; an unimplemented method must refuse", method)
				}
				var rpcErr *vgirpc.RpcError
				if !errors.As(err, &rpcErr) {
					t.Fatalf("%s: %T %v, want an *RpcError", method, err, err)
				}
				if rpcErr.Code != "UNIMPLEMENTED" || rpcErr.Kind != "method_not_implemented" {
					t.Fatalf("%s: code %q kind %q, want UNIMPLEMENTED / method_not_implemented (%v)", method, rpcErr.Code, rpcErr.Kind, err)
				}
				if want := method + " is not implemented by this worker"; !strings.Contains(rpcErr.Message, want) {
					t.Fatalf("%s: message %q, want %q", method, rpcErr.Message, want)
				}
			})
		}
	}
}

// wrapRequest wraps an inner request batch in the protocol's single `request`
// binary column (catalog_index_create's params).
func wrapRequest(t *testing.T, inner arrow.RecordBatch) arrow.RecordBatch {
	t.Helper()
	return jsonRow(t, generated.CatalogIndexCreateParamsSchema, map[string]any{"request": mustIPC(t, inner)})
}
