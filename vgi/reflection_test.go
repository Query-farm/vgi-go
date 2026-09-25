// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
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
