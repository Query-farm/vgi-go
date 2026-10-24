// Copyright 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestAttachDecisionCapabilitiesReachPublicWire(t *testing.T) {
	capabilities := mustIPC(t, jsonRow(t, generated.CatalogCapabilitiesSchema, map[string]any{
		"read": []string{"vgi.constraint.records.read.v1"}, "ddl": []string{}, "write": []string{}, "transaction": []string{},
	}))
	for _, enabled := range []bool{false, true} {
		worker := vgi.NewWorker(vgi.WithCatalogName("test"), vgi.WithAttachValidator(
			func(*vgi.CatalogAttachRequestWire, *vgirpc.CallContext) (*vgi.AttachDecision, error) {
				if !enabled {
					return nil, nil
				}
				return &vgi.AttachDecision{CatalogCapabilities: capabilities, SelectedCapabilities: capabilities}, nil
			},
		))
		client := newContentsClient(t, worker)
		request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, map[string]any{"name": "test"}))
		result := client.call("catalog_attach", generated.CatalogAttachParamsSchema, map[string]any{"request": request})
		for _, name := range []string{"catalog_capabilities", "selected_capabilities"} {
			values := column[*array.Binary](t, result, name)
			if enabled {
				if values.IsNull(0) || !bytes.Equal(values.Value(0), capabilities) {
					t.Fatalf("%s changed on the public wire", name)
				}
			} else if !values.IsNull(0) {
				t.Fatalf("default catalog unexpectedly selected %s", name)
			}
		}
		result.Release()
	}
}
