// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package generated_test

import (
	"reflect"
	"testing"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// Every struct in protocol_types.go must derive — through vgi-rpc-go's own
// struct-tag derivation, the one the serializer uses — exactly the schema
// protocol_schemas.go declares for that record. Both files are generated from
// the same vgi-python record, so a difference here is a bug in the type
// generator's mapping (vgi.codegen.go_types), not drift to hand-patch.
func TestGeneratedTypesDeriveGeneratedSchemas(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  any
		schema *arrow.Schema
	}{
		{"CatalogAttachResult", generated.CatalogAttachResult{}, generated.CatalogAttachResultSchema},
		{"CatalogContentsResponse", generated.CatalogContentsResponse{}, generated.CatalogContentsResultSchema},
		{"SchemaContents", generated.SchemaContents{}, generated.SchemaContentsSchema},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := vgirpc.SchemaForStruct(reflect.TypeOf(tc.value))
			if err != nil {
				t.Fatalf("deriving %s: %v", tc.name, err)
			}
			if !got.Equal(tc.schema) {
				t.Fatalf("%s derives\n%v\nbut the protocol declares\n%v", tc.name, got, tc.schema)
			}
		})
	}
}
