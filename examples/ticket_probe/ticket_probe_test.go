// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package ticket_probe

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
)

// Catalog discovery binds main.probe without an attach (resolving its
// columns for catalog_schema_contents_tables): OnBind must answer the schema
// rather than demand the attach the row is read from.
func TestOnBindNeedsNoAttach(t *testing.T) {
	resp, err := (&probeFunction{}).OnBind(&vgi.BindParams{})
	if err != nil || resp.OutputSchema == nil || resp.OutputSchema.NumFields() != 2 {
		t.Fatalf("OnBind without an attach: %v, %v", resp, err)
	}
}

func TestProbeRowFromAttachValue(t *testing.T) {
	req := &vgi.CatalogAttachRequestWire{Name: CatalogName}
	decision, err := attach(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	// As a routed sub-catalog attach: framework header, then the catalog's bytes.
	wrapped := append([]byte("\x00route\x00ticket_probe\x00"), decision.AttachOpaqueData...)
	region, digest, err := probeRow(wrapped)
	if err != nil || region != DefaultRegion || digest != APIKeyDigest("") {
		t.Fatalf("probeRow = %q, %q, %v", region, digest, err)
	}
}
