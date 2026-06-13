// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package cfdo_test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/cfdo"
	"github.com/Query-farm/vgi-go/vgi/storagetest"
)

// TestCfDoStorage_Live runs the shared FunctionStorage conformance suite
// against a REAL Cloudflare Worker + Durable Object over HTTP (a local
// `wrangler dev` or a deployed instance), proving the client speaks the actual
// protocol — not just the in-process mock. Skipped unless
// VGI_CF_DO_INTEGRATION_URL is set; VGI_CF_DO_TOKEN supplies the bearer key.
func TestCfDoStorage_Live(t *testing.T) {
	url := os.Getenv("VGI_CF_DO_INTEGRATION_URL")
	if url == "" {
		t.Skip("set VGI_CF_DO_INTEGRATION_URL to a running wrangler dev / deployed DO")
	}
	token := os.Getenv("VGI_CF_DO_TOKEN")

	// A fresh random shard per run keeps this run's DO isolated from prior runs.
	var u [16]byte
	_, _ = rand.Read(u[:])
	shard := "att-" + hex.EncodeToString(u[:])

	storagetest.RunConformanceFiltered(t, func(t *testing.T) vgi.FunctionStorage {
		s, err := cfdo.NewStorage(cfdo.Options{URL: url, Token: token})
		if err != nil {
			t.Fatal(err)
		}
		return s.ForShard(shard)
	})
}
