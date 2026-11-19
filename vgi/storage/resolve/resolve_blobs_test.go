// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package resolve_test

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/cfdo"
	"github.com/Query-farm/vgi-go/vgi/storage/resolve"
)

// cloudflare-do is always wrapped in the value envelope (its values are
// capped), vendor or not, so every process stores the same format.
func TestFromEnv_CloudflareDOAlwaysEnveloped(t *testing.T) {
	t.Setenv(resolve.EnvVar, "cloudflare-do")
	t.Setenv("VGI_CF_DO_URL", "https://storage.example.invalid")
	t.Setenv("VGI_BLOB_VENDOR_URL", "")
	t.Setenv(vgi.StorageExternalizeEnv, "")
	t.Setenv(vgi.StorageExternalizeBytesEnv, "")
	t.Setenv(vgi.StorageExternalizeBytesAliasEnv, "")
	s, err := resolve.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w, ok := s.(*vgi.ExternalizingStorage)
	if !ok {
		t.Fatalf("cloudflare-do storage is %T, want *vgi.ExternalizingStorage", s)
	}
	if _, ok := w.Base().(*cfdo.Storage); !ok {
		t.Fatalf("wrapped backend is %T", w.Base())
	}
	if w.Threshold() != vgi.DefaultStorageExternalizeBytes || w.Threshold() > cfdo.MaxValueBytes {
		t.Fatalf("threshold = %d", w.Threshold())
	}
}

// SQLite is wrapped only on request, and the request needs the vendor.
func TestFromEnv_StorageExternalizeOptIn(t *testing.T) {
	t.Setenv(resolve.EnvVar, "memory")
	t.Setenv("VGI_BLOB_VENDOR_URL", "")
	t.Setenv(vgi.StorageExternalizeEnv, "")
	s, err := resolve.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if _, wrapped := s.(*vgi.ExternalizingStorage); wrapped {
		t.Fatal("SQLite wrapped without VGI_STORAGE_EXTERNALIZE")
	}
	s.Close()

	t.Setenv(vgi.StorageExternalizeEnv, "1")
	if _, err := resolve.FromEnv(); err == nil {
		t.Fatal("VGI_STORAGE_EXTERNALIZE=1 without VGI_BLOB_VENDOR_URL accepted")
	}

	t.Setenv("VGI_BLOB_VENDOR_URL", "http://127.0.0.1:1")
	t.Setenv("VGI_BLOB_VENDOR_TOKEN", "tok")
	t.Setenv(vgi.StorageExternalizeBytesEnv, "4096")
	s, err = resolve.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if w, ok := s.(*vgi.ExternalizingStorage); !ok || w.Threshold() != 4096 {
		t.Fatalf("opted-in SQLite storage is %T", s)
	}
}
