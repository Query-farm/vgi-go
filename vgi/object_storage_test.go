// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// fakeObjectStorage is a direct backend with every role and a grants switch.
type fakeObjectStorage struct{ grants bool }

func (fakeObjectStorage) Upload([]byte, *arrow.Schema, string) (string, error) { return "", nil }
func (fakeObjectStorage) PutObject([]byte) (string, error)                     { return "obj", nil }
func (fakeObjectStorage) Get(string, int64) ([]byte, error)                    { return nil, nil }
func (fakeObjectStorage) GenerateUploadURL(int64, string) (vgirpc.UploadURL, error) {
	return vgirpc.UploadURL{ObjectID: "obj", ExpiresAt: time.Now()}, nil
}
func (f fakeObjectStorage) UploadGrantsEnabled() bool { return f.grants }
func (fakeObjectStorage) Describe() string            { return "fake" }

func clearStorageEnv(t *testing.T) {
	for _, k := range []string{vgirpc.EnvStorageBackend, vgirpc.EnvBlobVendorURL, vgirpc.EnvStorageS3Bucket,
		vgirpc.EnvStorageGCSBucket, vgirpc.EnvMaxRequestBytes, vgirpc.EnvMaxUploadBytes} {
		t.Setenv(k, "")
	}
}

func capabilities(t *testing.T, hs *vgirpc.HttpServer) http.Header {
	t.Helper()
	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, httptest.NewRequest(http.MethodOptions, "/__capabilities__", nil))
	return rec.Header()
}

func TestConfigureStorageDirectBackend(t *testing.T) {
	for _, grants := range []bool{true, false} {
		clearStorageEnv(t)
		RegisterObjectStorageBackend("s3", func(func(string) string) (ObjectStorage, error) {
			return fakeObjectStorage{grants: grants}, nil
		})
		t.Cleanup(func() {
			objectStorageMu.Lock()
			delete(objectStorageFactories, "s3")
			objectStorageMu.Unlock()
		})
		t.Setenv(vgirpc.EnvStorageS3Bucket, "bkt")
		t.Setenv(vgirpc.EnvMaxRequestBytes, "32768")
		w := NewWorker()
		hs := vgirpc.NewHttpServer(vgirpc.NewServer())
		config, err := w.configureStorage(hs, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := config.Storage.(fakeObjectStorage); !ok {
			t.Fatalf("storage %T", config.Storage)
		}
		refBytes, err := argumentsRefAdvertised(config, func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		if (refBytes > 0) != grants {
			t.Errorf("grants=%v: VGI-Arguments-Ref-Bytes %d", grants, refBytes)
		}
		hdr := capabilities(t, hs)
		if (hdr.Get("VGI-Upload-URL-Support") != "") != grants {
			t.Errorf("grants=%v: VGI-Upload-URL-Support %q", grants, hdr.Get("VGI-Upload-URL-Support"))
		}
	}
}

func TestConfigureStorageErrors(t *testing.T) {
	clearStorageEnv(t)
	t.Setenv(vgirpc.EnvStorageBackend, "gcs")
	t.Setenv(vgirpc.EnvStorageGCSBucket, "bkt")
	_, err := NewWorker().configureStorage(vgirpc.NewHttpServer(vgirpc.NewServer()), false)
	if err == nil || !strings.Contains(err.Error(), "vgi/storage/objectstore/gcs") {
		t.Errorf("unlinked backend: %v", err)
	}
	clearStorageEnv(t)
	t.Setenv(vgirpc.EnvBlobVendorURL, "https://vendor.example")
	t.Setenv(vgirpc.EnvStorageS3Bucket, "bkt")
	_, err = NewWorker().configureStorage(vgirpc.NewHttpServer(vgirpc.NewServer()), false)
	if err == nil || !strings.Contains(err.Error(), "VGI_BLOB_VENDOR_URL and VGI_STORAGE_S3_BUCKET") {
		t.Errorf("conflict: %v", err)
	}
	clearStorageEnv(t)
	if config, err := NewWorker().configureStorage(vgirpc.NewHttpServer(vgirpc.NewServer()), false); err != nil || config != nil {
		t.Errorf("no storage: %v %v", config, err)
	}
}
