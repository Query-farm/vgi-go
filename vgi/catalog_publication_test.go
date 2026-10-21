// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type publicationTestStorage struct {
	mu            sync.Mutex
	url           string
	objects       map[string][]byte
	uploads, gets int
	encodings     map[string]string
}

type publicationMetadataRecord struct {
	arrow.RecordBatch
	metadata arrow.Metadata
}

func (r publicationMetadataRecord) Metadata() arrow.Metadata { return r.metadata }

func TestCatalogPublicationRejectsNonResultEnvelopes(t *testing.T) {
	storage := &publicationTestStorage{objects: map[string][]byte{}}
	server := httptest.NewServer(storage)
	defer server.Close()
	storage.url = server.URL
	resolver := vgirpc.DefaultExternalLocationConfig(storage)
	resolver.URLValidator = func(string) error { return nil }
	repo := NewMemoryCatalogPublicationRepository("test", storage, resolver, func(vgirpc.ExternalRef) (CatalogPublicationLocation, error) {
		return CatalogPublicationLocation{Stable: true}, nil
	})
	manager := NewCatalogSnapshotManager(&CatalogSnapshotPolicy{Delivery: "published", Repository: repo})
	id := SnapshotIdentity{"demo", "1", "tenant", 1}
	manifest, err := manager.Warm(id, func() (generated.CatalogContentsResponse, error) {
		return generated.CatalogContentsResponse{CatalogVersion: 1}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	storage.mu.Lock()
	original := bytes.Clone(storage.objects["/1"])
	storage.mu.Unlock()
	reader, err := ipc.NewReader(bytes.NewReader(original))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing publication row")
	}
	record := reader.RecordBatch()
	for _, key := range []string{vgirpc.MetaLocation, vgirpc.MetaLogLevel, "extra-batch"} {
		t.Run(key, func(t *testing.T) {
			var output bytes.Buffer
			writer := ipc.NewWriter(&output, ipc.WithSchema(record.Schema()))
			var candidate arrow.RecordBatch = record
			if key != "extra-batch" {
				candidate = publicationMetadataRecord{record, arrow.NewMetadata([]string{key}, []string{"INFO"})}
			}
			if err := writer.Write(candidate); err != nil {
				t.Fatal(err)
			}
			if key == "extra-batch" {
				if err := writer.Write(record); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			data := bytes.Clone(output.Bytes())
			storage.mu.Lock()
			storage.objects["/1"] = data
			storage.mu.Unlock()
			bad := manifest
			digest := sha256.Sum256(data)
			bad.SHA256 = hex.EncodeToString(digest[:])
			bad.RawSize = int64(len(data))
			bad.StoredSize = bad.RawSize
			if err := manager.Import(bad, id); err == nil {
				t.Fatal("invalid RPC envelope was imported")
			}
		})
	}
}

func (s *publicationTestStorage) Upload(data []byte, _ *arrow.Schema, encoding string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads++
	key := fmt.Sprintf("/%d", s.uploads)
	s.objects[key] = append([]byte(nil), data...)
	if s.encodings == nil {
		s.encodings = map[string]string{}
	}
	s.encodings[key] = encoding
	return s.url + key, nil
}
func (s *publicationTestStorage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	data, ok := s.objects[r.URL.Path]
	if !ok {
		w.WriteHeader(404)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	if value := s.encodings[r.URL.Path]; value != "" {
		w.Header().Set("Content-Encoding", value)
	}
	w.Write(data)
}
func (s *publicationTestStorage) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploads, s.gets
}

func TestCatalogPublicationRecoveryAndIsolation(t *testing.T) {
	storage := &publicationTestStorage{objects: map[string][]byte{}}
	server := httptest.NewServer(storage)
	defer server.Close()
	storage.url = server.URL
	resolver := vgirpc.DefaultExternalLocationConfig(storage)
	resolver.URLValidator = func(string) error { return nil }
	repository := NewMemoryCatalogPublicationRepository("test-identity", storage, resolver, func(vgirpc.ExternalRef) (CatalogPublicationLocation, error) {
		return CatalogPublicationLocation{Stable: true}, nil
	})
	policy := &CatalogSnapshotPolicy{Delivery: "published", Repository: repository}
	manager := NewCatalogSnapshotManager(policy)
	id := SnapshotIdentity{"demo", "deploy-1", "tenant-a", 1}
	builds := 0
	build := func() (generated.CatalogContentsResponse, error) {
		builds++
		item, err := SerializeSchemaInfo(&SchemaInfo{Path: SchemaPath{"main"}})
		if err != nil {
			return generated.CatalogContentsResponse{}, err
		}
		return encodeCatalogStreams(1, nil, []generated.SchemaContents{{Path: []string{"main"}, Schema: item}})
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		wg.Go(func() { _, err := manager.Warm(id, build); failures <- err })
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if uploads, gets := storage.counts(); builds != 1 || uploads != 1 || gets != 1 {
		t.Fatalf("cold counters builds=%d uploads=%d gets=%d", builds, uploads, gets)
	}
	first, err := manager.Warm(id, build)
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewCatalogSnapshotManager(policy)
	second, err := restarted.Warm(id, build)
	if err != nil {
		t.Fatal(err)
	}
	if first.ManifestID != second.ManifestID {
		t.Fatal("restart rebuilt unchanged manifest")
	}
	if uploads, gets := storage.counts(); builds != 1 || uploads != 1 || gets != 2 {
		t.Fatalf("restart counters builds=%d uploads=%d gets=%d", builds, uploads, gets)
	}
	other := id
	other.VisibilityScope = "tenant-b"
	if _, err = restarted.Warm(other, build); err != nil {
		t.Fatal(err)
	}
	if uploads, gets := storage.counts(); builds != 2 || uploads != 2 || gets != 3 {
		t.Fatalf("scope counters builds=%d uploads=%d gets=%d", builds, uploads, gets)
	}
	if err = manager.Invalidate(first.CacheKey); err != nil {
		t.Fatal(err)
	}
	if _, err = manager.Warm(id, build); err != nil {
		t.Fatal(err)
	}
	if uploads, gets := storage.counts(); builds != 3 || uploads != 3 || gets != 4 {
		t.Fatalf("invalidation counters builds=%d uploads=%d gets=%d", builds, uploads, gets)
	}
}

func TestCatalogPublicationCapsAndCorruption(t *testing.T) {
	storage := &publicationTestStorage{objects: map[string][]byte{}}
	server := httptest.NewServer(storage)
	defer server.Close()
	storage.url = server.URL
	resolver := vgirpc.DefaultExternalLocationConfig(storage)
	resolver.URLValidator = func(string) error { return nil }
	repo := NewMemoryCatalogPublicationRepository("test-identity", storage, resolver, func(vgirpc.ExternalRef) (CatalogPublicationLocation, error) {
		return CatalogPublicationLocation{Stable: true}, nil
	})
	policy := &CatalogSnapshotPolicy{Delivery: "published", Repository: repo, MaxRawBytes: 10}
	id := SnapshotIdentity{"demo", "1", "tenant", 1}
	build := func() (generated.CatalogContentsResponse, error) {
		return generated.CatalogContentsResponse{CatalogVersion: 1}, nil
	}
	if _, err := NewCatalogSnapshotManager(policy).Warm(id, build); err == nil {
		t.Fatal("oversized publication succeeded")
	}
	if uploads, _ := storage.counts(); uploads != 0 {
		t.Fatal("oversized publication uploaded")
	}
	policy.MaxRawBytes = 0
	first, err := NewCatalogSnapshotManager(policy).Warm(id, build)
	if err != nil {
		t.Fatal(err)
	}
	storage.mu.Lock()
	storage.objects["/1"] = []byte{1, 2, 3}
	storage.mu.Unlock()
	second, err := NewCatalogSnapshotManager(policy).Warm(id, build)
	if err != nil {
		t.Fatal(err)
	}
	if first.URL == second.URL {
		t.Fatal("damaged publication reused")
	}
	if uploads, _ := storage.counts(); uploads != 2 {
		t.Fatalf("uploads=%d", uploads)
	}
}

func TestCatalogPublicationCompression(t *testing.T) {
	for _, compression := range []string{"gzip", "zstd"} {
		t.Run(compression, func(t *testing.T) {
			storage := &publicationTestStorage{objects: map[string][]byte{}}
			server := httptest.NewServer(storage)
			defer server.Close()
			storage.url = server.URL
			resolver := vgirpc.DefaultExternalLocationConfig(storage)
			resolver.URLValidator = func(string) error { return nil }
			repo := NewMemoryCatalogPublicationRepository("test", storage, resolver, func(vgirpc.ExternalRef) (CatalogPublicationLocation, error) {
				return CatalogPublicationLocation{Stable: true}, nil
			})
			policy := &CatalogSnapshotPolicy{Delivery: "published", Repository: repo, Compression: compression}
			id := SnapshotIdentity{"demo", "1", "tenant", 1}
			build := func() (generated.CatalogContentsResponse, error) {
				return generated.CatalogContentsResponse{CatalogVersion: 1}, nil
			}
			manifest, err := NewCatalogSnapshotManager(policy).Warm(id, build)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.StoredSize >= manifest.RawSize {
				t.Fatalf("compression: stored=%d raw=%d", manifest.StoredSize, manifest.RawSize)
			}
			if _, err = NewCatalogSnapshotManager(policy).Warm(id, build); err != nil {
				t.Fatal(err)
			}
			if uploads, gets := storage.counts(); uploads != 1 || gets != 2 {
				t.Fatalf("uploads=%d gets=%d", uploads, gets)
			}
		})
	}
}

func TestCatalogPublicationS3RecoveryAndRenewal(t *testing.T) {
	type object struct {
		body           []byte
		etag, encoding string
	}
	var mu sync.Mutex
	objects := map[string]object{}
	objectUploads, objectGets, revision, signatures := 0, 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		current, exists := objects[r.URL.Path]
		switch r.Method {
		case "PUT":
			if (r.Header.Get("If-None-Match") == "*" && exists) || (r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != current.etag) {
				w.WriteHeader(412)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			revision++
			objects[r.URL.Path] = object{body, fmt.Sprintf("\"%d\"", revision), r.Header.Get("Content-Encoding")}
			if r.Header.Get("Content-Type") == "application/vnd.apache.arrow.stream" {
				objectUploads++
			}
			w.WriteHeader(200)
		case "GET":
			if !exists {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("ETag", current.etag)
			if current.encoding != "" {
				w.Header().Set("Content-Encoding", current.encoding)
			}
			if len(r.URL.Path) >= 6 && r.URL.Path[len(r.URL.Path)-6:] == ".arrow" {
				objectGets++
			}
			w.Write(current.body)
		case "DELETE":
			if !exists || r.Header.Get("If-Match") != current.etag {
				w.WriteHeader(412)
				return
			}
			delete(objects, r.URL.Path)
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	signer := func(key, method string, headers http.Header) (CatalogS3SignedRequest, error) {
		mu.Lock()
		signatures++
		signature := signatures
		mu.Unlock()
		expiry := time.Now().Add(2 * time.Hour)
		return CatalogS3SignedRequest{URL: fmt.Sprintf("%s/%s?signed=%d", server.URL, key, signature), ExpiresAt: &expiry}, nil
	}
	makePolicy := func() *CatalogSnapshotPolicy {
		storage := &S3CatalogObjectStorage{Sign: signer, ObjectPrefix: "results/"}
		resolver := vgirpc.DefaultExternalLocationConfig(storage)
		resolver.URLValidator = func(string) error { return nil }
		return &CatalogSnapshotPolicy{Delivery: "published", Compression: "gzip", Repository: storage.Repository("s3-test", "manifests/", resolver)}
	}
	id := SnapshotIdentity{"demo", "1", "tenant", 1}
	builds := 0
	build := func() (generated.CatalogContentsResponse, error) {
		builds++
		return generated.CatalogContentsResponse{CatalogVersion: 1}, nil
	}
	first, err := NewCatalogSnapshotManager(makePolicy()).Warm(id, build)
	if err != nil {
		t.Fatal(err)
	}
	// Persist an expired URL to simulate a process returning after its signed URL expired.
	mu.Lock()
	key := "/manifests/" + first.CacheKey + ".json"
	current := objects[key]
	expired := first
	expiration := time.Now().Add(-time.Hour)
	expired.ExpiresAt = &expiration
	current.body, err = json.Marshal(expired)
	objects[key] = current
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCatalogSnapshotManager(makePolicy()).Warm(id, build)
	if err != nil {
		t.Fatal(err)
	}
	if second.URL == first.URL || second.ManifestID == first.ManifestID || second.ObjectID != first.ObjectID || second.SHA256 != first.SHA256 {
		t.Fatal("renewal did not preserve immutable content with a fresh URL/manifest")
	}
	mu.Lock()
	uploads, gets := objectUploads, objectGets
	mu.Unlock()
	if builds != 1 || uploads != 1 || gets != 2 {
		t.Fatalf("builds=%d uploads=%d gets=%d", builds, uploads, gets)
	}
	third, err := NewCatalogSnapshotManager(makePolicy()).Warm(id, build)
	if err != nil {
		t.Fatal(err)
	}
	if third.ManifestID != second.ManifestID {
		t.Fatal("renewed manifest was not durably committed")
	}
}
