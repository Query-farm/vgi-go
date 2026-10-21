// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

type httpCatalogPublicationStorage struct {
	mu            sync.Mutex
	url           string
	objects       map[string][]byte
	encodings     map[string]string
	uploads, gets int
}

type publicationRaceRepository struct {
	vgi.CatalogPublicationRepository
	afterCommit func()
}

func (r *publicationRaceRepository) CommitIfAbsent(m vgi.CatalogPublicationManifest) (vgi.CatalogPublicationManifest, error) {
	winner, err := r.CatalogPublicationRepository.CommitIfAbsent(m)
	if err == nil && r.afterCommit != nil {
		r.afterCommit()
	}
	return winner, err
}

func (s *httpCatalogPublicationStorage) Upload(data []byte, _ *arrow.Schema, encoding string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads++
	key := fmt.Sprintf("/%d", s.uploads)
	s.objects[key] = append([]byte(nil), data...)
	s.encodings[key] = encoding
	return s.url + key, nil
}
func (s *httpCatalogPublicationStorage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if data, ok := s.objects[r.URL.Path]; ok {
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		w.Header().Set("Content-Encoding", s.encodings[r.URL.Path])
		w.Write(data)
	} else {
		w.WriteHeader(404)
	}
}

func TestCatalogPublicationHTTPEnvelopeAndAttachmentScope(t *testing.T) {
	storage := &httpCatalogPublicationStorage{objects: map[string][]byte{}, encodings: map[string]string{}}
	server := httptest.NewServer(storage)
	defer server.Close()
	storage.url = server.URL
	resolver := vgirpc.DefaultExternalLocationConfig(storage)
	resolver.URLValidator = func(string) error { return nil }
	baseRepo := vgi.NewMemoryCatalogPublicationRepository("http-test", storage, resolver, func(vgirpc.ExternalRef) (vgi.CatalogPublicationLocation, error) {
		return vgi.CatalogPublicationLocation{Stable: true}, nil
	})
	repo := &publicationRaceRepository{CatalogPublicationRepository: baseRepo}
	var revision atomic.Int64
	policy := &vgi.CatalogSnapshotPolicy{
		Delivery: "published", Repository: repo, Compression: "gzip",
		Identity: func(call *vgi.CatalogContentsCall) (vgi.SnapshotIdentity, error) {
			// Omitted visibility scope must remain local to this sealed attachment.
			return vgi.SnapshotIdentity{CatalogNamespace: "example", SourceRevision: fmt.Sprint(revision.Load()), CatalogVersion: call.CatalogVersion}, nil
		},
	}
	c := newContentsClient(t, contentsWorker(vgi.WithCatalogSnapshotPolicy(policy)), vgirpc.WithClientExternalResolution(resolver))
	attach := c.attach("example")
	first := c.contents(attach.AttachOpaqueData, nil)
	if len(first.Schemas) < 2 || first.Etag == nil {
		t.Fatal("published full inventory was not resolved")
	}
	storage.mu.Lock()
	uploads, gets := storage.uploads, storage.gets
	storage.mu.Unlock()
	if uploads != 1 || gets != 2 {
		t.Fatalf("first call: uploads=%d gets=%d (expected validation and client GET)", uploads, gets)
	}
	conditional := c.contents(attach.AttachOpaqueData, first.Etag)
	if !conditional.NotModified || len(conditional.Schemas) != 0 {
		t.Fatal("conditional publication did not return inline not_modified")
	}
	storage.mu.Lock()
	uploads, gets = storage.uploads, storage.gets
	storage.mu.Unlock()
	if uploads != 1 || gets != 2 {
		t.Fatal("conditional hit fetched or uploaded an object")
	}
	secondAttach := c.attach("example")
	c.contents(secondAttach.AttachOpaqueData, nil)
	storage.mu.Lock()
	uploads = storage.uploads
	storage.mu.Unlock()
	if uploads != 2 {
		t.Fatal("different attachments unexpectedly shared the default publication scope")
	}
	tampered := append([]byte(nil), attach.AttachOpaqueData...)
	tampered[len(tampered)-1] ^= 1
	if err := c.contentsErr(tampered, first.Etag); err == nil {
		t.Fatal("tampered attachment accessed a cached publication")
	}
	revision.Store(1)
	repo.afterCommit = func() { revision.Store(2) }
	if err := c.contentsErr(attach.AttachOpaqueData, nil); err == nil || !strings.Contains(err.Error(), "catalog changed") {
		t.Fatalf("source changed inside CAS but stale publication was served: %v", err)
	}
	repo.afterCommit = nil
	if len(c.contents(attach.AttachOpaqueData, nil).Schemas) < 2 {
		t.Fatal("new revision could not be published")
	}
}
