// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
)

// CatalogS3SignedRequest is signed by the application's AWS/R2 SDK. The GET
// lifetime is required; Stable explicitly declares a durable non-expiring URL.
type CatalogS3SignedRequest struct {
	URL       string
	ExpiresAt *time.Time
	Stable    bool
}

// CatalogS3Signer signs a storage request, including its conditional headers and GET URL lifetime.
type CatalogS3Signer func(key, method string, headers http.Header) (CatalogS3SignedRequest, error)

// S3CatalogObjectStorage provides actual HTTP upload and conditional manifest
// operations. Signing uses the application's existing SDK and credentials.
type S3CatalogObjectStorage struct {
	Sign         CatalogS3Signer
	Client       *http.Client
	ObjectPrefix string
	Retention    time.Duration
	mu           sync.Mutex
	locations    map[string]CatalogPublicationLocation
}

func (s *S3CatalogObjectStorage) request(key, method string, headers http.Header, body []byte) (*http.Response, error) {
	signed, err := s.Sign(key, method, headers)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, signed.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header = headers.Clone()
	client := http.Client{}
	if s.Client != nil {
		client = *s.Client
	}
	if client.Timeout == 0 || client.Timeout > 30*time.Second {
		client.Timeout = 30 * time.Second
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return fmt.Errorf("catalog storage redirect refused") }
	return client.Do(req)
}

// Upload writes an immutable Arrow object and returns its signed GET URL.
func (s *S3CatalogObjectStorage) Upload(data []byte, _ *arrow.Schema, encoding string) (string, error) {
	key := s.ObjectPrefix + uuid.NewString() + ".arrow"
	headers := http.Header{"Content-Type": []string{"application/vnd.apache.arrow.stream"}, "If-None-Match": []string{"*"}}
	if encoding != "" {
		headers.Set("Content-Encoding", encoding)
	}
	response, err := s.request(key, "PUT", headers, data)
	if err != nil {
		return "", err
	}
	_ = response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("catalog object PUT failed (%d)", response.StatusCode)
	}
	get, err := s.Sign(key, "GET", http.Header{})
	if err != nil {
		return "", err
	}
	if !get.Stable && get.ExpiresAt == nil {
		return "", fmt.Errorf("S3 catalog signer must declare GET URL lifetime")
	}
	location := CatalogPublicationLocation{ObjectID: key, ExpiresAt: get.ExpiresAt, Stable: get.Stable}
	if s.Retention > 0 {
		until := time.Now().Add(s.Retention)
		location.RetainUntil = &until
	}
	s.mu.Lock()
	if s.locations == nil {
		s.locations = map[string]CatalogPublicationLocation{}
	}
	s.locations[get.URL] = location
	s.mu.Unlock()
	return get.URL, nil
}

// Location consumes the lifetime information recorded for a newly uploaded reference.
func (s *S3CatalogObjectStorage) Location(ref vgirpc.ExternalRef) (CatalogPublicationLocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.locations[ref.URL()]
	if !ok {
		return value, fmt.Errorf("unknown catalog storage location")
	}
	delete(s.locations, ref.URL())
	return value, nil
}

// Get reads a bounded manifest and its ETag, or returns nil bytes when absent.
func (s *S3CatalogObjectStorage) Get(key string) ([]byte, string, error) {
	response, err := s.request(key, "GET", http.Header{}, nil)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == 404 {
		return nil, "", nil
	}
	if response.StatusCode != 200 {
		return nil, "", fmt.Errorf("catalog manifest GET failed (%d)", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024+1))
	if err != nil {
		return nil, "", err
	}
	if len(body) > 64*1024 {
		return nil, "", fmt.Errorf("catalog manifest exceeds 64 KiB")
	}
	etag := response.Header.Get("ETag")
	if etag == "" {
		return nil, "", fmt.Errorf("S3 catalog manifest lacks conditional-operation ETag")
	}
	return body, etag, nil
}

// PutIfAbsent creates a manifest with an atomic If-None-Match condition.
func (s *S3CatalogObjectStorage) PutIfAbsent(key string, body []byte) (bool, error) {
	response, err := s.request(key, "PUT", http.Header{"If-None-Match": []string{"*"}, "Content-Type": []string{"application/json"}}, body)
	if err != nil {
		return false, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == 412 || response.StatusCode == 409 {
		return false, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, fmt.Errorf("catalog manifest PUT failed (%d)", response.StatusCode)
	}
	return true, nil
}

// PutIfMatch replaces a manifest with an atomic If-Match condition.
func (s *S3CatalogObjectStorage) PutIfMatch(key string, body []byte, etag string) (bool, error) {
	response, err := s.request(key, "PUT", http.Header{"If-Match": []string{etag}, "Content-Type": []string{"application/json"}}, body)
	if err != nil {
		return false, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == 412 || response.StatusCode == 409 {
		return false, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, fmt.Errorf("catalog manifest PUT failed (%d)", response.StatusCode)
	}
	return true, nil
}

// DeleteIfMatch deletes a manifest with an atomic If-Match condition.
func (s *S3CatalogObjectStorage) DeleteIfMatch(key, etag string) (bool, error) {
	response, err := s.request(key, "DELETE", http.Header{"If-Match": []string{etag}}, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == 412 || response.StatusCode == 404 {
		return false, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, fmt.Errorf("catalog manifest DELETE failed (%d)", response.StatusCode)
	}
	return true, nil
}

// Repository creates a durable manifest repository with conditional signed-URL renewal.
func (s *S3CatalogObjectStorage) Repository(profile, prefix string, resolver *vgirpc.ExternalLocationConfig) *S3CatalogPublicationRepository {
	if resolver == nil {
		resolver = vgirpc.DefaultExternalLocationConfig(s)
	}
	repository := NewS3CatalogPublicationRepository(profile, s, resolver, s.Location, s, prefix)
	repository.RenewReference = func(before CatalogPublicationManifest) (*CatalogPublicationManifest, error) {
		if before.ObjectID == "" {
			return nil, nil
		}
		signed, err := s.Sign(before.ObjectID, "GET", http.Header{})
		if err != nil {
			return nil, err
		}
		after := before
		after.URL = signed.URL
		after.ExpiresAt = signed.ExpiresAt
		after.Stable = signed.Stable
		after.ManifestID = uuid.NewString()
		return &after, nil
	}
	return repository
}
