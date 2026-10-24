// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"net/http"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// SnapshotIdentity identifies a committed inventory without constructing it.
// VisibilityScope must include every principal and option affecting metadata.
// An empty scope in a Worker policy defaults to the validated sealed attachment.
type SnapshotIdentity struct {
	CatalogNamespace string `json:"catalog_namespace"`
	SourceRevision   string `json:"source_revision"`
	VisibilityScope  string `json:"visibility_scope"`
	CatalogVersion   int64  `json:"catalog_version"`
}

// CatalogPublicationManifest describes a validated, immutable catalog object and its wire contract.
type CatalogPublicationManifest struct {
	CacheKey          string     `json:"cache_key"`
	ManifestID        string     `json:"manifest_id"`
	ProtocolName      string     `json:"protocol_name"`
	WireRevision      string     `json:"wire_revision"`
	ContractHash      string     `json:"contract_hash"`
	CodecRevision     string     `json:"codec_revision"`
	CatalogVersion    int64      `json:"catalog_version"`
	Etag              string     `json:"etag"`
	SHA256            string     `json:"sha256"`
	RawSize           int64      `json:"raw_size"`
	StoredSize        int64      `json:"stored_size"`
	URL               string     `json:"url"`
	ObjectID          string     `json:"object_id,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	RetainUntil       *time.Time `json:"retain_until,omitempty"`
	Stable            bool       `json:"stable"`
	FormatVersion     int        `json:"format_version"`
	ValidatorRevision int        `json:"validator_revision"`
}

// CatalogPublicationLocation declares the lifetime and storage identity of an external object.
type CatalogPublicationLocation struct {
	ObjectID    string
	ExpiresAt   *time.Time
	RetainUntil *time.Time
	// Stable declares a durable, non-expiring URL when ExpiresAt is nil.
	Stable bool
}

// CatalogPublicationRepository stores manifests and supplies external object storage and readback policy.
// Commit and invalidation operations must be atomic for each cache key.
type CatalogPublicationRepository interface {
	// Profile returns a stable identifier for the storage configuration.
	Profile() string
	// Storage returns the external object uploader.
	Storage() vgirpc.ExternalStorage
	// Resolver returns the URL access and fetch policy used for readback.
	Resolver() *vgirpc.ExternalLocationConfig
	// Lookup returns the current manifest, or nil when the key is absent.
	Lookup(key string) (*CatalogPublicationManifest, error)
	// CommitIfAbsent stores a validated manifest unless one exists, returning the winner.
	CommitIfAbsent(manifest CatalogPublicationManifest) (CatalogPublicationManifest, error)
	// Invalidate removes the manifest only if its ID still matches.
	Invalidate(key, manifestID string) (bool, error)
	// Location returns the lifetime and storage identity of an uploaded reference.
	Location(ref vgirpc.ExternalRef) (CatalogPublicationLocation, error)
}

// CatalogPublicationRenewer optionally refreshes an expiring location without changing its catalog content.
type CatalogPublicationRenewer interface {
	// Renew refreshes a location while preserving immutable identity and content.
	Renew(CatalogPublicationManifest) (*CatalogPublicationManifest, error)
}

// CatalogSnapshotPolicy configures opt-in publication, identity, limits, and failure handling.
type CatalogSnapshotPolicy struct {
	Delivery             string // ordinary (default) or published
	Repository           CatalogPublicationRepository
	Identity             func(*CatalogContentsCall) (SnapshotIdentity, error)
	Etag                 func(*CatalogContentsCall) (*string, error) // optional cheap conditional short circuit
	OnPublicationFailure string                                      // error (default) or ordinary
	Compression          string                                      // identity (default), gzip or zstd
	MaxRawBytes          int64
	MaxStoredBytes       int64
	MaxCachedManifests   int
	ReferenceGrace       time.Duration
}

// WithCatalogSnapshotPolicy enables immutable external catalog delivery.
func WithCatalogSnapshotPolicy(policy *CatalogSnapshotPolicy) WorkerOption {
	return func(w *Worker) {
		w.catalogSnapshotPolicy = policy
		if policy != nil {
			w.catalogSnapshotManager = NewCatalogSnapshotManager(policy)
		}
	}
}

// MemoryCatalogPublicationRepository keeps manifests in process while storing catalog objects externally.
type MemoryCatalogPublicationRepository struct {
	mu        sync.Mutex
	manifests map[string]CatalogPublicationManifest
	profile   string
	storage   vgirpc.ExternalStorage
	resolver  *vgirpc.ExternalLocationConfig
	location  func(vgirpc.ExternalRef) (CatalogPublicationLocation, error)
}

// NewMemoryCatalogPublicationRepository creates a process-local manifest repository.
// The location callback must declare the lifetime of every uploaded reference.
func NewMemoryCatalogPublicationRepository(profile string, storage vgirpc.ExternalStorage, resolver *vgirpc.ExternalLocationConfig, location func(vgirpc.ExternalRef) (CatalogPublicationLocation, error)) *MemoryCatalogPublicationRepository {
	return &MemoryCatalogPublicationRepository{manifests: map[string]CatalogPublicationManifest{}, profile: profile, storage: storage, resolver: resolver, location: location}
}

// Profile returns the storage configuration identifier used in snapshot cache keys.
func (r *MemoryCatalogPublicationRepository) Profile() string { return r.profile }

// Storage returns the external object uploader.
func (r *MemoryCatalogPublicationRepository) Storage() vgirpc.ExternalStorage { return r.storage }

// Resolver returns the URL access policy and HTTP client used for readback validation.
func (r *MemoryCatalogPublicationRepository) Resolver() *vgirpc.ExternalLocationConfig {
	return r.resolver
}

// Location returns the storage identity and lifetime of an uploaded reference.
func (r *MemoryCatalogPublicationRepository) Location(ref vgirpc.ExternalRef) (CatalogPublicationLocation, error) {
	if r.location == nil {
		return CatalogPublicationLocation{}, fmt.Errorf("publication storage must declare URL lifetime")
	}
	return r.location(ref)
}

// Lookup returns a manifest copy, or nil when the cache key is absent.
func (r *MemoryCatalogPublicationRepository) Lookup(key string) (*CatalogPublicationManifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.manifests[key]
	if !ok {
		return nil, nil
	}
	return &m, nil
}

// CommitIfAbsent stores a manifest unless its key already exists, returning the winning manifest.
func (r *MemoryCatalogPublicationRepository) CommitIfAbsent(m CatalogPublicationManifest) (CatalogPublicationManifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.manifests[m.CacheKey]; ok {
		return old, nil
	}
	r.manifests[m.CacheKey] = m
	return m, nil
}

// Invalidate removes a manifest only when its ID still matches the supplied ID.
func (r *MemoryCatalogPublicationRepository) Invalidate(key, id string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.manifests[key]; ok && m.ManifestID == id {
		delete(r.manifests, key)
		return true, nil
	}
	return false, nil
}

// CatalogManifestObjectStore maps onto S3/R2 conditional object operations.
// Get returns nil for a missing object; conditions must be enforced atomically.
type CatalogManifestObjectStore interface {
	// Get returns object bytes and its conditional-operation ETag, or nil bytes when absent.
	Get(key string) (body []byte, etag string, err error)
	// PutIfAbsent atomically writes an object only when its key is absent.
	PutIfAbsent(key string, body []byte) (created bool, err error)
	// DeleteIfMatch atomically deletes an object only when its ETag matches.
	DeleteIfMatch(key, etag string) (deleted bool, err error)
	// PutIfMatch atomically replaces an object only when its ETag matches.
	PutIfMatch(key string, body []byte, etag string) (bool, error)
}

// S3CatalogPublicationRepository persists manifests with conditional S3-compatible object operations.
type S3CatalogPublicationRepository struct {
	*MemoryCatalogPublicationRepository // storage configuration only; manifests live in Objects
	Objects                             CatalogManifestObjectStore
	Prefix                              string
	RenewReference                      func(CatalogPublicationManifest) (*CatalogPublicationManifest, error)
}

// NewS3CatalogPublicationRepository creates a durable manifest repository under prefix.
// Objects must enforce conditional writes and deletes atomically.
func NewS3CatalogPublicationRepository(profile string, storage vgirpc.ExternalStorage, resolver *vgirpc.ExternalLocationConfig, location func(vgirpc.ExternalRef) (CatalogPublicationLocation, error), objects CatalogManifestObjectStore, prefix string) *S3CatalogPublicationRepository {
	return &S3CatalogPublicationRepository{MemoryCatalogPublicationRepository: NewMemoryCatalogPublicationRepository(profile, storage, resolver, location), Objects: objects, Prefix: prefix}
}
func (r *S3CatalogPublicationRepository) objectKey(key string) (string, error) {
	if len(key) != 64 {
		return "", fmt.Errorf("invalid publication key")
	}
	if _, err := hex.DecodeString(key); err != nil {
		return "", err
	}
	return r.Prefix + key + ".json", nil
}

// Lookup reads a durable manifest, or returns nil when the cache key is absent.
func (r *S3CatalogPublicationRepository) Lookup(key string) (*CatalogPublicationManifest, error) {
	name, err := r.objectKey(key)
	if err != nil {
		return nil, err
	}
	body, _, err := r.Objects.Get(name)
	if err != nil || body == nil {
		return nil, err
	}
	if len(body) > 64*1024 {
		return nil, fmt.Errorf("catalog manifest exceeds 64 KiB")
	}
	var m CatalogPublicationManifest
	if err = json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// CommitIfAbsent conditionally stores a manifest and returns the winner if another writer committed first.
func (r *S3CatalogPublicationRepository) CommitIfAbsent(m CatalogPublicationManifest) (CatalogPublicationManifest, error) {
	name, err := r.objectKey(m.CacheKey)
	if err != nil {
		return m, err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return m, err
	}
	created, err := r.Objects.PutIfAbsent(name, body)
	if err != nil || created {
		return m, err
	}
	winner, err := r.Lookup(m.CacheKey)
	if err != nil {
		return m, err
	}
	if winner == nil {
		return m, fmt.Errorf("catalog manifest changed during commit")
	}
	return *winner, nil
}

// Renew refreshes a reference and conditionally persists it without replacing a concurrent manifest.
func (r *S3CatalogPublicationRepository) Renew(before CatalogPublicationManifest) (*CatalogPublicationManifest, error) {
	if r.RenewReference == nil {
		return nil, nil
	}
	name, err := r.objectKey(before.CacheKey)
	if err != nil {
		return nil, err
	}
	body, etag, err := r.Objects.Get(name)
	if err != nil || body == nil {
		return nil, err
	}
	var current CatalogPublicationManifest
	if err = json.Unmarshal(body, &current); err != nil {
		return nil, err
	}
	if current.ManifestID != before.ManifestID {
		return &current, nil
	}
	after, err := r.RenewReference(before)
	if err != nil || after == nil {
		return nil, err
	}
	if err = checkPublicationRenewal(before, *after); err != nil {
		return nil, err
	}
	data, err := json.Marshal(after)
	if err != nil {
		return nil, err
	}
	ok, err := r.Objects.PutIfMatch(name, data, etag)
	if err != nil {
		return nil, err
	}
	if ok {
		return after, nil
	}
	return r.Lookup(before.CacheKey)
}
func checkPublicationRenewal(before, after CatalogPublicationManifest) error {
	normalize := func(p CatalogPublicationManifest) string {
		p.URL = ""
		p.ExpiresAt = nil
		p.ManifestID = ""
		body, _ := json.Marshal(p)
		return string(body)
	}
	if normalize(before) != normalize(after) {
		return fmt.Errorf("renewal changed immutable catalog identity")
	}
	return nil
}

// Invalidate deletes a durable manifest only if its ID and storage ETag still match.
func (r *S3CatalogPublicationRepository) Invalidate(key, id string) (bool, error) {
	name, err := r.objectKey(key)
	if err != nil {
		return false, err
	}
	body, etag, err := r.Objects.Get(name)
	if err != nil || body == nil {
		return false, err
	}
	var m CatalogPublicationManifest
	if err = json.Unmarshal(body, &m); err != nil {
		return false, err
	}
	if m.ManifestID != id {
		return false, nil
	}
	return r.Objects.DeleteIfMatch(name, etag)
}

const catalogCodecRevision = "catalog-collections-1"
const catalogPublicationMax = int64(64 * 1024 * 1024)

func publicationContractHash() string { return generated.CatalogContractHash }

type publicationPending struct {
	done     chan struct{}
	manifest CatalogPublicationManifest
	err      error
}

// CatalogSnapshotManager validates, caches, and coalesces immutable catalog publications.
type CatalogSnapshotManager struct {
	policy  *CatalogSnapshotPolicy
	mu      sync.Mutex
	cached  map[string]CatalogPublicationManifest
	pending map[string]*publicationPending
}

// NewCatalogSnapshotManager creates a manager using the supplied publication policy.
func NewCatalogSnapshotManager(policy *CatalogSnapshotPolicy) *CatalogSnapshotManager {
	return &CatalogSnapshotManager{policy: policy, cached: map[string]CatalogPublicationManifest{}, pending: map[string]*publicationPending{}}
}

// Key derives a cache key from the catalog identity, wire contract, and storage profile.
func (m *CatalogSnapshotManager) Key(id SnapshotIdentity) (string, error) {
	if id.CatalogNamespace == "" || id.SourceRevision == "" || id.VisibilityScope == "" || m.policy.Repository == nil {
		return "", fmt.Errorf("published catalogs require repository, namespace, source revision and visibility scope")
	}
	body, _ := json.Marshal([]any{"vgi.v2", "2.2", publicationContractHash(), catalogCodecRevision, id, m.policy.Repository.Profile(), m.policy.Compression})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
func (m *CatalogSnapshotManager) limits() (int64, int64) {
	raw, stored := m.policy.MaxRawBytes, m.policy.MaxStoredBytes
	if raw <= 0 {
		raw = catalogPublicationMax
	}
	if stored <= 0 {
		stored = catalogPublicationMax
	}
	return raw, stored
}
func (m *CatalogSnapshotManager) usable(p CatalogPublicationManifest) bool {
	grace := m.policy.ReferenceGrace
	if grace <= 0 {
		grace = time.Minute
	}
	deadline := time.Now().Add(grace)
	return (p.Stable || p.ExpiresAt != nil) && (p.ExpiresAt == nil || p.ExpiresAt.After(deadline)) && (p.RetainUntil == nil || p.RetainUntil.After(deadline))
}

// Invalidate waits for an active publication, then removes its local and durable manifest.
func (m *CatalogSnapshotManager) Invalidate(key string) error {
	m.mu.Lock()
	pending := m.pending[key]
	m.mu.Unlock()
	if pending != nil {
		<-pending.done
	}
	m.mu.Lock()
	delete(m.cached, key)
	m.mu.Unlock()
	p, err := m.policy.Repository.Lookup(key)
	if err != nil || p == nil {
		return err
	}
	_, err = m.policy.Repository.Invalidate(key, p.ManifestID)
	return err
}

// Import validates an external manifest and commits it, validating any concurrent winner before caching it.
func (m *CatalogSnapshotManager) Import(p CatalogPublicationManifest, id SnapshotIdentity) error {
	key, err := m.Key(id)
	if err != nil {
		return err
	}
	if err = m.validate(p, key, id, nil); err != nil {
		return err
	}
	winner, err := m.policy.Repository.CommitIfAbsent(p)
	if err != nil {
		return err
	}
	if winner.ManifestID != p.ManifestID {
		if err = m.validate(winner, key, id, nil); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.remember(winner)
	m.mu.Unlock()
	return nil
}
func (m *CatalogSnapshotManager) remember(p CatalogPublicationManifest) {
	limit := m.policy.MaxCachedManifests
	if limit <= 0 {
		limit = 128
	}
	if len(m.cached) >= limit {
		for key := range m.cached {
			delete(m.cached, key)
			break
		}
	}
	m.cached[p.CacheKey] = p
}

// Warm returns a validated publication, recovering or building it once per cache key.
// The caller must ensure id still identifies the current catalog before serving the result.
func (m *CatalogSnapshotManager) Warm(id SnapshotIdentity, build func() (generated.CatalogContentsResponse, error)) (CatalogPublicationManifest, error) {
	key, err := m.Key(id)
	if err != nil {
		return CatalogPublicationManifest{}, err
	}
	m.mu.Lock()
	if p, ok := m.cached[key]; ok && m.usable(p) {
		m.mu.Unlock()
		return p, nil
	}
	if p, ok := m.pending[key]; ok {
		m.mu.Unlock()
		<-p.done
		return p.manifest, p.err
	}
	p := &publicationPending{done: make(chan struct{})}
	m.pending[key] = p
	m.mu.Unlock()
	p.manifest, p.err = m.recoverOrPublish(key, id, build)
	m.mu.Lock()
	if p.err == nil {
		m.remember(p.manifest)
	}
	delete(m.pending, key)
	close(p.done)
	m.mu.Unlock()
	return p.manifest, p.err
}

var catalogOuterSchema = arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.BinaryTypes.Binary}}, nil)

func (m *CatalogSnapshotManager) validate(p CatalogPublicationManifest, key string, id SnapshotIdentity, expected *generated.CatalogContentsResponse) error {
	rawCap, storedCap := m.limits()
	if p.CacheKey != key || p.ManifestID == "" || p.ProtocolName != "vgi.v2" || p.WireRevision != "2.2" || p.ContractHash != publicationContractHash() || p.CodecRevision != catalogCodecRevision || p.FormatVersion != 1 || p.ValidatorRevision != 1 || p.CatalogVersion != id.CatalogVersion || p.RawSize <= 0 || p.RawSize > rawCap || p.StoredSize <= 0 || p.StoredSize > storedCap || len(p.SHA256) != 64 || !m.usable(p) {
		return fmt.Errorf("invalid or expired catalog publication manifest")
	}
	ref, err := vgirpc.NewExternalRef(p.URL, p.SHA256)
	if err != nil {
		return err
	}
	pointer, meta := ref.PointerBatch(catalogOuterSchema)
	defer pointer.Release()
	config := m.policy.Repository.Resolver()
	if config == nil {
		return fmt.Errorf("publication repository requires bounded resolver")
	}
	bounded := *config
	client := http.Client{Timeout: 30 * time.Second}
	if config.HTTPClient != nil {
		client = *config.HTTPClient
		if client.Timeout <= 0 || client.Timeout > 30*time.Second {
			client.Timeout = 30 * time.Second
		}
	}
	bounded.HTTPClient = &client
	bounded.MaxFetchBytes = storedCap
	if config.MaxFetchBytes > 0 {
		bounded.MaxFetchBytes = min(storedCap, config.MaxFetchBytes)
	}
	bounded.MaxDecompressedBytes = rawCap
	if config.MaxDecompressedBytes > 0 {
		bounded.MaxDecompressedBytes = min(rawCap, config.MaxDecompressedBytes)
	}
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client.Transport = catalogReadbackTransport{next: transport, rawCap: bounded.MaxDecompressedBytes, storedCap: bounded.MaxFetchBytes, manifest: p}
	// The transport enforces the stored cap and returns the same decoded bytes
	// it validated. RPC still performs URL/redirect policy and SHA-256 checks.
	bounded.MaxFetchBytes = bounded.MaxDecompressedBytes
	resolved, _, err := vgirpc.ResolveExternalLocation(pointer, meta, &bounded)
	if err != nil {
		return err
	}
	defer resolved.Release()
	if resolved.NumRows() != 1 || !resolved.Schema().Equal(catalogOuterSchema) {
		return fmt.Errorf("invalid published RPC envelope")
	}
	result := resolved.Column(0).(*array.Binary)
	if result.IsNull(0) {
		return fmt.Errorf("published catalog has null result")
	}
	response, err := decodeCatalogPublication(result.Value(0))
	if err != nil {
		return err
	}
	if response.NotModified || response.CatalogVersion != id.CatalogVersion || response.Etag == nil || *response.Etag != p.Etag {
		return fmt.Errorf("published catalog version or ETag mismatch")
	}
	if _, err = decodeCatalogStreams(response, false); err != nil {
		return err
	}
	if expected != nil {
		for i, a := range catalogStreams(response) {
			b := catalogStreams(*expected)[i]
			if (a == nil) != (b == nil) || (a != nil && !bytes.Equal(*a, *b)) {
				return fmt.Errorf("catalog publication readback changed metadata")
			}
		}
	}
	return nil
}
func catalogStreams(r generated.CatalogContentsResponse) []*[]byte {
	return []*[]byte{r.Schemas, r.Tables, r.Views, r.Functions, r.Macros, r.Indexes}
}
func decodeCatalogPublication(data []byte) (generated.CatalogContentsResponse, error) {
	out := generated.CatalogContentsResponse{}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return out, err
	}
	defer reader.Release()
	if !reader.Schema().Equal(generated.CatalogContentsResultSchema) || !reader.Next() {
		return out, fmt.Errorf("invalid catalog result schema")
	}
	r := reader.RecordBatch()
	if r.NumRows() != 1 {
		return out, fmt.Errorf("catalog result must contain one row")
	}
	out.CatalogVersion = r.Column(0).(*array.Int64).Value(0)
	if col := r.Column(1).(*array.String); col.IsValid(0) {
		v := col.Value(0)
		out.Etag = &v
	}
	out.NotModified = r.Column(2).(*array.Boolean).Value(0)
	fields := []**[]byte{&out.Schemas, &out.Tables, &out.Views, &out.Functions, &out.Macros, &out.Indexes}
	for i, f := range fields {
		col := r.Column(i + 3).(*array.Binary)
		if col.IsValid(0) {
			v := bytes.Clone(col.Value(0))
			*f = &v
		}
	}
	if reader.Next() {
		return out, fmt.Errorf("catalog result contains extra batches")
	}
	return out, reader.Err()
}

type boundedCatalogStorage struct {
	storage                                vgirpc.ExternalStorage
	rawCap, storedCap, rawSize, storedSize int64
	compression                            string
}

// Upload enforces raw and stored byte limits before forwarding the encoded catalog to external storage.
func (s *boundedCatalogStorage) Upload(data []byte, schema *arrow.Schema, encoding string) (string, error) {
	if encoding != "" {
		return "", fmt.Errorf("catalog publication requires identity content encoding")
	}
	s.rawSize = int64(len(data))
	if s.rawSize > s.rawCap {
		return "", fmt.Errorf("catalog publication exceeds byte limit")
	}
	body := data
	switch s.compression {
	case "", "identity":
	case "gzip":
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return "", err
		}
		if err := writer.Close(); err != nil {
			return "", err
		}
		body = buf.Bytes()
		encoding = "gzip"
	case "zstd":
		writer, err := zstd.NewWriter(nil)
		if err != nil {
			return "", err
		}
		body = writer.EncodeAll(data, nil)
		if err := writer.Close(); err != nil {
			return "", err
		}
		encoding = "zstd"
	default:
		return "", fmt.Errorf("unsupported catalog publication compression")
	}
	s.storedSize = int64(len(body))
	if s.storedSize > s.storedCap {
		return "", fmt.Errorf("catalog publication exceeds byte limit")
	}
	return s.storage.Upload(body, schema, encoding)
}
func (m *CatalogSnapshotManager) recoverOrPublish(key string, id SnapshotIdentity, build func() (generated.CatalogContentsResponse, error)) (CatalogPublicationManifest, error) {
	repo := m.policy.Repository
	existing, err := repo.Lookup(key)
	if err != nil {
		return CatalogPublicationManifest{}, err
	}
	if existing != nil {
		if !m.usable(*existing) {
			if renewer, ok := repo.(CatalogPublicationRenewer); ok {
				renewed, renewErr := renewer.Renew(*existing)
				if renewErr != nil {
					return CatalogPublicationManifest{}, renewErr
				}
				if renewed != nil {
					if err = checkPublicationRenewal(*existing, *renewed); err != nil {
						return CatalogPublicationManifest{}, err
					}
					existing = renewed
				}
			}
		}
		if err = m.validate(*existing, key, id, nil); err == nil {
			return *existing, nil
		}
		if _, err = repo.Invalidate(key, existing.ManifestID); err != nil {
			return CatalogPublicationManifest{}, err
		}
	}
	response, err := build()
	if err != nil {
		return CatalogPublicationManifest{}, err
	}
	if response.NotModified || response.CatalogVersion != id.CatalogVersion {
		return CatalogPublicationManifest{}, fmt.Errorf("publication needs full snapshot of requested version")
	}
	if _, err = decodeCatalogStreams(response, false); err != nil {
		return CatalogPublicationManifest{}, err
	}
	if response.Etag == nil {
		etag := CatalogStreamsDigest(response)
		response.Etag = &etag
	}
	raw, stored := m.limits()
	bounded := &boundedCatalogStorage{storage: repo.Storage(), rawCap: raw, storedCap: stored, compression: m.policy.Compression}
	ref, err := vgirpc.PublishExternalResult(response, bounded, nil, true)
	if err != nil {
		return CatalogPublicationManifest{}, err
	}
	location, err := repo.Location(ref)
	if err != nil {
		return CatalogPublicationManifest{}, err
	}
	idHash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", ref.URL(), time.Now().UnixNano())))
	p := CatalogPublicationManifest{CacheKey: key, ManifestID: hex.EncodeToString(idHash[:]), ProtocolName: "vgi.v2", WireRevision: "2.2", ContractHash: publicationContractHash(), CodecRevision: catalogCodecRevision, CatalogVersion: id.CatalogVersion, Etag: *response.Etag, SHA256: ref.SHA256(), RawSize: bounded.rawSize, StoredSize: bounded.storedSize, URL: ref.URL(), ObjectID: location.ObjectID, ExpiresAt: location.ExpiresAt, RetainUntil: location.RetainUntil, Stable: location.Stable, FormatVersion: 1, ValidatorRevision: 1}
	if err = m.validate(p, key, id, &response); err != nil {
		return p, err
	}
	winner, err := repo.CommitIfAbsent(p)
	if err != nil {
		return p, err
	}
	if winner.ManifestID != p.ManifestID {
		err = m.validate(winner, key, id, nil)
	}
	return winner, err
}

type catalogAttachmentScopeKey struct{}
