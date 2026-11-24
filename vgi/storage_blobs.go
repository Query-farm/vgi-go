// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// ---------------------------------------------------------------------------
// ExternalizingStorage: value envelope + blob-vendor externalization for any
// FunctionStorage backend.
//
// ExternalizingStorage wraps a backend and stores values too large for it in
// the blob vendor (VGI_BLOB_VENDOR_URL), keeping only a small pointer in the
// backend. The FunctionStorage API does not change.
//
// Envelope (v1, byte-identical in every VGI SDK; vectors in
// testdata/storage_envelope_v1.json):
//
//	inline:  0x00 || raw
//	pointer: 0x01 || u64be(size) || sha256(raw) (32 bytes) || object_id (ASCII)
//
// Every value the wrapper writes is enveloped: worker and scan-worker state,
// work-queue items, aggregate states, constant arguments and window
// partitions, transaction state, state-log items and attach state. With a
// vendor client, a value of threshold bytes or more is uploaded (purpose "fs")
// before the backend write; reads fetch it back and verify it against the
// pointer's size and SHA-256. Without one every value is inline, and when the
// backend has a per-value cap (the Cloudflare Durable Object's 1 MiB) a value
// whose envelope exceeds it is refused with *StorageValueTooLargeError.
// Counters are not enveloped.
//
// Decoding is strict: an empty value, an unknown tag, a pointer of 41 bytes or
// fewer, or an object id outside [A-Za-z0-9._:-]{1,128} is a
// *StorageEnvelopeError, as is a pointer read with no vendor configured. User
// bytes that begin 0x01 are safe because every value is enveloped.
//
// A pointer whose blob cannot be read is a *StorageBlobError whose message
// follows the SDK-neutral contract (the vgi extension's
// docs/testing/blob-test-catalog.md):
//
//	storage blob <id> failed its integrity check: sha256 mismatch
//	storage blob <id> failed its integrity check: size mismatch (expected N bytes)
//	storage blob <id> is missing: not found in the blob vendor (expired or deleted?)
//	storage blob <id> could not be read: <detail>
//
// Vendor scopes: blobs are uploaded under (shard, "q.<hex execution id>") for
// queue items and (shard, "s.<hex scope id>") for everything else, where shard
// is the storage shard key ("att-<hex attach uuid>", "loc-anon" when empty,
// "h-<sha256 hex>" when outside [A-Za-z0-9._-]{1,128}). Each upload carries the
// idempotency key "<per-call id>.<index>". QueueClear and ExecutionClear delete
// their scope; QueuePop, WorkerCollect and AttachStateDrain delete the blobs
// they consumed. Clean-up runs in the background, best effort; the vendor's
// idle sweep and the bucket lifecycle are the backstop. Other deletes leave
// blobs until the scope is cleared.
//
// Sealed framework payloads (storage_seal.go) are sealed before they reach the
// wrapper, so an externalized sealed payload is still authenticated when read.
// ---------------------------------------------------------------------------

// Environment variables read by WrapStorageFromEnv.
const (
	// StorageExternalizeEnv ("1", "true", "yes", "on") wraps any backend;
	// it then requires blob storage: vgirpc.EnvBlobVendorURL, or a direct
	// bucket (VGI_STORAGE_BACKEND=s3|gcs), whose <prefix>fs/ area holds the
	// blobs.
	StorageExternalizeEnv = "VGI_STORAGE_EXTERNALIZE"
	// StorageExternalizeBytesEnv is the externalization threshold.
	StorageExternalizeBytesEnv = "VGI_STORAGE_EXTERNALIZE_BYTES"
	// StorageExternalizeBytesAliasEnv is an alias of StorageExternalizeBytesEnv.
	StorageExternalizeBytesAliasEnv = "VGI_CF_DO_EXTERNALIZE_BYTES"
)

// DefaultStorageExternalizeBytes is the default externalization threshold:
// values of at least this many bytes go to the blob vendor.
const DefaultStorageExternalizeBytes = 512 * 1024

// maxStorageBlobBytes is the largest blob a read fetches (the vendor's
// per-object cap).
const maxStorageBlobBytes int64 = 4 << 30

const (
	envelopeTagInline  byte = 0x00
	envelopeTagPointer byte = 0x01
	// envelopePointerFixed is tag + u64be size + sha256.
	envelopePointerFixed = 1 + 8 + 32
	// blobTransferParallelism bounds concurrent vendor transfers per call.
	blobTransferParallelism = 8
	maxBlobScopeIDLen       = 128
)

var blobLogger = slog.Default().With("logger", "vgi.storage.blobs")

// StorageEnvelopeError is a value read from storage that is not a valid
// envelope (empty, unknown tag, malformed pointer), or a pointer read by a
// process with no blob vendor configured.
type StorageEnvelopeError struct{ msg string }

// Error returns the diagnostic.
func (e *StorageEnvelopeError) Error() string { return e.msg }

// StorageBlobErrorKind classifies a *StorageBlobError.
type StorageBlobErrorKind int

const (
	// StorageBlobUnreadable is any other failure to read a blob.
	StorageBlobUnreadable StorageBlobErrorKind = iota
	// StorageBlobIntegrity is a downloaded blob that does not match the
	// pointer's size or SHA-256.
	StorageBlobIntegrity
	// StorageBlobMissing is a blob gone from the vendor or its storage
	// (expired, deleted, never uploaded).
	StorageBlobMissing
)

// StorageBlobError is an externalized storage value whose blob could not be
// read. Its message starts "storage blob <object id>".
type StorageBlobError struct {
	ObjectID string
	Kind     StorageBlobErrorKind
	msg      string
	cause    error
}

// Error returns the message, which starts "storage blob <object id>".
func (e *StorageBlobError) Error() string { return e.msg }

// Unwrap returns the vendor client's error.
func (e *StorageBlobError) Unwrap() error { return e.cause }

// StorageValueTooLargeError is a value too large for the backend with no blob
// vendor configured.
type StorageValueTooLargeError struct{ msg string }

// Error returns the diagnostic, which names the blob-vendor variable.
func (e *StorageValueTooLargeError) Error() string { return e.msg }

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

// validIDChars reports whether s is 1-128 bytes of [A-Za-z0-9._-], plus ':'
// when colon is set.
func validIDChars(s string, colon bool) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		case c == ':' && colon:
		default:
			return false
		}
	}
	return true
}

// validObjectID: the pointer's object-id charset, [A-Za-z0-9._:-]{1,128}.
func validObjectID(id string) bool { return validIDChars(id, true) }

// validShard: the wrapper's shard charset, [A-Za-z0-9._-]{1,128}.
func validShard(s string) bool { return validIDChars(s, false) }

// EncodeStorageInline returns the inline envelope of value.
func EncodeStorageInline(value []byte) []byte {
	out := make([]byte, 1+len(value))
	out[0] = envelopeTagInline
	copy(out[1:], value)
	return out
}

// EncodeStoragePointer returns the pointer envelope for a vendored blob.
func EncodeStoragePointer(ref vgirpc.BlobRef) ([]byte, error) {
	if !validObjectID(ref.ObjectID) {
		return nil, fmt.Errorf("blob vendor returned an unusable object id %q", ref.ObjectID)
	}
	digest, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(digest) != sha256.Size || ref.Size < 0 {
		return nil, fmt.Errorf("blob reference %q has a malformed size or sha256", ref.ObjectID)
	}
	out := make([]byte, envelopePointerFixed, envelopePointerFixed+len(ref.ObjectID))
	out[0] = envelopeTagPointer
	binary.BigEndian.PutUint64(out[1:9], uint64(ref.Size))
	copy(out[9:envelopePointerFixed], digest)
	return append(out, ref.ObjectID...), nil
}

// storageEnvelopeTag returns the envelope's tag, or an error for an empty
// value or an unknown tag.
func storageEnvelopeTag(envelope []byte) (byte, error) {
	if len(envelope) == 0 {
		return 0, &StorageEnvelopeError{"empty stored value: not a value envelope (written without the envelope?)"}
	}
	switch tag := envelope[0]; tag {
	case envelopeTagInline, envelopeTagPointer:
		return tag, nil
	default:
		return 0, &StorageEnvelopeError{fmt.Sprintf(
			"stored value has unknown envelope tag 0x%02x (written by an older client or without the envelope? "+
				"mixing client versions within one query is not supported)", tag)}
	}
}

// DecodeStoragePointer parses a pointer envelope.
func DecodeStoragePointer(envelope []byte) (vgirpc.BlobRef, error) {
	if len(envelope) <= envelopePointerFixed || envelope[0] != envelopeTagPointer ||
		!validObjectID(string(envelope[envelopePointerFixed:])) {
		return vgirpc.BlobRef{}, &StorageEnvelopeError{fmt.Sprintf("malformed blob pointer in stored value (%d bytes)", len(envelope))}
	}
	return vgirpc.BlobRef{
		ObjectID: string(envelope[envelopePointerFixed:]),
		Size:     int64(binary.BigEndian.Uint64(envelope[1:9])),
		SHA256:   hex.EncodeToString(envelope[9:envelopePointerFixed]),
	}, nil
}

// blobExecutionID is the vendor scope.execution_id for one queue ("q") or
// state scope ("s").
func blobExecutionID(kind string, ident []byte) string {
	text := kind + "." + hex.EncodeToString(ident)
	if len(text) <= maxBlobScopeIDLen {
		return text
	}
	sum := sha256.Sum256(ident)
	return kind + ".h" + hex.EncodeToString(sum[:])
}

// blobShard is the vendor scope.shard for a storage shard key.
func blobShard(shardKey string) string {
	shard := shardKey
	if shard == "" {
		shard = "loc-anon"
	}
	if validShard(shard) {
		return shard
	}
	sum := sha256.Sum256([]byte(shard))
	return "h-" + hex.EncodeToString(sum[:])
}

// blobReadError maps a vendor client failure onto the storage-blob error contract.
func blobReadError(ref vgirpc.BlobRef, err error) error {
	var ve *vgirpc.VendorError
	code, status := "", 0
	if errors.As(err, &ve) {
		code, status = ve.Code, ve.Status
	}
	switch {
	case code == vgirpc.VendorCodeSHA256Mismatch:
		return &StorageBlobError{ObjectID: ref.ObjectID, Kind: StorageBlobIntegrity, cause: err,
			msg: fmt.Sprintf("storage blob %s failed its integrity check: sha256 mismatch", ref.ObjectID)}
	case code == vgirpc.VendorCodeSizeMismatch:
		return &StorageBlobError{ObjectID: ref.ObjectID, Kind: StorageBlobIntegrity, cause: err,
			msg: fmt.Sprintf("storage blob %s failed its integrity check: size mismatch (expected %d bytes)", ref.ObjectID, ref.Size)}
	case status == 404 || code == "not_found" || code == "not_uploaded" || code == "NoSuchKey":
		return &StorageBlobError{ObjectID: ref.ObjectID, Kind: StorageBlobMissing, cause: err,
			msg: fmt.Sprintf("storage blob %s is missing: not found in the blob vendor (expired or deleted?)", ref.ObjectID)}
	default:
		return &StorageBlobError{ObjectID: ref.ObjectID, Kind: StorageBlobUnreadable, cause: err,
			msg: fmt.Sprintf("storage blob %s could not be read: %v", ref.ObjectID, err)}
	}
}

// parallelDo runs fn(0..n-1) with bounded concurrency and returns the first error.
func parallelDo(n int, fn func(i int) error) error {
	if n <= 1 {
		if n == 1 {
			return fn(0)
		}
		return nil
	}
	errs := make([]error, n)
	sem := make(chan struct{}, min(blobTransferParallelism, n))
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			errs[i] = fn(i)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

// StorageExternalizeBytesFromEnv reads VGI_STORAGE_EXTERNALIZE_BYTES (alias
// VGI_CF_DO_EXTERNALIZE_BYTES); DefaultStorageExternalizeBytes when unset.
func StorageExternalizeBytesFromEnv() (int, error) {
	for _, name := range []string{StorageExternalizeBytesEnv, StorageExternalizeBytesAliasEnv} {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			continue
		}
		v, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer, got %q", name, raw)
		}
		if v <= 0 {
			return 0, fmt.Errorf("%s must be positive, got %d", name, v)
		}
		return v, nil
	}
	return DefaultStorageExternalizeBytes, nil
}

// StorageBlobClientFromEnv builds the vendor client from VGI_BLOB_VENDOR_URL
// and its token, or returns nil when VGI_BLOB_VENDOR_URL is unset.
func StorageBlobClientFromEnv() (*vgirpc.VendorBlobClient, error) {
	if strings.TrimSpace(os.Getenv(vgirpc.EnvBlobVendorURL)) == "" {
		return nil, nil
	}
	if _, err := vgirpc.VendorBlobClientFromEnv(vgirpc.BlobPurposeFS, nil); err != nil {
		return nil, err
	}
	return vgirpc.NewVendorBlobClient(
		strings.TrimSpace(os.Getenv(vgirpc.EnvBlobVendorURL)),
		vendorTokenFromEnv(),
		vgirpc.BlobPurposeFS,
		vgirpc.VendorOptions{MaxFetchBytes: maxStorageBlobBytes},
	)
}

func vendorTokenFromEnv() string {
	if t := strings.TrimSpace(os.Getenv(vgirpc.EnvBlobVendorToken)); t != "" {
		return t
	}
	return strings.TrimSpace(os.Getenv(vgirpc.EnvBlobVendorTokenFallback))
}

// WrapStorageFromEnv wraps base in an ExternalizingStorage when the
// environment asks for it.
//
// A backend with a per-value cap (maxValueBytes > 0, i.e. the Cloudflare
// Durable Object) is always wrapped, vendor or not, so its stored format is
// the same in every process. Any other backend (maxValueBytes 0) is wrapped
// only with VGI_STORAGE_EXTERNALIZE=1, which then requires blob storage:
// VGI_BLOB_VENDOR_URL, or a direct bucket (VGI_STORAGE_BACKEND=s3|gcs, its
// <prefix>fs/ area).
func WrapStorageFromEnv(base FunctionStorage, maxValueBytes int) (FunctionStorage, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(StorageExternalizeEnv))) {
	case "1", "true", "yes", "on":
	default:
		if maxValueBytes <= 0 {
			return base, nil
		}
		blobs, err := storageBlobsFromEnv()
		if err != nil {
			return nil, err
		}
		return newExternalizingFromEnv(base, blobs, maxValueBytes)
	}
	blobs, err := storageBlobsFromEnv()
	if err != nil {
		return nil, err
	}
	if blobs == nil {
		return nil, fmt.Errorf("%s=1 needs blob storage: set %s, or %s=s3|gcs with its bucket",
			StorageExternalizeEnv, vgirpc.EnvBlobVendorURL, vgirpc.EnvStorageBackend)
	}
	return newExternalizingFromEnv(base, blobs, maxValueBytes)
}

func newExternalizingFromEnv(base FunctionStorage, blobs StorageBlobs, maxValueBytes int) (FunctionStorage, error) {
	threshold, err := StorageExternalizeBytesFromEnv()
	if err != nil {
		return nil, err
	}
	if maxValueBytes > 0 {
		threshold = min(threshold, maxValueBytes)
	}
	return NewExternalizingStorage(base, blobs, threshold, maxValueBytes)
}

// ---------------------------------------------------------------------------
// The wrapper
// ---------------------------------------------------------------------------

// ExternalizingStorage is a FunctionStorage that envelopes values and
// externalizes large ones to the blob vendor. See the comment at the top of
// this file. Safe for concurrent use when the wrapped backend is.
type ExternalizingStorage struct {
	base          FunctionStorage
	blobs         StorageBlobs
	threshold     int
	maxValueBytes int
	shardKey      string
	cleanup       *blobCleanup
}

// blobCleanup tracks background clean-up, shared by every shard view.
type blobCleanup struct {
	wg sync.WaitGroup
}

var (
	_ FunctionStorage    = (*ExternalizingStorage)(nil)
	_ StateLogStorage    = (*ExternalizingStorage)(nil)
	_ AttachStateStorage = (*ExternalizingStorage)(nil)
	_ ShardedBackend     = (*ExternalizingStorage)(nil)
)

// NewExternalizingStorage wraps base. blobs may be nil to keep every value
// inline. With a vendor, values of at least threshold bytes are stored as
// blobs. maxValueBytes is the backend's per-value cap (0 for none): threshold
// may not exceed it, and with no vendor an over-cap value is refused.
func NewExternalizingStorage(base FunctionStorage, blobs StorageBlobs, threshold, maxValueBytes int) (*ExternalizingStorage, error) {
	// A typed nil vendor client means "no blob store", not a store.
	if c, ok := blobs.(*vgirpc.VendorBlobClient); ok && c == nil {
		blobs = nil
	}
	if threshold < 1 || (maxValueBytes > 0 && threshold > maxValueBytes) {
		limit := "unbounded"
		if maxValueBytes > 0 {
			limit = strconv.Itoa(maxValueBytes)
		}
		return nil, fmt.Errorf("externalize threshold (%s) must be between 1 and %s, got %d", StorageExternalizeBytesEnv, limit, threshold)
	}
	return &ExternalizingStorage{base: base, blobs: blobs, threshold: threshold, maxValueBytes: maxValueBytes, cleanup: &blobCleanup{}}, nil
}

// Base returns the wrapped backend.
func (s *ExternalizingStorage) Base() FunctionStorage { return s.base }

// Threshold returns the externalization threshold in bytes.
func (s *ExternalizingStorage) Threshold() int { return s.threshold }

// ForShard returns a view pinned to shardKey: the backend's own shard view
// when it shards remotely, and vendor scopes named for the shard either way.
func (s *ExternalizingStorage) ForShard(shardKey string) FunctionStorage {
	base := s.base
	if sb, ok := base.(ShardedBackend); ok {
		base = sb.ForShard(shardKey)
	}
	view := *s
	view.base = base
	view.shardKey = shardKey
	return &view
}

// Close closes the backend and, after pending clean-up finishes, the vendor
// client. Safe to call more than once.
func (s *ExternalizingStorage) Close() error {
	err := s.base.Close()
	s.FlushBlobCleanup()
	if s.blobs != nil {
		s.blobs.Close()
	}
	return err
}

// FlushBlobCleanup waits for pending background blob clean-up.
func (s *ExternalizingStorage) FlushBlobCleanup() { s.cleanup.wg.Wait() }

// --- encode / decode -------------------------------------------------------

func newBlobCallID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// encodeValues envelopes each value, uploading large ones first.
func (s *ExternalizingStorage) encodeValues(values [][]byte, scopeExecutionID string) ([][]byte, error) {
	out := make([][]byte, len(values))
	var large []int
	for i, v := range values {
		if s.blobs != nil && len(v) >= s.threshold {
			large = append(large, i)
			continue
		}
		out[i] = EncodeStorageInline(v)
		if s.blobs == nil && s.maxValueBytes > 0 && len(out[i]) > s.maxValueBytes {
			return nil, &StorageValueTooLargeError{fmt.Sprintf(
				"storage value of %d bytes exceeds the backend's %d-byte limit; set %s (the blob vendor) so large values are stored as blobs",
				len(v), s.maxValueBytes, vgirpc.EnvBlobVendorURL)}
		}
	}
	if len(large) == 0 {
		return out, nil
	}
	callID := newBlobCallID()
	scope := &vgirpc.BlobScope{Shard: blobShard(s.shardKey), ExecutionID: scopeExecutionID}
	err := parallelDo(len(large), func(j int) error {
		i := large[j]
		ref, err := s.blobs.PutBytes(context.Background(), values[i], scope, fmt.Sprintf("%s.%d", callID, i))
		if err != nil {
			return fmt.Errorf("storage blob upload failed: %w", err)
		}
		env, err := EncodeStoragePointer(ref)
		if err != nil {
			return err
		}
		out[i] = env
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *ExternalizingStorage) encodeOne(value []byte, scopeExecutionID string) ([]byte, error) {
	out, err := s.encodeValues([][]byte{value}, scopeExecutionID)
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

// decodeValues decodes envelopes, fetching pointers in parallel (nil passes
// through). refsOut, when non-nil, receives the blob references read.
func (s *ExternalizingStorage) decodeValues(envelopes [][]byte, refsOut *[]vgirpc.BlobRef) ([][]byte, error) {
	out := make([][]byte, len(envelopes))
	var pointers []int
	var refs []vgirpc.BlobRef
	for i, env := range envelopes {
		if env == nil {
			continue
		}
		tag, err := storageEnvelopeTag(env)
		if err != nil {
			return nil, err
		}
		if tag == envelopeTagInline {
			out[i] = env[1:len(env):len(env)]
			continue
		}
		ref, err := DecodeStoragePointer(env)
		if err != nil {
			return nil, err
		}
		pointers = append(pointers, i)
		refs = append(refs, ref)
	}
	if len(pointers) == 0 {
		return out, nil
	}
	if s.blobs == nil {
		return nil, &StorageEnvelopeError{fmt.Sprintf(
			"stored value is a blob pointer but %s is not configured in this process", vgirpc.EnvBlobVendorURL)}
	}
	err := parallelDo(len(pointers), func(j int) error {
		data, err := s.blobs.GetBytes(context.Background(), refs[j])
		if err != nil {
			return blobReadError(refs[j], err)
		}
		if data == nil {
			data = []byte{}
		}
		out[pointers[j]] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	if refsOut != nil {
		*refsOut = append(*refsOut, refs...)
	}
	return out, nil
}

func (s *ExternalizingStorage) decodeOne(env []byte, refsOut *[]vgirpc.BlobRef) ([]byte, error) {
	if env == nil {
		return nil, nil
	}
	out, err := s.decodeValues([][]byte{env}, refsOut)
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

// --- background clean-up ---------------------------------------------------

func (s *ExternalizingStorage) submitCleanup(what string, fn func(ctx context.Context) error) {
	s.cleanup.wg.Add(1)
	go func() {
		defer s.cleanup.wg.Done()
		if err := fn(context.Background()); err != nil {
			blobLogger.Warn("blob vendor clean-up failed", "after", what, "error", err.Error())
		}
	}()
}

func (s *ExternalizingStorage) deleteBlobs(refs []vgirpc.BlobRef, what string) {
	if s.blobs == nil || len(refs) == 0 {
		return
	}
	ids := make([]string, len(refs))
	for i, r := range refs {
		ids[i] = r.ObjectID
	}
	blobs := s.blobs
	s.submitCleanup(what, func(ctx context.Context) error {
		_, err := blobs.Delete(ctx, ids)
		return err
	})
}

func (s *ExternalizingStorage) deleteBlobScope(scopeExecutionID, what string) {
	if s.blobs == nil {
		return
	}
	blobs, shard := s.blobs, blobShard(s.shardKey)
	s.submitCleanup(what, func(ctx context.Context) error {
		_, err := blobs.DeleteScope(ctx, shard, scopeExecutionID)
		return err
	})
}

// --- FunctionStorage: worker state -----------------------------------------

// WorkerPut envelopes (and externalizes) the state, then stores it.
func (s *ExternalizingStorage) WorkerPut(executionID, workerKey, state []byte) error {
	env, err := s.encodeOne(state, blobExecutionID("s", executionID))
	if err != nil {
		return err
	}
	return s.base.WorkerPut(executionID, workerKey, env)
}

// WorkerCollect collects and decodes; the collected blobs are deleted.
func (s *ExternalizingStorage) WorkerCollect(executionID []byte) ([][]byte, error) {
	envs, err := s.base.WorkerCollect(executionID)
	if err != nil {
		return nil, err
	}
	var consumed []vgirpc.BlobRef
	out, err := s.decodeValues(envs, &consumed)
	if err != nil {
		return nil, err
	}
	s.deleteBlobs(consumed, "worker_collect")
	return out, nil
}

// WorkerScan scans and decodes.
func (s *ExternalizingStorage) WorkerScan(executionID []byte) ([]WorkerStateEntry, error) {
	entries, err := s.base.WorkerScan(executionID)
	if err != nil {
		return nil, err
	}
	envs := make([][]byte, len(entries))
	for i, e := range entries {
		envs[i] = e.State
	}
	values, err := s.decodeValues(envs, nil)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].State = values[i]
	}
	return entries, nil
}

// ScanWorkerPut envelopes (and externalizes) the state, then stores it.
func (s *ExternalizingStorage) ScanWorkerPut(executionID, streamID, state []byte) error {
	env, err := s.encodeOne(state, blobExecutionID("s", executionID))
	if err != nil {
		return err
	}
	return s.base.ScanWorkerPut(executionID, streamID, env)
}

// ScanWorkerScan scans and decodes.
func (s *ExternalizingStorage) ScanWorkerScan(executionID []byte) ([]ScanWorkerStateEntry, error) {
	entries, err := s.base.ScanWorkerScan(executionID)
	if err != nil {
		return nil, err
	}
	envs := make([][]byte, len(entries))
	for i, e := range entries {
		envs[i] = e.State
	}
	values, err := s.decodeValues(envs, nil)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].State = values[i]
	}
	return entries, nil
}

// --- work queue -------------------------------------------------------------

// QueuePush envelopes (and externalizes) the items, then pushes them.
func (s *ExternalizingStorage) QueuePush(executionID []byte, items [][]byte) (int, error) {
	envs, err := s.encodeValues(items, blobExecutionID("q", executionID))
	if err != nil {
		return 0, err
	}
	return s.base.QueuePush(executionID, envs)
}

// QueuePop pops one item; its blob, if any, is deleted once fetched.
func (s *ExternalizingStorage) QueuePop(executionID []byte) ([]byte, error) {
	env, err := s.base.QueuePop(executionID)
	if err != nil || env == nil {
		return nil, err
	}
	var claimed []vgirpc.BlobRef
	value, err := s.decodeOne(env, &claimed)
	if err != nil {
		return nil, err
	}
	s.deleteBlobs(claimed, "queue_pop")
	return value, nil
}

// QueueClear clears the queue and deletes its blob scope.
func (s *ExternalizingStorage) QueueClear(executionID []byte) (int, error) {
	n, err := s.base.QueueClear(executionID)
	s.deleteBlobScope(blobExecutionID("q", executionID), "queue_clear")
	return n, err
}

// --- aggregates -------------------------------------------------------------

// AggregateStateGet reads and decodes group states.
func (s *ExternalizingStorage) AggregateStateGet(executionID []byte, groupIDs []int64) ([]AggregateStateEntry, error) {
	entries, err := s.base.AggregateStateGet(executionID, groupIDs)
	if err != nil {
		return nil, err
	}
	envs := make([][]byte, len(entries))
	for i, e := range entries {
		envs[i] = e.State
	}
	values, err := s.decodeValues(envs, nil)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].State = values[i]
	}
	return entries, nil
}

// AggregateStatePut envelopes (and externalizes) the states, then stores them.
func (s *ExternalizingStorage) AggregateStatePut(executionID []byte, entries []AggregateStateEntry) error {
	values := make([][]byte, len(entries))
	for i, e := range entries {
		values[i] = e.State
	}
	envs, err := s.encodeValues(values, blobExecutionID("s", executionID))
	if err != nil {
		return err
	}
	out := make([]AggregateStateEntry, len(entries))
	for i, e := range entries {
		out[i] = AggregateStateEntry{GroupID: e.GroupID, State: envs[i]}
	}
	return s.base.AggregateStatePut(executionID, out)
}

// AggregateStateClear passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) AggregateStateClear(executionID []byte) error {
	return s.base.AggregateStateClear(executionID)
}

// AggregateConstArgsPut envelopes (and externalizes) the arguments.
func (s *ExternalizingStorage) AggregateConstArgsPut(executionID []byte, functionName string, args []byte) error {
	env, err := s.encodeOne(args, blobExecutionID("s", executionID))
	if err != nil {
		return err
	}
	return s.base.AggregateConstArgsPut(executionID, functionName, env)
}

// AggregateConstArgsGet reads and decodes the arguments.
func (s *ExternalizingStorage) AggregateConstArgsGet(executionID []byte, functionName string) ([]byte, error) {
	env, err := s.base.AggregateConstArgsGet(executionID, functionName)
	if err != nil {
		return nil, err
	}
	return s.decodeOne(env, nil)
}

// AggregateWindowPartitionPut envelopes (and externalizes) the partition.
func (s *ExternalizingStorage) AggregateWindowPartitionPut(executionID []byte, partitionID int64, data []byte) error {
	env, err := s.encodeOne(data, blobExecutionID("s", executionID))
	if err != nil {
		return err
	}
	return s.base.AggregateWindowPartitionPut(executionID, partitionID, env)
}

// AggregateWindowPartitionGet reads and decodes the partition.
func (s *ExternalizingStorage) AggregateWindowPartitionGet(executionID []byte, partitionID int64) ([]byte, error) {
	env, err := s.base.AggregateWindowPartitionGet(executionID, partitionID)
	if err != nil {
		return nil, err
	}
	return s.decodeOne(env, nil)
}

// AggregateWindowPartitionDelete passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) AggregateWindowPartitionDelete(executionID []byte, partitionID int64) error {
	return s.base.AggregateWindowPartitionDelete(executionID, partitionID)
}

// AggregateWindowPartitionClear passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) AggregateWindowPartitionClear(executionID []byte) error {
	return s.base.AggregateWindowPartitionClear(executionID)
}

// --- transaction state ------------------------------------------------------

// TransactionStateGet reads and decodes values (nil for misses).
func (s *ExternalizingStorage) TransactionStateGet(transactionOpaqueData []byte, keys [][]byte) ([][]byte, error) {
	envs, err := s.base.TransactionStateGet(transactionOpaqueData, keys)
	if err != nil {
		return nil, err
	}
	return s.decodeValues(envs, nil)
}

// TransactionStatePut envelopes (and externalizes) the values, then stores them.
func (s *ExternalizingStorage) TransactionStatePut(transactionOpaqueData []byte, items []TransactionStateItem) error {
	values := make([][]byte, len(items))
	for i, it := range items {
		values[i] = it.Value
	}
	envs, err := s.encodeValues(values, blobExecutionID("s", transactionOpaqueData))
	if err != nil {
		return err
	}
	out := make([]TransactionStateItem, len(items))
	for i, it := range items {
		out[i] = TransactionStateItem{Key: it.Key, Value: envs[i]}
	}
	return s.base.TransactionStatePut(transactionOpaqueData, out)
}

// TransactionStateClear passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) TransactionStateClear(transactionOpaqueData []byte) error {
	return s.base.TransactionStateClear(transactionOpaqueData)
}

// ExecutionClear clears the scope and deletes its blob scope.
func (s *ExternalizingStorage) ExecutionClear(scope []byte) (int, error) {
	n, err := s.base.ExecutionClear(scope)
	s.deleteBlobScope(blobExecutionID("s", scope), "execution_clear")
	return n, err
}

// --- state log --------------------------------------------------------------

func (s *ExternalizingStorage) stateLogBase() (StateLogStorage, error) {
	sl, ok := s.base.(StateLogStorage)
	if !ok {
		return nil, errStateLogUnsupported
	}
	return sl, nil
}

// StateAppend envelopes (and externalizes) the item, then appends it.
func (s *ExternalizingStorage) StateAppend(executionID, key, value []byte) (int64, error) {
	sl, err := s.stateLogBase()
	if err != nil {
		return 0, err
	}
	env, err := s.encodeOne(value, blobExecutionID("s", executionID))
	if err != nil {
		return 0, err
	}
	return sl.StateAppend(executionID, key, env)
}

// StateLogScan scans a log and decodes its values.
func (s *ExternalizingStorage) StateLogScan(executionID, key []byte, afterID int64, limit int) ([]StateLogEntry, error) {
	sl, err := s.stateLogBase()
	if err != nil {
		return nil, err
	}
	entries, err := sl.StateLogScan(executionID, key, afterID, limit)
	if err != nil {
		return nil, err
	}
	envs := make([][]byte, len(entries))
	for i, e := range entries {
		envs[i] = e.Value
	}
	values, err := s.decodeValues(envs, nil)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].Value = values[i]
	}
	return entries, nil
}

// StateLogClear passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) StateLogClear(executionID []byte) error {
	sl, err := s.stateLogBase()
	if err != nil {
		return err
	}
	return sl.StateLogClear(executionID)
}

// --- attach state -----------------------------------------------------------

func (s *ExternalizingStorage) attachBase() (AttachStateStorage, error) {
	as, ok := s.base.(AttachStateStorage)
	if !ok {
		return nil, errAttachStateUnsupported
	}
	return as, nil
}

func (s *ExternalizingStorage) decodeKVs(kvs []AttachStateKV, refsOut *[]vgirpc.BlobRef) ([]AttachStateKV, error) {
	envs := make([][]byte, len(kvs))
	for i, kv := range kvs {
		envs[i] = kv.Value
	}
	values, err := s.decodeValues(envs, refsOut)
	if err != nil {
		return nil, err
	}
	for i := range kvs {
		kvs[i].Value = values[i]
	}
	return kvs, nil
}

// AttachStatePut envelopes (and externalizes) the value, then stores it.
func (s *ExternalizingStorage) AttachStatePut(scope, ns, key, value []byte) error {
	as, err := s.attachBase()
	if err != nil {
		return err
	}
	env, err := s.encodeOne(value, blobExecutionID("s", scope))
	if err != nil {
		return err
	}
	return as.AttachStatePut(scope, ns, key, env)
}

// AttachStateGet reads and decodes one value ((nil, nil) when absent).
func (s *ExternalizingStorage) AttachStateGet(scope, ns, key []byte) ([]byte, error) {
	as, err := s.attachBase()
	if err != nil {
		return nil, err
	}
	env, err := as.AttachStateGet(scope, ns, key)
	if err != nil {
		return nil, err
	}
	return s.decodeOne(env, nil)
}

// AttachStateScan scans and decodes.
func (s *ExternalizingStorage) AttachStateScan(scope, ns []byte, opts AttachScanOptions) ([]AttachStateKV, error) {
	as, err := s.attachBase()
	if err != nil {
		return nil, err
	}
	kvs, err := as.AttachStateScan(scope, ns, opts)
	if err != nil {
		return nil, err
	}
	return s.decodeKVs(kvs, nil)
}

// AttachStateDeleteKey passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) AttachStateDeleteKey(scope, ns, key []byte) error {
	as, err := s.attachBase()
	if err != nil {
		return err
	}
	return as.AttachStateDeleteKey(scope, ns, key)
}

// AttachStateDeleteNS passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) AttachStateDeleteNS(scope, ns []byte) error {
	as, err := s.attachBase()
	if err != nil {
		return err
	}
	return as.AttachStateDeleteNS(scope, ns)
}

// AttachStateDeleteRange passes through (blobs stay until the scope is cleared).
func (s *ExternalizingStorage) AttachStateDeleteRange(scope, ns, start, end []byte) (int, error) {
	as, err := s.attachBase()
	if err != nil {
		return 0, err
	}
	return as.AttachStateDeleteRange(scope, ns, start, end)
}

// AttachStateDrain drains and decodes; the drained blobs are deleted.
func (s *ExternalizingStorage) AttachStateDrain(scope, ns []byte) ([]AttachStateKV, error) {
	as, err := s.attachBase()
	if err != nil {
		return nil, err
	}
	kvs, err := as.AttachStateDrain(scope, ns)
	if err != nil {
		return nil, err
	}
	var drained []vgirpc.BlobRef
	kvs, err = s.decodeKVs(kvs, &drained)
	if err != nil {
		return nil, err
	}
	s.deleteBlobs(drained, "state_drain")
	return kvs, nil
}

// AttachCounterGet passes through (counters are not enveloped).
func (s *ExternalizingStorage) AttachCounterGet(scope, ns, key []byte) (int64, error) {
	as, err := s.attachBase()
	if err != nil {
		return 0, err
	}
	return as.AttachCounterGet(scope, ns, key)
}

// AttachCounterAdd passes through.
func (s *ExternalizingStorage) AttachCounterAdd(scope, ns, key []byte, delta int64) (int64, error) {
	as, err := s.attachBase()
	if err != nil {
		return 0, err
	}
	return as.AttachCounterAdd(scope, ns, key, delta)
}

// AttachCounterSet passes through.
func (s *ExternalizingStorage) AttachCounterSet(scope, ns, key []byte, value int64) error {
	as, err := s.attachBase()
	if err != nil {
		return err
	}
	return as.AttachCounterSet(scope, ns, key, value)
}

// AttachCounterDelete passes through.
func (s *ExternalizingStorage) AttachCounterDelete(scope, ns, key []byte) error {
	as, err := s.attachBase()
	if err != nil {
		return err
	}
	return as.AttachCounterDelete(scope, ns, key)
}
