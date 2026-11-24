// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// StorageBlobs is the blob store behind ExternalizingStorage: the blob
// vendor (*vgirpc.VendorBlobClient), or a direct bucket's function-storage
// area ([NewObjectStoreBlobs]).
type StorageBlobs interface {
	// PutBytes stores data in scope and returns its reference.
	PutBytes(ctx context.Context, data []byte, scope *vgirpc.BlobScope, idempotencyKey string) (vgirpc.BlobRef, error)
	// GetBytes fetches a blob and verifies its size and SHA-256.
	GetBytes(ctx context.Context, ref vgirpc.BlobRef) ([]byte, error)
	// Delete removes blobs by object id.
	Delete(ctx context.Context, objectIDs []string) (int, error)
	// DeleteScope removes every blob of a (shard, execution) scope.
	DeleteScope(ctx context.Context, shard, executionID string) (int, error)
	// Close releases the store.
	Close()
}

var _ StorageBlobs = (*vgirpc.VendorBlobClient)(nil)

// FunctionObjectStore is a direct bucket's function-storage area,
// <prefix>fs/ (vgirpc/s3 and vgirpc/gcs FunctionStore).
type FunctionObjectStore interface {
	// Prefix is the area's key prefix.
	Prefix() string
	// PutObjectScoped stores data under Prefix() + scope + "/" + a fresh
	// name (+ ".bin"), or Prefix() + name without a scope, and returns the key.
	PutObjectScoped(ctx context.Context, data []byte, scope string) (string, error)
	// Get fetches an object by key; a missing one satisfies vgirpc.IsObjectMissing.
	Get(objectID string, maxBytes int64) ([]byte, error)
	// DeleteObjects deletes keys, best effort.
	DeleteObjects(ctx context.Context, keys []string) (int, error)
	// DeleteScope deletes a scope's objects, best effort.
	DeleteScope(ctx context.Context, scope string) (int, error)
}

// NewObjectStoreBlobs adapts a bucket's function-storage area to
// StorageBlobs (direct-storage contract v1, Addendum A).
//
// Object ids in storage envelopes must be short ([A-Za-z0-9._:-]{1,128}), so
// a blob's id is "<scope tag>.<name>", where the scope tag is the first 8
// bytes (hex) of sha256(shard NUL execution id), or just "<name>" without a
// scope; its key is Prefix() + tag + "/" + name + ".bin". A scope delete lists
// the tag's prefix. Deletes are best effort; the bucket's lifecycle rule on
// fs/ (2 days) is the backstop. The idempotency key is ignored: a retried
// upload leaves an orphan the lifecycle rule removes.
func NewObjectStoreBlobs(store FunctionObjectStore) StorageBlobs {
	return objectStoreBlobs{store: store}
}

type objectStoreBlobs struct{ store FunctionObjectStore }

func scopeTag(scope *vgirpc.BlobScope) string {
	if scope == nil || (scope.Shard == "" && scope.ExecutionID == "") {
		return ""
	}
	sum := sha256.Sum256([]byte(scope.Shard + "\x00" + scope.ExecutionID))
	return hex.EncodeToString(sum[:8])
}

func (b objectStoreBlobs) keyFor(objectID string) string {
	if tag, name, ok := strings.Cut(objectID, "."); ok {
		return b.store.Prefix() + tag + "/" + name + ".bin"
	}
	return b.store.Prefix() + objectID + ".bin"
}

// PutBytes uploads data under the scope tag.
func (b objectStoreBlobs) PutBytes(ctx context.Context, data []byte, scope *vgirpc.BlobScope, _ string) (vgirpc.BlobRef, error) {
	tag := scopeTag(scope)
	key, err := b.store.PutObjectScoped(ctx, data, tag)
	if err != nil {
		return vgirpc.BlobRef{}, err
	}
	id := strings.TrimSuffix(path.Base(key), ".bin")
	if tag != "" {
		id = tag + "." + id
	}
	if b.keyFor(id) != key {
		return vgirpc.BlobRef{}, fmt.Errorf("function store wrote %q, not under the expected key layout", key)
	}
	sum := sha256.Sum256(data)
	return vgirpc.BlobRef{ObjectID: id, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}, nil
}

// GetBytes fetches by id and checks size and SHA-256.
func (b objectStoreBlobs) GetBytes(_ context.Context, ref vgirpc.BlobRef) ([]byte, error) {
	data, err := b.store.Get(b.keyFor(ref.ObjectID), maxStorageBlobBytes)
	if err != nil {
		if vgirpc.IsObjectMissing(err) {
			return nil, &vgirpc.VendorError{Message: "storage object is gone", Status: 404, Code: "not_found"}
		}
		return nil, err
	}
	if int64(len(data)) != ref.Size {
		return nil, &vgirpc.VendorError{Message: fmt.Sprintf("size %d, expected %d", len(data), ref.Size), Code: vgirpc.VendorCodeSizeMismatch}
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != strings.ToLower(ref.SHA256) {
		return nil, &vgirpc.VendorError{Message: "sha256 mismatch", Code: vgirpc.VendorCodeSHA256Mismatch}
	}
	return data, nil
}

// Delete removes ids, best effort.
func (b objectStoreBlobs) Delete(ctx context.Context, objectIDs []string) (int, error) {
	keys := make([]string, len(objectIDs))
	for i, id := range objectIDs {
		keys[i] = b.keyFor(id)
	}
	return b.store.DeleteObjects(ctx, keys)
}

// DeleteScope removes a scope tag's objects, best effort.
func (b objectStoreBlobs) DeleteScope(ctx context.Context, shard, executionID string) (int, error) {
	tag := scopeTag(&vgirpc.BlobScope{Shard: shard, ExecutionID: executionID})
	if tag == "" {
		return 0, nil
	}
	return b.store.DeleteScope(ctx, tag)
}

// Close is a no-op.
func (objectStoreBlobs) Close() {}

// storageBlobsFromEnv picks function storage's blob store: the blob vendor,
// the direct bucket (VGI_STORAGE_BACKEND=s3|gcs, its fs/ area), or nil.
func storageBlobsFromEnv() (StorageBlobs, error) {
	backend, err := vgirpc.StorageBackendFromEnv(nil)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	switch backend {
	case vgirpc.StorageBackendS3, vgirpc.StorageBackendGCS:
		storage, err := newObjectStorageFromEnv(backend, nil)
		if err != nil {
			return nil, err
		}
		fs, ok := storage.(interface{ FunctionObjectStore() FunctionObjectStore })
		if !ok {
			return nil, fmt.Errorf("the %s storage backend has no function-storage area", backend)
		}
		return NewObjectStoreBlobs(fs.FunctionObjectStore()), nil
	case vgirpc.StorageBackendVendor:
		client, err := StorageBlobClientFromEnv()
		if err != nil || client == nil {
			return nil, err
		}
		return client, nil
	}
	return nil, nil
}
