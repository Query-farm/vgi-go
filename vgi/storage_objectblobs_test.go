// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// memFunctionStore is an in-memory FunctionObjectStore with the S3/GCS key
// layout.
type memFunctionStore struct {
	mu   sync.Mutex
	objs map[string][]byte
	n    int
}

type memMissing struct{}

func (memMissing) Error() string       { return "gone" }
func (memMissing) ObjectMissing() bool { return true }

func (m *memFunctionStore) Prefix() string { return "pfx/fs/" }
func (m *memFunctionStore) PutObjectScoped(_ context.Context, data []byte, scope string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	key := m.Prefix()
	if scope != "" {
		key += scope + "/"
	}
	key += strings.Repeat("a", 31) + string(rune('a'+m.n%26)) + ".bin"
	m.objs[key] = append([]byte(nil), data...)
	return key, nil
}
func (m *memFunctionStore) Get(key string, _ int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d, ok := m.objs[key]; ok {
		return d, nil
	}
	return nil, memMissing{}
}
func (m *memFunctionStore) DeleteObjects(_ context.Context, keys []string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, k := range keys {
		if _, ok := m.objs[k]; ok {
			delete(m.objs, k)
			n++
		}
	}
	return n, nil
}
func (m *memFunctionStore) DeleteScope(ctx context.Context, scope string) (int, error) {
	m.mu.Lock()
	var keys []string
	for k := range m.objs {
		if strings.HasPrefix(k, m.Prefix()+scope+"/") {
			keys = append(keys, k)
		}
	}
	m.mu.Unlock()
	return m.DeleteObjects(ctx, keys)
}

func TestObjectStoreBlobs(t *testing.T) {
	store := &memFunctionStore{objs: map[string][]byte{}}
	blobs := NewObjectStoreBlobs(store)
	ctx := context.Background()
	scope := &vgirpc.BlobScope{Shard: "att-" + strings.Repeat("f", 32), ExecutionID: "s." + strings.Repeat("e", 32)}
	data := bytes.Repeat([]byte("v"), 5000)
	ref, err := blobs.PutBytes(ctx, data, scope, "call.0")
	if err != nil {
		t.Fatal(err)
	}
	// The id fits the storage envelope (short, [A-Za-z0-9._:-]).
	env, err := EncodeStoragePointer(ref)
	if err != nil {
		t.Fatalf("envelope: %v (id %q)", err, ref.ObjectID)
	}
	if back, err := DecodeStoragePointer(env); err != nil || back != ref {
		t.Fatalf("decode: %+v %v", back, err)
	}
	if got, err := blobs.GetBytes(ctx, ref); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("GetBytes: %v", err)
	}
	unscoped, err := blobs.PutBytes(ctx, data[:100], nil, "call.1")
	if err != nil || strings.Contains(unscoped.ObjectID, ".") {
		t.Fatalf("unscoped id %q, %v", unscoped.ObjectID, err)
	}

	// Integrity and missing objects map onto the storage-blob error contract.
	store.objs[blobs.(objectStoreBlobs).keyFor(ref.ObjectID)] = bytes.Repeat([]byte("w"), 5000)
	_, err = blobs.GetBytes(ctx, ref)
	var be *StorageBlobError
	if !errors.As(blobReadError(ref, err), &be) || be.Kind != StorageBlobIntegrity || !strings.Contains(be.Error(), "sha256 mismatch") {
		t.Errorf("tampered blob: %v", blobReadError(ref, err))
	}
	if n, _ := blobs.DeleteScope(ctx, scope.Shard, scope.ExecutionID); n != 1 {
		t.Errorf("DeleteScope removed %d", n)
	}
	_, err = blobs.GetBytes(ctx, ref)
	if !errors.As(blobReadError(ref, err), &be) || be.Kind != StorageBlobMissing {
		t.Errorf("deleted blob: %v", blobReadError(ref, err))
	}
	if _, err := blobs.GetBytes(ctx, unscoped); err != nil {
		t.Errorf("unscoped blob removed by a scope delete: %v", err)
	}
	if n, _ := blobs.Delete(ctx, []string{unscoped.ObjectID}); n != 1 {
		t.Errorf("Delete removed %d", n)
	}
}

// TestExternalizingStorageOnObjectStore: function storage round-trips large
// values through a bucket's fs/ area.
func TestExternalizingStorageOnObjectStore(t *testing.T) {
	base, err := NewSQLiteStorage(SQLiteStorageOptions{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	store := &memFunctionStore{objs: map[string][]byte{}}
	s, err := NewExternalizingStorage(base, NewObjectStoreBlobs(store), 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec := []byte("exec-1")
	big := bytes.Repeat([]byte("x"), 4096)
	if err := s.WorkerPut(exec, []byte("w1"), big); err != nil {
		t.Fatal(err)
	}
	if err := s.WorkerPut(exec, []byte("w2"), []byte("small")); err != nil {
		t.Fatal(err)
	}
	if len(store.objs) != 1 {
		t.Fatalf("%d objects in the bucket, want 1 (only the large value)", len(store.objs))
	}
	got, err := s.WorkerCollect(exec)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range got {
		found = found || bytes.Equal(v, big)
	}
	if !found {
		t.Fatalf("large value did not round-trip (%d values)", len(got))
	}
	s.FlushBlobCleanup()
	if len(store.objs) != 0 {
		t.Errorf("%d objects left after WorkerCollect consumed them", len(store.objs))
	}
	// A typed-nil vendor client is no blob store.
	var none *vgirpc.VendorBlobClient
	inline, err := NewExternalizingStorage(base, none, 1024, 0)
	if err != nil || inline.blobs != nil {
		t.Errorf("typed nil vendor client: blobs=%v err=%v", inline.blobs, err)
	}
}
