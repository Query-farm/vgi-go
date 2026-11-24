// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Direct object storage (direct-storage contract v1): instead of the blob
// vendor, a self-hosted worker can keep large values in the operator's own
// bucket. VGI_STORAGE_BACKEND (or, unset, which of VGI_BLOB_VENDOR_URL /
// VGI_STORAGE_S3_BUCKET / VGI_STORAGE_GCS_BUCKET is set) picks the backend;
// the S3 and GCS backends read their own VGI_STORAGE_S3_* / VGI_STORAGE_GCS_*
// settings.
//
// The backends are linked in by importing their packages, so a worker that
// never uses them does not carry the AWS and Google client libraries:
//
//	import _ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/all" // s3 + gcs
//	import _ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/s3"  // s3 only
//
// A worker started with VGI_STORAGE_BACKEND=s3 (or gcs) that did not link
// that backend refuses to start and names the import.

// ObjectStorage is what a direct storage backend provides: result
// externalization, objects by id (StateRef objects, argument references) and,
// usually, upload grants ([vgirpc.UploadURLProvider]).
type ObjectStorage interface {
	vgirpc.ExternalStorage
	vgirpc.ObjectStore
}

// ObjectStorageFactory builds a backend from the environment. It is called
// once, when the HTTP server is built.
type ObjectStorageFactory func(getenv func(string) string) (ObjectStorage, error)

var (
	objectStorageMu        sync.Mutex
	objectStorageFactories = map[string]ObjectStorageFactory{}
)

// RegisterObjectStorageBackend makes a direct storage backend available under
// name ("s3" or "gcs"). The objectstore packages call it from init.
func RegisterObjectStorageBackend(name string, factory ObjectStorageFactory) {
	objectStorageMu.Lock()
	defer objectStorageMu.Unlock()
	objectStorageFactories[strings.ToLower(name)] = factory
}

// objectStorageImports names the package that registers each backend.
var objectStorageImports = map[string]string{
	vgirpc.StorageBackendS3:  "github.com/Query-farm/vgi-go/vgi/storage/objectstore/s3",
	vgirpc.StorageBackendGCS: "github.com/Query-farm/vgi-go/vgi/storage/objectstore/gcs",
}

// newObjectStorageFromEnv builds the registered backend for name.
func newObjectStorageFromEnv(name string, getenv func(string) string) (ObjectStorage, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	objectStorageMu.Lock()
	factory := objectStorageFactories[name]
	var linked []string
	for n := range objectStorageFactories {
		linked = append(linked, n)
	}
	objectStorageMu.Unlock()
	if factory == nil {
		sort.Strings(linked)
		return nil, fmt.Errorf("%s=%s, but this worker was built without the %s storage backend (linked: %v): "+
			"add `import _ %q` (or .../objectstore/all) to its main package",
			vgirpc.EnvStorageBackend, name, name, linked, objectStorageImports[name])
	}
	storage, err := factory(getenv)
	if err != nil {
		return nil, fmt.Errorf("%s storage: %w", name, err)
	}
	return storage, nil
}

// objectStorageSummary describes a configured direct backend for the
// startup log (never a secret).
func objectStorageSummary(storage ObjectStorage) string {
	if d, ok := storage.(interface{ Describe() string }); ok {
		return d.Describe()
	}
	return fmt.Sprintf("%T", storage)
}
