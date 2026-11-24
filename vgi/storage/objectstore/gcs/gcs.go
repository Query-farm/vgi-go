// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package gcs links the direct Google Cloud Storage backend into a vgi
// worker: with VGI_STORAGE_BACKEND=gcs (or VGI_STORAGE_GCS_BUCKET set) the
// worker keeps large values in the operator's own bucket, configured by the
// VGI_STORAGE_GCS_* environment. Import it for its side effect:
//
//	import _ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/gcs"
package gcs

import (
	"github.com/Query-farm/vgi-go/vgi"
	vgigcs "github.com/Query-farm/vgi-rpc-go/vgirpc/gcs"
)

func init() {
	vgi.RegisterObjectStorageBackend("gcs", func(getenv func(string) string) (vgi.ObjectStorage, error) {
		storage, err := vgigcs.FromEnv(getenv)
		if err != nil {
			return nil, err
		}
		return withFunctionStore{storage}, nil
	})
}

// withFunctionStore exposes the bucket's function-storage area.
type withFunctionStore struct{ *vgigcs.GCSStorage }

// FunctionObjectStore is the bucket's function-storage area (<prefix>fs/).
func (w withFunctionStore) FunctionObjectStore() vgi.FunctionObjectStore { return w.FunctionStore() }
