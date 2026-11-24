// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package s3 links the direct S3 storage backend into a vgi worker: with
// VGI_STORAGE_BACKEND=s3 (or VGI_STORAGE_S3_BUCKET set) the worker keeps large
// values in the operator's own bucket (AWS S3 or an S3-compatible service),
// configured by the VGI_STORAGE_S3_* environment. Import it for its side
// effect:
//
//	import _ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/s3"
package s3

import (
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	vgis3 "github.com/Query-farm/vgi-rpc-go/vgirpc/s3"
)

func init() {
	vgi.RegisterObjectStorageBackend("s3", func(getenv func(string) string) (vgi.ObjectStorage, error) {
		storage, err := vgis3.FromEnv(getenv)
		if err != nil {
			return nil, err
		}
		return described{storage}, nil
	})
}

// described adds the startup summary.
type described struct{ *vgis3.S3Storage }

// FunctionObjectStore is the bucket's function-storage area (<prefix>fs/).
func (d described) FunctionObjectStore() vgi.FunctionObjectStore { return d.FunctionStore() }

// Describe is the startup summary line (no secrets).
func (d described) Describe() string {
	return fmt.Sprintf("s3 bucket=%s region=%s checksum=%s upload_grants=%v",
		d.Bucket(), d.Region(), d.Checksum(), d.UploadGrantsEnabled())
}
