// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package all links every direct storage backend (S3 and GCS) into a vgi
// worker. Import it for its side effect:
//
//	import _ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/all"
package all

import (
	_ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/gcs" // VGI_STORAGE_BACKEND=gcs
	_ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/s3"  // VGI_STORAGE_BACKEND=s3
)
