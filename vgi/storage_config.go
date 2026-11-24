// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"os"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// configureStorage applies the HTTP storage settings: the blob vendor
// (VGI_BLOB_VENDOR_URL), direct object storage (VGI_STORAGE_BACKEND=s3|gcs) or
// none, chosen by vgirpc.StorageBackendFromEnv. Returns the external-location
// config (nil when everything stays inline).
func (w *Worker) configureStorage(hs *vgirpc.HttpServer, hosted bool) (*vgirpc.ExternalLocationConfig, error) {
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
		config, err := vgirpc.ConfigureHTTPServerWithStorage(hs, storage, nil)
		if err != nil {
			return nil, fmt.Errorf("%s storage: %w", backend, err)
		}
		fmt.Fprintf(os.Stderr, "Direct storage enabled: %s threshold=%d bytes\n",
			objectStorageSummary(storage), config.ExternalizeThresholdBytes)
		if hosted {
			LogRPC.Warn("Orchard hosted mode with direct " + backend + " storage: large values go to the operator's bucket, not the blob vendor")
		}
		if !vgirpc.StorageVendsUploadGrants(storage) {
			LogRPC.Warn("direct storage vends no upload grants (VGI_STORAGE_S3_CHECKSUM=none): argument references and large requests stay inline; results are still externalized")
		}
		return config, nil
	}
	config, err := vgirpc.ConfigureHTTPServerFromEnv(hs, nil)
	if err != nil {
		return nil, fmt.Errorf("blob vendor: %w", err)
	}
	if config != nil {
		fmt.Fprintf(os.Stderr, "Blob vendor enabled: threshold=%d bytes\n", config.ExternalizeThresholdBytes)
	}
	return config, nil
}
