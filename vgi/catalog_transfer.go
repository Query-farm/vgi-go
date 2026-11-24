// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"net/http"
	"sync"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Catalog object transfers (the published snapshot PUT and its readback)
// keep a whole-request deadline -- a bounded readback is part of the
// publication contract -- but it scales with the object's size, so a large
// catalog on a slow link is not cut off: the deadline is the larger of 30 s
// and size / rate, where rate is a conservative floor transfer rate
// (MinTransferRate, default [DefaultCatalogTransferRate]). Connect and stall
// timeouts come from the storage transport (vgirpc.NewStorageTransport).
const (
	// DefaultCatalogTransferRate is the floor transfer rate, in bytes per
	// second, a catalog object deadline allows for: 64 KiB/s.
	DefaultCatalogTransferRate int64 = 64 << 10
	catalogTransferMinDeadline       = 30 * time.Second
)

// catalogTransferDeadline is the whole-request deadline for moving size
// bytes at no less than rate bytes/s (<= 0: DefaultCatalogTransferRate).
func catalogTransferDeadline(size, rate int64) time.Duration {
	if rate <= 0 {
		rate = DefaultCatalogTransferRate
	}
	return max(catalogTransferMinDeadline, time.Duration(float64(size)/float64(rate)*float64(time.Second)))
}

// catalogStorageTransport is the shared storage transport (30 s connect,
// stall timeout from VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS) for catalog
// object requests made without a caller-supplied client.
var catalogStorageTransport = sync.OnceValue(func() http.RoundTripper { return vgirpc.NewStorageTransport(0) })
