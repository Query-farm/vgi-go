// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package resolve picks a FunctionStorage backend at startup based on the
// VGI_WORKER_SHARED_STORAGE environment variable, mirroring vgi-python's
// vgi.function._resolve_storage(). Workers that want env-driven selection
// wire the result into vgi.NewWorker via vgi.WithFunctionStorage:
//
//	storage, err := resolve.FromEnv()
//	if err != nil { log.Fatal(err) }
//	w := vgi.NewWorker(vgi.WithFunctionStorage(storage), ...)
//
// Supported values:
//
//	memory                       → in-process SQLite at ":memory:"
//	                                (process-local, no cross-process
//	                                 coordination — single-process only)
//	sqlite (default, or unset)   → local SQLite at the per-user state path
//	cloudflare-do                → Cloudflare Worker + Durable Object
//	                                (requires VGI_CF_DO_URL,
//	                                 optionally VGI_CF_DO_TOKEN)
//
// The result is wrapped in vgi.ExternalizingStorage (the value envelope, with
// large values in the blob vendor at VGI_BLOB_VENDOR_URL) always for
// cloudflare-do, whose values are capped at cfdo.MaxValueBytes, and for any
// backend when VGI_STORAGE_EXTERNALIZE=1 (see vgi.WrapStorageFromEnv).
//
// Sits in its own sub-package so it can import every backend without
// pulling them all into the core vgi package's dependency graph; workers
// that don't import resolve don't pay for the imports it triggers.
package resolve

import (
	"fmt"
	"os"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/cfdo"
)

// EnvVar is the environment variable consulted by FromEnv. Matches
// vgi-python's value so deployments using both clients can configure them
// the same way.
const EnvVar = "VGI_WORKER_SHARED_STORAGE"

// FromEnv reads VGI_WORKER_SHARED_STORAGE and returns the configured
// FunctionStorage backend. Returns an error for unknown values or when a
// backend's required environment is missing (e.g. cloudflare-do without
// VGI_CF_DO_URL).
//
// An empty or unset variable resolves to the local SQLite backend so
// existing workers running without any env configuration keep working.
func FromEnv() (vgi.FunctionStorage, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv(EnvVar)))
	var base vgi.FunctionStorage
	var err error
	maxValueBytes := 0
	switch backend {
	case "memory":
		// In-process tier: ":memory:" is process-local with no cross-process
		// coordination. Correct only for single-process deployments.
		base, err = vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: ":memory:"})
	case "", "sqlite":
		base, err = vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{})
	case "cloudflare-do":
		base, err = cfdo.FromEnv()
		maxValueBytes = cfdo.MaxValueBytes
	default:
		return nil, fmt.Errorf(
			"resolve: unknown %s=%q (supported: memory, sqlite, cloudflare-do)",
			EnvVar, backend,
		)
	}
	if err != nil {
		return nil, err
	}
	// The value envelope / blob-vendor wrapper: always for cloudflare-do (its
	// per-value cap), and for any backend with VGI_STORAGE_EXTERNALIZE=1.
	wrapped, err := vgi.WrapStorageFromEnv(base, maxValueBytes)
	if err != nil {
		_ = base.Close()
		return nil, err
	}
	return wrapped, nil
}
