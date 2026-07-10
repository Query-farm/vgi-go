// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"strconv"
)

// Result-cache control metadata (vgi.cache.*).
//
// A table function advertises that its result is cacheable by the client (the
// DuckDB extension) by attaching vgi.cache.* metadata to the FIRST data batch
// it emits. The vocabulary mirrors HTTP caching (RFC 9111/9110): a freshness
// lifetime (Ttl/Expires), a reuse Scope, validators (ETag / LastModified) for
// conditional revalidation, and stale-serving grace windows.
//
// The key strings are the single source of truth shared with the C++ extension
// (which reads them by string) and with vgi-python's vgi/cache_control.py.
//
// Advertise cacheability by passing a *CacheControl to the first emit:
//
//	vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(300)}))
//
// or by passing the rendered keys directly:
//
//	out.EmitWithMetadata(batch, map[string]string{vgi.CacheTTLKey: "300"})
//
// Booleans render as "1" (present) and are omitted when false; timestamps are
// RFC 3339 UTC strings; durations are integer seconds.

// Response-side metadata keys (worker -> client). Defined once here; the C++
// extension reads these exact strings.
const (
	CacheTTLKey                  = "vgi.cache.ttl"
	CacheExpiresKey              = "vgi.cache.expires"
	CacheNoStoreKey              = "vgi.cache.no_store"
	CacheScopeKey                = "vgi.cache.scope"
	CacheETagKey                 = "vgi.cache.etag"
	CacheLastModifiedKey         = "vgi.cache.last_modified"
	CacheRevalidatableKey        = "vgi.cache.revalidatable"
	CacheStaleWhileRevalidateKey = "vgi.cache.stale_while_revalidate"
	CacheStaleIfErrorKey         = "vgi.cache.stale_if_error"
	CacheNotModifiedKey          = "vgi.cache.not_modified"
	CacheIfNoneMatchKey          = "vgi.cache.if_none_match"
	CacheIfModifiedSinceKey      = "vgi.cache.if_modified_since"
)

// Reuse-scope values for CacheControl.Scope.
const (
	// CacheScopeCatalog makes the result reusable across transactions within
	// the calling catalog identity. This is the default.
	CacheScopeCatalog = "catalog"
	// CacheScopeTransaction restricts reuse to the same transaction.
	CacheScopeTransaction = "transaction"
)

// CacheControl is the cacheability a table function advertises on its first
// result batch.
//
// Presence of Ttl or Expires is what makes a result cacheable; NoStore
// overrides any freshness key. Every field is optional except Scope, which
// defaults to CacheScopeCatalog when empty.
type CacheControl struct {
	// Ttl is the freshness lifetime in whole seconds, relative to full-result
	// receipt (skew-immune; wins over Expires). Nil leaves it unset — use
	// Seconds(0) for an always-revalidate ("no-cache") result.
	Ttl *int64
	// Expires is an absolute RFC 3339 UTC deadline. Lifetime is expires-now at
	// receipt.
	Expires string
	// Scope is the reuse scope: CacheScopeCatalog (default when empty) or
	// CacheScopeTransaction.
	Scope string
	// NoStore is an explicit "never cache"; it overrides any freshness key.
	NoStore bool
	// ETag is a strong validator (opaque quoted string) for conditional
	// revalidation.
	ETag string
	// LastModified is a weaker RFC 3339 UTC validator; the fallback when no
	// ETag is set.
	LastModified string
	// Revalidatable declares that the worker can check freshness cheaply
	// without recomputing. It gates whether the client ever sends a
	// conditional request.
	Revalidatable bool
	// StaleWhileRevalidate is a grace window (seconds) to serve stale
	// immediately while revalidating in the background.
	StaleWhileRevalidate *int64
	// StaleIfError is a grace window (seconds) to serve stale if a
	// revalidation RPC fails.
	StaleIfError *int64
	// NotModified is the 304 equivalent — set it on a 0-row batch in reply to
	// a conditional request to assert the client's stored payload is still
	// fresh (the client reuses it instead of re-streaming).
	NotModified bool
}

// Seconds returns a pointer to n, for the duration fields of CacheControl.
func Seconds(n int64) *int64 { return &n }

// Validate checks the scope and the non-negative duration invariants.
func (c *CacheControl) Validate() error {
	switch c.Scope {
	case "", CacheScopeCatalog, CacheScopeTransaction:
	default:
		return fmt.Errorf("CacheControl.Scope must be %q or %q, got %q", CacheScopeCatalog, CacheScopeTransaction, c.Scope)
	}
	for name, value := range map[string]*int64{
		"Ttl":                  c.Ttl,
		"StaleWhileRevalidate": c.StaleWhileRevalidate,
		"StaleIfError":         c.StaleIfError,
	} {
		if value != nil && *value < 0 {
			return fmt.Errorf("CacheControl.%s must be >= 0, got %d", name, *value)
		}
	}
	return nil
}

// Metadata renders the CacheControl to its vgi.cache.* batch-metadata keys.
// Booleans render as "1" and are omitted when false; unset optional fields are
// omitted entirely. Scope is always emitted so the client never has to infer
// the default.
func (c *CacheControl) Metadata() map[string]string {
	md := make(map[string]string, 6)
	if c.Ttl != nil {
		md[CacheTTLKey] = strconv.FormatInt(*c.Ttl, 10)
	}
	if c.Expires != "" {
		md[CacheExpiresKey] = c.Expires
	}
	if c.NoStore {
		md[CacheNoStoreKey] = "1"
	}
	scope := c.Scope
	if scope == "" {
		scope = CacheScopeCatalog
	}
	md[CacheScopeKey] = scope
	if c.ETag != "" {
		md[CacheETagKey] = c.ETag
	}
	if c.LastModified != "" {
		md[CacheLastModifiedKey] = c.LastModified
	}
	if c.Revalidatable {
		md[CacheRevalidatableKey] = "1"
	}
	if c.StaleWhileRevalidate != nil {
		md[CacheStaleWhileRevalidateKey] = strconv.FormatInt(*c.StaleWhileRevalidate, 10)
	}
	if c.StaleIfError != nil {
		md[CacheStaleIfErrorKey] = strconv.FormatInt(*c.StaleIfError, 10)
	}
	if c.NotModified {
		md[CacheNotModifiedKey] = "1"
	}
	return md
}
