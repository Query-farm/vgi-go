// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"testing"
)

// TestCacheControlMetadata pins the vgi.cache.* rendering: which keys appear,
// which are omitted, and the exact "1" spelling the C++ extension parses for
// the boolean advertisements.
func TestCacheControlMetadata(t *testing.T) {
	tests := []struct {
		name string
		cc   CacheControl
		// want is asserted key-by-key.
		want map[string]string
		// absent keys must not appear at all (a present-but-empty value would
		// still be "present" to the extension's parser).
		absent []string
	}{
		{
			name:   "zero value renders only the default scope",
			cc:     CacheControl{},
			want:   map[string]string{CacheScopeKey: CacheScopeCatalog},
			absent: []string{CacheTTLKey, CacheNoStoreKey, CachePartitionScopeKey, CachePerValueKey},
		},
		{
			name: "ttl and the default scope",
			cc:   CacheControl{Ttl: Seconds(300)},
			want: map[string]string{CacheTTLKey: "300", CacheScopeKey: CacheScopeCatalog},
			absent: []string{
				CacheNoStoreKey, CacheRevalidatableKey, CacheNotModifiedKey,
				CachePartitionScopeKey, CachePerValueKey,
			},
		},
		{
			name:   "no_store renders the flag without a freshness key",
			cc:     CacheControl{NoStore: true},
			want:   map[string]string{CacheNoStoreKey: "1", CacheScopeKey: CacheScopeCatalog},
			absent: []string{CacheTTLKey, CacheExpiresKey},
		},
		{
			name: "transaction scope",
			cc:   CacheControl{Ttl: Seconds(300), Scope: CacheScopeTransaction},
			want: map[string]string{CacheScopeKey: CacheScopeTransaction, CacheTTLKey: "300"},
		},
		{
			name: "revalidation contract",
			cc:   CacheControl{Ttl: Seconds(0), ETag: `"rev-v1"`, Revalidatable: true},
			want: map[string]string{
				CacheTTLKey:           "0",
				CacheETagKey:          `"rev-v1"`,
				CacheRevalidatableKey: "1",
			},
			absent: []string{CacheNotModifiedKey},
		},
		{
			name: "not_modified renders",
			cc:   CacheControl{Ttl: Seconds(0), ETag: `"rev-v1"`, Revalidatable: true, NotModified: true},
			want: map[string]string{CacheNotModifiedKey: "1"},
		},
		{
			name: "stale windows and timestamps",
			cc: CacheControl{
				Ttl:                  Seconds(10),
				StaleWhileRevalidate: Seconds(5),
				StaleIfError:         Seconds(7),
				Expires:              "2026-01-01T00:00:00Z",
				LastModified:         "2025-01-01T00:00:00Z",
			},
			want: map[string]string{
				CacheStaleWhileRevalidateKey: "5",
				CacheStaleIfErrorKey:         "7",
				CacheExpiresKey:              "2026-01-01T00:00:00Z",
				CacheLastModifiedKey:         "2025-01-01T00:00:00Z",
			},
		},
		{
			name: "partition_scope is additive to the whole-scan freshness keys",
			cc:   CacheControl{Ttl: Seconds(300), PartitionScope: true},
			want: map[string]string{CachePartitionScopeKey: "1", CacheTTLKey: "300"},
			// The two opt-ins are independent advertisements.
			absent: []string{CachePerValueKey},
		},
		{
			name: "per_value is additive to the whole-scan freshness keys",
			cc:   CacheControl{Ttl: Seconds(300), PerValue: true},
			want: map[string]string{CachePerValueKey: "1", CacheTTLKey: "300"},
			// Opting into per-value memoization must not imply per-partition.
			absent: []string{CachePartitionScopeKey},
		},
		{
			name: "per_value without a freshness key still advertises",
			cc:   CacheControl{PerValue: true},
			want: map[string]string{CachePerValueKey: "1", CacheScopeKey: CacheScopeCatalog},
		},
		{
			name: "both opt-ins together",
			cc:   CacheControl{Ttl: Seconds(300), PartitionScope: true, PerValue: true},
			want: map[string]string{CachePartitionScopeKey: "1", CachePerValueKey: "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := tt.cc.Metadata()
			for k, want := range tt.want {
				got, ok := md[k]
				if !ok {
					t.Fatalf("metadata missing %q (got %v)", k, md)
				}
				if got != want {
					t.Fatalf("metadata[%q] = %q, want %q", k, got, want)
				}
			}
			for _, k := range tt.absent {
				if got, ok := md[k]; ok {
					t.Fatalf("metadata[%q] = %q, want it absent", k, got)
				}
			}
		})
	}
}

// TestCacheControlKeysMatchWireContract pins the literal key strings; the C++
// extension matches them by string, so a rename here silently disables a tier.
func TestCacheControlKeysMatchWireContract(t *testing.T) {
	for key, want := range map[string]string{
		CacheTTLKey:            "vgi.cache.ttl",
		CacheExpiresKey:        "vgi.cache.expires",
		CacheNoStoreKey:        "vgi.cache.no_store",
		CacheScopeKey:          "vgi.cache.scope",
		CachePartitionScopeKey: "vgi.cache.partition_scope",
		CachePerValueKey:       "vgi.cache.per_value",
	} {
		if key != want {
			t.Fatalf("key = %q, want %q", key, want)
		}
	}
}

// TestCacheControlValidate covers the scope and non-negative duration rules.
func TestCacheControlValidate(t *testing.T) {
	ok := []CacheControl{
		{},
		{Scope: CacheScopeCatalog},
		{Scope: CacheScopeTransaction},
		{Ttl: Seconds(0), PerValue: true},
	}
	for _, cc := range ok {
		if err := cc.Validate(); err != nil {
			t.Fatalf("Validate(%+v) = %v, want nil", cc, err)
		}
	}
	bad := []CacheControl{
		{Scope: "session"},
		{Ttl: Seconds(-1)},
		{StaleWhileRevalidate: Seconds(-1)},
		{StaleIfError: Seconds(-1)},
	}
	for _, cc := range bad {
		if err := cc.Validate(); err == nil {
			t.Fatalf("Validate(%+v) = nil, want an error", cc)
		}
	}
}

// ExampleCacheControl_perValue shows when per-value memoization is worth
// advertising: a function whose single call is far more expensive than the
// cache probe + decode + assembly that a memo serve costs.
//
// Here each call geocodes an address against a rate-limited remote service —
// tens of milliseconds and a quota unit per distinct address. Addresses repeat
// heavily across rows, so memoizing per distinct input value removes almost all
// of that work. A worker doing arithmetic instead should leave PerValue at its
// false default: for a cheap map the memo serve costs far more than the call it
// replaces.
func ExampleCacheControl_perValue() {
	cc := &CacheControl{
		Ttl:      Seconds(86400), // geocodes are stable for a day
		PerValue: true,           // one call ≫ one cache probe, so memoize per address
	}
	md := cc.Metadata()
	fmt.Println(md[CachePerValueKey])
	fmt.Println(md[CacheTTLKey])
	// Output:
	// 1
	// 86400
}
