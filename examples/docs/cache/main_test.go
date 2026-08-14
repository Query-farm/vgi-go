// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Tests for the result-caching documentation example.
package main

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
)

// Presence of Ttl (or Expires) is what makes a result cacheable at all. A
// CacheControl carrying only validators advertises nothing, and the failure mode
// is silence — the query still returns the right answer, just without reuse.
func TestAdvertisedControlIsActuallyCacheable(t *testing.T) {
	cc := &vgi.CacheControl{
		Ttl:                  vgi.Seconds(300),
		ETag:                 dataVersion,
		Revalidatable:        true,
		StaleWhileRevalidate: vgi.Seconds(60),
		StaleIfError:         vgi.Seconds(600),
	}
	if cc.Ttl == nil {
		t.Fatal("without Ttl or Expires the result is not cacheable at all")
	}
	if *cc.Ttl != 300 {
		t.Errorf("ttl = %d, want 300", *cc.Ttl)
	}
	// Revalidatable is what gates whether the client ever sends a conditional
	// request; an ETag without it is never consulted.
	if cc.ETag != "" && !cc.Revalidatable {
		t.Error("an ETag is only useful alongside Revalidatable")
	}
}

// The 304-equivalent reply must be a zero-row batch carrying NotModified. A
// non-empty batch would be treated as fresh data and replace what the client
// already holds.
func TestNotModifiedReplyIsEmpty(t *testing.T) {
	cols := emptyColumns()
	for _, c := range cols {
		if c.Len() != 0 {
			t.Fatalf("the not-modified reply must carry zero rows, got %d", c.Len())
		}
		c.Release()
	}
}

func TestBuildRatesMatchesItsSchema(t *testing.T) {
	batch := buildRates()
	defer batch.Release()
	if got := int(batch.NumRows()); got != len(currencies) {
		t.Fatalf("rows = %d, want %d", got, len(currencies))
	}
	if !batch.Schema().Equal(ratesSchema) {
		t.Errorf("emitted schema %v does not match the bound schema %v", batch.Schema(), ratesSchema)
	}
}
