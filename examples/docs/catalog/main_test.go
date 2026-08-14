// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Tests for the catalog documentation example.
package main

import "testing"

// State has to be gob-encodable so it can survive an HTTP continuation, and the
// SDK checks at registration rather than mid-query. A struct whose fields are
// all unexported panics inside AsTableFunction — which is why `city` exports
// Name and Pop. Calling the constructor is the assertion.
func TestScanStateIsGobEncodable(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("AsTableFunction rejected the scan state — every type reachable "+
				"from citiesState must have exported, gob-encodable fields: %v", r)
		}
	}()
	if fn := NewCitiesScan(); fn.Name() != "cities_scan" {
		t.Fatalf("name = %q", fn.Name())
	}
}

func TestSelectCitiesFiltersByPopulation(t *testing.T) {
	if got := len(selectCities(0)); got != 3 {
		t.Errorf("minPopulation=0 should return every city, got %d", got)
	}
	got := selectCities(100_000)
	if len(got) != 2 {
		t.Fatalf("minPopulation=100000 should return 2 cities, got %d", len(got))
	}
	for _, c := range got {
		if c.Pop < 100_000 {
			t.Errorf("%s (%d) should have been filtered out", c.Name, c.Pop)
		}
	}
	if got := selectCities(1_000_000); got != nil {
		t.Errorf("a threshold above every city should return nothing, got %v", got)
	}
}
