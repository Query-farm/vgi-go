// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/hex"
	"regexp"
	"testing"

	"github.com/google/uuid"
)

func TestDeriveShardKey(t *testing.T) {
	u := uuid.New()
	key, err := deriveShardKey(u[:])
	if err != nil {
		t.Fatalf("deriveShardKey: %v", err)
	}
	if want := "att-" + hex.EncodeToString(u[:]); key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	// att- + 32 lowercase hex chars = 36, <= 128, regex-safe.
	if len(key) != 36 {
		t.Fatalf("len(key) = %d, want 36 (%q)", len(key), key)
	}
	if !regexp.MustCompile(`^att-[0-9a-f]{32}$`).MatchString(key) {
		t.Fatalf("key %q does not match ^att-[0-9a-f]{32}$", key)
	}

	// Stable across calls (would be stable across re-seals): same uuid -> same key.
	key2, _ := deriveShardKey(u[:])
	if key != key2 {
		t.Fatalf("not stable: %q != %q", key, key2)
	}
	// Distinct uuids -> distinct keys.
	other := uuid.New()
	keyOther, _ := deriveShardKey(other[:])
	if key == keyOther {
		t.Fatalf("distinct uuids produced the same key %q", key)
	}
}

func TestDeriveShardKeyRejectsBadLength(t *testing.T) {
	for _, bad := range [][]byte{nil, {}, make([]byte, 8), make([]byte, 32)} {
		if _, err := deriveShardKey(bad); err == nil {
			t.Fatalf("expected error for %d-byte uuid", len(bad))
		}
	}
}
