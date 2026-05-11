// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi_test

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storagetest"
)

// TestSQLiteStorage_Conformance runs the shared FunctionStorage behavioral
// contract against the SQLite backend. Future backends (Cloudflare DO, ...)
// plug into the same storagetest.RunConformance entrypoint.
func TestSQLiteStorage_Conformance(t *testing.T) {
	storagetest.RunConformance(t, func(t *testing.T) vgi.FunctionStorage {
		s, err := vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: ":memory:"})
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
