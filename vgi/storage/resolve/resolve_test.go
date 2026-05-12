// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package resolve_test

import (
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi/storage/resolve"
)

func TestFromEnv_DefaultsToSQLite(t *testing.T) {
	t.Setenv(resolve.EnvVar, "")
	s, err := resolve.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Sanity: a basic op works.
	if _, err := s.QueuePush([]byte("test-exec"), [][]byte{[]byte("x")}); err != nil {
		t.Errorf("QueuePush: %v", err)
	}
}

func TestFromEnv_ExplicitSQLite(t *testing.T) {
	t.Setenv(resolve.EnvVar, "sqlite")
	s, err := resolve.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func TestFromEnv_CloudflareDOMissingURL(t *testing.T) {
	t.Setenv(resolve.EnvVar, "cloudflare-do")
	t.Setenv("VGI_CF_DO_URL", "")
	_, err := resolve.FromEnv()
	if err == nil {
		t.Fatal("expected error when VGI_CF_DO_URL is unset")
	}
	if !strings.Contains(err.Error(), "VGI_CF_DO_URL") {
		t.Errorf("error should mention VGI_CF_DO_URL, got: %v", err)
	}
}

func TestFromEnv_UnknownBackend(t *testing.T) {
	t.Setenv(resolve.EnvVar, "azure-sql")
	_, err := resolve.FromEnv()
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
	if !strings.Contains(err.Error(), "azure-sql") {
		t.Errorf("error should mention the bad value, got: %v", err)
	}
}

func TestFromEnv_CaseInsensitive(t *testing.T) {
	t.Setenv(resolve.EnvVar, "  SQLITE  ")
	s, err := resolve.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}
