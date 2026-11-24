// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"
	"time"
)

func TestCatalogTransferDeadlineScalesWithSize(t *testing.T) {
	cases := []struct {
		size, rate int64
		want       time.Duration
	}{
		{0, 0, 30 * time.Second},
		{1 << 20, 0, 30 * time.Second},        // 16 s at 64 KiB/s: the 30 s floor
		{64 << 20, 0, 1024 * time.Second},     // the 64 MiB catalog cap at 64 KiB/s
		{64 << 20, 1 << 20, 64 * time.Second}, // a configured 1 MiB/s floor
		{100 << 20, -1, 1600 * time.Second},   // a non-positive rate is the default
	}
	for _, c := range cases {
		if got := catalogTransferDeadline(c.size, c.rate); got != c.want {
			t.Errorf("catalogTransferDeadline(%d, %d) = %s, want %s", c.size, c.rate, got, c.want)
		}
	}
}
