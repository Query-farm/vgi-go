// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"os"
	"runtime/coverage"
	"time"
)

// startCoverageFlusher periodically snapshots coverage counters to $GOCOVERDIR.
// See cmd/vgi-example-worker/coverage.go for the full rationale: the integration
// suite measures this worker via a `go build -cover` binary and the harness may
// terminate it without a clean exit, so we snapshot on an interval. No-op unless
// GOCOVERDIR is set; needs -covermode=atomic (Makefile COVER=1).
func startCoverageFlusher() {
	dir := os.Getenv("GOCOVERDIR")
	if dir == "" {
		return
	}
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			_ = coverage.WriteCountersDir(dir)
		}
	}()
}
