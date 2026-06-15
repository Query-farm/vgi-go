// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"os"
	"runtime/coverage"
	"time"
)

// startCoverageFlusher periodically snapshots coverage counters to $GOCOVERDIR.
//
// The integration suite runs this worker as a separate process and measures it
// through a `go build -cover` binary (Makefile COVER=1 / ci/run-integration.sh
// COVERAGE=1). A -cover binary normally writes its counters only on a clean
// exit, but the standalone test harness tears the long-lived worker down
// abruptly at the end of a run (its worker-pool cleanup signals the process
// before it can return from main). Snapshotting on an interval leaves
// near-complete coverage behind however the process is ultimately killed.
//
// No-op unless GOCOVERDIR is set — i.e. only during a coverage run; in normal
// operation it never starts a goroutine. WriteCountersDir returns an error
// (ignored) when the binary was not built with -cover, so this is safe to call
// unconditionally from main.
func startCoverageFlusher() {
	dir := os.Getenv("GOCOVERDIR")
	if dir == "" {
		return
	}
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Requires -covermode=atomic (live-readable counters); the build sets
			// it (Makefile COVER=1). Error ignored: a non-cover build no-ops here.
			_ = coverage.WriteCountersDir(dir)
		}
	}()
}
