// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package covflush makes `go build -cover` worker binaries flush their coverage
// counters reliably under the integration harness.
//
// The harness's worker pool terminates subprocess workers with SIGTERM (not a
// clean stdin-EOF exit), and different RPCs of one query often run in different
// pool subprocesses. A worker killed by SIGTERM never writes its covcounters
// pod via the normal exit path, so its coverage is lost — a systematic
// undercount (e.g. the INSERT worker's coverage vanishes while the SELECT
// worker's is kept). This flushes on SIGTERM/SIGINT before exiting, plus a
// periodic snapshot as a backstop for SIGKILL / hung workers.
//
// Requires the binary to be built with -covermode=atomic (live-readable
// counters; Makefile COVER=1). No-op unless GOCOVERDIR is set, so production
// runs are unaffected.
package covflush

import (
	"os"
	"os/signal"
	"runtime/coverage"
	"sync"
	"syscall"
	"time"
)

var once sync.Once

// Start begins coverage capture (idempotent; no-op unless GOCOVERDIR is set).
// Call it once early in a worker's main.
func Start() { once.Do(start) }

func start() {
	dir := os.Getenv("GOCOVERDIR")
	if dir == "" {
		return
	}
	flush := func() { _ = coverage.WriteCountersDir(dir) }

	// Periodic snapshot — backstop for workers killed by SIGKILL or hung in
	// shutdown, where the signal handler below never completes.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			flush()
		}
	}()

	// The pool kills workers with SIGTERM; flush counters before exiting. Also
	// covers SIGINT. This preempts any other SIGTERM handler (e.g. the HTTP
	// server's graceful shutdown), which is correct — the process is being torn
	// down and coverage is what we need to preserve.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-ch
		flush()
		os.Exit(0)
	}()
}
