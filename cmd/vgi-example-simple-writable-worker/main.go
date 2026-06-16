// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Example VGI worker exposing the simple_writable catalog. Mirrors vgi-python's
// vgi-fixture-simple-writable-worker so the shared
// test/sql/integration/simple_writable/*.test files run against the Go worker
// too, exercising the INSERT/UPDATE/DELETE/RETURNING wire path.
package main

import (
	"flag"
	"log"

	"github.com/Query-farm/vgi-go/examples/simple_writable"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/resolve"
)

func main() {
	httpMode := flag.Bool("http", false, "Run as HTTP server instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	// Snapshot coverage periodically during integration coverage runs (no-op
	// otherwise); see cmd/vgi-example-worker/coverage.go for the rationale.
	startCoverageFlusher()

	// Per-attach row storage uses AttachStore, which needs a FunctionStorage
	// backend (SQLite by default; honors VGI_WORKER_SHARED_STORAGE).
	storage, err := resolve.FromEnv()
	if err != nil {
		log.Fatalf("resolve storage backend: %v", err)
	}

	opts := append(simple_writable.Options(), vgi.WithFunctionStorage(storage))
	w := vgi.NewWorker(opts...)
	simple_writable.Register(w)

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
	} else {
		w.RunStdio()
	}
}
