// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Example VGI worker exposing the simple_writable catalog. Mirrors vgi-python's
// vgi-fixture-simple-writable-worker so the shared
// test/sql/integration/simple_writable/*.test files run against the Go worker
// too, exercising the INSERT/UPDATE/DELETE/RETURNING wire path.
package main

import (
	"log"
	"os"

	"github.com/Query-farm/vgi-go/examples/simple_writable"
	"github.com/Query-farm/vgi-go/internal/workercli"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/resolve"
)

func main() {
	// Transport flags (--http / --unix / --tcp / --idle-timeout) + logging.
	// --unix is what the launcher lane needs: `launch:<binary>` makes the C++
	// launcher spawn this worker with --unix <path> and wait for UNIX:<path>.
	cli := workercli.Register()
	if err := cli.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}

	// Per-attach row storage uses AttachStore, which needs a FunctionStorage
	// backend (SQLite by default; honors VGI_WORKER_SHARED_STORAGE).
	storage, err := resolve.FromEnv()
	if err != nil {
		log.Fatalf("resolve storage backend: %v", err)
	}

	opts := append(simple_writable.Options(), vgi.WithFunctionStorage(storage))
	w := vgi.NewWorker(opts...)
	simple_writable.Register(w)

	if err := cli.Serve(w); err != nil {
		log.Fatal(err)
	}
}
