// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Example VGI worker exposing the "txfix" catalog: a writable table declared
// on its CatalogTable descriptor, per-ATTACH transactions with worker
// transaction hooks, and a secret ATTACH option. Backs the vgi repo's
// test/sql/integration/writable_tx/*.test files (VGI_WRITABLE_TX_WORKER).
// State is shared across worker processes in $VGI_TXFIX_STATE_DIR (default
// <system temp dir>/vgi-txfix).
package main

import (
	"log"
	"os"

	"github.com/Query-farm/vgi-go/examples/writable_tx"
	"github.com/Query-farm/vgi-go/internal/workercli"
	"github.com/Query-farm/vgi-go/vgi"
)

func main() {
	// Transport flags (--http / --unix / --tcp / --idle-timeout) + logging.
	cli := workercli.Register()
	if err := cli.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	w := vgi.NewWorker(writable_tx.Options()...)
	writable_tx.Register(w)
	if err := cli.Serve(w); err != nil {
		log.Fatal(err)
	}
}
