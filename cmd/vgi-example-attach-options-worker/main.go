// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Example VGI worker exercising ATTACH-time options end-to-end. Mirrors
// vgi-python's vgi-example-attach-options-worker so the shared integration
// test (test/sql/integration/attach/attach_options_echo.test) runs against
// both implementations.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Query-farm/vgi-go/examples/attach_options"
	"github.com/Query-farm/vgi-go/internal/workercli"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

func main() {
	// Transport flags (--http / --unix / --tcp / --idle-timeout) + logging.
	// --unix is what the launcher lane needs: `launch:<binary>` makes the C++
	// launcher spawn this worker with --unix <path> and wait for UNIX:<path>.
	cli := workercli.Register()
	if err := cli.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}

	w := vgi.NewWorker(
		vgi.WithCatalogName(attach_options.CatalogName),
		vgi.WithAttachOptions(attach_options.AttachOptionSpecs()...),
		// A second catalog from the same worker, gated on an option the caller
		// must supply: it advertises that at discovery and refuses the
		// anonymous ATTACH, rather than attaching into something that reads as
		// an empty catalog. It carries no functions of its own — the attach IS
		// what's under test (attach/attach_options_required.test).
		vgi.WithCatalogAliasInfo(attach_options.RequiredCatalogName, vgi.CatalogInfo{
			Name: attach_options.RequiredCatalogName,
		}),
		vgi.WithAttachOptionsForCatalog(
			attach_options.RequiredCatalogName,
			attach_options.RequiredCatalogAttachOptionSpecs()...,
		),
		vgi.WithAttachValidator(func(req *vgi.CatalogAttachRequestWire, ctx *vgirpc.CallContext) (*vgi.AttachDecision, error) {
			var optBytes []byte
			if req.Options != nil {
				optBytes = *req.Options
			}
			attachOpaqueData, err := attach_options.EncodeAttachOpaqueData(optBytes)
			if err != nil {
				return nil, fmt.Errorf("encoding attach_opaque_data: %w", err)
			}
			return &vgi.AttachDecision{AttachOpaqueData: attachOpaqueData}, nil
		}),
	)

	w.RegisterTable(attach_options.NewEchoAttachOptionsFunction())

	if err := cli.Serve(w); err != nil {
		log.Fatal(err)
	}
}
