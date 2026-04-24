// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Example VGI worker exercising ATTACH-time options end-to-end. Mirrors
// vgi-python's vgi-example-attach-options-worker so the shared integration
// test (test/sql/integration/attach/attach_options_echo.test) runs against
// both implementations.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Query-farm/vgi-go/examples/attach_options"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
)

func main() {
	httpMode := flag.Bool("http", false, "Run as HTTP server instead of stdio")
	flag.Parse()

	w := vgi.NewWorker(
		vgi.WithCatalogName(attach_options.CatalogName),
		vgi.WithAttachOptions(attach_options.AttachOptionSpecs()...),
		vgi.WithAttachValidator(func(req *vgi.CatalogAttachRequestWire, ctx *vgirpc.CallContext) (*vgi.AttachDecision, error) {
			var optBytes []byte
			if req.Options != nil {
				optBytes = *req.Options
			}
			attachID, err := attach_options.EncodeAttachID(optBytes)
			if err != nil {
				return nil, fmt.Errorf("encoding attach_id: %w", err)
			}
			return &vgi.AttachDecision{AttachID: attachID}, nil
		}),
	)

	w.RegisterTable(attach_options.NewEchoAttachOptionsFunction())

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
	} else {
		w.RunStdio()
	}
}
