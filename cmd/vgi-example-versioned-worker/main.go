// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Example VGI worker that exercises ATTACH-time data_version_spec and
// implementation_version validation. Mirrors the reference
// vgi-example-versioned-worker from vgi-python so the shared versioning
// integration tests (test/sql/integration/attach/versioning*.test) can run
// against either implementation.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
)

const (
	catalogName           = "versioned"
	implementationVersion = "1.0.0"
	dataVersionSpec       = ">=1.0.0,<2.0.0"
	defaultDataVersion    = "1.2.0"
	stickyCookieName      = "vgi_sticky"
)

var supportedDataVersions = map[string]struct{}{
	"1.0.0": {},
	"1.1.0": {},
	"1.2.0": {},
}

func main() {
	httpMode := flag.Bool("http", false, "Run as HTTP server instead of stdio")
	flag.Parse()

	dvs := dataVersionSpec
	impl := implementationVersion

	w := vgi.NewWorker(
		vgi.WithCatalogName(catalogName),
		vgi.WithCatalogComment("Example catalog demonstrating data_version_spec validation and cookie stickiness"),
		vgi.WithCatalogInfo(vgi.CatalogInfo{
			Name:                  catalogName,
			ImplementationVersion: &impl,
			DataVersionSpec:       &dvs,
		}),
		vgi.WithAttachValidator(func(req *vgi.CatalogAttachRequestWire, ctx *vgirpc.CallContext) (*vgi.AttachDecision, error) {
			if req.ImplementationVersion != nil && *req.ImplementationVersion != implementationVersion {
				return nil, fmt.Errorf("Unsupported implementation_version %q; this worker serves %q",
					*req.ImplementationVersion, implementationVersion)
			}
			resolvedData := defaultDataVersion
			if req.DataVersionSpec != nil {
				v := *req.DataVersionSpec
				if _, ok := supportedDataVersions[v]; !ok {
					return nil, fmt.Errorf("Unsupported data_version_spec %q; this worker serves exact matches from {1.0.0, 1.1.0, 1.2.0}", v)
				}
				resolvedData = v
			}
			// Best-effort sticky cookie (ignored on subprocess transport).
			if ctx != nil {
				_ = ctx.SetCookie(stickyCookieName, newSessionID(), vgirpc.CookieAttrs{Path: "/"})
			}
			return &vgi.AttachDecision{
				ResolvedDataVersion:           resolvedData,
				ResolvedImplementationVersion: implementationVersion,
			}, nil
		}),
	)

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
	} else {
		w.RunStdio()
	}
}

func newSessionID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "0000000000000000"
	}
	return hex.EncodeToString(buf[:])
}
