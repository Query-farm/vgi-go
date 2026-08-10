// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Standardized VGI worker HTTP landing surface.
//
// Two routes, registered by RunHttp:
//
//   - GET {prefix}/               — landing.html (browsers), or a small JSON
//     status document (?format=json / Accept: application/json)
//   - GET {prefix}/vgi-client.js  — the vendored browser build of the
//     @query-farm/vgi JS client, which the page imports
//
// The page reads catalog metadata by speaking the VGI protocol through that
// client — the same protocol the DuckDB extension speaks. There is no
// worker-side metadata document: what used to be a per-language describe.json
// producer here (plus a separately versioned JSON contract and a
// cross-language conformance harness) is now one client implementation shared
// by every worker, authored in vgi-web-frontend.
//
// The worker serves the bundle itself rather than the page importing it from a
// CDN: the page is same-origin with an authenticated worker and carries its
// session cookie, so third-party script here would run with full access to
// that origin — and a CDN dependency would break air-gapped deployments, which
// today need nothing but the worker.
//
// Worker identity (name, version, language) is not catalog data and has no
// protocol method, so it rides on the status document above.

package vgi

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"runtime/debug"
	"strings"
)

// landingHTML is the vendored, self-contained shared landing page. It is
// byte-identical to the copy vendored by every other VGI language worker (see
// vgi-web-frontend/public/landing.html) and carries a
// "<!-- vgi-landing-asset vN -->" marker.
//
//go:embed landing.html
var landingHTML []byte

// clientBundle is the vendored browser build of the VGI JS client that
// landing.html imports. Rebuild both together from vgi-web-frontend
// (`bun run build:landing-client`) and re-vendor as a pair.
//
//go:embed vgi-client.js
var clientBundle []byte

const cupolaBase = "https://cupola.query-farm.services"

// vgiGoVersion returns the build version of the module that embeds this
// package, falling back to "dev". Surfaced as the status document's version.
func vgiGoVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if strings.HasSuffix(dep.Path, "/vgi-go") {
				if dep.Version != "" {
					return dep.Version
				}
			}
		}
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
	}
	return "dev"
}

// makeLandingHandler returns the GET {prefix}/ handler: HTML for browsers, a
// JSON status document for health checks and for the page's own identity read.
func makeLandingHandler(name, serverID string, oauth bool) http.HandlerFunc {
	status := map[string]any{
		"status":      "ok",
		"server_id":   serverID,
		"protocol":    "vgi",
		"worker":      name,
		"doc":         "",
		"version":     vgiGoVersion(),
		"lang":        "go",
		"oauth":       oauth,
		"cupola_base": cupolaBase,
	}
	return func(rw http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		wantJSON := r.URL.Query().Get("format") == "json" ||
			(strings.Contains(accept, "application/json") && !strings.Contains(accept, "text/html"))
		if wantJSON {
			rw.Header().Set("Content-Type", "application/json")
			writeJSON(rw, status)
			return
		}
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write(landingHTML)
	}
}

// makeClientBundleHandler returns the GET {prefix}/vgi-client.js handler.
func makeClientBundleHandler() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		// Immutable for a given worker build: the page and the bundle are
		// vendored and released together.
		rw.Header().Set("Cache-Control", "public, max-age=3600")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write(clientBundle)
	}
}

func writeJSON(rw http.ResponseWriter, v any) {
	enc := json.NewEncoder(rw)
	if err := enc.Encode(v); err != nil {
		LogRPC.Debug("landing: json encode failed", "err", err)
	}
}
