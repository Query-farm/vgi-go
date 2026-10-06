#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run vgi-rpc's hosted-protocols conformance group against the example worker
# on stdio, a unix socket and HTTP (with vgi_rpc.Identity.v1 opted in).
#
# The worker hosts conformance.Secondary.v1 beside vgi.v2 through
# vgi.WithHostedProtocols, so this is the acceptance test for that hook and for
# vgi.WithIdentity. The expected list, vgi.v2,conformance.Secondary.v1, is not
# alphabetical, which is what lets it catch a server that sorts its listing.
#
# Environment: PYTHON (interpreter with vgi-rpc[http,conformance], pytest,
# pytest-timeout), WORKER (default ./vgi-example-worker-go).
set -euo pipefail

PYTHON="${PYTHON:-python3}"
WORKER="${WORKER:-$(pwd)/vgi-example-worker-go}"
EXPECT="vgi.v2,conformance.Secondary.v1"
hosted() { "$PYTHON" -m vgi_rpc.conformance.hosted_protocols "$@" -- -q; }

pids=()
sock="$(mktemp -u /tmp/vgo-hp-XXXXXX).sock"   # short: AF_UNIX paths are bounded
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; rm -f "$sock"; }
trap cleanup EXIT

echo "== stdio"
hosted --cmd "$WORKER" --expect "$EXPECT"

echo "== unix"
"$WORKER" --unix "$sock" --idle-timeout 0 >/dev/null & pids+=($!)
for _ in $(seq 100); do [ -S "$sock" ] && break; sleep 0.1; done
hosted --unix "$sock" --expect "$EXPECT"

echo "== http (identity)"
out="$(mktemp)"
"$WORKER" --http >"$out" & pids+=($!)
for _ in $(seq 100); do grep -q '^PORT:' "$out" && break; sleep 0.1; done
port="$(sed -n 's/^PORT://p' "$out")"
[ -n "$port" ] || { echo "worker printed no PORT:" >&2; exit 1; }
hosted --url "http://127.0.0.1:$port" --expect "$EXPECT" --identity
