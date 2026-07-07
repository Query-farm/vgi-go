#!/usr/bin/env bash
# Boot the Go example HTTP worker and run the cross-language landing-page
# conformance checker against it.
#
# The checker (run_landing_conformance.py) is language-agnostic: it drives the
# worker's HTTP landing surface and asserts it conforms to the contract in
# ~/Development/vgi/docs/http-landing-contract.md — describe.json validates
# against describe.schema.json (vendored here), GET / serves the pinned shared
# landing.html (asset marker), and every table/view exposes a valid columns
# endpoint. The Python-generated golden is NOT enforced here: it was produced
# from the Python ExampleWorker and does not match the Go catalog. Pass a Go
# golden path via --golden to opt into a Go-specific golden-diff guard.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
SCHEMA="$HERE/describe.schema.json"
CHECKER="$HERE/run_landing_conformance.py"

WORKER_BIN="${WORKER_BIN:-}"
if [[ -z "$WORKER_BIN" ]]; then
  WORKER_BIN="$(mktemp -d)/vgi-example-worker"
  echo "building example worker -> $WORKER_BIN"
  (cd "$REPO" && go build -o "$WORKER_BIN" ./cmd/vgi-example-worker)
fi

echo "starting worker: $WORKER_BIN --http"
WORKER_OUT="$(mktemp)"
"$WORKER_BIN" --http >"$WORKER_OUT" 2>/dev/null &
WORKER_PID=$!
cleanup() { kill "$WORKER_PID" 2>/dev/null || true; }
trap cleanup EXIT

# Wait for the "PORT:<n>" readiness marker (up to ~10s).
PORT=""
for _ in $(seq 1 100); do
  PORT="$(grep -o 'PORT:[0-9]*' "$WORKER_OUT" 2>/dev/null | head -1 | cut -d: -f2 || true)"
  [[ -n "$PORT" ]] && break
  sleep 0.1
done
if [[ -z "$PORT" ]]; then
  echo "worker did not report a PORT; output was:" >&2
  cat "$WORKER_OUT" >&2
  exit 1
fi
echo "worker listening on port $PORT"

# PYTHON may be a multi-word launcher (e.g. "uv run python"); leave it unquoted.
PYTHON="${PYTHON:-python3}"
${PYTHON} "$CHECKER" --url "http://127.0.0.1:$PORT" --schema "$SCHEMA" "$@"
