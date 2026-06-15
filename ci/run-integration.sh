#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run the canonical Query-farm/vgi integration sqllogictest suite against the Go
# example worker, using a prebuilt standalone `haybarn-unittest` and the signed
# community vgi extension — no C++ build from source. See ci/README.md.
#
# Ported from vgi-java's harness. Two simplifications for the Go port:
#   * vgi-go builds FOUR dedicated worker binaries (one per catalog), so there
#     are no catalog "wrapper" scripts — the binaries are used directly.
#   * the RPC layer is the published github.com/Query-farm/vgi-rpc-go module, so
#     nothing is built from source beyond the worker itself.
#
# Required environment:
#   VGI_SRC           path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST  path to the haybarn-unittest binary
# Optional:
#   BIN_DIR           dir holding the built worker binaries (default: repo root)
#   TRANSPORT         stdio | shm | launch | http   (default: stdio)
#   VGI_RPC_SHM_SIZE_BYTES  shm side-channel segment size (the shm lane)
#   STAGE             scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
BIN_DIR="${BIN_DIR:-$REPO}"
STAGE="${STAGE:-$(mktemp -d)}"
TRANSPORT="${TRANSPORT:-stdio}"
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }

# The four dedicated worker binaries (make build).
WORKER="$BIN_DIR/vgi-example-worker-go"
VERSIONED="$BIN_DIR/vgi-example-versioned-worker-go"
VERSIONED_TABLES="$BIN_DIR/vgi-example-versioned-tables-worker-go"
ATTACH_OPTIONS="$BIN_DIR/vgi-example-attach-options-worker-go"
for b in "$WORKER" "$VERSIONED" "$VERSIONED_TABLES" "$ATTACH_OPTIONS"; do
  [ -x "$b" ] || { echo "::error::missing worker binary $b (run: make build)"; exit 1; }
done

# ---------------------------------------------------------------------------
# Stage a preprocessed copy of the suite. preprocess-require.awk rewrites each
# `require <ext>` gate into a signed INSTALL+LOAD so the standalone runner
# (which links none of these extensions) can run them. On the http lane it also
# injects `LOAD httpfs` before each worker ATTACH.
# ---------------------------------------------------------------------------
AWK_HTTP=0
EXTRA_SKIP=()
if [ "$TRANSPORT" = "http" ]; then
  AWK_HTTP=1
  # Dropped on http only — transport-agnostic coverage that the prebuilt binary
  # cannot serve over http (matches upstream's make test_http and vgi-java):
  #   * projection_pushdown_repro.test — one POST round-trip per two rows; fully
  #     covered by the stdio lane.
  #   * dynamic_filter.test — Top-N + dynamic-filter continuation terminates
  #     early over http in the prebuilt binary (a property of that C++ build).
  EXTRA_SKIP=(-not -name 'projection_pushdown_repro.test' -not -name 'dynamic_filter.test')
fi

echo "Staging preprocessed tests into $STAGE (transport=$TRANSPORT) ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  # Out of scope:
  #   writable/, simple_writable/ — opt-in writable catalog (VGI_WORKER_ENABLE_WRITABLE).
  #   nested_type_combinations.test — segfaults the prebuilt standalone runner
  #     (a property of that C++ build, not the worker — the worker passes it
  #     against a locally-built unittest).
  find . -name '*.test' \
       -not -path './writable/*' -not -path './simple_writable/*' \
       -not -name 'nested_type_combinations.test' \
       "${EXTRA_SKIP[@]}" | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -v http="$AWK_HTTP" -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# Empty VGI_RPC_SHM_SIZE_BYTES must not reach the C++ client (it would try to
# attach a zero-size segment); only a real value enables the shm side channel.
[ -n "${VGI_RPC_SHM_SIZE_BYTES:-}" ] || unset VGI_RPC_SHM_SIZE_BYTES

# Force the C++ extension's init_global RPC to run synchronously so multi-conn
# parallel-init tests observe the worker's real max_workers (mirrors the Makefile).
export VGI_SYNC_INIT_GLOBAL=1

# Background workers (http servers) are tracked and killed on exit.
BG_PIDS=()
cleanup() { for p in "${BG_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# boot_http_worker <binary> — start it as an HTTP server on an ephemeral port
# and echo the port it reports (PORT:<n>, the worker's readiness contract).
boot_http_worker() {
  local exe="$1" log pid port=""
  log="$(mktemp)"
  "$exe" --http >"$log" 2>&1 &
  pid=$!
  BG_PIDS+=("$pid")
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never reported a port" >&2; cat "$log" >&2; return 1; }
  echo "$port"
}

# The plain (non-pooled) worker for the crash / pool-recovery tests.
export VGI_TEST_DEDICATED_WORKER="$WORKER"

case "$TRANSPORT" in
  stdio|shm)
    # Subprocess transport (the primary lane). shm is identical plus the POSIX
    # shared-memory side channel via VGI_RPC_SHM_SIZE_BYTES.
    export VGI_TEST_WORKER="$WORKER"
    export VGI_VERSIONED_WORKER="$VERSIONED"
    export VGI_VERSIONED_TABLES_WORKER="$VERSIONED_TABLES"
    export VGI_ATTACH_OPTIONS_WORKER="$ATTACH_OPTIONS"
    # Serve the versioned catalogs over HTTP too: attach/versioned_tables_*_http
    # and versioning_http attach an http:// worker regardless of the main transport.
    vth_port="$(boot_http_worker "$VERSIONED_TABLES")"
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${vth_port}"
    vh_port="$(boot_http_worker "$VERSIONED")"
    export VGI_VERSIONED_HTTP_WORKER="http://localhost:${vh_port}"
    SUITE_GLOB="test/sql/integration/*"
    ;;
  launch)
    # AF_UNIX launcher transport. Only the launcher-only tests opt in here
    # (the rest of the suite runs on the stdio lane); mirrors make test-launcher.
    export VGI_TEST_WORKER="launch:${WORKER}"
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    SUITE_GLOB="test/sql/integration/launcher/*"
    ;;
  http)
    # Whole-suite-over-HTTP (mirrors make test-http). Every ATTACH goes over
    # http://, so staging injected `LOAD httpfs` (AWK_HTTP=1) and dropped the
    # http-incompatible files. VGI_REQUIRE_LAUNCHER_TRANSPORT is NOT set (the
    # launcher-only tests must skip on this lane).
    #
    # Only the main worker is booted as an http server. The versioned /
    # versioned_tables http-worker env vars are deliberately left UNSET, so the
    # attach/versioned_tables_*_http and versioning_http tests skip (require-env)
    # — they are covered over http on the stdio lane, which boots those workers.
    # (Running three concurrent http workers under the full-suite load destabilises
    # the secondary workers; the single-worker http lane is reliable.)
    port="$(boot_http_worker "$WORKER")"
    export VGI_TEST_WORKER="http://localhost:${port}"
    SUITE_GLOB="test/sql/integration/*"
    ;;
  *)
    echo "::error::unknown TRANSPORT=$TRANSPORT (expected stdio|shm|launch|http)"; exit 1 ;;
esac

cd "$STAGE"

echo "Warming the extension cache (vgi from community, deps from core) ..."
mkdir -p "$STAGE/test"
# FORCE INSTALL vgi re-downloads the currently-published community build,
# overriding any older cached copy, so the suite runs against what users can
# install today (and so a freshly-published extension is picked up immediately).
cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
FORCE INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;

statement ok
INSTALL parquet FROM core;

statement ok
INSTALL spatial FROM core;
EOF
"$HAYBARN_UNITTEST" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
rm -f "$STAGE/test/_warm.test"

# Run the whole lane in one invocation, streaming the native sqllogictest report
# (a progress line per file + the final "All tests passed (.. N assertions ..)"
# summary). Out-of-scope tests were dropped at staging, so the glob never
# matches them; any failed assertion exits non-zero and fails the job.
echo "Running suite ($SUITE_GLOB, transport=$TRANSPORT) ..."
"$HAYBARN_UNITTEST" "$SUITE_GLOB"
