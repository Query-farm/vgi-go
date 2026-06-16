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
SIMPLE_WRITABLE="$BIN_DIR/vgi-example-simple-writable-worker-go"
for b in "$WORKER" "$VERSIONED" "$VERSIONED_TABLES" "$ATTACH_OPTIONS" "$SIMPLE_WRITABLE"; do
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
  #   writable/ — opt-in generic writable catalog (VGI_WORKER_ENABLE_WRITABLE),
  #     not modelled by a cross-language fixture.
  #   nested_type_combinations.test — segfaults the prebuilt standalone runner
  #     (a property of that C++ build, not the worker — the worker passes it
  #     against a locally-built unittest).
  # simple_writable/ IS staged: VGI_SIMPLE_WRITABLE_WORKER (below) points at the
  # Go fixture worker, so the 5 cross-language write tests run here too. They
  # self-skip on the http lane (skip-on-error 'HTTP').
  find . -name '*.test' \
       -not -path './writable/*' \
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

# Coverage: when COVERAGE=1 the workers were built with `make build COVER=1`
# (go build -cover -covermode=atomic). Exporting GOCOVERDIR makes every worker
# process this run spawns — the main worker, the versioned HTTP workers, and the
# per-scan multi-worker connections — write its coverage pods there. Subprocess
# workers flush on their clean exit (stdin EOF); the long-lived HTTP worker is
# torn down abruptly by the harness, so it snapshots counters on an interval
# instead (see cmd/vgi-example-worker/coverage.go — hence -covermode=atomic).
# Pods are uniquely named, so many workers share one dir; `go tool covdata`
# merges them. A report is emitted after the suite (see end of file).
if [ "${COVERAGE:-0}" = "1" ]; then
  GOCOVERDIR="${GOCOVERDIR:-$(mktemp -d)}"
  mkdir -p "$GOCOVERDIR"
  export GOCOVERDIR
  echo "Coverage on — pods -> $GOCOVERDIR"
fi

# Background workers (http servers) are tracked in a file and SIGTERMed on exit.
# A file (not a shell array) keeps the teardown robust regardless of how
# boot_http_worker is invoked, and SIGTERM (not SIGKILL) lets each worker shut
# down gracefully — which writes a final coverage pod on a COVER=1 build.
BG_PIDS_FILE="$(mktemp)"
cleanup() {
  [ -f "$BG_PIDS_FILE" ] || return 0
  while read -r p; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done < "$BG_PIDS_FILE"
}
trap cleanup EXIT

# boot_http_worker <binary> — start it as an HTTP server on an ephemeral port and
# set BOOTED_PORT to the port it reports (PORT:<n>, the worker's readiness
# contract). Sets a global rather than echoing because the caller must NOT wrap
# it in $(...): a command-substitution subshell reparents the backgrounded worker
# out of the main shell, so it can't be `wait`ed on and dies abnormally on script
# teardown — which skips the coverage-pod flush of a `-cover` build.
BOOTED_PORT=""
boot_http_worker() {
  local exe="$1" log pid port=""
  BOOTED_PORT=""
  log="$(mktemp)"
  # Start the worker in its OWN session/process group. The standalone runner's
  # worker-pool teardown signals its whole process group on exit; an HTTP worker
  # sharing that group would be killed there (uncleanly, before its -cover pods
  # flush). A new session lets it survive until we SIGTERM it ourselves below,
  # where graceful shutdown flushes coverage. setsid execs in place (so $! is the
  # worker); perl is the macOS fallback (no setsid); plain exec is last resort.
  if command -v setsid >/dev/null 2>&1; then
    setsid "$exe" --http >"$log" 2>&1 &
  elif command -v perl >/dev/null 2>&1; then
    perl -e 'use POSIX qw(setsid); setsid(); exec @ARGV' "$exe" --http >"$log" 2>&1 &
  else
    "$exe" --http >"$log" 2>&1 &
  fi
  pid=$!
  echo "$pid" >> "$BG_PIDS_FILE"
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never reported a port" >&2; cat "$log" >&2; return 1; }
  BOOTED_PORT="$port"
}

# The plain (non-pooled) worker for the crash / pool-recovery tests.
export VGI_TEST_DEDICATED_WORKER="$WORKER"

# The simple_writable fixture worker (binary path → stdio subprocess) un-skips
# the cross-language simple_writable/*.test write-path tests. Set on every lane;
# they self-skip over http.
export VGI_SIMPLE_WRITABLE_WORKER="$SIMPLE_WRITABLE"

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
    boot_http_worker "$VERSIONED_TABLES"; vth_port="$BOOTED_PORT"
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${vth_port}"
    boot_http_worker "$VERSIONED"; vh_port="$BOOTED_PORT"
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
    boot_http_worker "$WORKER"; port="$BOOTED_PORT"
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
#
# simple_writable runs in its OWN invocation (a fresh DuckDB process / worker
# pool). Its table-in-out write workers otherwise leave warm pooled connections
# that perturb the immediately-following crash-recovery test
# (table_in_out/table_buffering_pool_recovery), changing how a re-crash surfaces
# (Broken-pipe-on-write vs the expected stream-EOF). A separate process gives
# the crash test a clean pool. The launcher lane runs only launcher/* (no
# simple_writable); the http lane's writes self-skip.
echo "Running suite ($SUITE_GLOB, transport=$TRANSPORT) ..."
suite_rc=0
if [ "$TRANSPORT" = "launch" ]; then
  "$HAYBARN_UNITTEST" "$SUITE_GLOB" || suite_rc=$?
else
  "$HAYBARN_UNITTEST" "$SUITE_GLOB" "~test/sql/integration/simple_writable/*" || suite_rc=$?
  echo "Running simple_writable (isolated process) ..."
  "$HAYBARN_UNITTEST" "test/sql/integration/simple_writable/*" || suite_rc=$?
fi

# Coverage report. Fire the EXIT trap now (kill the background HTTP workers) so
# they flush their pods before we read GOCOVERDIR, then summarise. Done even on
# failure — coverage of a failing run is still useful — but the suite's exit
# code is preserved.
if [ "${COVERAGE:-0}" = "1" ]; then
  # SIGTERM the HTTP workers and wait for them to exit — graceful shutdown flushes
  # their coverage pods. They're direct children of this shell (boot_http_worker
  # is not wrapped in $(...)), so `wait` blocks until each has finished writing.
  trap - EXIT
  while read -r p; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done < "$BG_PIDS_FILE"
  while read -r p; do [ -n "$p" ] && wait "$p" 2>/dev/null || true; done < "$BG_PIDS_FILE"
  echo "=== coverage (transport=$TRANSPORT, pods in $GOCOVERDIR) ==="
  if ls "$GOCOVERDIR"/covcounters.* >/dev/null 2>&1; then
    go tool covdata percent -i="$GOCOVERDIR" || echo "::warning::covdata percent failed"
    if [ -n "${COVERAGE_OUT:-}" ]; then
      go tool covdata textfmt -i="$GOCOVERDIR" -o "$COVERAGE_OUT" \
        && echo "wrote profile: $COVERAGE_OUT" || echo "::warning::covdata textfmt failed"
    fi
  else
    echo "::warning::no coverage pods in $GOCOVERDIR (were workers built with 'make build COVER=1'?)"
  fi
fi

exit "$suite_rc"
