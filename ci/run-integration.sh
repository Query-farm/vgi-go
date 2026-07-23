#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run the canonical Query-farm/vgi integration sqllogictest suite against the Go
# example worker, using a prebuilt standalone `haybarn-unittest` and the signed
# community vgi extension — no C++ build from source. See ci/README.md.
#
# Ported from vgi-java's harness. Two simplifications for the Go port:
#   * vgi-go builds FIVE dedicated worker binaries (one per catalog), so there
#     are no catalog "wrapper" scripts — the binaries are used directly.
#   * the RPC layer is the published github.com/Query-farm/vgi-rpc-go module, so
#     nothing is built from source beyond the worker itself.
#
# The lanes mirror the env wiring of the vgi repo's Makefile `test_subprocess` /
# `test_launcher` / `test_http` targets, with one deliberate divergence: the
# `shm` lane layers the shared-memory side channel on the LAUNCHER rather than
# on raw subprocess, to avoid re-paying the fork-per-connection cost the stdio
# lane already covers. See the shm|launch case below.
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

# The five dedicated worker binaries (make build).
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
# Nothing is dropped on the launcher lanes. The vgi Makefile's test_launcher
# excludes two files, and neither applies here:
#   * test/sql/vgi_worker_pool.test — outside test/sql/integration, so the find
#     below never stages it.
#   * table/filter_echo_partitioned.test — excluded upstream for asserting >1
#     distinct worker_pid, which AF_UNIX socket pooling can't satisfy. That
#     rationale is stale: the test now counts transport-neutral `conn=` ids (see
#     its own comment), and it passes over launch: against the Go fixture worker
#     (verified: 36 assertions). Keeping it preserves coverage of exactly what
#     the launcher changes — connection multiplexing.
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
  #   bool_in_union.test — a pre-existing, arch-dependent union-bool bug; its
  #     pinned expected output matches arm64 but not amd64 (CI is amd64), so it
  #     is dropped on all platforms (mirrors vgi-rust's ci/run-integration.sh).
  # simple_writable/ IS staged: VGI_SIMPLE_WRITABLE_WORKER (below) points at the
  # Go fixture worker, so the 5 cross-language write tests run on every lane —
  # that var is always a spawned binary, so they run over subprocess (or the
  # launcher) even on the http lane.
  find . -name '*.test' \
       -not -path './writable/*' \
       -not -name 'nested_type_combinations.test' \
       -not -name 'bool_in_union.test' \
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
# instead (see internal/covflush — hence -covermode=atomic). Launcher workers
# outlive the run (idle timeout), so they rely on that same interval snapshot.
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
# Each line is "<pid>\t<critical>\t<binary>\t<logfile>" (see boot_http_worker).
BG_PIDS_FILE="$(mktemp)"
cleanup() {
  [ -f "$BG_PIDS_FILE" ] || return 0
  while IFS="$(printf '\t')" read -r p _ _ _; do
    [ -n "$p" ] && kill "$p" 2>/dev/null || true
  done < "$BG_PIDS_FILE"
}
trap cleanup EXIT

# assert_bg_workers_alive — check the background http workers survived the run
# just finished, dumping the log of any that did not.
#
# A dead worker is otherwise near-invisible: every later ATTACH against it fails
# and the runner's default error handling turns that cascade into *skips*, so
# the lane still prints "All tests passed" having tested nothing past the point
# of death. That is how the http lane came to skip 175 of its 287 files while
# reporting green (run 30025057387) — the crash tests SIGKILLed the one shared
# worker at file 133.
#
# Only a CRITICAL worker (the http lane's VGI_TEST_WORKER, which the whole suite
# attaches) fails the lane. The stdio lane's secondary versioned/versioned-tables
# http workers are best-effort: they serve five files in the middle of the run,
# and an exit after those has no bearing on the result, so they only warn.
assert_bg_workers_alive() {
  local rc=0 p critical exe log
  [ -s "$BG_PIDS_FILE" ] || return 0
  while IFS="$(printf '\t')" read -r p critical exe log; do
    [ -n "$p" ] || continue
    kill -0 "$p" 2>/dev/null && continue
    if [ "$critical" = "1" ]; then
      echo "::error::the shared http worker '$exe' (pid $p) died during the run —" \
           "everything attached to it after that point was not really tested." \
           "Its log follows."
      rc=1
    else
      echo "::warning::background http worker '$exe' (pid $p) exited during the run." \
           "It only backs the versioned_*_http tests; its log follows."
    fi
    [ -n "$log" ] && [ -f "$log" ] && sed -n '1,200p' "$log"
  done < "$BG_PIDS_FILE"
  return "$rc"
}

# boot_http_worker <binary> <critical> — start it as an HTTP server on an
# ephemeral port and set BOOTED_PORT to the port it reports (PORT:<n>, the
# worker's readiness contract). <critical>=1 means the whole suite attaches it,
# so assert_bg_workers_alive fails the lane if it dies (see there). Sets a global rather than echoing because the caller must NOT wrap
# it in $(...): a command-substitution subshell reparents the backgrounded worker
# out of the main shell, so it can't be `wait`ed on and dies abnormally on script
# teardown — which skips the coverage-pod flush of a `-cover` build.
BOOTED_PORT=""
boot_http_worker() {
  local exe="$1" critical="${2:-0}" log pid port=""
  BOOTED_PORT=""
  log="$(mktemp)"
  # Start the worker in its OWN session/process group. The standalone runner's
  # worker-pool teardown signals its whole process group on exit; an HTTP worker
  # sharing that group would be killed there (uncleanly, before its -cover pods
  # flush). A new session lets it survive until we SIGTERM it ourselves below,
  # where graceful shutdown flushes coverage. setsid execs in place (so $! is the
  # worker); perl is the macOS fallback (no setsid); plain exec is last resort.
  # Start the worker with its cwd set to $STAGE — the same directory the unittest
  # runs from, so DuckDB's per-test temp dir (__TEST_DIR__ → duckdb_unittest_tempdir/
  # <pid>) and the worker resolve the SAME relative path. Without this the http
  # worker (a separate process started from the repo root) cannot create the
  # COPY ... TO destination the test hands it as a relative path.
  if command -v setsid >/dev/null 2>&1; then
    ( cd "$STAGE" && exec setsid "$exe" --http ) >"$log" 2>&1 &
  elif command -v perl >/dev/null 2>&1; then
    ( cd "$STAGE" && exec perl -e 'use POSIX qw(setsid); setsid(); exec @ARGV' "$exe" --http ) >"$log" 2>&1 &
  else
    ( cd "$STAGE" && exec "$exe" --http ) >"$log" 2>&1 &
  fi
  pid=$!
  printf '%s\t%s\t%s\t%s\n' "$pid" "$critical" "$(basename "$exe")" "$log" >> "$BG_PIDS_FILE"
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never reported a port" >&2; cat "$log" >&2; return 1; }
  BOOTED_PORT="$port"
}

# On the launcher-family lanes EVERY worker is fronted by `launch:` so the C++
# launcher serves it (ResolveLauncherSocketPath -> AF_UNIX -> UnixSocketWorker),
# as the vgi Makefile's test_launcher does. A worker left unprefixed would
# silently run over stdio inside a launcher lane. Empty on stdio and http.
LAUNCH_PREFIX=""
case "$TRANSPORT" in shm|launch) LAUNCH_PREFIX="launch:" ;; esac

# The simple_writable fixture worker un-skips the cross-language
# simple_writable/*.test write-path tests. Set on every lane — on the http lane
# it stays a plain binary path, so those tests run over subprocess there rather
# than skipping.
export VGI_SIMPLE_WRITABLE_WORKER="${LAUNCH_PREFIX}${SIMPLE_WRITABLE}"

case "$TRANSPORT" in
  stdio)
    # Subprocess transport (the primary lane) — the only lane that spawns a
    # fresh worker process per DuckDB connection, and so the only one that can
    # host the crash / pool-recovery tests below.
    export VGI_TEST_WORKER="$WORKER"
    # Presence gate for the crash / pool-recovery tests
    # (table_in_out/table_buffering_{worker_crash,pool_recovery}). The value is
    # not used as a worker spec — the SQL attaches ${VGI_TEST_WORKER}; this only
    # un-skips the tests.
    #
    # DEDICATED (subprocess) LANE ONLY. Those tests call the crash_on_process
    # fixture, which SIGKILLs its own process. Under subprocess transport that
    # kills the per-DuckDB-process worker child and the pool recovers — exactly
    # what the tests assert. Under a SHARED-worker transport (http://, launch:)
    # it kills the single process serving the whole suite and every later ATTACH
    # fails. Exporting it unconditionally silently voided 175 of 287 files on
    # the http lane while still printing "All tests passed" (run 30025057387).
    # Since shm now rides the launcher, stdio is the ONLY lane that may set it.
    export VGI_TEST_DEDICATED_WORKER="$WORKER"
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
  shm|launch)
    # AF_UNIX launcher transport — the WHOLE suite with every worker fronted by
    # `launch:`, mirroring the vgi Makefile's test_launcher. The point is that
    # the launcher path produces identical query results to the subprocess path,
    # which only a full-suite run demonstrates; this glob used to be
    # launcher/* (2 files / 20 assertions), so the lane never tested the
    # transport it is named for. VGI_REQUIRE_LAUNCHER_TRANSPORT additionally
    # un-skips launcher/*, whose options apply only to the launch: dispatch path
    # (the other lanes skip them via require-env).
    #
    # `shm` is this same lane plus the POSIX shared-memory side channel
    # (VGI_RPC_SHM_SIZE_BYTES, exported by the workflow). It rides the launcher
    # rather than raw subprocess because stdio already covers fork-per-connection
    # and spends most of its wall-clock doing it; re-paying that cost just to
    # exercise shm is waste. The side channel is transport-independent — it is
    # negotiated via the __transport_options__ handshake and carried in POSIX
    # shm, not in the pipe or socket — and engages identically over AF_UNIX
    # (verified locally: the same 18 resolved batches under VGI_RPC_SHM_DEBUG=1
    # on stdio and on launch). This diverges from Makefile test_shm, which
    # layers shm on raw subprocess.
    #
    # The versioned / versioned_tables *http* worker env vars are deliberately
    # left UNSET (as in test_launcher), so attach/versioned_tables_*_http and
    # versioning_http skip here — the stdio lane covers them.
    export VGI_TEST_WORKER="launch:${WORKER}"
    export VGI_VERSIONED_WORKER="launch:${VERSIONED}"
    export VGI_VERSIONED_TABLES_WORKER="launch:${VERSIONED_TABLES}"
    export VGI_ATTACH_OPTIONS_WORKER="launch:${ATTACH_OPTIONS}"
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    if [ "$TRANSPORT" = "shm" ]; then
      # Both the C++ client and vgi-rpc-go skip shm for batches under
      # VGI_RPC_SHM_MIN_BATCH_BYTES (128 KiB on POSIX by default) because the
      # pipe wins below the crossover. Nearly every batch in this suite is
      # smaller than that, so with the default the lane proves only that the
      # handshake advertises shm — not one batch travels through the segment.
      # Force the gate to 0 so every batch takes the zero-copy path and the
      # lane actually exercises allocate/write/resolve/free.
      export VGI_RPC_SHM_MIN_BATCH_BYTES=0
    fi
    SUITE_GLOB="test/sql/integration/*"
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
    boot_http_worker "$WORKER" 1; port="$BOOTED_PORT"
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
# the crash test a clean pool. Every lane does the split: simple_writable always
# attaches a spawned binary (VGI_SIMPLE_WRITABLE_WORKER), even on the http lane.
echo "Running suite ($SUITE_GLOB, transport=$TRANSPORT) ..."
suite_rc=0
"$HAYBARN_UNITTEST" "$SUITE_GLOB" "~test/sql/integration/simple_writable/*" || suite_rc=$?
assert_bg_workers_alive || suite_rc=1

echo "Running simple_writable (isolated process) ..."
"$HAYBARN_UNITTEST" "test/sql/integration/simple_writable/*" || suite_rc=$?
assert_bg_workers_alive || suite_rc=1

# Coverage report. Fire the EXIT trap now (kill the background HTTP workers) so
# they flush their pods before we read GOCOVERDIR, then summarise. Done even on
# failure — coverage of a failing run is still useful — but the suite's exit
# code is preserved.
if [ "${COVERAGE:-0}" = "1" ]; then
  # SIGTERM the HTTP workers and wait for them to exit — graceful shutdown flushes
  # their coverage pods. They're direct children of this shell (boot_http_worker
  # is not wrapped in $(...)), so `wait` blocks until each has finished writing.
  trap - EXIT
  while IFS="$(printf '\t')" read -r p _ _ _; do
    [ -n "$p" ] && kill "$p" 2>/dev/null || true
  done < "$BG_PIDS_FILE"
  while IFS="$(printf '\t')" read -r p _ _ _; do
    [ -n "$p" ] && wait "$p" 2>/dev/null || true
  done < "$BG_PIDS_FILE"
  # Launcher-spawned workers are children of the C++ launcher, not of this
  # shell, and they idle for 300s before self-shutdown — well past the end of
  # the job, so nothing would flush their meta-data pod (covflush's periodic
  # snapshot writes counters only; the runtime writes covmeta at exit). SIGTERM
  # them so covflush's handler runs and each exits cleanly with both pods.
  if [ -n "$LAUNCH_PREFIX" ]; then
    pkill -TERM -f "$BIN_DIR/vgi-example-" 2>/dev/null || true
    sleep 2
  fi
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
