# CI: the vgi integration suite

[`.github/workflows/integration.yml`](../.github/workflows/integration.yml)
runs the canonical [Query-farm/vgi](https://github.com/Query-farm/vgi)
integration sqllogictest suite against this repo's Go example worker on every
push / PR. The same `.test` files run against the Python, Java, and Go ports, so
a green run here is real wire-compatibility evidence — it exercises the worker
through the *published* DuckDB extension, not a mock.

(The separate [`ci.yml`](../.github/workflows/ci.yml) covers fmt / vet / lint /
build / unit tests.)

## How it works (no C++ build)

Rather than building the vgi DuckDB extension from source (which needs the
Haybarn vcpkg pipeline), CI drives a **prebuilt** standalone `haybarn-unittest`
(the DuckDB/Haybarn sqllogictest runner, published in Haybarn's releases) and
installs the **signed** vgi extension from the Haybarn community channel:

1. **Build the workers** — `make build` produces the five dedicated worker
   binaries (`vgi-example-worker-go` plus the versioned / versioned-tables /
   attach-options / simple-writable workers). All five accept `--unix`, so the
   launcher lane can front every one of them with `launch:`. The RPC layer is
   the published `github.com/Query-farm/vgi-rpc-go` module, so nothing else
   builds from source.
2. **Checkout the test suite** — `Query-farm/vgi` at a pinned commit; its
   `test/sql/integration/*.test` files are the suite.
3. **Download the runner** — `haybarn_unittest-linux-amd64.zip` from the pinned
   Haybarn release.
4. **Preprocess** — the standalone runner links none of the extensions the tests
   gate on, so [`preprocess-require.awk`](preprocess-require.awk) rewrites each
   `require <ext>` into an explicit signed `INSTALL <ext> FROM {community,core};
   LOAD <ext>;`. `require-env` and everything else pass through.
5. **Run** — [`run-integration.sh`](run-integration.sh) stages the preprocessed
   tree, wires the `VGI_*_WORKER` env vars at the five binaries for the selected
   transport, `FORCE INSTALL`s the vgi extension (so the run uses what users can
   install today), then runs the suite in a single `haybarn-unittest`
   invocation. The CI log streams the runner's native report (a `[i/N] (..%):`
   line per file and the final `All tests passed (.. N assertions in M test
   cases)`); any failed assertion exits non-zero and fails the job.

Unlike the Java port, vgi-go needs **no catalog wrapper scripts** (it ships a
dedicated binary per catalog) and **no RPC build from source** (vgi-rpc-go is a
published module).

## Transport lanes

`run-integration.sh` honours `TRANSPORT=stdio|shm|launch|http` (the workflow
runs them as a matrix), mirroring the vgi Makefile's `test_subprocess` /
`test_launcher` / `test_http` targets:

- **`stdio`** — the subprocess transport (the primary lane); the whole suite.
  The only lane that spawns a fresh worker per DuckDB connection, so the only
  one that can host the crash / pool-recovery tests (below).
- **`launch`** — the AF_UNIX launcher transport; the whole suite, with *every*
  `VGI_*_WORKER` fronted by `launch:` so traffic flows through the C++ launcher
  (`ResolveLauncherSocketPath` → AF_UNIX → `UnixSocketWorker`). Mirrors
  `test_launcher`: the point is that the launcher path produces identical query
  results to the subprocess path, which only a full-suite run demonstrates.
  `VGI_REQUIRE_LAUNCHER_TRANSPORT` additionally un-skips the launcher-only tests
  (`launcher/*`), whose options apply solely to the `launch:` dispatch path and
  which the other lanes skip via `require-env`. The versioned /
  versioned-tables *http* worker vars are left unset (as in `test_launcher`), so
  those tests skip here — stdio covers them.
- **`shm`** — the **`launch`** lane plus the POSIX shared-memory side channel
  (`VGI_RPC_SHM_SIZE_BYTES`); the whole suite. Uses the
  `github.com/Query-farm/arrow-go` fork (RecordBatch custom metadata), pulled in
  via the `replace` in `go.mod`. This deliberately differs from the Makefile's
  `test-shm`, which layers shm on raw subprocess: `stdio` already covers the
  fork-per-connection path and spends most of its wall-clock doing it, so
  paying that cost twice just to exercise shm is waste. The side channel is
  transport-independent — negotiated via the `__transport_options__` handshake
  and carried in POSIX shm rather than in the pipe or socket — and engages
  identically over the launcher (verified: the same 18 resolved batches under
  `VGI_RPC_SHM_DEBUG=1` on both). The lane also sets
  `VGI_RPC_SHM_MIN_BATCH_BYTES=0`: both sides skip shm below 128 KiB by default
  and nearly every batch in this suite is smaller, so without it the lane would
  prove only that the handshake advertises shm.
- **`http`** — the whole suite over the stateless HTTP transport. Staging injects
  `LOAD httpfs` before each worker ATTACH (the prebuilt binary doesn't statically
  link httpfs), and two transport-agnostic files are dropped because the prebuilt
  binary can't serve them over http: `projection_pushdown_repro.test` (one POST
  per two rows; covered by `stdio`) and `dynamic_filter.test` (Top-N +
  dynamic-filter continuation terminates early in that C++ build).

The `simple_writable/*.test` write-path tests (INSERT/UPDATE/DELETE/RETURNING)
run on **every** lane: `VGI_SIMPLE_WRITABLE_WORKER` points at a dedicated Go
fixture worker (`cmd/vgi-example-simple-writable-worker`, the
`examples/simple_writable` catalog), so the same cross-language tests exercise
the Go write-function plumbing. That var is always a spawned binary — a
`launch:` path on the launcher lanes, a plain path elsewhere — so those tests
run over subprocess even on the http lane rather than skipping. They run in
their own `haybarn-unittest` invocation so their warm pooled connections don't
perturb the immediately-following crash-recovery test.

The crash / pool-recovery tests (`table_in_out/table_buffering_{worker_crash,
pool_recovery}`) are gated on `VGI_TEST_DEDICATED_WORKER`, set on the **`stdio`
lane only**. Their `crash_on_process` fixture SIGKILLs its own process: a
recoverable per-DuckDB-process child kill under subprocess transport, but under
any shared-worker transport (`http://`, `launch:`) it kills the one process
serving the whole suite and every later `ATTACH` fails. Setting it on the http
lane silently voided 175 of 287 files while still reporting "All tests passed"
(run 30025057387) — `assert_bg_workers_alive` in the runner now fails the lane
if the shared http worker dies mid-run (the stdio lane's secondary
versioned/versioned-tables http workers only warn: they back five files in the
middle of the run, so an exit after those changes nothing). Because `shm` rides
the launcher, `stdio` is the only lane that can host these tests.

Nothing is dropped on the **launcher** lanes. `test_launcher` upstream excludes
two files, and neither applies: `test/sql/vgi_worker_pool.test` lives outside
`test/sql/integration` so the harness never stages it, and
`table/filter_echo_partitioned.test` is excluded there for asserting >1 distinct
`worker_pid` (which AF_UNIX socket pooling can't satisfy) — a stale rationale,
since the test now counts transport-neutral `conn=` ids and passes over
`launch:` against the Go fixture worker. Keeping it covers exactly what the
launcher changes: connection multiplexing.

Out of scope and excluded from every lane: `writable/` (the opt-in *generic*
writable catalog, `VGI_WORKER_ENABLE_WRITABLE` — no cross-language fixture), and
`nested_type_combinations.test` (segfaults the prebuilt standalone runner — a
property of that C++ build, not the worker, which passes it against a
locally-built unittest). The HTTP-attach / bearer-auth / dynamic-code /
schema-reconcile tests skip via their `require-env` gates (we don't set those
workers), exactly as in the reference harness.

## Run it locally

```bash
make build
VGI_SRC=~/path/to/vgi-checkout \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
TRANSPORT=stdio \
  ci/run-integration.sh
```

Download `haybarn-unittest` for your platform from the pinned Haybarn release
(`gh release download "$HAYBARN_RELEASE" --repo Query-farm-haybarn/haybarn
--pattern 'haybarn_unittest-*.zip'`).

## Worker coverage

The suite runs the workers as separate processes, so `go test -cover` can't see
them. Instead the workers are built as **coverage binaries** (`make build
COVER=1` → `go build -cover -covermode=atomic -coverpkg=./...`) and
`run-integration.sh`, when `COVERAGE=1`, points `GOCOVERDIR` at a pod directory
that every worker process inherits. This measures how much of the **`vgi/` SDK**
real DuckDB protocol traffic exercises — including the HTTP-only paths (state
continuation in `vgi/state_serialize.go`, opaque-data sealing in
`vgi/crypto.go`, rehydration) that unit tests don't reach.

```bash
make build COVER=1
VGI_SRC=~/vgi HAYBARN_UNITTEST=/path/to/haybarn-unittest TRANSPORT=http \
  COVERAGE=1 GOCOVERDIR=$(mktemp -d) COVERAGE_OUT=cover.txt \
  ci/run-integration.sh
# COVERAGE_OUT is a legacy profile: `go tool cover -html=cover.txt`
```

Subprocess workers flush coverage on their clean exit (stdin EOF). The long-lived
HTTP worker is torn down abruptly by the harness, so it snapshots counters on an
interval (`internal/covflush`) — which needs `-covermode=atomic` (live-readable
counters), hence that build flag. Launcher-spawned workers are children of the
C++ launcher and idle for 300s before self-shutdown, well past the end of a job,
so `run-integration.sh` SIGTERMs them after the suite: covflush's handler runs
and each exits cleanly, writing the `covmeta` pod that the periodic counter
snapshot alone can't produce. (Before the launcher lanes ran the full suite they
emitted no counters at all.) The workflow runs all four lanes with coverage,
uploads each lane's pods as an artifact, and the `coverage` job merges them
(`go tool covdata merge`) into one report (also a Step Summary).

## Version pins (and their coupling)

Two pins live in the workflow's `env:` block:

| Pin | What | Why |
|-----|------|-----|
| `VGI_REF` | the `Query-farm/vgi` commit supplying the `.test` files | reproducibility — bump deliberately |
| `HAYBARN_RELEASE` | the Haybarn release supplying `haybarn-unittest` | must be ABI-compatible with the community vgi extension |

**The coupling to know about:** the vgi extension is pulled live from the
community channel (`FORCE INSTALL vgi FROM community`), which always serves the
*currently published* build — it is not version-pinned here. So CI verifies the
worker against what users can install today. If `VGI_REF` points at a commit
whose tests exercise a protocol feature the published extension doesn't yet ship
(or vice-versa), that test can fail or skip — bump `VGI_REF` deliberately and
re-validate against the current community extension.
