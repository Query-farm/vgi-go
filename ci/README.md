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

1. **Build the workers** — `make build` produces the four dedicated worker
   binaries (`vgi-example-worker-go` plus the versioned / versioned-tables /
   attach-options workers). The RPC layer is the published
   `github.com/Query-farm/vgi-rpc-go` module, so nothing else builds from source.
2. **Checkout the test suite** — `Query-farm/vgi` at a pinned commit; its
   `test/sql/integration/*.test` files are the suite.
3. **Download the runner** — `haybarn_unittest-linux-amd64.zip` from the pinned
   Haybarn release.
4. **Preprocess** — the standalone runner links none of the extensions the tests
   gate on, so [`preprocess-require.awk`](preprocess-require.awk) rewrites each
   `require <ext>` into an explicit signed `INSTALL <ext> FROM {community,core};
   LOAD <ext>;`. `require-env` and everything else pass through.
5. **Run** — [`run-integration.sh`](run-integration.sh) stages the preprocessed
   tree, wires the `VGI_*_WORKER` env vars at the four binaries for the selected
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
runs them as a matrix), mirroring the `make test` / `test-shm` / `test-launcher`
/ `test-http` targets:

- **`stdio`** — the subprocess transport (the primary lane); the whole suite.
- **`shm`** — `stdio` plus the POSIX shared-memory side channel
  (`VGI_RPC_SHM_SIZE_BYTES`); the whole suite. Uses the
  `github.com/Query-farm/arrow-go` fork (RecordBatch custom metadata), pulled in
  via the `replace` in `go.mod`.
- **`launch`** — the AF_UNIX launcher transport; the launcher-only tests
  (`launcher/*`), which the other lanes skip via `require-env`.
- **`http`** — the whole suite over the stateless HTTP transport. Staging injects
  `LOAD httpfs` before each worker ATTACH (the prebuilt binary doesn't statically
  link httpfs), and two transport-agnostic files are dropped because the prebuilt
  binary can't serve them over http: `projection_pushdown_repro.test` (one POST
  per two rows; covered by `stdio`) and `dynamic_filter.test` (Top-N +
  dynamic-filter continuation terminates early in that C++ build).

The `simple_writable/*.test` write-path tests (INSERT/UPDATE/DELETE/RETURNING)
run on the subprocess lanes: `VGI_SIMPLE_WRITABLE_WORKER` points at a dedicated
Go fixture worker (`cmd/vgi-example-simple-writable-worker`, the
`examples/simple_writable` catalog), so the same cross-language tests now
exercise the Go write-function plumbing. They self-skip on the http lane
(skip-on-error `'HTTP'`).

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
interval (`cmd/vgi-example-worker/coverage.go`) — which needs `-covermode=atomic`
(live-readable counters), hence that build flag. The workflow runs all four lanes
with coverage, uploads each lane's pods as an artifact, and the `coverage` job
merges them (`go tool covdata merge`) into one report (also a Step Summary).

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
