# VGI-Go

Go implementation of the VGI (Vector Gateway Interface) protocol for DuckDB. VGI enables DuckDB to call functions hosted in external worker processes via Arrow IPC over stdin/stdout.

## Project Structure

- `vgi/` — Core VGI SDK package (protocol, worker, function interfaces)
- `examples/scalar/` — Scalar function examples (1:1 row mapping)
- `examples/table/` — Table function examples (row generators)
- `examples/table_in_out/` — Table-in-out function examples (transform + optional finalize)
- `cmd/vgi-example-worker/` — Example worker binary that registers all example functions

## Build & Test

See the `Makefile` for all available targets. Common commands:

```bash
make build                                  # Build the worker binary
make test                                   # Run all integration tests (release, stdio)
make test BUILD_TYPE=debug                  # Run all integration tests (debug, stdio)
make test-single TEST=test/sql/integration/scalar/add_values.test  # Single test
make test-http                              # Run all tests over HTTP transport
make test-all                               # Run both stdio and HTTP tests
make fmt                                    # Format Go source
make vet                                    # Static analysis
```

Always rebuild the worker before running tests (`make test` does this automatically).

Tests live in the VGI DuckDB extension repo at `../vgi/test/sql/` and use the DuckDB sqllogictest format. Refer to the documentation at https://duckdb.org/docs/stable/dev/sqllogictest/intro when debugging test files.

### HTTP tests

`make test-http` runs each test in its own HTTP worker process. A fresh worker is started per test using `--http` mode, the port is discovered via a FIFO, and the worker is killed after the test completes. Tests listed in `HTTP_XFAIL_TESTS` in the Makefile are expected failures (reported as XFAIL). If an expected failure starts passing, it is reported as XPASS so you can remove it from the list. Unexpected failures rerun automatically with the debug binary for diagnostics.

### Transport modes

The worker supports two transport modes:
- **Stdio** (default): `./vgi-example-worker-go` — communicates over stdin/stdout
- **HTTP**: `./vgi-example-worker-go --http` — listens on a random port, prints `PORT:<n>` to stdout

## Dependencies

- `github.com/Query-farm/vgi-rpc` — VGI RPC framework (local replace: `../vgi-rpc-go`)
- `github.com/apache/arrow-go/v18` — Arrow IPC (uses fork: `github.com/rustyconover/arrow-go/v18`)

## Logging

Both `vgi-go` and `vgi-rpc-go` use `log/slog` for structured logging. By default, the worker logs at Info level to stderr. Since VGI communicates over stdin/stdout, stderr is safe for logging.

Configure logging via `WorkerOption`s:

```go
// Debug-level logging (shows all protocol trace messages)
w := vgi.NewWorker(vgi.WithLogLevel(slog.LevelDebug))

// Custom handler (e.g. JSON to a file)
f, _ := os.Create("/tmp/vgi-debug.log")
h := slog.NewJSONHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug})
w := vgi.NewWorker(vgi.WithLogHandler(h))
```

**Log levels in protocol.go**: Errors in `handleBind`/`handleInit` are logged at Debug, not Error. These are operational errors returned to DuckDB as RPC error responses — they're expected control flow, not system failures. Use `WithLogLevel(slog.LevelDebug)` to see them.

## Conventions

- All source files begin with the copyright header:
  ```
  // Copyright 2025, 2026 Query Farm LLC - https://query.farm
  ```
- Function implementations follow the interface pattern: `Name()`, `Metadata()`, `ArgumentSpecs()`, `OnBind()`, `Process()`
- New functions must be registered in `cmd/vgi-example-worker/main.go`
