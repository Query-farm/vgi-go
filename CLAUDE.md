# VGI-Go

Go implementation of the VGI (Vector Gateway Interface) protocol for DuckDB. VGI enables DuckDB to call functions hosted in external worker processes via Arrow IPC over stdin/stdout.

## Project Structure

- `vgi/` — Core VGI SDK package (protocol, worker, function interfaces)
- `examples/scalar/` — Scalar function examples (1:1 row mapping)
- `examples/table/` — Table function examples (row generators)
- `examples/table_in_out/` — Table-in-out function examples (transform + optional finalize)
- `cmd/vgi-example-worker/` — Example worker binary that registers all example functions

## Build

```bash
go build -o vgi-example-worker-go ./cmd/vgi-example-worker
```

## Testing

Tests live in the VGI DuckDB extension repo at `../vgi/test/sql/`. Run them with:

```bash
# All tests (release build)
cd ../vgi && VGI_TEST_WORKER=$(pwd)/../vgi-go/vgi-example-worker-go ./build/release/test/unittest "test/*"

# Single test
cd ../vgi && VGI_TEST_WORKER=$(pwd)/../vgi-go/vgi-example-worker-go ./build/release/test/unittest "test/sql/integration/scalar/add_values.test"
```

Always rebuild the worker before running tests. Use the release build (`./build/release/`) by default.

## Dependencies

- `github.com/Query-farm/vgi-rpc` — VGI RPC framework (local replace: `../vgi-rpc-go`)
- `github.com/apache/arrow-go/v18` — Arrow IPC (uses fork: `github.com/rustyconover/arrow-go/v18`)

## Conventions

- All source files begin with the copyright header:
  ```
  // © Copyright 2025-2026, Query.Farm LLC - https://query.farm
  // SPDX-License-Identifier: Apache-2.0
  ```
- Function implementations follow the interface pattern: `Name()`, `Metadata()`, `ArgumentSpecs()`, `OnBind()`, `Process()`
- New functions must be registered in `cmd/vgi-example-worker/main.go`
