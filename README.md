# vgi-go

Go SDK for the **VGI** (Vector Gateway Interface) protocol. VGI lets DuckDB
call user-defined scalar / table / aggregate / table-in-out functions hosted
in an external worker process over Arrow IPC.

- Sibling reference port (Python): [`vgi-python`](https://github.com/Query-farm/vgi-python)
- DuckDB extension: [`vgi`](https://github.com/Query-farm/vgi)

## Install

```bash
go get github.com/Query-farm/vgi-go/vgi
```

Requires Go 1.25+.

## Quickstart

A minimum-viable worker — a scalar that adds two integers:

```go
package main

import (
    "context"

    "github.com/Query-farm/vgi-go/vgi"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
)

type AddInts struct{}

type addArgs struct {
    A int64 `vgi:"pos=0,const=false,doc=Left operand"`
    B int64 `vgi:"pos=1,const=false,doc=Right operand"`
}

func (*AddInts) Name() string               { return "add_ints" }
func (*AddInts) Metadata() vgi.FunctionMetadata { return vgi.FunctionMetadata{} }

func (*AddInts) OnBindTyped(_ *addArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
    return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (*AddInts) ProcessTyped(_ context.Context, _ *addArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
    return vgi.MapAllColumns(params, batch, array.NewInt64Builder,
        func(cols []arrow.Array, i int) int64 {
            return vgi.GetInt64Value(cols[0], i) + vgi.GetInt64Value(cols[1], i)
        })
}

func main() {
    w := vgi.NewWorker(vgi.WithCatalogName("demo"))
    w.RegisterScalar(vgi.AsScalarFunction[addArgs](&AddInts{}))
    w.RunStdio()
}
```

Build it, then attach from DuckDB:

```sql
LOAD vgi;
ATTACH 'demo:./my-worker' AS demo;
SELECT demo.add_ints(2, 3); -- => 5
```

## Function shapes

| Shape                   | Interface                                        | Use case                              |
| ----------------------- | ------------------------------------------------ | ------------------------------------- |
| Scalar                  | `ScalarFunction`, `TypedScalarFunc[A]`           | 1:1 row mapping                       |
| Table generator         | `TableFunction`, `TypedTableFunc[S]`             | Row generator (no streamed input)     |
| Table-in-out            | `TableInOutFunction`                             | Stream input rows → output rows       |
| Table-buffering         | `TableBufferingFunction`                         | Sort / aggregate / join-style buffer  |
| Aggregate               | `AggregateFunction`                              | Cumulative state + final emit         |

Pick the typed variants (`TypedScalarFunc`, `TypedTableFunc`) when you want
the framework to derive `ArgumentSpecs` from a Go struct with `vgi:"..."`
tags. See `examples/scalar/add_values.go` and `examples/table/sequence.go`.

## Logging

Workers emit structured logs through named slog loggers (`vgi`, `vgi.worker`,
`vgi.catalog`, `vgi.rpc`, `vgi.client`, `vgi.filter_pushdown`). Configure them
with the standard CLI flags by registering them in `main()`:

```go
fs := flag.CommandLine
logFlags := vgi.RegisterLoggingFlags(fs)
flag.Parse()
if err := logFlags.Apply(); err != nil {
    log.Fatal(err)
}
```

Then:

```bash
./my-worker --log-level=debug --log-format=json
./my-worker --log-logger=vgi.catalog --log-logger=vgi.rpc
VGI_WORKER_DEBUG=1 ./my-worker        # equivalent to --debug
```

Env-var fallbacks: `VGI_LOG_LEVEL`, `VGI_LOG_FORMAT`, `VGI_LOG_LOGGER`,
`VGI_WORKER_DEBUG`.

## Errors

The SDK defines a few error types that surface clean RPC-error messages to
DuckDB rather than the generic `RuntimeError`:

- `ArgumentError` — bad / missing argument at bind time
- `SchemaValidationError` — schema mismatch with per-field detail
- `TypeBoundError` — column type doesn't satisfy a declared type predicate
- `WorkerPanicError` — captured panic from user code; worker stays alive

Panics inside user functions during `bind`, `init`, `cardinality`, and
`statistics` dispatch are recovered automatically.

## Examples

The `cmd/vgi-example-worker` binary registers every example function via
`examples/all.RegisterAll(w)`. Browse:

- `examples/scalar/` — 30+ scalar examples (typed + classic)
- `examples/table/` — 50+ table generators (partitioned, paged, etc.)
- `examples/table_in_out/` — transform / buffering / aggregation patterns
- `examples/aggregate/` — cumulative aggregates
- `examples/schema_reconcile/`, `examples/versioned_tables/` — catalog + write paths

## Build & test

```bash
make build              # build all example worker binaries
make fmt                # gofmt
make vet                # go vet
make lint               # golangci-lint (requires golangci-lint in PATH)
make test               # full integration suite over stdio (requires ../vgi)
make test-http          # full suite over HTTP transport
make test-all           # both transports
make test-single TEST=test/sql/integration/scalar/add_values.test
go test ./...           # pure Go unit tests
```

Integration tests live in the sibling DuckDB extension repo at `../vgi` and
use the DuckDB sqllogictest format.

## Repo layout

```
vgi/                            # SDK package
examples/scalar/                # scalar example functions
examples/table/                 # table example functions
examples/table_in_out/          # table-in-out + buffering examples
examples/aggregate/             # aggregate examples
examples/schema_reconcile/      # catalog-handlers fixture
examples/all/                   # RegisterAll(w) helper
cmd/vgi-example-worker/         # fixture worker (used by integration tests)
cmd/vgi-example-versioned-worker/
cmd/vgi-example-versioned-tables-worker/
cmd/vgi-example-attach-options-worker/
```

## License

Apache-2.0. © 2025-2026 Query.Farm LLC.
