// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package vgi implements the VGI (Vector Gateway Interface) protocol for Go.
//
// VGI lets DuckDB call functions hosted in external worker processes via Arrow
// IPC over stdin/stdout, an AF_UNIX socket, or HTTP. This package is the
// framework for building VGI worker processes in Go; a worker registers one
// or more functions on a [Worker] and then calls a transport method like
// [Worker.RunStdio], [Worker.RunUnix], or [Worker.RunHttp].
//
// # Function types
//
// VGI supports several function shapes, each with a corresponding interface:
//
//   - [ScalarFunction]: 1:1 row mapping; each input batch produces an output
//     batch with the same number of rows. See [TypedScalarFunc] and
//     [AsScalarFunction] for the declarative struct-tag variant.
//   - [TableFunction]: row generator with no streamed input. See [TypedTableFunc]
//     and [AsTableFunction] for typed state.
//   - [TableInOutFunction]: streams input rows to output, optionally with a
//     final flush.
//   - [TableBufferingFunction]: buffers all input before producing output (sort,
//     aggregation, join-style operations).
//   - [AggregateFunction]: cumulative state update + final emit.
//
// # Declarative arguments
//
// Function arguments can be described once as a Go struct with `vgi:"..."`
// tags. The framework derives ArgumentSpecs from the tags ([DeriveArgSpecs])
// and binds runtime values back into the struct ([BindArgs]). See the
// [TypedScalarFunc] / [TypedTableFunc] interfaces and the examples in
// examples/scalar and examples/table.
//
// # Logging
//
// The worker writes structured logs through named slog loggers — [Log],
// [LogWorker], [LogCatalog], [LogRPC], [LogClient], [LogFilterPushdown].
// Configure logging from main() with [RegisterLoggingFlags] and
// [LoggingFlags.Apply], or programmatically with [ConfigureLogging].
//
// # Errors
//
// Custom error types ([ArgumentError], [SchemaValidationError], [TypeBoundError],
// [WorkerPanicError]) carry richer context than bare fmt.Errorf and surface
// over the wire with a matching RpcError.Type. Panics in user code during
// bind/init/cardinality/statistics dispatch are recovered into
// [WorkerPanicError] so the worker process stays alive.
//
// # Minimal worker
//
//	w := vgi.NewWorker(vgi.WithCatalogName("example"))
//	w.RegisterScalar(scalar.NewAddValues())
//	w.RegisterTable(table.NewSequenceFunction())
//	w.RunStdio()
package vgi
