// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package vgi implements the VGI (Vector Gateway Interface) protocol for Go.
//
// VGI enables DuckDB to call functions hosted in external worker processes via
// Arrow IPC over stdin/stdout. This package provides the framework for building
// VGI worker processes in Go.
//
// # Function Types
//
// VGI supports three function types:
//
//   - ScalarFunction: 1:1 row mapping, transforms input columns to a single output column
//   - TableFunction: generates output without input (Producer mode)
//   - TableInOutFunction: transforms input tables, with optional finalize phase
//
// # Usage
//
// Create a Worker, register functions, and call RunStdio:
//
//	w := vgi.NewWorker()
//	w.RegisterScalar(&MyScalarFunc{})
//	w.RegisterTable(&MyTableFunc{})
//	w.RunStdio()
package vgi
