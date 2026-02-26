// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// BindParams holds the parameters available during the bind phase.
type BindParams struct {
	// FunctionName is the name of the function being bound.
	FunctionName string
	// FunctionType is the type of the function.
	FunctionType FunctionType
	// Args are the parsed function arguments.
	Args *Arguments
	// InputSchema is the input table schema (nil for table functions).
	InputSchema *arrow.Schema
	// Settings is a map of DuckDB setting names to their scalar values.
	Settings map[string]interface{}
	// Secrets is a map of secret names to their value maps.
	Secrets map[string]map[string]interface{}
	// AttachID is the catalog attachment identifier.
	AttachID []byte
	// TransactionID is the transaction identifier.
	TransactionID []byte
}

// BindResponse is returned by a function's OnBind method.
type BindResponse struct {
	// OutputSchema is the Arrow schema for the function's output.
	OutputSchema *arrow.Schema
	// OpaqueData is optional opaque data passed to the init phase.
	OpaqueData []byte
}
