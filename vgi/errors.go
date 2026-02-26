// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogReadOnlyError is returned when a write operation is attempted on a read-only catalog.
type CatalogReadOnlyError struct {
	Operation string
}

func (e *CatalogReadOnlyError) Error() string {
	return fmt.Sprintf("Catalog is read-only: %s not supported", e.Operation)
}

// UnknownFunctionError is returned when a function name cannot be resolved.
type UnknownFunctionError struct {
	Name         string
	FunctionType string
}

func (e *UnknownFunctionError) Error() string {
	return fmt.Sprintf("Unknown function: '%s' (type: %s)", e.Name, e.FunctionType)
}

// TypeBoundError is returned when an input schema field type does not satisfy
// the type bound predicates declared for an argument.
type TypeBoundError struct {
	ArgName   string
	Position  int
	FieldType arrow.DataType
}

func (e *TypeBoundError) Error() string {
	name := e.ArgName
	if name == "" {
		name = fmt.Sprintf("position %d", e.Position)
	}
	return fmt.Sprintf("argument '%s': column type %v does not match type bound constraints", name, e.FieldType)
}

// AsRpcError converts an error to an RpcError for wire transmission.
func AsRpcError(err error) *vgirpc.RpcError {
	if rpcErr, ok := err.(*vgirpc.RpcError); ok {
		return rpcErr
	}
	return &vgirpc.RpcError{
		Type:    "RuntimeError",
		Message: err.Error(),
	}
}
