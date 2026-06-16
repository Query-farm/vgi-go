// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogReadOnlyError is returned when a write operation is attempted on a read-only catalog.
type CatalogReadOnlyError struct {
	Operation string
}

// Error implements the error interface.
func (e *CatalogReadOnlyError) Error() string {
	return fmt.Sprintf("Catalog is read-only: %s not supported", e.Operation)
}

// UnknownFunctionError is returned when a function name cannot be resolved.
type UnknownFunctionError struct {
	Name         string
	FunctionType string
}

// Error implements the error interface.
func (e *UnknownFunctionError) Error() string {
	return fmt.Sprintf("Unknown function: '%s' (type: %s)", e.Name, e.FunctionType)
}

// TypeBoundError is returned when an input schema field type does not satisfy
// the type bound predicates declared for an argument.
type TypeBoundError struct {
	ArgName   string
	Position  int
	FieldType arrow.DataType
	// PredicateNames are the runtime-resolved names of the failed predicates,
	// e.g. ["IsMultipliableType"]. Empty when reflection couldn't recover them.
	PredicateNames []string
}

// Error implements the error interface.
func (e *TypeBoundError) Error() string {
	name := e.ArgName
	if name == "" {
		name = fmt.Sprintf("position %d", e.Position)
	}
	if len(e.PredicateNames) > 0 {
		return fmt.Sprintf("argument '%s': column type %v does not satisfy %s",
			name, e.FieldType, strings.Join(e.PredicateNames, " OR "))
	}
	return fmt.Sprintf("argument '%s': column type %v does not match type bound constraints", name, e.FieldType)
}

// ArgumentError is returned when a function argument is missing, of the
// wrong shape, or fails inline validation at bind/init time. Use it from
// inside OnBind/OnBindTyped to surface a clean error to the caller rather
// than the generic "RuntimeError: ..." default.
type ArgumentError struct {
	ArgName  string
	Position int // -1 when name-only
	Detail   string
}

// Error implements the error interface.
func (e *ArgumentError) Error() string {
	switch {
	case e.ArgName != "" && e.Position >= 0:
		return fmt.Sprintf("argument %q (position %d): %s", e.ArgName, e.Position, e.Detail)
	case e.ArgName != "":
		return fmt.Sprintf("argument %q: %s", e.ArgName, e.Detail)
	case e.Position >= 0:
		return fmt.Sprintf("argument at position %d: %s", e.Position, e.Detail)
	default:
		return "argument: " + e.Detail
	}
}

// SchemaValidationError describes one or more field-level type mismatches
// between an expected schema and an actual schema. The message lists each
// mismatched field with expected vs. actual types — analogous to
// vgi-python's SchemaValidationError.
type SchemaValidationError struct {
	Context    string // e.g. "table function output schema"
	Mismatches []SchemaFieldMismatch
}

// SchemaFieldMismatch is one field-level disagreement between two schemas.
type SchemaFieldMismatch struct {
	FieldName string
	Expected  arrow.DataType // nil when missing on the expected side
	Actual    arrow.DataType // nil when missing on the actual side
	Reason    string         // optional — overrides the default phrasing
}

// Error implements the error interface.
func (e *SchemaValidationError) Error() string {
	var b strings.Builder
	b.WriteString("schema validation failed")
	if e.Context != "" {
		b.WriteString(" (" + e.Context + ")")
	}
	b.WriteString(": ")
	for i, m := range e.Mismatches {
		if i > 0 {
			b.WriteString("; ")
		}
		switch {
		case m.Reason != "":
			fmt.Fprintf(&b, "field %q: %s", m.FieldName, m.Reason)
		case m.Expected != nil && m.Actual != nil:
			fmt.Fprintf(&b, "field %q: expected %v, got %v", m.FieldName, m.Expected, m.Actual)
		case m.Expected != nil:
			fmt.Fprintf(&b, "field %q: missing (expected %v)", m.FieldName, m.Expected)
		case m.Actual != nil:
			fmt.Fprintf(&b, "field %q: unexpected (got %v)", m.FieldName, m.Actual)
		default:
			fmt.Fprintf(&b, "field %q: mismatch", m.FieldName)
		}
	}
	return b.String()
}

// WorkerPanicError is returned when a registered function panics during
// bind/init/process/finalize. The dispatcher (see RecoverPanic) catches the
// panic, captures the stack, and returns this error to the caller so the
// worker process stays alive and the RPC client sees a clean message.
type WorkerPanicError struct {
	FunctionName string
	Phase        string // bind, init, process, finalize, etc.
	Recovered    any    // value passed to panic()
	Stack        []byte
}

// Error implements the error interface.
func (e *WorkerPanicError) Error() string {
	name := e.FunctionName
	if name == "" {
		name = "<unknown>"
	}
	phase := e.Phase
	if phase == "" {
		phase = "dispatch"
	}
	return fmt.Sprintf("worker panic in %s(%s): %v", phase, name, e.Recovered)
}

// RecoverPanic is meant to be deferred at the top of an RPC handler that may
// invoke user-supplied function code. On panic it stores a WorkerPanicError
// into *errOut so the framework returns a clean RpcError instead of crashing.
//
// Usage:
//
//	func (w *Worker) handleBind(...) (resp BindResponseWire, err error) {
//	    defer vgi.RecoverPanic("bind", req.FunctionName, &err)
//	    ...
//	}
func RecoverPanic(phase, fnName string, errOut *error) {
	if r := recover(); r != nil {
		*errOut = &WorkerPanicError{
			FunctionName: fnName,
			Phase:        phase,
			Recovered:    r,
			Stack:        debug.Stack(),
		}
	}
}

// AsRpcError converts an error to an RpcError for wire transmission. Maps
// known custom error types to clearer Type strings so DuckDB-side error
// surfacing matches vgi-python's behaviour.
func AsRpcError(err error) *vgirpc.RpcError {
	if rpcErr, ok := err.(*vgirpc.RpcError); ok {
		return rpcErr
	}
	switch err.(type) {
	case *ArgumentError:
		return &vgirpc.RpcError{Type: "ArgumentError", Message: err.Error()}
	case *SchemaValidationError:
		return &vgirpc.RpcError{Type: "SchemaValidationError", Message: err.Error()}
	case *TypeBoundError:
		return &vgirpc.RpcError{Type: "TypeBoundError", Message: err.Error()}
	case *UnknownFunctionError:
		return &vgirpc.RpcError{Type: "UnknownFunctionError", Message: err.Error()}
	case *CatalogReadOnlyError:
		return &vgirpc.RpcError{Type: "CatalogReadOnlyError", Message: err.Error()}
	case *WorkerPanicError:
		return &vgirpc.RpcError{Type: "WorkerPanicError", Message: err.Error()}
	}
	return &vgirpc.RpcError{
		Type:    "RuntimeError",
		Message: err.Error(),
	}
}
