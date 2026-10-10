// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestArgumentError_Format(t *testing.T) {
	cases := []struct {
		name string
		err  *ArgumentError
		want string
	}{
		{"name+pos", &ArgumentError{ArgName: "n", Position: 0, Detail: "must be >= 1"}, `argument "n" (position 0): must be >= 1`},
		{"name only", &ArgumentError{ArgName: "n", Position: -1, Detail: "missing"}, `argument "n": missing`},
		{"pos only", &ArgumentError{Position: 2, Detail: "wrong type"}, `argument at position 2: wrong type`},
		{"neither", &ArgumentError{Position: -1, Detail: "bad"}, `argument: bad`},
	}
	for _, c := range cases {
		if got := c.err.Error(); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestSchemaValidationError_Format(t *testing.T) {
	e := &SchemaValidationError{
		Context: "output schema",
		Mismatches: []SchemaFieldMismatch{
			{FieldName: "a", Expected: arrow.PrimitiveTypes.Int64, Actual: arrow.PrimitiveTypes.Float64},
			{FieldName: "b", Expected: arrow.PrimitiveTypes.Int64},
			{FieldName: "c", Reason: "must be non-nullable"},
		},
	}
	got := e.Error()
	for _, want := range []string{
		"output schema",
		`field "a": expected int64, got float64`,
		`field "b": missing (expected int64)`,
		`field "c": must be non-nullable`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in: %q", want, got)
		}
	}
}

func TestRecoverPanic_CapturesPanic(t *testing.T) {
	var err error
	func() {
		defer RecoverPanic("bind", "boom", &err)
		var p *int
		_ = *p // nil-deref panic
	}()
	if err == nil {
		t.Fatal("expected error after panic")
	}
	var pe *WorkerPanicError
	if !errors.As(err, &pe) {
		t.Fatalf("expected WorkerPanicError, got %T: %v", err, err)
	}
	if pe.Phase != "bind" || pe.FunctionName != "boom" {
		t.Errorf("phase/fn: %+v", pe)
	}
	if len(pe.Stack) == 0 {
		t.Errorf("expected stack trace captured")
	}
	if !strings.Contains(pe.Error(), "worker panic in bind(boom)") {
		t.Errorf("error message: %q", pe.Error())
	}
}

func TestRecoverPanic_NoPanicNoChange(t *testing.T) {
	err := errors.New("preexisting")
	func() {
		defer RecoverPanic("bind", "fn", &err)
		// no panic
	}()
	if err == nil || err.Error() != "preexisting" {
		t.Errorf("RecoverPanic clobbered err: %v", err)
	}
}

func TestAsRpcError_TypeMapping(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&ArgumentError{Detail: "x"}, "ArgumentError"},
		{&SchemaValidationError{}, "SchemaValidationError"},
		{&TypeBoundError{}, "TypeBoundError"},
		{&UnknownFunctionError{}, "UnknownFunctionError"},
		{&CatalogReadOnlyError{}, "CatalogReadOnlyError"},
		{&WorkerPanicError{}, "WorkerPanicError"},
		{errors.New("misc"), "RuntimeError"},
	}
	for _, c := range cases {
		got := AsRpcError(c.err)
		if got.Type != c.want {
			t.Errorf("err=%T: got Type=%q, want %q", c.err, got.Type, c.want)
		}
	}
}

func TestErrorCodes(t *testing.T) {
	cases := []struct {
		err  error
		want vgirpc.Code
	}{
		{&ArgumentError{Detail: "x"}, vgirpc.CodeInvalidArgument},
		{&TypeBoundError{}, vgirpc.CodeInvalidArgument},
		{&FunctionShapeMismatchError{}, vgirpc.CodeInvalidArgument},
		{&UnknownFunctionError{}, vgirpc.CodeNotFound},
		{&CatalogReadOnlyError{}, vgirpc.CodeFailedPrecondition},
		// Worker bugs stay unclassified.
		{&SchemaValidationError{}, vgirpc.CodeUnknown},
		{&WorkerPanicError{}, vgirpc.CodeUnknown},
		{errors.New("misc"), vgirpc.CodeUnknown},
	}
	for _, c := range cases {
		if got := vgirpc.ErrorCodeOf(c.err); got != c.want {
			t.Errorf("%T: code %q, want %q", c.err, got, c.want)
		}
		// Wrapping with %w keeps the code (vgirpc looks it up with errors.As).
		if got := vgirpc.ErrorCodeOf(fmt.Errorf("ctx: %w", c.err)); got != c.want {
			t.Errorf("wrapped %T: code %q, want %q", c.err, got, c.want)
		}
		// AsRpcError carries it over.
		wantName := string(c.want)
		if c.want == vgirpc.CodeUnknown {
			wantName = ""
		}
		if got := AsRpcError(c.err).Code; got != wantName {
			t.Errorf("AsRpcError(%T).Code = %q, want %q", c.err, got, wantName)
		}
	}
	// A hook's coded error keeps its code when the SDK rewraps it.
	if got := rewrapError("ValueError", &ArgumentError{Detail: "x"}); got.Code != string(vgirpc.CodeInvalidArgument) || got.Type != "ValueError" {
		t.Errorf("rewrapError: %+v", got)
	}
}
