// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/examples/all"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// The SDK's own errors carry a gRPC-style vgi_rpc.error_code
// (WIRE_PROTOCOL.md §8), so a client can tell "your input was wrong" from a
// worker bug. Driven over the example worker's real HTTP handler: what is
// asserted is the code the client decodes, not a Go method.

func newErrorCodeClient(t *testing.T) *vgirpc.HttpClient {
	t.Helper()
	w := vgi.NewWorker(vgi.WithCatalogName("example"))
	all.RegisterAll(w)
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	c, err := vgirpc.NewHttpClient(ts.URL, vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// bindError runs bind with the given request row and returns the decoded
// RpcError (failing if the call succeeds or fails some other way).
func bindError(t *testing.T, c *vgirpc.HttpClient, row map[string]any) *vgirpc.RpcError {
	t.Helper()
	row["resolved_secrets_provided"] = false
	if _, ok := row["arguments"]; !ok {
		row["arguments"] = []byte{}
	}
	params := jsonRow(t, generated.BindParamsSchema, map[string]any{
		"request": mustIPC(t, jsonRow(t, generated.BindRequestSchema, row)),
	})
	defer params.Release()
	res, err := c.CallUnary(context.Background(), "bind", params, nil)
	if err == nil {
		res.Release()
		t.Fatalf("bind %v: succeeded, want an error", row["function_name"])
	}
	var rpcErr *vgirpc.RpcError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("bind %v: %T %v, want an *RpcError", row["function_name"], err, err)
	}
	return rpcErr
}

func TestErrorCodes_E2E(t *testing.T) {
	c := newErrorCodeClient(t)

	// SELECT example.main.double('abc'): a type-bound rejection.
	t.Run("scalar type bound", func(t *testing.T) {
		input, err := vgi.SerializeSchema(arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil))
		if err != nil {
			t.Fatal(err)
		}
		e := bindError(t, c, map[string]any{
			"function_name": "double",
			"function_type": string(vgi.FunctionTypeScalar),
			"input_schema":  input,
		})
		if e.Code != string(vgirpc.CodeInvalidArgument) {
			t.Fatalf("double('abc'): code %q (%v), want INVALID_ARGUMENT", e.Code, e)
		}
	})

	// SELECT * FROM example.main.sequence(10, batch_size := 0): an argument
	// constraint the example checks in OnBind.
	t.Run("table argument constraint", func(t *testing.T) {
		argsSchema := arrow.NewSchema([]arrow.Field{{Name: "args", Type: arrow.StructOf(
			arrow.Field{Name: "positional_0", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "named_batch_size", Type: arrow.PrimitiveTypes.Int64},
		)}}, nil)
		args := mustIPC(t, jsonRow(t, argsSchema, map[string]any{
			"args": map[string]any{"positional_0": 10, "named_batch_size": 0},
		}))
		e := bindError(t, c, map[string]any{
			"function_name": "sequence",
			"function_type": string(vgi.FunctionTypeTable),
			"arguments":     args,
		})
		if e.Code != string(vgirpc.CodeInvalidArgument) {
			t.Fatalf("sequence(batch_size := 0): code %q (%v), want INVALID_ARGUMENT", e.Code, e)
		}
		if !strings.Contains(e.Message, "must be >= 1") {
			t.Fatalf("sequence(batch_size := 0): message %q lost \"must be >= 1\"", e.Message)
		}
	})

	t.Run("unknown function", func(t *testing.T) {
		e := bindError(t, c, map[string]any{
			"function_name": "no_such_function",
			"function_type": string(vgi.FunctionTypeScalar),
		})
		if e.Code != string(vgirpc.CodeNotFound) {
			t.Fatalf("unknown function: code %q (%v), want NOT_FOUND", e.Code, e)
		}
	})
}
