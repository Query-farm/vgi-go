// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// Regression tests: dispatching a function via the RPC call shape belonging
// to a *different* kind of function.
//
// Found live: a client called a table-in-out / blended row-transform
// function via the plain-producer RPC method (table_function(), no input
// stream) instead of the correct table_in_out_function(input=...) -- or the
// mirror-image mistake -- against a real deployed worker. This produced a
// silent, non-terminating hang rather than a clean error: a table-in-out
// function only stops once Finalize() is reached, which requires input
// batches that never arrive when no input stream was sent; a plain producer
// only stops once it calls out.Finish() on its own accord, which a
// row-transform function never does (it's designed to consume input rows
// that likewise never arrive). Each side was independently, locally correct,
// waiting on a completion signal only the other, mismatched side could have
// produced.
//
// These tests pin that the mismatch is rejected immediately -- at bind and
// at init, in both directions -- with a message naming the fix. Mirrors
// vgi-python's tests/test_function_shape_mismatch.py and vgi-typescript's
// src/functions/__tests__/shape-mismatch.test.ts.

// shapeTestTableFunction is a minimal plain producer (TableFunction) fixture.
type shapeTestTableFunction struct{}

func (shapeTestTableFunction) Name() string               { return "shape_test_table" }
func (shapeTestTableFunction) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (shapeTestTableFunction) ArgumentSpecs() []ArgSpec   { return nil }

func (shapeTestTableFunction) OnBind(params *BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (shapeTestTableFunction) OnInit(params *InitParams) (*GlobalInitResponse, error) {
	return DefaultInit()
}

func (shapeTestTableFunction) NewState(params *ProcessParams) (interface{}, error) {
	return nil, nil
}

func (shapeTestTableFunction) Process(ctx context.Context, params *ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	return out.Finish()
}

// shapeTestInOutFunction is a minimal table-in-out (classic exchange) fixture.
type shapeTestInOutFunction struct{}

func (shapeTestInOutFunction) Name() string               { return "shape_test_in_out" }
func (shapeTestInOutFunction) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (shapeTestInOutFunction) ArgumentSpecs() []ArgSpec   { return nil }

func (shapeTestInOutFunction) OnBind(params *BindParams) (*BindResponse, error) {
	return BindInputSchema(params)
}

func (shapeTestInOutFunction) OnInit(params *InitParams) (*GlobalInitResponse, error) {
	return DefaultInit()
}

func (shapeTestInOutFunction) NewState(params *ProcessParams) (interface{}, error) {
	return nil, nil
}

func (shapeTestInOutFunction) Process(ctx context.Context, params *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	return out.Emit(batch)
}

func (shapeTestInOutFunction) Finalize(ctx context.Context, params *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func shapeTestCallCtx() *vgirpc.CallContext {
	return &vgirpc.CallContext{Ctx: context.Background(), Auth: vgirpc.Anonymous()}
}

func shapeTestInputSchema(t *testing.T) []byte {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b, err := SerializeSchema(sch)
	if err != nil {
		t.Fatalf("serializing schema: %v", err)
	}
	return b
}

// --- bind()-time guards ------------------------------------------------

func TestHandleBindRejectsTableInOutWithNoInputSchema(t *testing.T) {
	w := NewWorker()
	w.RegisterTableInOut(shapeTestInOutFunction{})

	req := BindRequestWire{
		FunctionName: "shape_test_in_out",
		FunctionType: string(FunctionTypeTable),
		InputSchema:  nil, // table_function() sends no input stream at all
	}
	_, err := w.handleBind(context.Background(), shapeTestCallCtx(), req)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "table_in_out_function") {
		t.Errorf("error should name the fix (table_in_out_function): %v", err)
	}
	if !strings.Contains(err.Error(), "shape_test_in_out") {
		t.Errorf("error should name the function: %v", err)
	}
	var shapeErr *FunctionShapeMismatchError
	if !errors.As(err, &shapeErr) {
		t.Errorf("expected *FunctionShapeMismatchError, got %T: %v", err, err)
	}
}

func TestHandleBindAcceptsTableInOutWithInputSchema(t *testing.T) {
	w := NewWorker()
	w.RegisterTableInOut(shapeTestInOutFunction{})

	schemaBytes := shapeTestInputSchema(t)
	req := BindRequestWire{
		FunctionName: "shape_test_in_out",
		FunctionType: string(FunctionTypeTable),
		InputSchema:  &schemaBytes,
	}
	if _, err := w.handleBind(context.Background(), shapeTestCallCtx(), req); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
}

func TestHandleBindRejectsPlainTableWithInputSchema(t *testing.T) {
	w := NewWorker()
	w.RegisterTable(shapeTestTableFunction{})

	schemaBytes := shapeTestInputSchema(t)
	req := BindRequestWire{
		FunctionName: "shape_test_table",
		FunctionType: string(FunctionTypeTable),
		InputSchema:  &schemaBytes, // table_in_out_function(input=...) sends one
	}
	_, err := w.handleBind(context.Background(), shapeTestCallCtx(), req)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "table_function()") {
		t.Errorf("error should name the fix (table_function()): %v", err)
	}
	if !strings.Contains(err.Error(), "shape_test_table") {
		t.Errorf("error should name the function: %v", err)
	}
	var shapeErr *FunctionShapeMismatchError
	if !errors.As(err, &shapeErr) {
		t.Errorf("expected *FunctionShapeMismatchError, got %T: %v", err, err)
	}
}

func TestHandleBindAcceptsPlainTableWithNoInputSchema(t *testing.T) {
	w := NewWorker()
	w.RegisterTable(shapeTestTableFunction{})

	req := BindRequestWire{
		FunctionName: "shape_test_table",
		FunctionType: string(FunctionTypeTable),
		InputSchema:  nil,
	}
	if _, err := w.handleBind(context.Background(), shapeTestCallCtx(), req); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
}

// --- init()-time defense-in-depth guards --------------------------------
//
// These call handleInit directly with a bind_call shape that would never
// pass through handleBind unmodified (defense-in-depth for a request that
// reaches init without the corresponding bind-time check having run against
// it -- e.g. a rehydrated/hand-built request).

func TestHandleInitRejectsTableInOutWithNoPhase(t *testing.T) {
	w := NewWorker()
	w.RegisterTableInOut(shapeTestInOutFunction{})

	outSchema, err := SerializeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	if err != nil {
		t.Fatalf("serializing output schema: %v", err)
	}

	req := InitRequestWire{
		BindCall: BindRequestWire{
			FunctionName: "shape_test_in_out",
			FunctionType: string(FunctionTypeTable),
			InputSchema:  nil,
		},
		OutputSchema: outSchema,
		Phase:        nil, // no INPUT/FINALIZE phase at all
	}
	_, err = w.handleInit(context.Background(), shapeTestCallCtx(), req)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "table_in_out_function") {
		t.Errorf("error should name the fix (table_in_out_function): %v", err)
	}
	var shapeErr *FunctionShapeMismatchError
	if !errors.As(err, &shapeErr) {
		t.Errorf("expected *FunctionShapeMismatchError, got %T: %v", err, err)
	}
}

func TestHandleInitAcceptsTableInOutWithInputPhase(t *testing.T) {
	w := NewWorker()
	w.RegisterTableInOut(shapeTestInOutFunction{})

	schemaBytes := shapeTestInputSchema(t)
	outSchema, err := SerializeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	if err != nil {
		t.Fatalf("serializing output schema: %v", err)
	}
	phase := string(PhaseInput)

	req := InitRequestWire{
		BindCall: BindRequestWire{
			FunctionName: "shape_test_in_out",
			FunctionType: string(FunctionTypeTable),
			InputSchema:  &schemaBytes,
		},
		OutputSchema: outSchema,
		Phase:        &phase,
	}
	if _, err := w.handleInit(context.Background(), shapeTestCallCtx(), req); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
}

func TestHandleInitRejectsPlainTableWithPhaseSet(t *testing.T) {
	w := NewWorker()
	w.RegisterTable(shapeTestTableFunction{})

	schemaBytes := shapeTestInputSchema(t)
	outSchema, err := SerializeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	if err != nil {
		t.Fatalf("serializing output schema: %v", err)
	}
	phase := string(PhaseInput)

	req := InitRequestWire{
		BindCall: BindRequestWire{
			FunctionName: "shape_test_table",
			FunctionType: string(FunctionTypeTable),
			InputSchema:  &schemaBytes, // table_in_out_function(input=...) sends one
		},
		OutputSchema: outSchema,
		Phase:        &phase, // ...and an init phase
	}
	_, err = w.handleInit(context.Background(), shapeTestCallCtx(), req)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "table_function()") {
		t.Errorf("error should name the fix (table_function()): %v", err)
	}
	var shapeErr *FunctionShapeMismatchError
	if !errors.As(err, &shapeErr) {
		t.Errorf("expected *FunctionShapeMismatchError, got %T: %v", err, err)
	}
}

func TestHandleInitAcceptsPlainTableWithNoPhase(t *testing.T) {
	w := NewWorker()
	w.RegisterTable(shapeTestTableFunction{})

	outSchema, err := SerializeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	if err != nil {
		t.Fatalf("serializing output schema: %v", err)
	}

	req := InitRequestWire{
		BindCall: BindRequestWire{
			FunctionName: "shape_test_table",
			FunctionType: string(FunctionTypeTable),
			InputSchema:  nil,
		},
		OutputSchema: outSchema,
		Phase:        nil,
	}
	if _, err := w.handleInit(context.Background(), shapeTestCallCtx(), req); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
}

// AsRpcError maps FunctionShapeMismatchError to a distinct, named RpcError
// type rather than the generic "RuntimeError" bucket every other unclassified
// error falls into -- so a caller can distinguish this from an ordinary
// user-code failure.
func TestFunctionShapeMismatchErrorMapsToNamedRpcError(t *testing.T) {
	err := errTableInOutMissingInputSchema("some_fn")
	rpcErr := AsRpcError(err)
	if rpcErr.Type != "FunctionShapeMismatchError" {
		t.Errorf("Type = %q, want %q", rpcErr.Type, "FunctionShapeMismatchError")
	}
}
