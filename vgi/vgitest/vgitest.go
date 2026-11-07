// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package vgitest unit-tests VGI table functions without DuckDB or a worker
// process: it drives a function through bind -> init -> NewState -> Process
// in-process, the way the worker does, and returns what it emitted.
//
// Between Process calls the state is gob round-tripped, exactly as the HTTP
// transport carries it in the continuation token, so a state that would lose
// its cursor over HTTP fails here too, rather than only in production.
//
//	res, err := vgitest.RunTable(vgi.AsTableFunction[seqState](&Seq{}), vgitest.Call{
//	    Named: map[string]any{"count": 5},
//	})
//	if err != nil { t.Fatal(err) }
//	defer res.Release()
//	if res.Rows() != 5 { ... }
package vgitest

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Call describes one invocation of a function under test.
type Call struct {
	// Positional are the positional const arguments, in declared order. Each is
	// a Go scalar (int, int32, int64, float32, float64, string, bool, []byte,
	// time.Time, time.Duration) or a one-row arrow.Array.
	Positional []any
	// Named are the named const arguments (name := value), same value kinds.
	Named map[string]any
	// Settings and Secrets are passed through to bind, init and process.
	Settings map[string]any
	Secrets  vgi.Secrets
	// MaxCalls bounds the Process loop (default 100000), so a function that
	// never finishes -- the HTTP-cursor bug -- fails instead of hanging.
	MaxCalls int
	// NoGobRoundTrip keeps the live state between calls, as stdio does.
	NoGobRoundTrip bool
	// Context is passed to Process (default context.Background()).
	Context context.Context
}

// Result is what a function run emitted.
type Result struct {
	// Bind is the function's bind response.
	Bind *vgi.BindResponse
	// Schema is the output schema batches are emitted with.
	Schema *arrow.Schema
	// Batches are the emitted data batches, in order. Release with Release.
	Batches []arrow.RecordBatch
	// Logs are the client-log messages emitted, in order.
	Logs []vgirpc.TestLog
	// Calls is how many times Process ran.
	Calls int
}

// Rows is the total row count across Batches.
func (r *Result) Rows() int64 {
	var n int64
	for _, b := range r.Batches {
		n += b.NumRows()
	}
	return n
}

// Release releases the emitted batches.
func (r *Result) Release() {
	for _, b := range r.Batches {
		b.Release()
	}
	r.Batches = nil
}

// Arguments builds the vgi.Arguments a function's OnBind/NewState receive (and
// vgi.BindArgs decodes) from Go values; see Call.Positional for value kinds.
func Arguments(positional []any, named map[string]any) (*vgi.Arguments, error) {
	args := &vgi.Arguments{Named: map[string]arrow.Array{}}
	for i, v := range positional {
		arr, err := ScalarArray(v)
		if err != nil {
			return nil, fmt.Errorf("positional argument %d: %w", i, err)
		}
		args.Positional = append(args.Positional, arr)
		args.Named[fmt.Sprintf("positional_%d", i)] = arr
	}
	for name, v := range named {
		arr, err := ScalarArray(v)
		if err != nil {
			return nil, fmt.Errorf("named argument %q: %w", name, err)
		}
		args.Named[name] = arr
		args.Named["named_"+name] = arr
	}
	return args, nil
}

// ScalarArray converts a Go scalar into the one-row Arrow array DuckDB would
// send for a const argument. An arrow.Array is returned as is.
func ScalarArray(v any) (arrow.Array, error) {
	mem := memory.DefaultAllocator
	switch x := v.(type) {
	case arrow.Array:
		return x, nil
	case nil:
		b := array.NewNullBuilder(mem)
		defer b.Release()
		b.AppendNull()
		return b.NewArray(), nil
	case int:
		return one(array.NewInt64Builder(mem), func(b *array.Int64Builder) { b.Append(int64(x)) }), nil
	case int64:
		return one(array.NewInt64Builder(mem), func(b *array.Int64Builder) { b.Append(x) }), nil
	case int32:
		return one(array.NewInt32Builder(mem), func(b *array.Int32Builder) { b.Append(x) }), nil
	case float64:
		return one(array.NewFloat64Builder(mem), func(b *array.Float64Builder) { b.Append(x) }), nil
	case float32:
		return one(array.NewFloat32Builder(mem), func(b *array.Float32Builder) { b.Append(x) }), nil
	case string:
		return one(array.NewStringBuilder(mem), func(b *array.StringBuilder) { b.Append(x) }), nil
	case bool:
		return one(array.NewBooleanBuilder(mem), func(b *array.BooleanBuilder) { b.Append(x) }), nil
	case []byte:
		return one(array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary), func(b *array.BinaryBuilder) { b.Append(x) }), nil
	case time.Time:
		b := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
		return one(b, func(b *array.TimestampBuilder) { b.Append(arrow.Timestamp(x.UnixMicro())) }), nil
	case time.Duration:
		b := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Microsecond})
		return one(b, func(b *array.DurationBuilder) { b.Append(arrow.Duration(x.Microseconds())) }), nil
	}
	return nil, fmt.Errorf("unsupported argument value %T (pass a one-row arrow.Array)", v)
}

func one[B interface {
	NewArray() arrow.Array
	Release()
}](b B, appendOne func(B)) arrow.Array {
	defer b.Release()
	appendOne(b)
	return b.NewArray()
}

// RunTable drives fn through bind, init, NewState and Process until it
// finishes, gob round-tripping the state between Process calls (unless
// call.NoGobRoundTrip). Every turn must obey the wire contract -- exactly one
// data batch, or Finish -- or RunTable returns the error dispatch would raise.
func RunTable(fn vgi.TableFunction, call Call) (*Result, error) {
	ctx := call.Context
	if ctx == nil {
		ctx = context.Background()
	}
	maxCalls := call.MaxCalls
	if maxCalls <= 0 {
		maxCalls = 100000
	}
	args, err := Arguments(call.Positional, call.Named)
	if err != nil {
		return nil, err
	}
	args.RemapPositionalArgs(fn.ArgumentSpecs())

	bind, err := fn.OnBind(&vgi.BindParams{
		FunctionName: fn.Name(),
		FunctionType: vgi.FunctionTypeTable,
		Args:         args,
		Settings:     call.Settings,
		Secrets:      call.Secrets,
	})
	if err != nil {
		return nil, fmt.Errorf("bind: %w", err)
	}
	if bind == nil || bind.OutputSchema == nil {
		return nil, fmt.Errorf("bind: no output schema")
	}
	schema := bind.OutputSchema

	initResp, err := fn.OnInit(&vgi.InitParams{
		FunctionName:   fn.Name(),
		FunctionType:   vgi.FunctionTypeTable,
		Args:           args,
		OutputSchema:   schema,
		BindOpaqueData: bind.OpaqueData,
		Settings:       call.Settings,
		Secrets:        call.Secrets,
	})
	if err != nil {
		return nil, fmt.Errorf("init: %w", err)
	}
	pp := &vgi.ProcessParams{
		FunctionName:     fn.Name(),
		FunctionType:     vgi.FunctionTypeTable,
		Args:             args,
		OutputSchema:     schema,
		BindOutputSchema: schema,
		BindOpaqueData:   bind.OpaqueData,
		Settings:         call.Settings,
		Secrets:          call.Secrets,
	}
	if initResp != nil {
		pp.ExecutionID = initResp.ExecutionID
		pp.InitOpaqueData = initResp.OpaqueData
	}
	state, err := fn.NewState(pp)
	if err != nil {
		return nil, fmt.Errorf("new state: %w", err)
	}

	res := &Result{Bind: bind, Schema: schema}
	for {
		if res.Calls >= maxCalls {
			res.Release()
			return nil, fmt.Errorf("process did not finish after %d calls (does the state keep its position across a gob round trip?)", maxCalls)
		}
		if !call.NoGobRoundTrip && res.Calls > 0 {
			if state, err = GobRoundTrip(state); err != nil {
				res.Release()
				return nil, err
			}
		}
		out := vgirpc.NewTestOutputCollector(schema)
		res.Calls++
		if err := fn.Process(ctx, pp, state, out); err != nil {
			out.Reset()
			res.Release()
			return nil, fmt.Errorf("process call %d: %w", res.Calls, err)
		}
		if err := out.Validate(); err != nil {
			out.Reset()
			res.Release()
			return nil, fmt.Errorf("process call %d: %w", res.Calls, err)
		}
		for _, b := range out.Batches() {
			b.Retain()
			res.Batches = append(res.Batches, b)
		}
		res.Logs = append(res.Logs, out.Logs()...)
		finished := out.Finished()
		out.Reset()
		if finished {
			return res, nil
		}
	}
}

// GobRoundTrip encodes state and decodes it into a fresh value the way the HTTP
// transport carries it between turns (as an interface, so the concrete type
// must be gob-registered -- vgi.AsTableFunction does that). Fields gob drops
// come back zeroed.
func GobRoundTrip(state any) (any, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&state); err != nil {
		return nil, fmt.Errorf("gob-encode state %T: %w", state, err)
	}
	var out any
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		return nil, fmt.Errorf("gob-decode state %T: %w", state, err)
	}
	return out, nil
}
