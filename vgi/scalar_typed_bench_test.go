// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// These benchmarks measure the PER-BATCH framework overhead on the typed-scalar
// data-plane path — specifically the reflection work in BindArgs (const-arg
// binding) and bindColumnArgs (column-arg binding) that the deferred
// "BindArgs reflection caching" optimization would remove or reduce.
//
// The question they answer: is that overhead worth caching? Compare the
// isolated bind cost against the full Process cost at a full 2048-row DuckDB
// vector vs a 1-row batch (correlated-LATERAL / heavily-filtered case), where
// per-batch overhead is not amortized.

// benchScalarArgs mirrors the shape of a real typed scalar (e.g. multiply):
// one const scalar arg + one polymorphic column arg.
type benchScalarArgs struct {
	Factor int64       `vgi:"pos=0,const=true,default=3,doc=constant multiplier"`
	Value  arrow.Array `vgi:"pos=1,const=false,bound=multipliable,doc=values to multiply"`
}

type benchScalarFn struct{}

func (benchScalarFn) Name() string               { return "bench_mul" }
func (benchScalarFn) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (benchScalarFn) OnBindTyped(_ *benchScalarArgs, _ *BindParams) (*BindResponse, error) {
	return BindResult(arrow.PrimitiveTypes.Int64)
}
func (benchScalarFn) ProcessTyped(_ context.Context, a *benchScalarArgs, p *ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	get := Int64Accessor(a.Value)
	f := a.Factor
	return MapColumn(p, batch, 0, array.NewInt64Builder, func(_ arrow.Array, i int) int64 {
		return get(i) * f
	})
}

// makeBenchArgs builds an *Arguments carrying a single positional_0 const int64
// (the factor), via the same IPC path the worker uses at runtime.
func makeBenchArgs(b *testing.B, factor int64) *Arguments {
	b.Helper()
	mem := memory.NewGoAllocator()
	fb := array.NewInt64Builder(mem)
	fb.Append(factor)
	factorArr := fb.NewArray()
	fb.Release()
	defer factorArr.Release()

	structType := arrow.StructOf(arrow.Field{Name: "positional_0", Type: arrow.PrimitiveTypes.Int64})
	structData := array.NewData(structType, 1, []*memory.Buffer{nil}, []arrow.ArrayData{factorArr.Data()}, 0, 0)
	defer structData.Release()
	structArr := array.NewStructData(structData)
	defer structArr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "args", Type: structType}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{structArr}, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(batch); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	args, err := ParseArguments(buf.Bytes())
	if err != nil {
		b.Fatal(err)
	}
	return args
}

// makeBenchValueBatch builds an n-row single-column int64 input batch.
func makeBenchValueBatch(n int) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	vb := array.NewInt64Builder(mem)
	vb.Reserve(n)
	for i := 0; i < n; i++ {
		vb.Append(int64(i))
	}
	valArr := vb.NewArray()
	vb.Release()
	defer valArr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "positional_1", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{valArr}, int64(n))
}

func benchmarkTypedScalarProcess(b *testing.B, rows int) {
	fn := AsScalarFunction[benchScalarArgs](benchScalarFn{})
	args := makeBenchArgs(b, 3)
	defer args.Release()
	batch := makeBenchValueBatch(rows)
	defer batch.Release()
	params := &ProcessParams{
		Args:         args,
		OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int64}}, nil),
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := fn.Process(ctx, params, batch)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}

// Full Process = BindArgs + bindColumnArgs + the actual multiply work.
func BenchmarkTypedScalarProcess_2048(b *testing.B) { benchmarkTypedScalarProcess(b, 2048) }
func BenchmarkTypedScalarProcess_1(b *testing.B)    { benchmarkTypedScalarProcess(b, 1) }

// BindArgs in isolation — the const-arg reflection binding that is invariant
// across every batch of an exchange and is therefore fully cacheable.
func BenchmarkBindArgsOnly(b *testing.B) {
	args := makeBenchArgs(b, 3)
	defer args.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var a benchScalarArgs
		if err := BindArgs(args, &a); err != nil {
			b.Fatal(err)
		}
	}
}

// bindColumnArgs in isolation — the per-batch column-arg reflection Set
// (reflect.ValueOf(column) boxing) that runs on every batch.
func BenchmarkBindColumnArgsOnly(b *testing.B) {
	adapter := AsScalarFunction[benchScalarArgs](benchScalarFn{}).(*typedScalarAdapter[benchScalarArgs])
	batch := makeBenchValueBatch(2048)
	defer batch.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var a benchScalarArgs
		adapter.bindColumnArgs(&a, batch)
	}
}
