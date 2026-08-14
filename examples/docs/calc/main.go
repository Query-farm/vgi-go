// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Command calc is the worker built across the vgi-go tutorial: one scalar
// function and one table function in a single catalog.
//
// The scalar `double` transforms a column in place. The table function `series`
// *generates* rows from an argument, so it is called in a FROM clause rather
// than an expression. One worker can serve any mix of shapes.
//
//	go build -o calc .
//	# then, in a Haybarn shell:
//	ATTACH 'calc' (TYPE vgi, LOCATION './calc');
//	SELECT calc.double(21);
//	SELECT * FROM calc.series(3);
package main

import (
	"context"
	"flag"
	"log"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ── scalar: double(n) ───────────────────────────────────────────────────────

type doubleArgs struct {
	N int64 `vgi:"pos=0,const=false,doc=Value to double"`
}

// DoubleFn doubles each value in its input column.
type DoubleFn struct{}

func (*DoubleFn) Name() string { return "double" }

func (*DoubleFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Doubles a BIGINT",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}

func (*DoubleFn) OnBindTyped(_ *doubleArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (*DoubleFn) ProcessTyped(_ context.Context, _ *doubleArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumn(params, batch, 0, array.NewInt64Builder,
		func(col arrow.Array, i int) int64 { return vgi.GetInt64Value(col, i) * 2 })
}

// NewDouble returns the registration-ready scalar function.
func NewDouble() vgi.ScalarFunction {
	return vgi.AsScalarFunction[doubleArgs](&DoubleFn{})
}

// ── table: series(count) ────────────────────────────────────────────────────

// seriesOutputSchema is fixed, so OnBind can return it without inspecting the
// call. A table function that shapes its output from its arguments would build
// the schema here instead.
var seriesOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type seriesArgs struct {
	Count int64 `vgi:"pos=0,ge=0,doc=How many numbers to generate"`
}

// seriesState is the per-scan cursor. Embedding BatchState gives the generator
// helpers the remaining/batch-size bookkeeping; anything else the function needs
// between Process calls goes alongside it.
type seriesState struct {
	vgi.BatchState
}

// SeriesFn generates the integers 0..count-1.
type SeriesFn struct{}

// Compile-time proof that the shape is satisfied. Cheap, and it turns a missing
// method into a build error rather than a registration-time surprise.
var _ vgi.TypedTableFunc[seriesState] = (*SeriesFn)(nil)

func (*SeriesFn) Name() string { return "series" }

func (*SeriesFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates the integers 0..count-1",
		Stability:   vgi.StabilityConsistent,
	}
}

func (*SeriesFn) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(seriesArgs{}) }

func (*SeriesFn) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(seriesOutputSchema)
}

// NewState runs once per scan, after bind. This is where arguments are read:
// they are fixed for the whole scan, so decoding them per batch would be waste.
func (*SeriesFn) NewState(params *vgi.ProcessParams) (*seriesState, error) {
	var args seriesArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &seriesState{BatchState: vgi.NewBatchState(args.Count, 1024)}, nil
}

// Process is called repeatedly until the state reports no rows remain — that is
// what makes a table function a *generator*. GenerateBatch handles the chunking
// and signals completion for you; the callback only fills `size` rows.
func (*SeriesFn) Process(_ context.Context, _ *vgi.ProcessParams, state *seriesState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + i }),
		}, nil
	})
}

// NewSeries returns the registration-ready table function.
func NewSeries() vgi.TableFunction {
	return vgi.AsTableFunction[seriesState](&SeriesFn{})
}

func main() {
	httpMode := flag.Bool("http", false, "serve over HTTP instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	w := vgi.NewWorker(
		vgi.WithCatalogName("calc"),
		vgi.WithCatalogComment("Tutorial worker: a scalar and a table function"),
	)
	w.RegisterScalar(NewDouble())
	w.RegisterTable(NewSeries())

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}
