// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Command calcscalar is the worker built in step 1 of the vgi-go tutorial: one
// scalar function, served over stdio, callable from DuckDB as calc.double().
//
// A scalar function is the simplest shape — one row in, one value out, with no
// state and no finalize phase. DuckDB hands the worker a whole Arrow column and
// expects a column of the same length back.
//
//	go build -o calcscalar ./examples/docs/calcscalar
//	# then, in a Haybarn shell:
//	ATTACH 'calc' (TYPE vgi, LOCATION './calcscalar');
//	SELECT calc.double(21);
package main

import (
	"context"
	"flag"
	"log"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// doubleArgs declares the function's arguments. The `vgi:"..."` tags are the
// whole signature: they drive both the ArgumentSpecs the catalog advertises and
// the runtime binding of values into this struct, so the SQL signature and the
// Go type can never drift apart.
type doubleArgs struct {
	// No type= needed: the Go field type infers the Arrow type (int64 here).
	// When you do override it, the tag takes ARROW names ("int64"), not SQL
	// names ("bigint") — an unrecognised name silently falls back to VARCHAR.
	N int64 `vgi:"pos=0,const=false,doc=Value to double"`
}

// DoubleFn doubles each value in its input column.
type DoubleFn struct{}

func (*DoubleFn) Name() string { return "double" }

func (*DoubleFn) Metadata() vgi.FunctionMetadata {
	fortyTwo := "42"
	return vgi.FunctionMetadata{
		Description: "Doubles a BIGINT",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT calc.double(21)", Description: "Doubles a literal", ExpectedOutput: &fortyTwo},
		},
	}
}

// OnBindTyped runs once per query, before any data moves. Returning the output
// type here is what lets DuckDB plan the query.
func (*DoubleFn) OnBindTyped(_ *doubleArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

// ProcessTyped runs per input batch. MapColumn walks column 0 and builds the
// output column, preserving nulls — the row count out must equal the row count
// in, which is the contract that makes this a *scalar* function.
func (*DoubleFn) ProcessTyped(_ context.Context, _ *doubleArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumn(params, batch, 0, array.NewInt64Builder,
		func(col arrow.Array, i int) int64 {
			return vgi.GetInt64Value(col, i) * 2
		})
}

// NewDouble returns the registration-ready function. Tests use this too, so the
// wiring under test is the wiring that ships.
func NewDouble() vgi.ScalarFunction {
	return vgi.AsScalarFunction[doubleArgs](&DoubleFn{})
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
		vgi.WithCatalogComment("Tutorial worker: a single scalar function"),
	)
	w.RegisterScalar(NewDouble())

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}
