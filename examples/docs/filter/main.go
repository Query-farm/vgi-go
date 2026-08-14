// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Command filter is the table-in-out example for the vgi-go documentation.
//
// A table-in-out function consumes a relation and streams a transformed relation
// back, batch by batch. Unlike a scalar it may change the row count, and unlike a
// buffering function it never holds the whole input — each Process call emits
// what it can from the batch in hand, which is what keeps memory flat over an
// arbitrarily large scan.
//
//	go build -o filterworker ./examples/docs/filter
//	# then, in a Haybarn shell:
//	ATTACH 'filters' (TYPE vgi, LOCATION './filterworker');
//	SELECT * FROM filters.filter_positive((SELECT * FROM t));
package main

import (
	"context"
	"flag"
	"log"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var filterOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// FilterPositiveFn keeps only rows whose value is greater than zero.
type FilterPositiveFn struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*FilterPositiveFn)(nil)

func (*FilterPositiveFn) Name() string { return "filter_positive" }

func (*FilterPositiveFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Keeps only rows whose value is positive",
		Stability:   vgi.StabilityConsistent,
	}
}

// ArgumentSpecs declares the input relation. A TABLE argument is written as an
// explicit ArgSpec rather than a struct tag: the struct-tag path binds scalar
// values into fields, and a relation is not a value — it arrives as the stream
// of batches Process is called with.
func (*FilterPositiveFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Rows to filter"},
	}
}

func (*FilterPositiveFn) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(filterOutputSchema)
}

// NewState returns the per-scan state. This function is stateless across
// batches — each one is decided on its own — so the state type is empty.
func (*FilterPositiveFn) NewState(_ *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

// keepPositive is the predicate, kept separate from the plumbing so the rule can
// be read — and tested — on its own.
func keepPositive(col arrow.Array, rows int) []int64 {
	var kept []int64
	for row := 0; row < rows; row++ {
		if col.IsNull(row) {
			continue // a NULL is not positive
		}
		if v := vgi.GetInt64Value(col, row); v > 0 {
			kept = append(kept, v)
		}
	}
	return kept
}

// Process is called once per input batch. Emitting fewer rows than arrived is
// the whole point of the shape; emitting none is fine too.
func (*FilterPositiveFn) Process(_ context.Context, _ *vgi.ProcessParams, _ *struct{},
	batch arrow.RecordBatch, out *vgirpc.OutputCollector,
) error {
	kept := keepPositive(batch.Column(0), int(batch.NumRows()))

	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues(kept, nil)

	arr := b.NewArray()
	defer arr.Release()
	// A zero-row batch is legitimate — this batch simply had nothing to keep.
	return out.Emit(array.NewRecordBatch(filterOutputSchema, []arrow.Array{arr}, int64(arr.Len())))
}

// Finalize runs once after the last input batch. A streaming filter holds nothing
// back, so it has nothing to flush.
func (*FilterPositiveFn) Finalize(_ context.Context, _ *vgi.ProcessParams, _ *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewFilterPositive returns the registration-ready function.
func NewFilterPositive() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&FilterPositiveFn{})
}

func main() {
	httpMode := flag.Bool("http", false, "serve over HTTP instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	w := vgi.NewWorker(
		vgi.WithCatalogName("filters"),
		vgi.WithCatalogComment("Documentation example: a streaming row filter"),
	)
	w.RegisterTableInOut(NewFilterPositive())

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}
