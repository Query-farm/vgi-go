// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Command rowcount is the buffering example for the vgi-go documentation.
//
// A buffering function is for the case where output depends on the WHOLE input —
// a global sort, a top-k, a full reduction. It runs in three phases:
//
//   - Process  (sink)   — called per input batch, in parallel across DuckDB
//     threads. Stash what you need and return a state id.
//   - Combine           — called once, on the coordinator, with every state id
//     the sink produced. Reduce them into the ids the source will drain.
//   - Finalize (source) — called per finalize id, streaming the result out.
//
// The phases can run in different worker processes, so nothing may live in
// memory between them. State goes in params.Storage, which is scoped to this
// execution and shared across the workers serving it.
//
//	go build -o rowcountworker .
//	# then, in a Haybarn shell:
//	ATTACH 'buffers' (TYPE vgi, LOCATION './rowcountworker');
//	SELECT * FROM buffers.row_count((SELECT * FROM big_table));
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"log"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var rowCountOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "count", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// countsKey names the append-log inside this execution's storage scope. It does
// not need to be unique across queries — params.Storage is already scoped by
// execution id, so two concurrent scans cannot see each other's entries.
var countsKey = []byte("row_counts")

// RowCountFn counts every input row and emits a single total.
type RowCountFn struct{}

var _ vgi.TableBufferingFunction = (*RowCountFn)(nil)

func (*RowCountFn) Name() string { return "row_count" }

func (*RowCountFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Counts every input row and returns one total",
		Stability:   vgi.StabilityConsistent,
	}
}

func (*RowCountFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Rows to count"},
	}
}

// OnBind resolves the output schema. It is one int64 column whatever the input
// looked like, so the schema is fixed rather than derived from the input.
func (*RowCountFn) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(rowCountOutputSchema)
}

// Process is the sink. It runs per batch and in parallel across DuckDB threads,
// so it appends rather than read-modify-writes: StateAppend is a log, and
// concurrent appends cannot lose each other the way a get-then-put would.
//
// Returning the execution id says "my partial lives in this execution's scope".
func (*RowCountFn) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(batch.NumRows()))
	if _, err := params.Storage.StateAppend(countsKey, buf[:]); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

// Combine runs once, after every sink call, on the coordinator. Returning a
// single id means Finalize is called once and produces one stream; returning
// several would fan the source phase out.
func (*RowCountFn) Combine(_ context.Context, params *vgi.ProcessParams, _ [][]byte) ([][]byte, error) {
	return [][]byte{params.ExecutionID}, nil
}

// Finalize is the source. It reads the reduced state back and emits the result.
func (*RowCountFn) Finalize(_ context.Context, params *vgi.ProcessParams, _ []byte) ([]arrow.RecordBatch, error) {
	// -1 / 0 means "from the beginning, no limit".
	entries, err := params.Storage.StateLogScan(countsKey, -1, 0)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, e := range entries {
		total += int64(binary.LittleEndian.Uint64(e.Value))
	}
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(total)
	arr := b.NewArray()
	defer arr.Release()
	return []arrow.RecordBatch{array.NewRecordBatch(rowCountOutputSchema, []arrow.Array{arr}, 1)}, nil
}

func main() {
	httpMode := flag.Bool("http", false, "serve over HTTP instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	w := vgi.NewWorker(
		vgi.WithCatalogName("buffers"),
		vgi.WithCatalogComment("Documentation example: a full-input reduction"),
	)
	w.RegisterTableBuffering(&RowCountFn{})

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}
