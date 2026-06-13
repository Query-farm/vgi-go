// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// bufferEmitWideSchema is the fixed output schema: a single int64 column `n`.
var bufferEmitWideSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// BufferEmitWideFunction is a table-buffering function whose Source (finalize)
// phase emits ONE batch of `rows` rows. Unlike BufferInputFunction (which
// echoes input batches, each already capped at DuckDB's standard vector size),
// this emits a single, arbitrarily large output batch from finalize — a minimal
// repro for whether the buffering Source path supports output batches larger
// than the standard vector size (2048 rows). Mirrors vgi-python's
// BufferEmitWideFunction and backs
// test/sql/integration/table_in_out/table_buffering_large_batch.test.
type BufferEmitWideFunction struct{}

var _ vgi.TableBufferingFunction = (*BufferEmitWideFunction)(nil)

func (f *BufferEmitWideFunction) Name() string { return "buffer_emit_wide" }

func (f *BufferEmitWideFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emit a single finalize batch of N rows (vector-size repro)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"test", "buffer"},
	}
}

func (f *BufferEmitWideFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "rows", Position: 0, ArrowType: "int64", Doc: "Number of rows to emit in one finalize batch", IsConst: true},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "Input table (content ignored)"},
	}
}

func (f *BufferEmitWideFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(bufferEmitWideSchema)
}

func (f *BufferEmitWideFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	return params.ExecutionID, nil
}

func (f *BufferEmitWideFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	return [][]byte{params.ExecutionID}, nil
}

func (f *BufferEmitWideFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	rows, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	n := vgi.BuildInt64Array(rows, func(i int64) int64 { return i })
	return []arrow.RecordBatch{array.NewRecordBatch(bufferEmitWideSchema, []arrow.Array{n}, rows)}, nil
}

func NewBufferEmitWideFunction() vgi.TableBufferingFunction {
	return &BufferEmitWideFunction{}
}
