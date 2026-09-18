// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Every evaluator connection compares strings with binary collation: the
// setting is applied as each connection opens, not per evaluation, so this
// holds on connections the pool opened for concurrent evaluations too.
func TestEvaluatorComparesWithBinaryCollation(t *testing.T) {
	builder := array.NewStringBuilder(memory.NewGoAllocator())
	builder.AppendValues([]string{"B", "b", "a", "A"}, nil)
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	batch := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil), []arrow.Array{column}, 4)
	defer batch.Release()

	results := make(chan error, 8)
	for i := 0; i < cap(results); i++ {
		go func() {
			mask, err := evalExpressionAgainstBatch(context.Background(), batch, `"s" < 'a'`)
			if err != nil {
				results <- err
				return
			}
			defer mask.Release()
			got := mask.(*array.Boolean)
			// Binary: 'A' (0x41) and 'B' (0x42) sort before 'a' (0x61).
			for i, want := range []bool{true, false, false, true} {
				if got.Value(i) != want {
					results <- fmt.Errorf("row %d compared under a non-binary collation", i)
					return
				}
			}
			results <- nil
		}()
	}
	for i := 0; i < cap(results); i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

// The evaluator runs each query on one thread: it evaluates one small batch at
// a time, where intra-query parallelism costs more than it saves.
func TestEvaluatorIsSingleThreaded(t *testing.T) {
	db, err := ensureEvalDB()
	if err != nil {
		t.Fatal(err)
	}
	var threads int64
	if err := db.QueryRow("SELECT current_setting('threads')").Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if threads != 1 {
		t.Fatalf("the evaluator runs %d threads per query, want 1", threads)
	}
}
