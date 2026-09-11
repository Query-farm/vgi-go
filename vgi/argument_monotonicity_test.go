// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func TestSerializeArgumentMonotonicity(t *testing.T) {
	args := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		{Name: "offset", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	data, err := SerializeFunctionInfo(&FunctionInfo{
		Name:         "shift",
		SchemaPath:   []string{"main"},
		FunctionType: FunctionTypeScalar,
		ArgSchema:    args,
		OutputSchema: arrow.NewSchema(nil, nil),
		ArgumentMonotonicity: []ArgumentMonotonicity{
			ArgumentMonotonicityStrictlyIncreasing,
			ArgumentMonotonicityNonDecreasing,
		},
	})
	if err != nil {
		t.Fatalf("SerializeFunctionInfo: %v", err)
	}

	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ipc.NewReader: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing FunctionInfo record")
	}
	index := reader.Schema().FieldIndices("argument_monotonicity")[0]
	list := reader.RecordBatch().Column(index).(*array.List)
	values := list.ListValues().(*array.String)
	if got := []string{values.Value(0), values.Value(1)}; got[0] != "STRICTLY_INCREASING" || got[1] != "NON_DECREASING" {
		t.Fatalf("argument_monotonicity = %v", got)
	}
}

func TestSerializeArgumentMonotonicityRejectsInvalidMetadata(t *testing.T) {
	args := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	base := FunctionInfo{Name: "bad", SchemaPath: []string{"main"}, ArgSchema: args, OutputSchema: arrow.NewSchema(nil, nil)}

	base.FunctionType = FunctionTypeTable
	base.ArgumentMonotonicity = []ArgumentMonotonicity{ArgumentMonotonicityUnknown}
	if _, err := SerializeFunctionInfo(&base); err == nil || !strings.Contains(err.Error(), "only valid for scalar") {
		t.Fatalf("non-scalar error = %v", err)
	}

	base.FunctionType = FunctionTypeScalar
	base.ArgumentMonotonicity = []ArgumentMonotonicity{}
	if _, err := SerializeFunctionInfo(&base); err == nil || !strings.Contains(err.Error(), "expected 1") {
		t.Fatalf("length error = %v", err)
	}

	base.ArgumentMonotonicity = []ArgumentMonotonicity{"BOGUS"}
	if _, err := SerializeFunctionInfo(&base); err == nil || !strings.Contains(err.Error(), "unknown value") {
		t.Fatalf("value error = %v", err)
	}
}
