// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

var partialKey = []byte("partial")

// sumColumnsArgSpecs is the shared argument specification for sum column functions.
var sumColumnsArgSpecs = []vgi.ArgSpec{
	{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	{Name: "logging", Position: -1, ArrowType: "boolean", Doc: "Whether to log during processing",
		HasDefault: true, DefaultValue: "false", IsConst: true},
}

// sumColumnsOnBind filters input schema to numeric columns,
// promoting integers to Int64 and floats to Float64.
func sumColumnsOnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil {
		return &vgi.BindResponse{
			OutputSchema: arrow.NewSchema(nil, nil),
		}, nil
	}

	var fields []arrow.Field
	for _, field := range params.InputSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
			arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
			fields = append(fields, arrow.Field{Name: field.Name, Type: arrow.PrimitiveTypes.Int64})
		case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64, arrow.DECIMAL128, arrow.DECIMAL256:
			fields = append(fields, arrow.Field{Name: field.Name, Type: arrow.PrimitiveTypes.Float64})
		}
	}

	if len(fields) == 0 {
		var summary []string
		for _, f := range params.InputSchema.Fields() {
			summary = append(summary, fmt.Sprintf("%s: %s", f.Name, f.Type))
		}
		return nil, fmt.Errorf("sum_all_columns requires at least one numeric (integer, floating-point, or decimal) input column, got [%s]", strings.Join(summary, ", "))
	}

	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema(fields, nil),
	}, nil
}

// SumAllColumnsFunction computes column-wise sums across all batches. It is a
// table-buffering function: Process stores per-batch partial sums, Combine
// reduces them to one row, Finalize emits it.
type SumAllColumnsFunction struct{}

var _ vgi.TableBufferingFunction = (*SumAllColumnsFunction)(nil)
var _ vgi.TableBufferingFunctionWithCardinality = (*SumAllColumnsFunction)(nil)

func (f *SumAllColumnsFunction) Name() string { return "sum_all_columns" }

func (f *SumAllColumnsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Computes column-wise sums across all batches",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"aggregation", "numeric"},
	}
}

func (f *SumAllColumnsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return sumColumnsArgSpecs
}

func (f *SumAllColumnsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return sumColumnsOnBind(params)
}

func (f *SumAllColumnsFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: 1, Max: 1}, nil
}

// zeroSums returns zero-initialized int/float sum maps for the output schema.
func zeroSums(schema *arrow.Schema) (map[string]int64, map[string]float64) {
	intSums := make(map[string]int64)
	floatSums := make(map[string]float64)
	for _, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			intSums[field.Name] = 0
		case arrow.FLOAT64:
			floatSums[field.Name] = 0
		}
	}
	return intSums, floatSums
}

// addBatchSums folds a batch's per-column sums into the running maps.
func addBatchSums(schema *arrow.Schema, batch arrow.RecordBatch, intSums map[string]int64, floatSums map[string]float64) {
	for _, field := range schema.Fields() {
		col := vgi.FindColumn(batch, field.Name)
		if col == nil {
			continue
		}
		if _, ok := intSums[field.Name]; ok {
			intSums[field.Name] += sumInt64Column(col)
		}
		if _, ok := floatSums[field.Name]; ok {
			floatSums[field.Name] += sumFloat64Column(col)
		}
	}
}

func (f *SumAllColumnsFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	if vgi.OptionalBool(params.Args, "logging", false) {
		params.ClientLog(vgirpc.LogInfo, fmt.Sprintf("Processing batch with %d rows", batch.NumRows()))
	}
	intSums, floatSums := zeroSums(params.OutputSchema)
	addBatchSums(params.OutputSchema, batch, intSums, floatSums)
	data, err := vgi.SerializeRecordBatch(buildSumBatch(params.OutputSchema, intSums, floatSums))
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(partialKey, data); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

func (f *SumAllColumnsFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	if vgi.OptionalBool(params.Args, "logging", false) {
		params.ClientLog(vgirpc.LogInfo, fmt.Sprintf("Combining %d state_ids", len(stateIDs)))
	}
	intSums, floatSums := zeroSums(params.OutputSchema)
	entries, err := params.Storage.StateLogScan(partialKey, -1, 0)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		batch, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		addBatchSums(params.OutputSchema, batch, intSums, floatSums)
		batch.Release()
	}
	// Always write one merged row (zeros on empty input).
	data, err := vgi.SerializeRecordBatch(buildSumBatch(params.OutputSchema, intSums, floatSums))
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(bufKey, data); err != nil {
		return nil, err
	}
	return [][]byte{params.ExecutionID}, nil
}

func (f *SumAllColumnsFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	return drainBufBatches(params)
}

func sumInt64Column(col arrow.Array) int64 {
	var sum int64
	n := col.Len()
	switch c := col.(type) {
	case *array.Int64:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += c.Value(i)
			}
		}
	case *array.Int32:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += int64(c.Value(i))
			}
		}
	case *array.Int16:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += int64(c.Value(i))
			}
		}
	case *array.Int8:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += int64(c.Value(i))
			}
		}
	case *array.Uint64:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += int64(c.Value(i))
			}
		}
	case *array.Uint32:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += int64(c.Value(i))
			}
		}
	}
	return sum
}

func sumFloat64Column(col arrow.Array) float64 {
	var sum float64
	n := col.Len()
	switch c := col.(type) {
	case *array.Float64:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += c.Value(i)
			}
		}
	case *array.Float32:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += float64(c.Value(i))
			}
		}
	case *array.Int64:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += float64(c.Value(i))
			}
		}
	case *array.Int32:
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += float64(c.Value(i))
			}
		}
	case *array.Decimal128:
		dt := c.DataType().(*arrow.Decimal128Type)
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += c.Value(i).ToFloat64(dt.Scale)
			}
		}
	case *array.Decimal256:
		dt := c.DataType().(*arrow.Decimal256Type)
		for i := 0; i < n; i++ {
			if !c.IsNull(i) {
				sum += c.Value(i).ToFloat64(dt.Scale)
			}
		}
	}
	return sum
}

func buildSumBatch(schema *arrow.Schema, intSums map[string]int64, floatSums map[string]float64) arrow.RecordBatch {
	colMap := make(map[string]arrow.Array)
	for _, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			colMap[field.Name] = vgi.BuildInt64Array(1, func(_ int64) int64 { return intSums[field.Name] })
		case arrow.FLOAT64:
			colMap[field.Name] = vgi.BuildFloat64Array(1, func(_ int64) float64 { return floatSums[field.Name] })
		}
	}
	batch, _ := vgi.BatchFromMap(schema, colMap, 1)
	return batch
}
