// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

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
		case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
			fields = append(fields, arrow.Field{Name: field.Name, Type: arrow.PrimitiveTypes.Float64})
		}
	}

	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema(fields, nil),
	}, nil
}

// SumAllColumnsFunction computes column-wise sums across all batches.
type SumAllColumnsFunction struct{}

var _ vgi.TypedTableInOutFunc[sumAllColumnsState] = (*SumAllColumnsFunction)(nil)

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

type sumAllColumnsState struct {
	IntSums   map[string]int64
	FloatSums map[string]float64
	Logging   bool
}

func (f *SumAllColumnsFunction) NewState(params *vgi.ProcessParams) (*sumAllColumnsState, error) {
	intSums := make(map[string]int64)
	floatSums := make(map[string]float64)

	for _, field := range params.OutputSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			intSums[field.Name] = 0
		case arrow.FLOAT64:
			floatSums[field.Name] = 0
		}
	}

	return &sumAllColumnsState{
		IntSums:   intSums,
		FloatSums: floatSums,
		Logging:   vgi.OptionalBool(params.Args, "logging", false),
	}, nil
}

func (f *SumAllColumnsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *sumAllColumnsState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	if state.Logging {
		out.ClientLog(vgirpc.LogInfo, fmt.Sprintf("Processing batch with %d rows", batch.NumRows()))
	}

	// Accumulate sums from this batch
	for _, field := range params.OutputSchema.Fields() {
		col := vgi.FindColumn(batch, field.Name)
		if col == nil {
			continue
		}

		if _, ok := state.IntSums[field.Name]; ok {
			state.IntSums[field.Name] += sumInt64Column(col)
		}
		if _, ok := state.FloatSums[field.Name]; ok {
			state.FloatSums[field.Name] += sumFloat64Column(col)
		}
	}

	// Store accumulated sums for finalize
	if params.Storage != nil {
		sumBatch := buildSumBatch(params.OutputSchema, state.IntSums, state.FloatSums)
		data, err := vgi.SerializeRecordBatch(sumBatch)
		if err != nil {
			return err
		}
		if err := params.Storage.Put(data); err != nil {
			return err
		}
	}

	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *SumAllColumnsFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *sumAllColumnsState) ([]arrow.RecordBatch, error) {
	intSums := make(map[string]int64)
	floatSums := make(map[string]float64)

	// Initialize to zero
	for _, field := range params.OutputSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			intSums[field.Name] = 0
		case arrow.FLOAT64:
			floatSums[field.Name] = 0
		}
	}

	// Collect from storage
	if params.Storage != nil {
		workerData, err := params.Storage.Collect()
		if err != nil {
			return nil, err
		}
		for _, data := range workerData {
			batch, err := vgi.DeserializeRecordBatch(data)
			if err != nil {
				continue
			}
			for _, field := range params.OutputSchema.Fields() {
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
			batch.Release()
		}
	}

	return []arrow.RecordBatch{buildSumBatch(params.OutputSchema, intSums, floatSums)}, nil
}

// NewSumAllColumnsFunction creates a SumAllColumnsFunction wrapped for registration.
func NewSumAllColumnsFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[sumAllColumnsState](&SumAllColumnsFunction{})
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
