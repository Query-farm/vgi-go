// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SumAllColumnsFunction computes column-wise sums across all batches.
type SumAllColumnsFunction struct{}

func (f *SumAllColumnsFunction) Name() string { return "sum_all_columns" }

func (f *SumAllColumnsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Computes column-wise sums across all batches",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"aggregation", "numeric"},
	}
}

func (f *SumAllColumnsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
		{Name: "logging", Position: -1, ArrowType: "boolean", Doc: "Whether to log during processing", HasDefault: true, DefaultValue: "false", IsConst: true},
	}
}

func (f *SumAllColumnsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
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

func (f *SumAllColumnsFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

type sumAllColumnsState struct {
	intSums   map[string]int64
	floatSums map[string]float64
	logging   bool
}

func (f *SumAllColumnsFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
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

	logging := false
	if !params.Args.IsNull("logging") {
		if v, err := params.Args.GetScalarBool("logging"); err == nil {
			logging = v
		}
	}

	return &sumAllColumnsState{
		intSums:   intSums,
		floatSums: floatSums,
		logging:   logging,
	}, nil
}

func (f *SumAllColumnsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	s := state.(*sumAllColumnsState)

	if s.logging {
		out.ClientLog(vgirpc.LogInfo, "Processing batch")
	}

	// Accumulate sums from this batch
	for _, field := range params.OutputSchema.Fields() {
		// Find the column in the input batch
		colIdx := -1
		for i := 0; i < int(batch.NumCols()); i++ {
			if batch.ColumnName(i) == field.Name {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			continue
		}
		col := batch.Column(colIdx)

		if _, ok := s.intSums[field.Name]; ok {
			s.intSums[field.Name] += sumInt64Column(col)
		}
		if _, ok := s.floatSums[field.Name]; ok {
			s.floatSums[field.Name] += sumFloat64Column(col)
		}
	}

	// Store accumulated sums for finalize
	if params.Storage != nil {
		sumBatch := buildSumBatch(params.OutputSchema, s.intSums, s.floatSums)
		data, err := vgi.SerializeRecordBatch(sumBatch)
		if err == nil {
			params.Storage.Put(data)
		}
	}

	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *SumAllColumnsFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
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
		for _, data := range params.Storage.Collect() {
			batch, err := vgi.DeserializeRecordBatch(data)
			if err != nil {
				continue
			}
			for _, field := range params.OutputSchema.Fields() {
				colIdx := -1
				for i := 0; i < int(batch.NumCols()); i++ {
					if batch.ColumnName(i) == field.Name {
						colIdx = i
						break
					}
				}
				if colIdx < 0 {
					continue
				}
				col := batch.Column(colIdx)
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
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())

	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			b := array.NewInt64Builder(mem)
			b.Append(intSums[field.Name])
			cols[i] = b.NewArray()
			b.Release()
		case arrow.FLOAT64:
			b := array.NewFloat64Builder(mem)
			b.Append(floatSums[field.Name])
			cols[i] = b.NewArray()
			b.Release()
		}
	}

	batch := array.NewRecordBatch(schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return batch
}
