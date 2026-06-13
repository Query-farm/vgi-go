// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	debugLogOnce sync.Once
	debugLogF    *os.File
	debugLogMu   sync.Mutex
)

func debugLog(format string, args ...interface{}) {
	debugLogOnce.Do(func() {
		path := os.Getenv("VGI_GO_AGG_DEBUG_LOG")
		if path == "" {
			return
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return
		}
		debugLogF = f
	})
	if debugLogF == nil {
		return
	}
	debugLogMu.Lock()
	defer debugLogMu.Unlock()
	fmt.Fprintf(debugLogF, "[pid=%d] "+format+"\n", append([]interface{}{os.Getpid()}, args...)...)
}

// ============================================================================
// Window partition payload encode/decode (gob; never leaves the worker).
// ============================================================================

type windowPartitionPayload struct {
	PartitionBatch []byte
	OutputSchema   []byte
	FilterMask     []byte
	FrameStats     []byte
	AllValid       []byte
	RowCount       int64
	WindowState    []byte // optional gob-encoded WindowInit return value
}

func encodeWindowPartitionPayload(req AggregateWindowInitRequestWire, windowState []byte) []byte {
	p := windowPartitionPayload{
		PartitionBatch: req.PartitionBatch,
		OutputSchema:   req.OutputSchema,
		FilterMask:     req.FilterMask,
		FrameStats:     req.FrameStats,
		AllValid:       req.AllValid,
		RowCount:       req.RowCount,
		WindowState:    windowState,
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&p); err != nil {
		// Encoding plain bytes can't realistically fail.
		panic(fmt.Sprintf("encodeWindowPartitionPayload: %v", err))
	}
	return buf.Bytes()
}

func decodeWindowPartitionPayload(data []byte) (windowPartitionPayload, error) {
	var p windowPartitionPayload
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("decodeWindowPartitionPayload: %w", err)
	}
	return p, nil
}

func unpackWindowPartition(req AggregateWindowInitRequestWire) (*WindowPartition, error) {
	batch, err := DeserializeRecordBatch(req.PartitionBatch)
	if err != nil {
		return nil, fmt.Errorf("partition_batch: %w", err)
	}
	outSchema, err := DeserializeSchema(req.OutputSchema)
	if err != nil {
		batch.Release()
		return nil, fmt.Errorf("output_schema: %w", err)
	}
	mask := unpackBoolMask(req.FilterMask, req.RowCount)
	frameStats := unpackFrameStats(req.FrameStats)
	allValid := unpackAllValid(req.AllValid, int(batch.NumCols()))
	return &WindowPartition{
		Inputs:       batch,
		RowCount:     req.RowCount,
		FilterMask:   mask,
		FrameStats:   frameStats,
		AllValid:     allValid,
		OutputSchema: outSchema,
	}, nil
}

// loadCachedPartition rebuilds a WindowPartition from the gob-encoded payload
// stored at WindowInit time, plus the optional gob-encoded WindowInit state.
func (w *Worker) loadCachedPartition(funcName string, execID []byte, partitionID int64, shardKey string) (*WindowPartition, interface{}, error) {
	bucket := w.aggStorage.bucket(funcName, execID, shardKey)
	data, err := bucket.getWindowPartition(partitionID)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, fmt.Errorf("aggregate_window: unknown partition_id=%d (window_init never ran or destructor already fired)", partitionID)
	}
	payload, err := decodeWindowPartitionPayload(data)
	if err != nil {
		return nil, nil, err
	}
	batch, err := DeserializeRecordBatch(payload.PartitionBatch)
	if err != nil {
		return nil, nil, fmt.Errorf("partition_batch: %w", err)
	}
	outSchema, err := DeserializeSchema(payload.OutputSchema)
	if err != nil {
		batch.Release()
		return nil, nil, fmt.Errorf("output_schema: %w", err)
	}
	partition := &WindowPartition{
		Inputs:       batch,
		RowCount:     payload.RowCount,
		FilterMask:   unpackBoolMask(payload.FilterMask, payload.RowCount),
		FrameStats:   unpackFrameStats(payload.FrameStats),
		AllValid:     unpackAllValid(payload.AllValid, int(batch.NumCols())),
		OutputSchema: outSchema,
	}
	var ws interface{}
	if len(payload.WindowState) > 0 {
		ws, err = gobDecodeState(payload.WindowState)
		if err != nil {
			return nil, nil, err
		}
	}
	return partition, ws, nil
}

// unpackBoolMask converts the C++ extension's packed-bit boolean encoding into
// a flat []bool of length rowCount.
func unpackBoolMask(packed []byte, rowCount int64) []bool {
	if len(packed) == 0 {
		return nil
	}
	out := make([]bool, rowCount)
	for i := int64(0); i < rowCount; i++ {
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		if int(byteIdx) < len(packed) {
			out[i] = (packed[byteIdx]>>bitIdx)&1 == 1
		}
	}
	return out
}

// unpackFrameStats reads ((begin_delta, end_delta), (begin_delta, end_delta))
// from 4×int64 little-endian bytes.
func unpackFrameStats(data []byte) [2][2]int64 {
	var s [2][2]int64
	if len(data) < 32 {
		return s
	}
	s[0][0] = int64(binary.LittleEndian.Uint64(data[0:8]))
	s[0][1] = int64(binary.LittleEndian.Uint64(data[8:16]))
	s[1][0] = int64(binary.LittleEndian.Uint64(data[16:24]))
	s[1][1] = int64(binary.LittleEndian.Uint64(data[24:32]))
	return s
}

// unpackAllValid reads numCols bytes — one per input column — into a []bool.
func unpackAllValid(data []byte, numCols int) []bool {
	out := make([]bool, numCols)
	for i := 0; i < numCols && i < len(data); i++ {
		out[i] = data[i] != 0
	}
	return out
}

// ============================================================================
// Result batch builders for aggregate_window / window_batch.
// ============================================================================

// buildScalarResultBatch wraps a single scalar value in a one-row RecordBatch
// matching outputSchema (which must have exactly one field).
func buildScalarResultBatch(value interface{}, outputSchema *arrow.Schema) (arrow.RecordBatch, error) {
	if outputSchema.NumFields() != 1 {
		return nil, fmt.Errorf("buildScalarResultBatch: output schema must have exactly 1 field, got %d", outputSchema.NumFields())
	}
	mem := memory.NewGoAllocator()
	field := outputSchema.Field(0)
	col, err := buildSingleScalarArray(mem, field.Type, value)
	if err != nil {
		return nil, err
	}
	defer col.Release()
	return array.NewRecordBatch(outputSchema, []arrow.Array{col}, 1), nil
}

// buildBatchResult builds a count-row RecordBatch from a slice of scalar values.
func buildBatchResult(values []interface{}, outputSchema *arrow.Schema) (arrow.RecordBatch, error) {
	if outputSchema.NumFields() != 1 {
		return nil, fmt.Errorf("buildBatchResult: output schema must have exactly 1 field, got %d", outputSchema.NumFields())
	}
	mem := memory.NewGoAllocator()
	field := outputSchema.Field(0)
	col, err := buildScalarColumn(mem, field.Type, values)
	if err != nil {
		return nil, err
	}
	defer col.Release()
	return array.NewRecordBatch(outputSchema, []arrow.Array{col}, int64(len(values))), nil
}

func buildSingleScalarArray(mem memory.Allocator, dt arrow.DataType, v interface{}) (arrow.Array, error) {
	return buildScalarColumn(mem, dt, []interface{}{v})
}

// buildScalarColumn appends each value (or null) into a column of the given type.
func buildScalarColumn(mem memory.Allocator, dt arrow.DataType, values []interface{}) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, v := range values {
			switch x := v.(type) {
			case nil:
				b.AppendNull()
			case int:
				b.Append(int64(x))
			case int64:
				b.Append(x)
			case int32:
				b.Append(int64(x))
			default:
				return nil, fmt.Errorf("buildScalarColumn int64: unsupported value %T", v)
			}
		}
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, v := range values {
			switch x := v.(type) {
			case nil:
				b.AppendNull()
			case float64:
				b.Append(x)
			case float32:
				b.Append(float64(x))
			case int:
				b.Append(float64(x))
			case int64:
				b.Append(float64(x))
			default:
				return nil, fmt.Errorf("buildScalarColumn float64: unsupported value %T", v)
			}
		}
		return b.NewArray(), nil
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, v := range values {
			switch x := v.(type) {
			case nil:
				b.AppendNull()
			case string:
				b.Append(x)
			default:
				return nil, fmt.Errorf("buildScalarColumn string: unsupported value %T", v)
			}
		}
		return b.NewArray(), nil
	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, v := range values {
			switch x := v.(type) {
			case nil:
				b.AppendNull()
			case bool:
				b.Append(x)
			default:
				return nil, fmt.Errorf("buildScalarColumn bool: unsupported value %T", v)
			}
		}
		return b.NewArray(), nil
	case arrow.BINARY:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		for _, v := range values {
			switch x := v.(type) {
			case nil:
				b.AppendNull()
			case []byte:
				b.Append(x)
			case string:
				b.Append([]byte(x))
			default:
				return nil, fmt.Errorf("buildScalarColumn binary: unsupported value %T", v)
			}
		}
		return b.NewArray(), nil
	}
	return nil, fmt.Errorf("buildScalarColumn: unsupported type %s", dt)
}
