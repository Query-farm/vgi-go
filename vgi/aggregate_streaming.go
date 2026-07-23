// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

// StreamingAggregateFunction is the optional interface an aggregate may
// implement to participate in DuckDB's streaming-window optimizer rule.
// When eligible (cumulative frame, no EXCLUDE/DISTINCT/FILTER), DuckDB
// pipes input chunks to the worker and expects an output array of the same
// length as the input.
//
// Functions that don't implement this still work as windowed aggregates via
// the standard aggregate_window callbacks; the streaming path is purely an
// optimization.
type StreamingAggregateFunction interface {
	AggregateFunction
	// StreamingOpen prepares cross-partition state for the session. The
	// returned value is opaque to the framework — it's threaded back into
	// StreamingChunk and StreamingClose.
	StreamingOpen(params *AggregateProcessParams) (interface{}, error)
	// StreamingChunk processes one input chunk. Implementations must return
	// an arrow.Array of the same length as `chunk.NumRows()`. The first
	// `partitionKeyCount` columns of `chunk` are the partition keys; the
	// next `orderKeyCount` are order keys; remaining columns are the
	// function's value arguments.
	StreamingChunk(state interface{}, chunk arrow.RecordBatch, partitionKeyCount, orderKeyCount int, params *AggregateProcessParams) (arrow.Array, error)
	// StreamingClose drops the session. Always called once per execution_id.
	StreamingClose(state interface{}, params *AggregateProcessParams) error
}

// AggregateStreamingOpenRequestWire mirrors vgi-python's
// AggregateStreamingOpenRequest dataclass.
type AggregateStreamingOpenRequestWire struct {
	FunctionName      string  `vgirpc:"function_name"`
	Arguments         []byte  `vgirpc:"arguments"`
	InputSchema       []byte  `vgirpc:"input_schema"`
	PartitionKeyCount int64   `vgirpc:"partition_key_count"`
	OrderKeyCount     int64   `vgirpc:"order_key_count"`
	OutputSchema      []byte  `vgirpc:"output_schema"`
	Settings          *[]byte `vgirpc:"settings"`
	Secrets           *[]byte `vgirpc:"secrets"`
	AttachOpaqueData  *[]byte `vgirpc:"attach_opaque_data"`
	// SchemaName is the catalog schema that declares the function. A name is
	// unique only within a schema, so this is what lets the worker resolve
	// (schema, name) on a request that re-resolves by name; nil when the caller
	// names no schema. Protocol 1.2.0.
	SchemaName *string `vgirpc:"schema_name"`
}

// AggregateStreamingOpenResponseWire returns the execution_id keying the session.
type AggregateStreamingOpenResponseWire struct {
	ExecutionID []byte `vgirpc:"execution_id"`
}

// AggregateStreamingChunkRequestWire mirrors AggregateStreamingChunkRequest.
type AggregateStreamingChunkRequestWire struct {
	FunctionName     string  `vgirpc:"function_name"`
	ExecutionID      []byte  `vgirpc:"execution_id"`
	InputBatch       []byte  `vgirpc:"input_batch"`
	AttachOpaqueData *[]byte `vgirpc:"attach_opaque_data"`
	// SchemaName is the catalog schema that declares the function. A name is
	// unique only within a schema, so this is what lets the worker resolve
	// (schema, name) on a request that re-resolves by name; nil when the caller
	// names no schema. Protocol 1.2.0.
	SchemaName *string `vgirpc:"schema_name"`
}

// AggregateStreamingChunkResponseWire returns the per-row output batch.
type AggregateStreamingChunkResponseWire struct {
	ResultBatch []byte `vgirpc:"result_batch"`
}

// AggregateStreamingCloseRequestWire mirrors AggregateStreamingCloseRequest.
type AggregateStreamingCloseRequestWire struct {
	FunctionName     string  `vgirpc:"function_name"`
	ExecutionID      []byte  `vgirpc:"execution_id"`
	AttachOpaqueData *[]byte `vgirpc:"attach_opaque_data"`
	// SchemaName is the catalog schema that declares the function. A name is
	// unique only within a schema, so this is what lets the worker resolve
	// (schema, name) on a request that re-resolves by name; nil when the caller
	// names no schema. Protocol 1.2.0.
	SchemaName *string `vgirpc:"schema_name"`
}

// AggregateStreamingCloseResponseWire is the empty ack.
type AggregateStreamingCloseResponseWire struct{}

// streamingSession holds per-execution_id state for a streaming-aggregate
// invocation. The session is removed in handleAggregateStreamingClose.
type streamingSession struct {
	fn                StreamingAggregateFunction
	state             interface{}
	outputSchema      *arrow.Schema
	partitionKeyCount int
	orderKeyCount     int
	args              *Arguments
	settings          map[string]interface{}
	secrets           map[string]map[string]interface{}
	attachOpaqueData  []byte
}

type streamingSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*streamingSession
}

func (s *streamingSessionStore) put(execID []byte, sess *streamingSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]*streamingSession)
	}
	s.sessions[string(execID)] = sess
}

func (s *streamingSessionStore) get(execID []byte) *streamingSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[string(execID)]
}

func (s *streamingSessionStore) drop(execID []byte) *streamingSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.sessions[string(execID)]
	delete(s.sessions, string(execID))
	return sess
}

func (w *Worker) registerAggregateStreamingRPCs(s *vgirpc.Server) {
	vgirpc.Unary[AggregateStreamingOpenRequestWire, AggregateStreamingOpenResponseWire](s, "aggregate_streaming_open", w.handleAggregateStreamingOpen)
	vgirpc.Unary[AggregateStreamingChunkRequestWire, AggregateStreamingChunkResponseWire](s, "aggregate_streaming_chunk", w.handleAggregateStreamingChunk)
	vgirpc.Unary[AggregateStreamingCloseRequestWire, AggregateStreamingCloseResponseWire](s, "aggregate_streaming_close", w.handleAggregateStreamingClose)
}

func (w *Worker) handleAggregateStreamingOpen(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateStreamingOpenRequestWire) (AggregateStreamingOpenResponseWire, error) {
	fn, err := w.lookupAggregate(req.FunctionName, req.SchemaName, req.AttachOpaqueData, callCtx)
	if err != nil {
		return AggregateStreamingOpenResponseWire{}, err
	}
	sFn, ok := fn.(StreamingAggregateFunction)
	if !ok {
		return AggregateStreamingOpenResponseWire{}, &vgirpc.RpcError{
			Type:    "TypeError",
			Message: fmt.Sprintf("aggregate '%s' does not implement StreamingAggregateFunction", req.FunctionName),
		}
	}

	args, err := ParseArguments(req.Arguments)
	if err != nil {
		return AggregateStreamingOpenResponseWire{}, fmt.Errorf("aggregate_streaming_open: parse arguments: %w", err)
	}

	outputSchema, err := DeserializeSchema(req.OutputSchema)
	if err != nil {
		return AggregateStreamingOpenResponseWire{}, fmt.Errorf("aggregate_streaming_open: parse output_schema: %w", err)
	}

	var settings map[string]interface{}
	var secrets map[string]map[string]interface{}
	if req.Settings != nil && len(*req.Settings) > 0 {
		if b, err := DeserializeRecordBatch(*req.Settings); err == nil {
			defer b.Release()
			settings = BatchToSettingsMap(b)
		}
	}
	if req.Secrets != nil && len(*req.Secrets) > 0 {
		if b, err := DeserializeRecordBatch(*req.Secrets); err == nil {
			defer b.Release()
			secrets = BatchToSecretsMap(b)
		}
	}

	execID := uuid.New()
	executionID := execID[:]

	params := &AggregateProcessParams{
		Args:         args,
		OutputSchema: outputSchema,
		Settings:     settings,
		Secrets:      secrets,
		Auth:         callCtx.Auth,
	}
	if req.AttachOpaqueData != nil {
		params.AttachOpaqueData, _ = w.openAttach(*req.AttachOpaqueData, callCtx)
	}

	state, err := sFn.StreamingOpen(params)
	if err != nil {
		return AggregateStreamingOpenResponseWire{}, err
	}

	w.streamingSessions.put(executionID, &streamingSession{
		fn:                sFn,
		state:             state,
		outputSchema:      outputSchema,
		partitionKeyCount: int(req.PartitionKeyCount),
		orderKeyCount:     int(req.OrderKeyCount),
		args:              args,
		settings:          settings,
		secrets:           secrets,
		attachOpaqueData:  params.AttachOpaqueData,
	})
	return AggregateStreamingOpenResponseWire{ExecutionID: executionID}, nil
}

func (w *Worker) handleAggregateStreamingChunk(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateStreamingChunkRequestWire) (AggregateStreamingChunkResponseWire, error) {
	sess := w.streamingSessions.get(req.ExecutionID)
	if sess == nil {
		return AggregateStreamingChunkResponseWire{}, &vgirpc.RpcError{
			Type:    "OSError",
			Message: "aggregate_streaming_chunk: unknown execution_id (streaming_open never ran or close already fired)",
		}
	}

	chunk, err := DeserializeRecordBatch(req.InputBatch)
	if err != nil {
		return AggregateStreamingChunkResponseWire{}, fmt.Errorf("aggregate_streaming_chunk: parse input_batch: %w", err)
	}
	defer chunk.Release()

	params := &AggregateProcessParams{
		Args:             sess.args,
		OutputSchema:     sess.outputSchema,
		Settings:         sess.settings,
		Secrets:          sess.secrets,
		Auth:             callCtx.Auth,
		AttachOpaqueData: sess.attachOpaqueData,
	}

	resultArr, err := sess.fn.StreamingChunk(sess.state, chunk, sess.partitionKeyCount, sess.orderKeyCount, params)
	if err != nil {
		return AggregateStreamingChunkResponseWire{}, err
	}
	defer resultArr.Release()

	if int64(resultArr.Len()) != chunk.NumRows() {
		return AggregateStreamingChunkResponseWire{}, fmt.Errorf("streaming_chunk returned %d values for %d input rows", resultArr.Len(), chunk.NumRows())
	}

	resultBatch := array.NewRecordBatch(sess.outputSchema, []arrow.Array{resultArr}, int64(resultArr.Len()))
	defer resultBatch.Release()
	resultBytes, err := SerializeRecordBatch(resultBatch)
	if err != nil {
		return AggregateStreamingChunkResponseWire{}, err
	}
	return AggregateStreamingChunkResponseWire{ResultBatch: resultBytes}, nil
}

func (w *Worker) handleAggregateStreamingClose(ctx context.Context, callCtx *vgirpc.CallContext, req AggregateStreamingCloseRequestWire) (AggregateStreamingCloseResponseWire, error) {
	sess := w.streamingSessions.drop(req.ExecutionID)
	if sess == nil {
		return AggregateStreamingCloseResponseWire{}, nil
	}
	params := &AggregateProcessParams{
		Args:             sess.args,
		OutputSchema:     sess.outputSchema,
		Settings:         sess.settings,
		Secrets:          sess.secrets,
		Auth:             callCtx.Auth,
		AttachOpaqueData: sess.attachOpaqueData,
	}
	_ = sess.fn.StreamingClose(sess.state, params)
	return AggregateStreamingCloseResponseWire{}, nil
}

// PartitionKey is a helper to derive a stable hash key from the partition-key
// columns at row i. Use it from StreamingChunk implementations to look up
// per-partition state.
func PartitionKey(chunk arrow.RecordBatch, partitionKeyCount, i int) uint64 {
	if partitionKeyCount == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [8]byte
	for c := 0; c < partitionKeyCount; c++ {
		col := chunk.Column(c)
		if col.IsNull(i) {
			h.Write([]byte{0xff})
			continue
		}
		switch a := col.(type) {
		case *array.Int64:
			binary.LittleEndian.PutUint64(buf[:], uint64(a.Value(i)))
			h.Write(buf[:])
		case *array.Int32:
			binary.LittleEndian.PutUint32(buf[:4], uint32(a.Value(i)))
			h.Write(buf[:4])
		case *array.String:
			h.Write([]byte(a.Value(i)))
		case *array.Binary:
			h.Write(a.Value(i))
		default:
			// Fall back to a per-column dispatch via the stringer.
			h.Write([]byte(fmt.Sprintf("%v", a)))
		}
		h.Write([]byte{0x00})
	}
	return h.Sum64()
}

var _ = memory.NewGoAllocator
