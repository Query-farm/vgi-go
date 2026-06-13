// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// GroupColumnName is the reserved column name the C++ extension prepends to
// UPDATE batches to carry per-row group_id.
const GroupColumnName = "__vgi_group_id"

// AggregateBindParams holds the parameters passed to AggregateFunction.OnBind.
type AggregateBindParams struct {
	Args        *Arguments
	InputSchema *arrow.Schema
	Settings    map[string]interface{}
	Secrets     map[string]map[string]interface{}
	Auth        *vgirpc.AuthContext
}

// AggregateProcessParams is shared by Update/Combine/Finalize/Window callbacks.
type AggregateProcessParams struct {
	Args         *Arguments
	OutputSchema *arrow.Schema
	Settings     map[string]interface{}
	Secrets      map[string]map[string]interface{}
	Auth         *vgirpc.AuthContext
	// AttachOpaqueData is the catalog the function was invoked under (nil for ad-hoc calls).
	AttachOpaqueData []byte
}

// AggregateFunction is the interface every aggregate implements.
//
// State is held on the worker side keyed by group_id; the Go SDK
// gob-serializes it across RPCs for cross-call continuity. Implementations
// register a concrete state type via `NewState` (returning a fresh zero
// value) so the dispatcher can reflect on it.
type AggregateFunction interface {
	// Name is the SQL-visible function name.
	Name() string
	// Metadata describes function attributes (stability, null-handling, etc.).
	Metadata() FunctionMetadata
	// ArgumentSpecs declares the input columns and any const params.
	ArgumentSpecs() []ArgSpec
	// OnBind resolves the output schema. Must return a single-column schema.
	OnBind(params *AggregateBindParams) (*BindResponse, error)
	// NewState returns a fresh state pointer for a new group. The pointer
	// type is what the dispatcher uses for gob register/serialize.
	NewState(params *AggregateProcessParams) interface{}
	// Update accumulates rows into per-group state. The states map is
	// pre-populated with existing states, but groups that aren't yet in
	// the map MUST be created on demand by the function — call NewState
	// only when the row genuinely contributes (e.g. non-null value with
	// NullHandlingDefault). Groups never inserted into states stay absent
	// from storage so finalize() returns NULL.
	Update(states map[int64]interface{}, groupIDs *Int64Slice, columns []arrow.Array, params *AggregateProcessParams) error
	// Combine merges source into target, returning the new target state.
	Combine(source, target interface{}, params *AggregateProcessParams) (interface{}, error)
	// Finalize produces one result row per group_id. groupIDs entries map
	// to states[gid] which may be nil if the group was never updated
	// (NULL-only inputs with NullHandlingDefault).
	Finalize(groupIDs []int64, states map[int64]interface{}, params *AggregateProcessParams) (arrow.RecordBatch, error)
}

// AggregateWindowFunction is implemented by aggregates that also support
// SQL OVER windowing. Functions opt in by also setting Metadata.SupportsWindow.
type AggregateWindowFunction interface {
	AggregateFunction
	// WindowInit can derive optional per-partition state. Returns nil if
	// no derived state is needed; otherwise the value is gob-serialized
	// and passed back to Window.
	WindowInit(partition *WindowPartition, params *AggregateProcessParams) (interface{}, error)
	// Window computes the aggregate value for one output row.
	// Subframes are usually a single (begin,end) pair; up to 3 for
	// EXCLUDE TIES/GROUP. Returns a Go scalar matching the output schema.
	Window(rid int64, subframes [][2]int64, partition *WindowPartition, windowState interface{}, params *AggregateProcessParams) (interface{}, error)
}

// WindowPartition is the partition data available during windowed evaluation.
type WindowPartition struct {
	// Inputs is the partition's input columns (excludes reserved group_id).
	Inputs arrow.RecordBatch
	// RowCount is the number of rows in the partition.
	RowCount int64
	// FilterMask is a per-row boolean from FILTER (WHERE ...); nil when absent.
	FilterMask []bool
	// FrameStats is the optimizer's per-partition frame statistics:
	// ((begin_delta, end_delta), (begin_delta, end_delta)).
	FrameStats [2][2]int64
	// AllValid[i] is true if input column i has no nulls.
	AllValid []bool
	// OutputSchema is the function's resolved output schema.
	OutputSchema *arrow.Schema
}

// Int64Slice wraps an int64 slice to ease passing to Update without
// importing arrow types in user code. The underlying array is borrowed.
type Int64Slice struct {
	Data []int64
}

// Len returns the number of entries.
func (s *Int64Slice) Len() int { return len(s.Data) }

// At returns the i-th entry.
func (s *Int64Slice) At(i int) int64 { return s.Data[i] }

// EnsureState is a helper for Update implementations: returns the existing
// state for gid, or creates one via newFn() and registers it. Functions
// should call this only when the row genuinely contributes (e.g. non-null
// with NullHandlingDefault) — groups never inserted stay absent from
// storage so finalize() returns NULL.
func EnsureState[T any](states map[int64]interface{}, gid int64, newFn func() *T) *T {
	if s, ok := states[gid]; ok {
		return s.(*T)
	}
	s := newFn()
	states[gid] = s
	return s
}

// gobEncodeState serializes a state value (passed by interface, typically a
// pointer to a registered struct) to bytes.
func gobEncodeState(state interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&state); err != nil {
		return nil, fmt.Errorf("encoding aggregate state: %w", err)
	}
	return buf.Bytes(), nil
}

// gobDecodeState deserializes bytes into a state value via gob's interface
// machinery. The concrete type must have been previously registered with
// gob.Register.
func gobDecodeState(data []byte) (interface{}, error) {
	var state interface{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&state); err != nil {
		return nil, fmt.Errorf("decoding aggregate state: %w", err)
	}
	return state, nil
}
