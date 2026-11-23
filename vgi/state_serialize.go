// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Continuation-token state serialization.
//
// Over the HTTP transport a stream's state is round-tripped through an opaque
// state token: the framework gob-encodes the [ProducerState] / [ExchangeState]
// after each tick, the client returns it, and the worker gob-decodes it to
// resume. Subprocess / unix / shm transports keep the live state in memory
// across ticks, so they never serialize it.
//
// TableProducerState and TableInOutExchangeState hold the user's mutable state
// in a *transient* field (`state interface{}`) and carry its serialized form in
// the exported `UserStateBytes`. That blob is gob-encoded once at construction
// (the initial state) — but the live `state` is what Process mutates each tick.
// Without re-syncing, every continuation token would carry the *initial* state,
// so any producer that emits ≥ the server's producer batch limit (forcing a
// mid-stream continuation) would restart from row 0 on each resume and never
// terminate (an infinite loop over HTTP; subprocess transports are unaffected
// because the live state persists in memory).
//
// GobEncode below snapshots the live `state` into `UserStateBytes` at the exact
// moment the framework serializes the token, so resumes observe the current
// position. It runs only when a token is actually written (HTTP continuations),
// adding no per-tick cost to the in-memory transports. FinalizeProducerState
// keeps its position in an exported field, so it needs no state snapshot — but
// it uses the same "only when a token is written" hook for a different reason:
// to move its flush out of the token and into the state log the first time one
// is written. See FinalizeProducerState.GobEncode below.

// scalarExchangeWire is the gob wire form of ScalarExchangeState: the init
// StateRef only. A scalar stream has no per-turn state to carry.
type scalarExchangeWire struct {
	Init vgirpc.StateRef
}

// GobEncode encodes the wire form: Recipe rides as the Init StateRef, never as
// itself.
func (s *ScalarExchangeState) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(scalarExchangeWire{Init: s.Init}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode restores the wire form; the turn's entry resolves Init and
// rebuilds the transient fields (resume).
func (s *ScalarExchangeState) GobDecode(data []byte) error {
	var w scalarExchangeWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	s.Init = w.Init
	return nil
}

// tableProducerWire is the gob wire form of TableProducerState: the exported
// snapshot fields only (the transient fn/params/state are rebuilt by rehydrate).
//
// FilterDeltaIPC and FilterPredicateOrder are the stream's compacted
// dynamic-filter history (filter_delta_history.go). DuckDB sends each filter
// change once per stream, so a wire form without them rebuilt every later turn
// from the init snapshot alone and the worker stopped pruning after the turn
// that received the change.
type tableProducerWire struct {
	Init                 vgirpc.StateRef
	UserStateBytes       []byte
	AutoProjectIDs       []int32
	FilterDeltaIPC       [][]byte
	FilterPredicateOrder []string
}

// GobEncode snapshots the live user state into UserStateBytes, then encodes the
// wire form. See the file-level comment for why this is required.
func (s *TableProducerState) GobEncode() ([]byte, error) {
	usb := s.UserStateBytes
	if s.state != nil {
		b, err := gobEncode(s.state)
		if err != nil {
			return nil, fmt.Errorf("snapshot table producer state: %w", err)
		}
		usb = b
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(tableProducerWire{
		Init:                 s.Init,
		UserStateBytes:       usb,
		AutoProjectIDs:       s.AutoProjectIDs,
		FilterDeltaIPC:       s.FilterDeltaIPC,
		FilterPredicateOrder: s.FilterPredicateOrder,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode restores the exported wire fields; transient fields are rebuilt by
// rehydrateTableProducer.
func (s *TableProducerState) GobDecode(data []byte) error {
	var w tableProducerWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	s.Init = w.Init
	s.UserStateBytes = w.UserStateBytes
	s.AutoProjectIDs = w.AutoProjectIDs
	s.FilterDeltaIPC = w.FilterDeltaIPC
	s.FilterPredicateOrder = w.FilterPredicateOrder
	return nil
}

// finalizeProducerWire is the gob wire form of FinalizeProducerState. Once the
// flush has been offloaded to the state log BatchIPC is empty, so the token is
// a fixed handful of bytes however large the flush was.
type finalizeProducerWire struct {
	Init      vgirpc.StateRef
	BatchIPC  [][]byte
	BatchIdx  int
	LogKey    []byte
	LogCursor int64
	CacheMeta map[string]string
	LogPacked bool
	LogSub    int
}

// GobEncode offloads the remaining flush to the execution-scoped state log the
// first time a continuation token is written for this stream, then encodes the
// wire form. Doing it here rather than at init is what keeps the byte-stream
// transports free: they never write a token, so they never touch storage and
// keep the in-memory drain they have today. See FinalizeProducerState.
func (s *FinalizeProducerState) GobEncode() ([]byte, error) {
	s.offloadFlush()
	// Only the batches still to emit ride the token: an emitted batch is never
	// read again, and carrying it put the whole flush (a single wide batch,
	// emitted on the init turn) on the next continuation request.
	start := min(s.BatchIdx, len(s.BatchIPC))
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(finalizeProducerWire{
		Init:      s.Init,
		BatchIPC:  s.BatchIPC[start:],
		BatchIdx:  0,
		LogKey:    s.LogKey,
		LogCursor: s.LogCursor,
		CacheMeta: s.CacheMeta,
		LogPacked: s.LogPacked,
		LogSub:    s.LogSub,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode restores the exported wire fields; the storage handle and any
// inline batches are rebuilt by rehydrateFinalize.
func (s *FinalizeProducerState) GobDecode(data []byte) error {
	var w finalizeProducerWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	s.Init = w.Init
	s.BatchIPC = w.BatchIPC
	s.BatchIdx = w.BatchIdx
	s.LogKey = w.LogKey
	s.LogCursor = w.LogCursor
	s.CacheMeta = w.CacheMeta
	s.LogPacked = w.LogPacked
	s.LogSub = w.LogSub
	return nil
}

// tableInOutWire is the gob wire form of TableInOutExchangeState. It carries
// the dynamic-filter history for the reason tableProducerWire does, and
// MorePending so the next turn knows whether only a continuation tick is valid.
type tableInOutWire struct {
	Init                 vgirpc.StateRef
	UserStateBytes       []byte
	FilterDeltaIPC       [][]byte
	FilterPredicateOrder []string
	MorePending          bool
}

// GobEncode snapshots the live user state into UserStateBytes, then encodes the
// wire form. Each table-in-out exchange tick re-serializes the token, so the
// INPUT-phase state must reflect the latest Process mutation.
func (s *TableInOutExchangeState) GobEncode() ([]byte, error) {
	usb := s.UserStateBytes
	if s.state != nil {
		b, err := gobEncode(s.state)
		if err != nil {
			return nil, fmt.Errorf("snapshot table-in-out state: %w", err)
		}
		usb = b
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(tableInOutWire{
		Init:                 s.Init,
		UserStateBytes:       usb,
		FilterDeltaIPC:       s.FilterDeltaIPC,
		FilterPredicateOrder: s.FilterPredicateOrder,
		MorePending:          s.MorePending,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode restores the exported wire fields; transient fields are rebuilt by
// rehydrateTableInOut.
func (s *TableInOutExchangeState) GobDecode(data []byte) error {
	var w tableInOutWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	s.Init = w.Init
	s.UserStateBytes = w.UserStateBytes
	s.FilterDeltaIPC = w.FilterDeltaIPC
	s.FilterPredicateOrder = w.FilterPredicateOrder
	s.MorePending = w.MorePending
	return nil
}
