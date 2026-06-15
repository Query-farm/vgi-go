// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/gob"
	"fmt"
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
// does not need this — it keeps its position in an exported field (BatchIdx).

// tableProducerWire is the gob wire form of TableProducerState: the exported
// snapshot fields only (the transient fn/params/state are rebuilt by rehydrate).
type tableProducerWire struct {
	Recipe         InitRecipe
	UserStateBytes []byte
	AutoProjectIDs []int32
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
		Recipe:         s.Recipe,
		UserStateBytes: usb,
		AutoProjectIDs: s.AutoProjectIDs,
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
	s.Recipe = w.Recipe
	s.UserStateBytes = w.UserStateBytes
	s.AutoProjectIDs = w.AutoProjectIDs
	return nil
}

// tableInOutWire is the gob wire form of TableInOutExchangeState.
type tableInOutWire struct {
	Recipe         InitRecipe
	UserStateBytes []byte
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
		Recipe:         s.Recipe,
		UserStateBytes: usb,
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
	s.Recipe = w.Recipe
	s.UserStateBytes = w.UserStateBytes
	return nil
}
