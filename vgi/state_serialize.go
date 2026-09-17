// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// nullableStringWire is the gob-safe form of a nullable string list element.
// encoding/gob rejects nil elements in slices of pointers, which is the wire
// shape used by BindRequestWire.ArgumentNames for unnamed arguments.
type nullableStringWire struct {
	Value string
	Valid bool
}

// bindRequestGobWire keeps BindRequestWire's Arrow-facing representation out
// of HTTP state tokens. The plain request has ArgumentNames cleared so gob
// never encounters a nil slice element; the lossless nullable representation
// is carried alongside it.
type bindRequestGobWire struct {
	Request              bindRequestGobPlain
	ArgumentNamesPresent bool
	ArgumentNames        []nullableStringWire
}

type bindRequestGobPlain BindRequestWire

// GobEncode preserves nullable argument names when a bind request is embedded
// in an HTTP continuation token.
func (r BindRequestWire) GobEncode() ([]byte, error) {
	plain := bindRequestGobPlain(r)
	plain.ArgumentNames = nil
	w := bindRequestGobWire{Request: plain}
	if r.ArgumentNames != nil {
		w.ArgumentNamesPresent = true
		w.ArgumentNames = make([]nullableStringWire, len(*r.ArgumentNames))
		for i, name := range *r.ArgumentNames {
			if name != nil {
				w.ArgumentNames[i] = nullableStringWire{Value: *name, Valid: true}
			}
		}
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(w); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode restores the Arrow-facing nullable argument-name representation.
func (r *BindRequestWire) GobDecode(data []byte) error {
	var w bindRequestGobWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	*r = BindRequestWire(w.Request)
	if w.ArgumentNamesPresent {
		names := make([]*string, len(w.ArgumentNames))
		for i, name := range w.ArgumentNames {
			if name.Valid {
				value := name.Value
				names[i] = &value
			}
		}
		r.ArgumentNames = &names
	}
	return nil
}

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

// finalizeProducerWire is the gob wire form of FinalizeProducerState. Once the
// flush has been offloaded to the state log BatchIPC is empty, so the token is
// a fixed handful of bytes however large the flush was.
type finalizeProducerWire struct {
	Recipe    InitRecipe
	BatchIPC  [][]byte
	BatchIdx  int
	LogKey    []byte
	LogCursor int64
	CacheMeta map[string]string
}

// GobEncode offloads the remaining flush to the execution-scoped state log the
// first time a continuation token is written for this stream, then encodes the
// wire form. Doing it here rather than at init is what keeps the byte-stream
// transports free: they never write a token, so they never touch storage and
// keep the in-memory drain they have today. See FinalizeProducerState.
func (s *FinalizeProducerState) GobEncode() ([]byte, error) {
	s.offloadFlush()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(finalizeProducerWire{
		Recipe:    s.Recipe,
		BatchIPC:  s.BatchIPC,
		BatchIdx:  s.BatchIdx,
		LogKey:    s.LogKey,
		LogCursor: s.LogCursor,
		CacheMeta: s.CacheMeta,
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
	s.Recipe = w.Recipe
	s.BatchIPC = w.BatchIPC
	s.BatchIdx = w.BatchIdx
	s.LogKey = w.LogKey
	s.LogCursor = w.LogCursor
	s.CacheMeta = w.CacheMeta
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
