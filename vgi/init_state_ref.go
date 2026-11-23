// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

// init: StateRef (protocol 3.4; vgi docs/large-values-3.4.md §6).
//
// A stream's init recipe (the bind call as received, output schema, filters,
// join keys, opaque data, scopes) never changes for the life of the stream.
// It rides the cursor as ONE vgi-rpc StateRef in each stream state's Init
// field, created while handling the init request (the only place vgi-rpc
// allows it) and copied, never re-created, by later turns. Small recipes are
// carried inline (compressed); a large one is stored once at init and costs a
// ~130-byte sealed tag per turn after that. With argument references the
// recipe holds the reference, not the argument, so it normally stays inline.
//
// On HTTP a continuation arrives with only the token. vgi-rpc's rehydrate hook
// has no call context, so rehydrateState only records the worker; the turn's
// entry (Produce / Exchange) resolves Init with the turn's context, resolves
// any argument reference, and rebuilds the transient fields. The byte-stream
// transports keep the live state in memory and never resolve per turn.

import (
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// initRef wraps a recipe as the stream's init StateRef.
func initRef(cc *vgirpc.CallContext, recipe *InitRecipe) (vgirpc.StateRef, error) {
	data, err := encodeInitRecipe(recipe)
	if err != nil {
		return vgirpc.StateRef{}, err
	}
	if cc == nil || cc.Method == "" {
		// A context no transport built (in-process callers driving handleInit
		// directly) cannot mint a ref; carry the recipe in the inline wire
		// form (STATE_REF_V1.md §4: 0x01 || codec 0x00 raw || value), which
		// resolves anywhere.
		return vgirpc.StateRefFromBytes(append([]byte{0x01, 0x00}, data...)), nil
	}
	return cc.StateRef(data)
}

// attachInitRef sets the Init StateRef of the stream state init returned,
// from the recipe that state carries (its final value: the init functions
// fill in the execution id and init opaque data). Other state types carry no
// recipe.
func attachInitRef(cc *vgirpc.CallContext, state interface{}) error {
	var recipe *InitRecipe
	var ref *vgirpc.StateRef
	switch s := state.(type) {
	case *ScalarExchangeState:
		recipe, ref = &s.Recipe, &s.Init
	case *TableProducerState:
		recipe, ref = &s.Recipe, &s.Init
	case *TableInOutExchangeState:
		recipe, ref = &s.Recipe, &s.Init
	case *FinalizeProducerState:
		recipe, ref = &s.Recipe, &s.Init
	default:
		return nil
	}
	r, err := initRef(cc, recipe)
	if err != nil {
		return fmt.Errorf("init state ref: %w", err)
	}
	*ref = r
	return nil
}

// resolveInitRecipe reads a stream's init recipe back from its StateRef on
// the turn's context.
func resolveInitRecipe(cc *vgirpc.CallContext, ref vgirpc.StateRef) (InitRecipe, error) {
	if ref.IsZero() {
		return InitRecipe{}, fmt.Errorf("stream state carries no init recipe")
	}
	if cc == nil {
		cc = &vgirpc.CallContext{}
	}
	data, err := cc.Resolve(ref)
	if err != nil {
		return InitRecipe{}, err
	}
	recipe, err := decodeInitRecipe(data)
	if err != nil {
		return InitRecipe{}, err
	}
	return *recipe, nil
}

// resume finishes an HTTP rehydration at the turn's entry; a no-op on every
// other turn.
func (s *ScalarExchangeState) resume(cc *vgirpc.CallContext) error {
	w := s.pending
	if w == nil {
		return nil
	}
	recipe, err := resolveInitRecipe(cc, s.Init)
	if err != nil {
		return err
	}
	s.Recipe = recipe
	if err := w.rehydrateScalar(s, cc); err != nil {
		return err
	}
	s.pending = nil
	return nil
}

// resume finishes an HTTP rehydration at the turn's entry.
func (s *TableProducerState) resume(cc *vgirpc.CallContext) error {
	w := s.pending
	if w == nil {
		return nil
	}
	recipe, err := resolveInitRecipe(cc, s.Init)
	if err != nil {
		return err
	}
	s.Recipe = recipe
	if err := w.rehydrateTableProducer(s, cc); err != nil {
		return err
	}
	s.pending = nil
	return nil
}

// resume finishes an HTTP rehydration at the turn's entry.
func (s *TableInOutExchangeState) resume(cc *vgirpc.CallContext) error {
	w := s.pending
	if w == nil {
		return nil
	}
	recipe, err := resolveInitRecipe(cc, s.Init)
	if err != nil {
		return err
	}
	s.Recipe = recipe
	if err := w.rehydrateTableInOut(s, cc); err != nil {
		return err
	}
	s.pending = nil
	return nil
}

// resume finishes an HTTP rehydration at the turn's entry.
func (s *FinalizeProducerState) resume(cc *vgirpc.CallContext) error {
	w := s.pending
	if w == nil {
		return nil
	}
	recipe, err := resolveInitRecipe(cc, s.Init)
	if err != nil {
		return err
	}
	s.Recipe = recipe
	if err := w.rehydrateFinalize(s); err != nil {
		return err
	}
	s.pending = nil
	return nil
}
