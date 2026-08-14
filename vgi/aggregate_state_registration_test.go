// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

type probeState struct{ Total int64 }

type probeAggregate struct{ AggregateFunction }

func (probeAggregate) Name() string                                 { return "probe_agg" }
func (probeAggregate) NewState(*AggregateProcessParams) interface{} { return &probeState{} }
func (probeAggregate) Metadata() FunctionMetadata                   { return FunctionMetadata{} }
func (probeAggregate) ArgumentSpecs() []ArgSpec                     { return nil }
func (probeAggregate) OnBind(*AggregateBindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema(nil, nil))
}

// Per-group state is gob-encoded between phases, which may run in different
// processes. Registering it is not the author's job: the typed adapters already
// do it for table and table-in-out state, and an unregistered aggregate state
// used to compile, attach, and only fail on the first GROUP BY.
func TestRegisterAggregateRegistersItsStateType(t *testing.T) {
	NewWorker().RegisterAggregate(probeAggregate{})

	var buf bytes.Buffer
	var iface interface{} = &probeState{Total: 42}
	if err := gob.NewEncoder(&buf).Encode(&iface); err != nil {
		t.Fatalf("state should be gob-encodable through an interface after registration: %v", err)
	}

	var out interface{}
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := out.(*probeState)
	if !ok {
		t.Fatalf("decoded %T, want *probeState", out)
	}
	if got.Total != 42 {
		t.Errorf("Total = %d, want 42", got.Total)
	}
}

// A NewState that dereferences its params must not take the worker down at
// registration — we skip registration and leave it to the author.
type panickyAggregate struct{ probeAggregate }

func (panickyAggregate) Name() string { return "panicky_agg" }
func (panickyAggregate) NewState(p *AggregateProcessParams) interface{} {
	_ = p.AttachOpaqueData // nil deref when called speculatively
	return &probeState{}
}

func TestRegisterAggregateSurvivesAPanickyNewState(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("registration must not propagate a panic from NewState: %v", r)
		}
	}()
	NewWorker().RegisterAggregate(panickyAggregate{})
}
