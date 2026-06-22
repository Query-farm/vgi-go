// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"encoding/gob"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// goodState has an exported field and gob-encodes fine.
type goodState struct{ Done bool }

// badState has only unexported fields — gob would reject it at HTTP rehydration.
type badState struct{ done bool } //nolint:unused

// emptyState is a truly empty struct — gob encodes it fine, so it must NOT panic.
type emptyState struct{}

// encoderState has no exported fields but provides its own gob encoding.
type encoderState struct{ n int } //nolint:unused

func (e encoderState) GobEncode() ([]byte, error) { return nil, nil }
func (e *encoderState) GobDecode(_ []byte) error  { return nil }

// recordIface mimics an Arrow arrow.Record stashed in state: an interface value.
type recordIface interface{ NumRows() int64 }

// arrowState is the real-world footgun — an exported interface field (e.g. an
// arrow.Record) that the shallow exported-field check used to miss.
type arrowState struct { //nolint:unused
	Batch recordIface
	Done  bool
}

// chanState holds a chan, which gob cannot encode.
type chanState struct { //nolint:unused
	Ch   chan int
	Done bool
}

// nestedBadState reaches an un-encodable interface field one struct down.
type nestedBadState struct { //nolint:unused
	Inner struct{ Rec recordIface }
}

// richGoodState exercises slices/maps/nested structs of encodable types.
type richGoodState struct { //nolint:unused
	Names   []string
	Counts  map[string]int64
	Nested  goodState
	Pointer *goodState
}

type validateProbe[S any] struct{}

func (validateProbe[S]) Name() string               { return "validate_probe" }
func (validateProbe[S]) Metadata() FunctionMetadata { return DefaultMetadata() }
func (validateProbe[S]) ArgumentSpecs() []ArgSpec   { return nil }
func (validateProbe[S]) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(nil)
}
func (validateProbe[S]) NewState(*ProcessParams) (*S, error) { return new(S), nil }
func (validateProbe[S]) Process(context.Context, *ProcessParams, *S, *vgirpc.OutputCollector) error {
	return nil
}

func TestValidateGobState(t *testing.T) {
	// good / empty / custom-encoder states must NOT panic.
	mustNotPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("%s: unexpected panic: %v", name, r)
			}
		}()
		fn()
	}
	mustNotPanic("goodState", func() { validateGobState[goodState]() })
	mustNotPanic("emptyState", func() { validateGobState[emptyState]() })
	mustNotPanic("encoderState", func() { validateGobState[encoderState]() })
	mustNotPanic("richGoodState", func() { validateGobState[richGoodState]() })
	// pointer-receiver GobEncoder is detected too.
	var _ gob.GobEncoder = (*encoderState)(nil)

	// States gob cannot encode must panic at registration, not mid-query.
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("%s: expected panic, got none", name)
			}
		}()
		fn()
	}
	// A struct with fields but none exported.
	mustPanic("badState", func() { validateGobState[badState]() })
	// An exported Arrow-like interface field (the bug this guard now closes).
	mustPanic("arrowState", func() { validateGobState[arrowState]() })
	// A chan field.
	mustPanic("chanState", func() { validateGobState[chanState]() })
	// An un-encodable field reached through a nested struct.
	mustPanic("nestedBadState", func() { validateGobState[nestedBadState]() })
}

func TestAsTableFunctionRejectsUnexportedState(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("AsTableFunction with unexported-only state: expected panic, got none")
		}
	}()
	_ = AsTableFunction[badState](validateProbe[badState]{})
}
