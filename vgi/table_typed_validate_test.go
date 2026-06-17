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
	// pointer-receiver GobEncoder is detected too.
	var _ gob.GobEncoder = (*encoderState)(nil)

	// A struct with fields but none exported must panic.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("badState: expected panic, got none")
			}
		}()
		validateGobState[badState]()
	}()
}

func TestAsTableFunctionRejectsUnexportedState(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("AsTableFunction with unexported-only state: expected panic, got none")
		}
	}()
	_ = AsTableFunction[badState](validateProbe[badState]{})
}
