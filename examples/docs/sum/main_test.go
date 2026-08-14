// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Tests for the aggregate documentation example.
//
// The four phases are driven directly here rather than through DuckDB: an
// aggregate has no client entry point, and the point of the shape is that
// Update/Combine/Finalize compose correctly however DuckDB chooses to
// parallelise them. Exercising them by hand is what proves that.
package main

import (
	"encoding/gob"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Aggregate state is gob-encoded between phases and nothing registers it for
// you — RegisterAggregate takes the interface directly, unlike the table
// adapters which call gob.Register themselves. An unregistered state compiles
// and attaches, then fails on the first GROUP BY.
func TestSumStateIsGobRegistered(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected gob.Register to panic on a duplicate — meaning init() " +
				"already registered *SumState. Without that registration the worker " +
				"fails at runtime with: gob: type not registered for interface")
		}
	}()
	// Registering the same concrete type under a second name panics, which is a
	// cheap way to assert the first registration happened.
	gob.RegisterName("probe.SumState", &SumState{})
}

func TestSumAccumulatesPerGroup(t *testing.T) {
	fn := &SumFn{}
	states := map[int64]interface{}{}

	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	b.AppendValues([]int64{10, 5, 1, 2, 3}, nil)
	col := b.NewArray()
	defer col.Release()

	gids := &vgi.Int64Slice{Data: []int64{0, 0, 1, 1, 1}}
	if err := fn.Update(states, gids, []arrow.Array{col}, nil); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := states[0].(*SumState).Total; got != 15 {
		t.Errorf("group 0 = %d, want 15", got)
	}
	if got := states[1].(*SumState).Total; got != 6 {
		t.Errorf("group 1 = %d, want 6", got)
	}
}

// A group whose every row is NULL must never be created, so SUM returns NULL
// rather than 0 — the SQL semantics people expect.
func TestSumSkipsNullsAndLeavesTheGroupAbsent(t *testing.T) {
	fn := &SumFn{}
	states := map[int64]interface{}{}

	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	b.AppendValues([]int64{0, 0}, []bool{false, false}) // both NULL
	col := b.NewArray()
	defer col.Release()

	if err := fn.Update(states, &vgi.Int64Slice{Data: []int64{7, 7}}, []arrow.Array{col}, nil); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, present := states[7]; present {
		t.Fatal("an all-NULL group must not be created, or SUM returns 0 instead of NULL")
	}

	out, err := fn.Finalize([]int64{7}, states, nil)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	defer out.Release()
	if !out.Column(0).IsNull(0) {
		t.Error("a group with no state must finalize to NULL")
	}
}

// Combine has to be associative and commutative: DuckDB decides the worker count
// and the merge order.
func TestSumCombineIsOrderIndependent(t *testing.T) {
	fn := &SumFn{}
	ab, _ := fn.Combine(&SumState{Total: 15}, &SumState{Total: 100}, nil)
	ba, _ := fn.Combine(&SumState{Total: 100}, &SumState{Total: 15}, nil)
	if ab.(*SumState).Total != 115 || ba.(*SumState).Total != 115 {
		t.Fatalf("combine is order-dependent: %d vs %d", ab.(*SumState).Total, ba.(*SumState).Total)
	}
}

func TestSumFinalizeEmitsOneRowPerGroupInOrder(t *testing.T) {
	fn := &SumFn{}
	states := map[int64]interface{}{0: &SumState{Total: 15}, 1: &SumState{Total: 6}}
	out, err := fn.Finalize([]int64{1, 0}, states, nil) // deliberately not sorted
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	defer out.Release()
	if out.NumRows() != 2 {
		t.Fatalf("row count = %d, want 2", out.NumRows())
	}
	got := out.Column(0).(*array.Int64)
	if got.Value(0) != 6 || got.Value(1) != 15 {
		t.Errorf("rows must follow the group order given: got %d, %d", got.Value(0), got.Value(1))
	}
}
