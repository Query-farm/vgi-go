// Copyright 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// The recipe carries the bind call as the bytes the client sent, so a null
// argument name (which the generated []string cannot hold) and every other
// field survive the stream untouched, and the decoded copy reads it back.
func TestInitRecipeCarriesBindCallBytes(t *testing.T) {
	names := []string{"left", "", "right"}
	bind := testNested(t, BindRequestWire{
		FunctionName:  "probe",
		FunctionType:  string(FunctionTypeScalar),
		Arguments:     testNoArgs(t),
		ArgumentNames: &names,
	})
	want := InitRecipe{BindCallIPC: bind.Bytes(), FunctionName: "probe"}
	data, err := encodeInitRecipe(&want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeInitRecipe(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("round trip = %#v, want %#v", *got, want)
	}
	call, err := got.bindCall()
	if err != nil {
		t.Fatal(err)
	}
	if call.FunctionName != "probe" || call.ArgumentNames == nil || !reflect.DeepEqual(*call.ArgumentNames, names) {
		t.Fatalf("decoded bind call = %+v", call)
	}
	if !bytes.Equal(got.BindCallIPC, bind.Bytes()) {
		t.Fatal("recipe bind_call bytes changed")
	}
}

// A state token carries the init recipe only as the Init StateRef: the
// Recipe field never rides the token.
func TestStateTokensCarryInitRefNotRecipe(t *testing.T) {
	recipe := InitRecipe{FunctionName: "probe", OutputSchemaIPC: bytes.Repeat([]byte("s"), 64)}
	ref, err := initRef(nil, &recipe)
	if err != nil {
		t.Fatal(err)
	}
	type token interface {
		GobEncode() ([]byte, error)
		GobDecode([]byte) error
	}
	pairs := [][2]token{
		{&ScalarExchangeState{Init: ref, Recipe: recipe}, &ScalarExchangeState{}},
		{&TableProducerState{Init: ref, Recipe: recipe}, &TableProducerState{}},
		{&TableInOutExchangeState{Init: ref, Recipe: recipe}, &TableInOutExchangeState{}},
		{&FinalizeProducerState{Init: ref, Recipe: recipe}, &FinalizeProducerState{}},
	}
	for _, pair := range pairs {
		data, err := pair[0].GobEncode()
		if err != nil {
			t.Fatal(err)
		}
		if err := pair[1].GobDecode(data); err != nil {
			t.Fatal(err)
		}
		var got InitRecipe
		var init vgirpc.StateRef
		switch d := pair[1].(type) {
		case *ScalarExchangeState:
			got, init = d.Recipe, d.Init
		case *TableProducerState:
			got, init = d.Recipe, d.Init
		case *TableInOutExchangeState:
			got, init = d.Recipe, d.Init
		case *FinalizeProducerState:
			got, init = d.Recipe, d.Init
		}
		if !reflect.DeepEqual(got, InitRecipe{}) || init.IsZero() {
			t.Fatalf("%T token: Recipe %#v rode the token or Init is missing", pair[0], got)
		}
	}
	var s ScalarExchangeState
	data, _ := (&ScalarExchangeState{Init: ref, Recipe: recipe}).GobEncode()
	if err := s.GobDecode(data); err != nil {
		t.Fatal(err)
	}
	got, err := resolveInitRecipe(nil, s.Init)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, recipe) {
		t.Fatalf("resolved recipe = %#v, want %#v", got, recipe)
	}
}

// A table stream's continuation token carries its dynamic-filter history. The
// wire forms used to leave it out, so an HTTP turn rebuilt the stream's filters
// from the init snapshot alone.
func TestTableStateTokensCarryFilterHistory(t *testing.T) {
	deltas := [][]byte{[]byte("delta-a"), []byte("delta-b")}
	order := []string{"top_n:1", "top_n:0"}

	producer := &TableProducerState{FilterDeltaIPC: deltas, FilterPredicateOrder: order}
	data, err := producer.GobEncode()
	if err != nil {
		t.Fatal(err)
	}
	var gotProducer TableProducerState
	if err := gotProducer.GobDecode(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotProducer.FilterDeltaIPC, deltas) || !reflect.DeepEqual(gotProducer.FilterPredicateOrder, order) {
		t.Fatalf("producer token carried history %q order %q, want %q %q",
			gotProducer.FilterDeltaIPC, gotProducer.FilterPredicateOrder, deltas, order)
	}

	exchange := &TableInOutExchangeState{FilterDeltaIPC: deltas, FilterPredicateOrder: order}
	data, err = exchange.GobEncode()
	if err != nil {
		t.Fatal(err)
	}
	var gotExchange TableInOutExchangeState
	if err := gotExchange.GobDecode(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotExchange.FilterDeltaIPC, deltas) || !reflect.DeepEqual(gotExchange.FilterPredicateOrder, order) {
		t.Fatalf("table-in-out token carried history %q order %q, want %q %q",
			gotExchange.FilterDeltaIPC, gotExchange.FilterPredicateOrder, deltas, order)
	}
}
