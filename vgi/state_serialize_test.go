// Copyright 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
)

func TestInitRecipeGobRoundTripNullableArgumentNames(t *testing.T) {
	left := "left"
	right := "right"
	names := []*string{&left, nil, &right}
	want := InitRecipe{
		BindCall: BindRequestWire{
			FunctionName:  "probe",
			FunctionType:  string(FunctionTypeScalar),
			ArgumentNames: &names,
		},
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(want); err != nil {
		t.Fatalf("encode InitRecipe: %v", err)
	}

	var got InitRecipe
	if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decode InitRecipe: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}

func TestInitRecipeGobRoundTripAbsentArgumentNames(t *testing.T) {
	want := InitRecipe{BindCall: BindRequestWire{FunctionName: "probe"}}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(want); err != nil {
		t.Fatalf("encode InitRecipe: %v", err)
	}

	var got InitRecipe
	if err := gob.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decode InitRecipe: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %#v, want %#v", got, want)
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
