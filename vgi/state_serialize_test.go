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
