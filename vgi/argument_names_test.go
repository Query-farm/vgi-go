// Copyright 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"reflect"
	"testing"
)

func TestParseBindRequestPreservesArgumentNames(t *testing.T) {
	left := "left"
	right := "right"
	want := []*string{&left, nil, &right}

	// The generated record's []string cannot hold a null item: an unnamed
	// vararg arrives as "" and reads back as nil.
	wire := []string{"left", "", "right"}
	params, err := NewWorker().parseBindRequest(BindRequestWire{
		FunctionName:  "probe",
		FunctionType:  string(FunctionTypeScalar),
		Arguments:     testNoArgs(t),
		ArgumentNames: &wire,
	}, shapeTestCallCtx())
	if err != nil {
		t.Fatalf("parseBindRequest: %v", err)
	}
	if !reflect.DeepEqual(params.ArgumentNames, want) {
		t.Fatalf("ArgumentNames = %v, want %v", params.ArgumentNames, want)
	}
}

type argumentNamesAggregate struct {
	probeAggregate
	got []*string
}

func (*argumentNamesAggregate) Name() string { return "argument_names_aggregate" }

func (f *argumentNamesAggregate) OnBind(params *AggregateBindParams) (*BindResponse, error) {
	f.got = params.ArgumentNames
	return f.probeAggregate.OnBind(params)
}

func TestAggregateBindPreservesArgumentNames(t *testing.T) {
	value := "value"
	want := []*string{&value}
	fn := &argumentNamesAggregate{}
	w := NewWorker()
	w.RegisterAggregate(fn)

	_, err := w.handleAggregateBind(context.Background(), shapeTestCallCtx(), AggregateBindRequestWire{
		FunctionName:  fn.Name(),
		ArgumentNames: &want,
	})
	if err != nil {
		t.Fatalf("handleAggregateBind: %v", err)
	}
	if !reflect.DeepEqual(fn.got, want) {
		t.Fatalf("ArgumentNames = %v, want %v", fn.got, want)
	}
}
