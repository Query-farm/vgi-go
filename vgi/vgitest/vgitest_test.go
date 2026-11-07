// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgitest_test

import (
	"context"
	"encoding/gob"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/vgitest"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

var seqSchema = arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)

type seqArgs struct {
	Count int64 `vgi:"pos=0"`
	Batch int64 `vgi:"default=2"`
}

type seqState struct{ vgi.BatchState }

type seq struct{}

func (seq) Name() string                   { return "seq" }
func (seq) Metadata() vgi.FunctionMetadata { return vgi.DefaultMetadata() }
func (seq) ArgumentSpecs() []vgi.ArgSpec   { return vgi.DeriveArgSpecs(seqArgs{}) }
func (seq) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	var a seqArgs
	if err := vgi.BindArgs(p.Args, &a); err != nil {
		return nil, err
	}
	if a.Count < 0 {
		return nil, &vgi.ArgumentError{ArgName: "count", Position: 0, Detail: "must be >= 0"}
	}
	return vgi.BindSchema(seqSchema)
}
func (seq) NewState(p *vgi.ProcessParams) (*seqState, error) {
	a := seqArgs{Batch: 2}
	if err := vgi.BindArgs(p.Args, &a); err != nil {
		return nil, err
	}
	return &seqState{vgi.NewBatchState(a.Count, a.Batch)}, nil
}
func (seq) Process(_ context.Context, _ *vgi.ProcessParams, s *seqState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&s.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := s.Index
		return []arrow.Array{vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })}, nil
	})
}

func TestRunTableAcrossGobRoundTrips(t *testing.T) {
	res, err := vgitest.RunTable(vgi.AsTableFunction[seqState](seq{}), vgitest.Call{Positional: []any{5}})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	if res.Rows() != 5 || len(res.Batches) != 3 {
		t.Fatalf("rows=%d batches=%d", res.Rows(), len(res.Batches))
	}
	last := res.Batches[2].Column(0).(*array.Int64)
	if last.Value(0) != 4 {
		t.Fatalf("last batch starts at %d, want 4", last.Value(0))
	}
	if _, err := vgitest.RunTable(vgi.AsTableFunction[seqState](seq{}), vgitest.Call{Positional: []any{-1}}); err == nil ||
		!strings.Contains(err.Error(), "must be >= 0") {
		t.Fatalf("bind error = %v", err)
	}
}

// lossyState's cursor is unexported. AsTableFunction refuses it, so this raw
// TableFunction shows what vgitest catches for a hand-written adapter: over a
// gob round trip the cursor resets and the scan never ends.
type lossyState struct {
	Done   bool
	cursor int64
}

func init() { gob.Register(&lossyState{}) }

type lossy struct{}

func (lossy) Name() string                   { return "lossy" }
func (lossy) Metadata() vgi.FunctionMetadata { return vgi.DefaultMetadata() }
func (lossy) ArgumentSpecs() []vgi.ArgSpec   { return nil }
func (lossy) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(seqSchema)
}
func (lossy) OnInit(*vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (lossy) NewState(*vgi.ProcessParams) (interface{}, error) { return &lossyState{}, nil }
func (lossy) Process(_ context.Context, _ *vgi.ProcessParams, st interface{}, out *vgirpc.OutputCollector) error {
	s := st.(*lossyState)
	if s.cursor >= 3 {
		return out.Finish()
	}
	s.cursor++
	return out.EmitArrays([]arrow.Array{vgi.BuildInt64Array(1, func(int64) int64 { return s.cursor })}, 1)
}

func TestRunTableCatchesAStateThatLosesItsCursor(t *testing.T) {
	_, err := vgitest.RunTable(lossy{}, vgitest.Call{MaxCalls: 20})
	if err == nil || !strings.Contains(err.Error(), "did not finish") {
		t.Fatalf("err = %v", err)
	}
	res, err := vgitest.RunTable(lossy{}, vgitest.Call{NoGobRoundTrip: true})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	if res.Rows() != 3 {
		t.Fatalf("stdio-like run rows = %d", res.Rows())
	}
}
