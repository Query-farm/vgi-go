// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// The exchange turn itself (ticks, MorePending, Resume) runs end to end in the
// extension's table_in_out/*continuation*.test files over stdio and HTTP. These
// cover the pieces that decide it.

func continuationBatch(t *testing.T, vals ...int64) arrow.RecordBatch {
	t.Helper()
	b := array.NewInt64Builder(memory.NewGoAllocator())
	defer b.Release()
	b.AppendValues(vals, nil)
	col := b.NewArray()
	defer col.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{col}, int64(len(vals)))
}

func TestWithMoreOutputSetsStatus(t *testing.T) {
	md, err := applyEmitOptions(nil, []EmitOption{WithMoreOutput(true)})
	if err != nil {
		t.Fatal(err)
	}
	if md[statusKey] != statusHaveMoreOutput {
		t.Fatalf("WithMoreOutput(true) metadata = %v, want %s=%s", md, statusKey, statusHaveMoreOutput)
	}
	md, err = applyEmitOptions(nil, []EmitOption{WithMoreOutput(false)})
	if err != nil {
		t.Fatal(err)
	}
	if md != nil {
		t.Fatalf("WithMoreOutput(false) metadata = %v, want none", md)
	}
}

func TestWithMoreOutputRefusedOutsideTableInOut(t *testing.T) {
	// No tracker: not a table-in-out exchange turn. Refused before out is used.
	err := Emit(nil, continuationBatch(t, 1), WithMoreOutput(true))
	if err == nil || !strings.Contains(err.Error(), "only valid in a table-in-out") {
		t.Fatalf("Emit(more) outside table-in-out = %v, want a refusal", err)
	}
}

func TestWithMoreOutputRefusedWithoutResume(t *testing.T) {
	var out *vgirpc.OutputCollector
	defer trackReplies(out, &replyTracker{name: "probe"})()
	err := Emit(out, continuationBatch(t, 1), WithMoreOutput(true))
	if err == nil || !strings.Contains(err.Error(), "does not implement Resume") {
		t.Fatalf("Emit(more) without Resume = %v, want a refusal", err)
	}
}

func TestCacheControlRefusedOnTickReply(t *testing.T) {
	var out *vgirpc.OutputCollector
	defer trackReplies(out, &replyTracker{name: "probe", resumable: true, continuation: true})()
	err := Emit(out, continuationBatch(t), WithCacheControl(&CacheControl{Ttl: Seconds(30)}))
	if err == nil || !strings.Contains(err.Error(), "first reply") {
		t.Fatalf("cache control on a tick's reply = %v, want a refusal", err)
	}
}

func TestIsContinuationTick(t *testing.T) {
	for _, tc := range []struct {
		meta arrow.Metadata
		want bool
	}{
		{arrow.Metadata{}, false},
		{arrow.NewMetadata([]string{statusKey}, []string{statusContinue}), true},
		{arrow.NewMetadata([]string{statusKey}, []string{statusHaveMoreOutput}), false},
		{arrow.NewMetadata([]string{"other"}, []string{statusContinue}), false},
	} {
		if got := isContinuationTick(tc.meta); got != tc.want {
			t.Errorf("isContinuationTick(%v) = %v, want %v", tc.meta, got, tc.want)
		}
	}
}

func TestFilterParentRowsKeepsProvenanceAligned(t *testing.T) {
	// i > 1 over [0, 3, 1, 2] keeps rows 1 and 3, so their parents, 7 and 9.
	filters := &PushdownFilters{Filters: []Filter{&ConstantFilter{
		columnName: "i", columnIndex: 0, Op: OpGT, Value: scalar.NewInt64Scalar(1),
	}}}
	batch := continuationBatch(t, 0, 3, 1, 2)
	filtered, parents, err := filterParentRows(context.Background(), filters, batch, []int32{6, 7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	got := filtered.Column(0).(*array.Int64).Int64Values()
	if !reflect.DeepEqual(got, []int64{3, 2}) || !reflect.DeepEqual(parents, []int32{7, 9}) {
		t.Fatalf("filtered rows %v parents %v, want [3 2] [7 9]", got, parents)
	}
}

func TestTableInOutTokenCarriesMorePending(t *testing.T) {
	data, err := (&TableInOutExchangeState{MorePending: true}).GobEncode()
	if err != nil {
		t.Fatal(err)
	}
	var got TableInOutExchangeState
	if err := got.GobDecode(data); err != nil {
		t.Fatal(err)
	}
	if !got.MorePending {
		t.Fatal("the state token lost MorePending: the next HTTP turn would refuse the tick")
	}
}

// contractProbe is a minimal typed table-in-out function for the startup checks.
type contractProbe[S any] struct{ meta FunctionMetadata }

func (contractProbe[S]) Name() string                              { return "probe" }
func (p contractProbe[S]) Metadata() FunctionMetadata              { return p.meta }
func (contractProbe[S]) ArgumentSpecs() []ArgSpec                  { return nil }
func (contractProbe[S]) OnBind(*BindParams) (*BindResponse, error) { return nil, nil }
func (contractProbe[S]) NewState(*ProcessParams) (*S, error)       { return new(S), nil }
func (contractProbe[S]) Process(context.Context, *ProcessParams, *S, arrow.RecordBatch, *vgirpc.OutputCollector) error {
	return nil
}
func (contractProbe[S]) Finalize(context.Context, *ProcessParams, *S) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// resumeProbe adds Resume.
type resumeProbe[S any] struct{ contractProbe[S] }

func (resumeProbe[S]) Resume(context.Context, *ProcessParams, *S, *vgirpc.OutputCollector) error {
	return nil
}

type resumePosition struct{ Row int64 }

// hiddenPosition's field is unexported on purpose: gob cannot encode it.
type hiddenPosition struct {
	row int64 //nolint:unused // never read: the type exists to fail the gob check
}

func TestTableInOutContracts(t *testing.T) {
	three, zero := int64(3), int64(0)
	for _, tc := range []struct {
		name string
		fn   TableInOutFunction
		want string // "" = accepted
	}{
		{"plain", AsTableInOutFunction[struct{}](contractProbe[struct{}]{}), ""},
		{"resume", AsTableInOutFunction[resumePosition](resumeProbe[resumePosition]{}), ""},
		{"resume with an empty state", AsTableInOutFunction[struct{}](resumeProbe[struct{}]{}), "must hold the resume position"},
		{"resume with an unexported state", AsTableInOutFunction[hiddenPosition](resumeProbe[hiddenPosition]{}), "gob-serializable"},
		{"rows per call on a blended function",
			AsTableInOutFunction[struct{}](contractProbe[struct{}]{FunctionMetadata{InputFromArgs: true, InputRowsPerCall: &three}}), ""},
		{"rows per call on a classic function",
			AsTableInOutFunction[struct{}](contractProbe[struct{}]{FunctionMetadata{InputRowsPerCall: &three}}), "only to a blended"},
		{"zero rows per call",
			AsTableInOutFunction[struct{}](contractProbe[struct{}]{FunctionMetadata{InputFromArgs: true, InputRowsPerCall: &zero}}), "positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, resumable := tc.fn.(TableInOutResumer)
			if wantResumable := strings.HasPrefix(tc.name, "resume"); resumable != wantResumable {
				t.Fatalf("TableInOutResumer = %v, want %v", resumable, wantResumable)
			}
			w := NewWorker()
			w.RegisterTableInOut(tc.fn)
			err := w.checkTableInOutContracts()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestFunctionInfoCarriesInputRowsPerCall(t *testing.T) {
	three := int64(3)
	w := NewWorker()
	w.RegisterTableInOut(AsTableInOutFunction[struct{}](contractProbe[struct{}]{
		FunctionMetadata{InputFromArgs: true, InputRowsPerCall: &three},
	}))
	for _, si := range NewDefaultReadOnlyCatalog("example", w).schemas {
		for _, fi := range si.functions {
			if fi.Name == "probe" {
				if fi.InputRowsPerCall == nil || *fi.InputRowsPerCall != 3 {
					t.Fatalf("FunctionInfo.InputRowsPerCall = %v, want 3", fi.InputRowsPerCall)
				}
				return
			}
		}
	}
	t.Fatal("probe not listed")
}
