// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TableInOutExchangeState.Exchange driven turn by turn, each with a fresh
// OutputCollector as the transports give it. The HTTP round trip through real
// state tokens is in continuation_e2e_test.go.

var pagerSchema = arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)

// newTestCollector builds the OutputCollector a transport hands an exchange
// turn. vgirpc has no exported constructor, so its two unexported fields are
// set the way newOutputCollector sets them.
func newTestCollector(t *testing.T) *vgirpc.OutputCollector {
	t.Helper()
	out := &vgirpc.OutputCollector{}
	v := reflect.ValueOf(out).Elem()
	set := func(name string, val any) {
		f := v.FieldByName(name)
		if !f.IsValid() {
			t.Fatalf("vgirpc.OutputCollector has no field %q", name)
		}
		reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(val))
	}
	set("schema", pagerSchema)
	set("dataBatchIdx", -1)
	return out
}

// emittedValues returns the int64 values of the data batch out holds, and
// whether it holds one.
func emittedValues(t *testing.T, out *vgirpc.OutputCollector) ([]int64, bool) {
	t.Helper()
	v := reflect.ValueOf(out).Elem()
	i := int(v.FieldByName("dataBatchIdx").Int())
	if i < 0 {
		return nil, false
	}
	f := v.FieldByName("batches").Index(i).FieldByName("batch")
	b := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface().(arrow.RecordBatch)
	return append([]int64{}, b.Column(0).(*array.Int64).Int64Values()...), true
}

// pagerState is where pager resumes: the values still to send.
type pagerState struct{ Left int64 }

// pager answers an input batch whose values sum to n with n one-row replies
// n, n-1, ..., 1, every reply but the last saying more. Without Resume it is a
// function that cannot continue; resumePager adds Resume.
type pager struct {
	// nilState: NewState returns nil, as a raw function with no state may.
	nilState bool
	// emit: how a reply says more — "helper" (WithMoreOutput), "option"
	// (WithMetadata on the vgi.status key), "raw" (out.EmitWithMetadata).
	emit string

	processCalls, resumeCalls int
	// ifNoneMatch is the validator each call saw, in call order ("" = none).
	ifNoneMatch []string
}

func (*pager) Name() string                                    { return "pager" }
func (*pager) Metadata() FunctionMetadata                      { return FunctionMetadata{} }
func (*pager) ArgumentSpecs() []ArgSpec                        { return nil }
func (*pager) OnBind(*BindParams) (*BindResponse, error)       { return nil, nil }
func (*pager) OnInit(*InitParams) (*GlobalInitResponse, error) { return nil, nil }
func (*pager) Finalize(context.Context, *ProcessParams, interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func (p *pager) NewState(*ProcessParams) (interface{}, error) {
	if p.nilState {
		return nil, nil
	}
	return &pagerState{}, nil
}

func (p *pager) seen(params *ProcessParams) {
	v := ""
	if params.IfNoneMatch != nil {
		v = *params.IfNoneMatch
	}
	p.ifNoneMatch = append(p.ifNoneMatch, v)
}

func (p *pager) Process(_ context.Context, params *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	p.processCalls++
	p.seen(params)
	var n int64
	for _, v := range batch.Column(0).(*array.Int64).Int64Values() {
		n += v
	}
	st, _ := state.(*pagerState)
	if st == nil {
		st = &pagerState{} // nilState: the position lives nowhere the next turn sees
	}
	st.Left = n
	return p.emitPage(st, out)
}

func (p *pager) emitPage(st *pagerState, out *vgirpc.OutputCollector) error {
	batch := continuationBatch(nil, st.Left)
	st.Left--
	more := st.Left > 0
	switch p.emit {
	case "raw":
		md := map[string]string{}
		if more {
			md[statusKey] = statusHaveMoreOutput
		}
		return out.EmitWithMetadata(batch, md)
	case "option":
		if more {
			return Emit(out, batch, WithMetadata(statusKey, statusHaveMoreOutput))
		}
		return Emit(out, batch)
	default:
		return Emit(out, batch, WithMoreOutput(more))
	}
}

type resumePager struct{ *pager }

func (p resumePager) Resume(_ context.Context, params *ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	p.resumeCalls++
	p.seen(params)
	st, _ := state.(*pagerState)
	if st == nil {
		st = &pagerState{}
	}
	return p.emitPage(st, out)
}

func newPagerExchange(fn TableInOutFunction) *TableInOutExchangeState {
	state, _ := fn.NewState(nil)
	return &TableInOutExchangeState{
		Recipe: InitRecipe{FunctionName: "pager"},
		fn:     fn,
		params: &ProcessParams{},
		state:  state,
	}
}

// turn runs one exchange turn on a fresh collector and returns its reply.
func turn(t *testing.T, s *TableInOutExchangeState, input arrow.RecordBatch) ([]int64, error) {
	t.Helper()
	out := newTestCollector(t)
	if err := s.Exchange(context.Background(), input, out, &vgirpc.CallContext{}); err != nil {
		return nil, err
	}
	vals, ok := emittedValues(t, out)
	if !ok {
		t.Fatal("the turn emitted no data batch")
	}
	return vals, nil
}

func withMeta(batch arrow.RecordBatch, kv ...string) arrow.RecordBatch {
	var keys, vals []string
	for i := 0; i < len(kv); i += 2 {
		keys, vals = append(keys, kv[i]), append(vals, kv[i+1])
	}
	return array.NewRecordBatchWithMetadata(batch.Schema(), batch.Columns(), batch.NumRows(), arrow.NewMetadata(keys, vals))
}

func tickBatch() arrow.RecordBatch {
	return withMeta(continuationBatch(nil), statusKey, statusContinue)
}

func wantErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want an error containing %q", err, want)
	}
}

// A tick is routed to Resume, and MorePending follows every reply: set while a
// reply says more, cleared by the one that does not.
func TestExchangeTickRoutesToResume(t *testing.T) {
	for _, emit := range []string{"helper", "option"} {
		t.Run(emit, func(t *testing.T) {
			p := &pager{emit: emit}
			s := newPagerExchange(resumePager{p})
			steps := []struct {
				input       arrow.RecordBatch
				want        int64
				morePending bool
			}{
				{continuationBatch(nil, 1, 2), 3, true},
				{tickBatch(), 2, true},
				{tickBatch(), 1, false},
				{continuationBatch(nil, 1), 1, false}, // a new input once the output is done
			}
			for i, step := range steps {
				got, err := turn(t, s, step.input)
				if err != nil {
					t.Fatalf("turn %d: %v", i, err)
				}
				if len(got) != 1 || got[0] != step.want {
					t.Fatalf("turn %d replied %v, want [%d]", i, got, step.want)
				}
				if s.MorePending != step.morePending {
					t.Fatalf("turn %d left MorePending %v, want %v", i, s.MorePending, step.morePending)
				}
			}
			if p.processCalls != 2 || p.resumeCalls != 2 {
				t.Fatalf("Process ran %d times and Resume %d, want 2 and 2", p.processCalls, p.resumeCalls)
			}
		})
	}
}

// The tick can also arrive on the transport's input metadata (pipe transports).
func TestExchangeTickOnCallContextMetadata(t *testing.T) {
	p := &pager{}
	s := newPagerExchange(resumePager{p})
	if _, err := turn(t, s, continuationBatch(nil, 2)); err != nil {
		t.Fatal(err)
	}
	out := newTestCollector(t)
	callCtx := &vgirpc.CallContext{InputMetadata: arrow.NewMetadata([]string{statusKey}, []string{statusContinue})}
	if err := s.Exchange(context.Background(), continuationBatch(nil), out, callCtx); err != nil {
		t.Fatal(err)
	}
	if p.resumeCalls != 1 || s.MorePending {
		t.Fatalf("Resume ran %d times, MorePending %v; want 1, false", p.resumeCalls, s.MorePending)
	}
}

func TestExchangeRefusals(t *testing.T) {
	t.Run("tick with nothing pending", func(t *testing.T) {
		s := newPagerExchange(resumePager{&pager{}})
		_, err := turn(t, s, tickBatch())
		wantErr(t, err, "no output is pending")
	})
	t.Run("tick with rows", func(t *testing.T) {
		s := newPagerExchange(resumePager{&pager{}})
		if _, err := turn(t, s, continuationBatch(nil, 3)); err != nil {
			t.Fatal(err)
		}
		_, err := turn(t, s, withMeta(continuationBatch(nil, 7), statusKey, statusContinue))
		wantErr(t, err, "must have no rows, got 1")
	})
	t.Run("input while output is pending", func(t *testing.T) {
		p := &pager{}
		s := newPagerExchange(resumePager{p})
		if _, err := turn(t, s, continuationBatch(nil, 3)); err != nil {
			t.Fatal(err)
		}
		_, err := turn(t, s, continuationBatch(nil, 1))
		wantErr(t, err, "expected a continuation tick")
		if p.processCalls != 1 {
			t.Fatalf("Process ran %d times, want 1: the refused input reached it", p.processCalls)
		}
	})
	t.Run("tick for a function without Resume", func(t *testing.T) {
		// Only a state token can claim output is pending for a function with no
		// Resume (a reply from it cannot say more): it must fail, not panic.
		s := newPagerExchange(&pager{})
		s.MorePending = true
		_, err := turn(t, s, tickBatch())
		wantErr(t, err, "does not implement Resume")
	})
	t.Run("more with a nil state", func(t *testing.T) {
		s := newPagerExchange(resumePager{&pager{nilState: true}})
		_, err := turn(t, s, continuationBatch(nil, 3))
		wantErr(t, err, "must resume from its saved state")
		if s.MorePending {
			t.Fatal("a refused reply left MorePending set")
		}
	})
	t.Run("raw HAVE_MORE_OUTPUT", func(t *testing.T) {
		s := newPagerExchange(resumePager{&pager{emit: "raw"}})
		_, err := turn(t, s, continuationBatch(nil, 3))
		wantErr(t, err, "directly with out.EmitWithMetadata")
		if s.MorePending {
			t.Fatal("a refused reply left MorePending set")
		}
	})
	t.Run("raw HAVE_MORE_OUTPUT without Resume", func(t *testing.T) {
		s := newPagerExchange(&pager{emit: "raw"})
		_, err := turn(t, s, continuationBatch(nil, 3))
		wantErr(t, err, "directly with out.EmitWithMetadata")
	})
	t.Run("vgi.status option without Resume", func(t *testing.T) {
		s := newPagerExchange(&pager{emit: "option"})
		_, err := turn(t, s, continuationBatch(nil, 3))
		wantErr(t, err, "does not implement Resume")
	})
}

// A raw emit with no status (the last reply, or a function that never
// continues) is fine: only a status the helpers did not record is refused.
func TestRawEmitWithoutStatusAccepted(t *testing.T) {
	s := newPagerExchange(&pager{emit: "raw"})
	got, err := turn(t, s, continuationBatch(nil, 1))
	if err != nil || len(got) != 1 || got[0] != 1 || s.MorePending {
		t.Fatalf("got %v, %v, MorePending %v; want [1], nil, false", got, err, s.MorePending)
	}
}

// emittedStatus reads vgirpc's collector by reflection; this fails if that
// layout changes, which would otherwise silently disable the raw-status check.
func TestRawHaveMoreOutputRefused(t *testing.T) {
	out := newTestCollector(t)
	if status, ok := emittedStatus(out); !ok || status != "" {
		t.Fatalf("empty collector: emittedStatus = %q, %v; want \"\", true", status, ok)
	}
	if err := out.EmitWithMetadata(continuationBatch(t, 1), map[string]string{statusKey: statusHaveMoreOutput, "x": "y"}); err != nil {
		t.Fatal(err)
	}
	if status, ok := emittedStatus(out); !ok || status != statusHaveMoreOutput {
		t.Fatalf("emittedStatus = %q, %v; want %q, true (has vgirpc.OutputCollector's layout changed?)", status, ok, statusHaveMoreOutput)
	}
	err := checkReplyStatus(out, &replyTracker{name: "probe", resumable: true})
	wantErr(t, err, "directly with out.EmitWithMetadata")
	if err := checkReplyStatus(out, &replyTracker{name: "probe", resumable: true, more: true}); err != nil {
		t.Fatalf("a status the helpers recorded was refused: %v", err)
	}
}

// Conditional-request validators belong to the input they arrive with: its
// first reply sees them, a tick's Resume does not, and the next input without
// them does not inherit them.
func TestExchangeValidatorsResetPerInput(t *testing.T) {
	p := &pager{}
	s := newPagerExchange(resumePager{p})
	inputs := []arrow.RecordBatch{
		withMeta(continuationBatch(nil, 2), CacheIfNoneMatchKey, "v1"),
		tickBatch(),
		continuationBatch(nil, 1),
		withMeta(continuationBatch(nil, 1), CacheIfNoneMatchKey, "v2"),
	}
	for i, in := range inputs {
		if _, err := turn(t, s, in); err != nil {
			t.Fatalf("turn %d: %v", i, err)
		}
	}
	if want := []string{"v1", "", "", "v2"}; !reflect.DeepEqual(p.ifNoneMatch, want) {
		t.Fatalf("validators seen per call %q, want %q", p.ifNoneMatch, want)
	}
}

// A state token that says output is pending but carries no state is refused,
// not rehydrated onto a fresh NewState.
func TestRehydratePendingWithoutStateRefused(t *testing.T) {
	// No state bytes: what GobEncode writes for a nil state.
	data, err := (&TableInOutExchangeState{Recipe: InitRecipe{FunctionName: "pager"}, MorePending: true}).GobEncode()
	if err != nil {
		t.Fatal(err)
	}
	var s TableInOutExchangeState
	if err := s.GobDecode(data); err != nil {
		t.Fatal(err)
	}
	wantErr(t, NewWorker().rehydrateTableInOut(&s), "carries no state")
}
