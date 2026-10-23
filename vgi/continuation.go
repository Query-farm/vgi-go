// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// Continued output (table-in-out exchange).
//
// A table-in-out exchange answers each input batch with one reply. A function
// can answer one input with several: it emits each reply but the last with
// WithMoreOutput(true), which puts vgi.status: HAVE_MORE_OUTPUT on the reply.
// The client then asks for the next reply with a continuation tick — a zero-row
// batch of the input schema carrying vgi.status: CONTINUE — and the framework
// answers it by calling the function's Resume. Every reply's parent rows index
// the last real (non-tick) input batch.
//
// Resume gets no input, so the function's state must hold where to resume. Over
// HTTP that state reaches the next turn only inside the stream's state token, so
// keep it small: positions to resume from, never buffered output rows.
//
// The protocol is strict, with no capability negotiation: while output is
// pending a tick is the only valid input, and a tick is valid only then. Cache
// control may only ride an input's first reply, and WithMoreOutput is refused
// outside a table-in-out exchange. Mirrors vgi-python's emit(more=True) /
// resume().
const (
	statusKey            = "vgi.status"
	statusHaveMoreOutput = "HAVE_MORE_OUTPUT"
	statusContinue       = "CONTINUE"
)

// WithMoreOutput marks an emitted table-in-out reply as one of several for the
// same input batch when more is true: the client asks for the next one with a
// continuation tick, which the framework answers through the function's
// Resume. False is a no-op, so the last reply can pass its own condition:
//
//	vgi.EmitParentRows(out, batch, parents, vgi.WithMoreOutput(state.Row < len(state.Counts)))
//
// Valid only from the Process or Resume of a table-in-out function that
// implements Resume, and only through the vgi emit helpers (Emit,
// EmitParentRows, ...): the framework learns that output is pending from them,
// so a reply that sets the vgi.status key on out.EmitWithMetadata directly is
// refused. (vgi.WithMetadata("vgi.status", "HAVE_MORE_OUTPUT") goes through the
// helpers, so it counts as WithMoreOutput(true), with the same checks.)
func WithMoreOutput(more bool) EmitOption {
	return func(md map[string]string) error {
		if more {
			md[statusKey] = statusHaveMoreOutput
		}
		return nil
	}
}

// TableInOutResumer is implemented by a table-in-out function that continues
// one input batch's output across several replies (see WithMoreOutput).
//
// Resume emits the next reply for the last input batch. Like Process it must
// emit exactly one batch, with WithMoreOutput(true) unless it is the last; its
// parent rows index the input batch Process received. state is what Process (or
// the previous Resume) left; it must be gob-serializable and hold everything
// Resume needs, since over HTTP it is all that reaches the next turn.
//
// The client can continue output only on a per-substream connection. A function
// on the one connection shared by every pipeline (catalog max_workers = 1)
// cannot leave output pending; this SDK never advertises that, so every
// table-in-out function may implement Resume.
type TableInOutResumer interface {
	// Resume emits the next reply for the last input batch.
	Resume(ctx context.Context, params *ProcessParams, state interface{}, out *vgirpc.OutputCollector) error
}

// TypedTableInOutResumer is the typed form of TableInOutResumer. A
// TypedTableInOutFunc[S] that also implements it is wrapped by
// AsTableInOutFunction into a function whose Resume the framework drives. S
// must be a gob-serializable struct with at least one exported field: the
// resume position lives there.
type TypedTableInOutResumer[S any] interface {
	// Resume emits the next reply for the last input batch.
	Resume(ctx context.Context, params *ProcessParams, state *S, out *vgirpc.OutputCollector) error
}

// replyTracker follows one table-in-out exchange turn's emit: whether the reply
// said more, and what it may carry. It is reachable from the vgi emit helpers
// only through the OutputCollector they are given (see replyTrackers), since
// vgirpc's collector has no slot for it.
type replyTracker struct {
	name string
	// resumable: the function implements Resume, so it may say more.
	resumable bool
	// continuation: this turn answers a tick, so its reply is not the input's
	// first and may not carry cache control.
	continuation bool
	// more: the emitted reply said HAVE_MORE_OUTPUT.
	more bool
	// ctx and filters: the auto-applied pushdown filters in force for this
	// turn (nil when the function does not auto-apply). EmitParentRows filters
	// the provenance with the batch's mask and sets prefiltered, so the
	// exchange's EmitInterceptor does not filter the batch a second time.
	ctx         context.Context
	filters     *PushdownFilters
	prefiltered bool
}

// replyTrackers maps a table-in-out exchange turn's *vgirpc.OutputCollector to
// its replyTracker for the duration of that turn.
var replyTrackers sync.Map

// trackReplies registers t for out until the returned func is called.
func trackReplies(out *vgirpc.OutputCollector, t *replyTracker) func() {
	replyTrackers.Store(out, t)
	return func() { replyTrackers.Delete(out) }
}

// replyTrackerFor returns out's tracker, or nil outside a table-in-out turn.
func replyTrackerFor(out *vgirpc.OutputCollector) *replyTracker {
	if v, ok := replyTrackers.Load(out); ok {
		return v.(*replyTracker)
	}
	return nil
}

// emitAnnotated is the single exit of the vgi emit helpers: it enforces the
// continued-output rules against out's tracker, emits, and records whether the
// reply said more.
func emitAnnotated(out *vgirpc.OutputCollector, batch arrow.RecordBatch, md map[string]string) error {
	t := replyTrackerFor(out)
	more := md[statusKey] == statusHaveMoreOutput
	if more {
		if t == nil {
			return fmt.Errorf("vgi.WithMoreOutput is only valid in a table-in-out function's Process or Resume")
		}
		if !t.resumable {
			return fmt.Errorf("%s: emitted WithMoreOutput(true) but does not implement Resume", t.name)
		}
	}
	if t != nil && t.continuation && hasCacheControlKeys(md) {
		return fmt.Errorf("%s: cache control is only valid on an input's first reply, not on one answering a continuation tick", t.name)
	}
	var err error
	if md == nil {
		err = out.Emit(batch)
	} else {
		err = out.EmitWithMetadata(batch, md)
	}
	if t != nil {
		t.prefiltered = false
		if err == nil {
			t.more = more
		}
	}
	return err
}

// checkReplyStatus refuses a turn whose emitted reply says HAVE_MORE_OUTPUT on
// the wire though the vgi emit helpers did not record it: the function put
// vgi.status on out.EmitWithMetadata itself. The client would then send a tick
// that the next turn, with MorePending false, refuses as unexpected; refusing
// here names the real mistake instead.
func checkReplyStatus(out *vgirpc.OutputCollector, t *replyTracker) error {
	status, ok := emittedStatus(out)
	if !ok || status != statusHaveMoreOutput || t.more {
		return nil
	}
	return fmt.Errorf("%s: emitted %s: %s directly with out.EmitWithMetadata; continued output must be "+
		"emitted with vgi.WithMoreOutput through the vgi emit helpers (Emit, EmitParentRows, ...)",
		t.name, statusKey, statusHaveMoreOutput)
}

// emittedStatus returns the vgi.status metadata of the data batch out holds for
// this turn ("" when it has none). ok is false when out's layout is not the one
// read here. vgirpc's OutputCollector exposes neither the stored batch's
// metadata nor a hook that sees it, so it is read by reflection (field reads
// only; TestRawHaveMoreOutputRefused fails if the layout changes).
func emittedStatus(out *vgirpc.OutputCollector) (status string, ok bool) {
	if out == nil {
		return "", false
	}
	v := reflect.ValueOf(out).Elem()
	idx, batches := v.FieldByName("dataBatchIdx"), v.FieldByName("batches")
	if idx.Kind() != reflect.Int || batches.Kind() != reflect.Slice {
		return "", false
	}
	i := int(idx.Int())
	if i < 0 || i >= batches.Len() {
		return "", true // no data batch
	}
	ab := batches.Index(i)
	if ab.Kind() != reflect.Struct {
		return "", false
	}
	meta := ab.FieldByName("meta")
	if meta.Kind() != reflect.Pointer {
		return "", false
	}
	if meta.IsNil() {
		return "", true
	}
	keys, vals := meta.Elem().FieldByName("keys"), meta.Elem().FieldByName("values")
	if keys.Kind() != reflect.Slice || vals.Kind() != reflect.Slice || keys.Len() != vals.Len() {
		return "", false
	}
	for j := 0; j < keys.Len(); j++ {
		if keys.Index(j).String() == statusKey {
			return vals.Index(j).String(), true
		}
	}
	return "", true
}

// hasCacheControlKeys reports whether md carries any response-side vgi.cache.*
// key.
func hasCacheControlKeys(md map[string]string) bool {
	for k := range md {
		if strings.HasPrefix(k, "vgi.cache.") {
			return true
		}
	}
	return false
}

// isContinuationTick reports whether an exchange input's metadata marks it as a
// continuation tick rather than real input.
func isContinuationTick(meta arrow.Metadata) bool {
	idx := meta.FindKey(statusKey)
	return idx >= 0 && meta.Values()[idx] == statusContinue
}

// filterParentRows applies the auto-apply filters to batch and drops the same
// rows from parentRows, so provenance stays aligned with the rows that survive.
// Mirrors vgi-python's _FilteringOutputCollector.
func filterParentRows(ctx context.Context, filters *PushdownFilters, batch arrow.RecordBatch, parentRows []int32) (arrow.RecordBatch, []int32, error) {
	mask, err := filters.Evaluate(ctx, batch)
	if err != nil {
		return nil, nil, err
	}
	defer mask.Release()
	bools, ok := mask.(*array.Boolean)
	if !ok {
		return nil, nil, fmt.Errorf("filter mask is %s, not boolean", mask.DataType())
	}
	var filtered arrow.RecordBatch
	if batchHasDictionary(batch) {
		filtered, err = filterRecordBatchDictAware(ctx, batch, mask)
	} else {
		filtered, err = compute.FilterRecordBatch(ctx, batch, mask, compute.DefaultFilterOptions())
	}
	if err != nil {
		return nil, nil, fmt.Errorf("filtering record batch: %w", err)
	}
	// The default filter options drop null mask slots, as the batch filter did.
	kept := make([]int32, 0, filtered.NumRows())
	for i, p := range parentRows {
		if bools.IsValid(i) && bools.Value(i) {
			kept = append(kept, p)
		}
	}
	return filtered, kept, nil
}

// stateTyped is implemented by the typed table-in-out adapters, so startup can
// check a resumable function's state type.
type stateTyped interface {
	stateType() reflect.Type
}

// checkTableInOutContracts refuses, at startup, a table-in-out registration the
// client cannot drive: Resume with a state that cannot carry a resume position,
// and InputRowsPerCall that is not positive or not on a blended function. It
// covers every routed sub-catalog too.
func (w *Worker) checkTableInOutContracts() error {
	for name, fns := range w.tableInOuts {
		for _, fn := range fns {
			if err := checkTableInOutContract(name, fn); err != nil {
				return err
			}
		}
	}
	for _, b := range w.routes {
		if sc, ok := b.(*subCatalog); ok {
			if err := sc.child.checkTableInOutContracts(); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkTableInOutContract(name string, fn TableInOutFunction) error {
	meta := fn.Metadata()
	if n := meta.InputRowsPerCall; n != nil {
		if !meta.InputFromArgs {
			return fmt.Errorf("%s: InputRowsPerCall applies only to a blended (InputFromArgs) function, "+
				"whose per-row input the client can split into calls", name)
		}
		if *n < 1 {
			return fmt.Errorf("%s: InputRowsPerCall must be a positive row count, got %d", name, *n)
		}
	}
	if _, ok := fn.(TableInOutResumer); !ok {
		return nil
	}
	// A raw (untyped) TableInOutResumer's state type is unknown until NewState
	// runs, so it is checked per turn instead: Exchange refuses WithMoreOutput
	// with a nil state, and HTTP rehydration a pending token with no state.
	if st, ok := fn.(stateTyped); ok {
		t := st.stateType()
		if t.Kind() == reflect.Struct && t.NumField() == 0 {
			return fmt.Errorf("%s: Resume continues from its state alone, so the state type %s must hold the resume position", name, t)
		}
		if _, err := gobEncode(reflect.New(t).Interface()); err != nil {
			return fmt.Errorf("%s: Resume continues from its state alone, so the state type %s must be gob-serializable: %w", name, t, err)
		}
	}
	return nil
}
