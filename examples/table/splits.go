// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// Split-capable table generators, the Go half of the cross-SDK splits suite.
//
// These are TWINS of functions already in the suite: split_sequence(n) must
// return row-for-row what sequence(n) returns, because that equivalence is the
// baseline every other split test rests on. If the twins ever disagree, nothing
// else in the suite means anything.
//
// The shapes cover the ways a split scan goes WRONG rather than the ways it goes
// right: zero splits (legal, must be an empty result), zero-ROW splits (the
// likelier shape — a filter pruned one — and the one that silently truncates a
// scan if a reader treats an empty split as EOS), skew (so greedy claiming can
// be told apart from static assignment), and far more splits than threads (which
// forces sequential re-init on a reused connection).

var splitOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type splitArgs struct {
	N      int64 `vgi:"name=n,ge=0,doc=Number of rows to produce"`
	Splits int64 `vgi:"name=splits,default=4,ge=0,doc=How many splits to divide the scan into"`
}

// splitRange is the half-open range [Lo, Hi) one split owns.
//
// This NAMES the work rather than describing it: a redemption reads the same
// rows however many times it runs and whichever process runs it, which is
// exactly what a retrying engine requires. "Rows 0-999 of whatever this returns
// now" would not survive a retry.
type splitRange struct{ Lo, Hi int64 }

func encodeSplit(r splitRange) []byte {
	out := binary.LittleEndian.AppendUint64(nil, uint64(r.Lo))
	return binary.LittleEndian.AppendUint64(out, uint64(r.Hi))
}

func decodeSplit(payload []byte) (splitRange, error) {
	if len(payload) != 16 {
		return splitRange{}, fmt.Errorf("split payload must be 16 bytes, got %d", len(payload))
	}
	return splitRange{
		Lo: int64(binary.LittleEndian.Uint64(payload[0:8])),
		Hi: int64(binary.LittleEndian.Uint64(payload[8:16])),
	}, nil
}

// splitRanges divides [0, n) into k contiguous ranges, remainder over the first few.
func splitRanges(n, k int64) []splitRange {
	if k <= 0 {
		return nil
	}
	if n < 0 {
		n = 0
	}
	base, extra := n/k, n%k
	out := make([]splitRange, 0, k)
	lo := int64(0)
	for i := int64(0); i < k; i++ {
		hi := lo + base
		if i < extra {
			hi++
		}
		out = append(out, splitRange{Lo: lo, Hi: hi})
		lo = hi
	}
	return out
}

// splitState is the cursor over the ranges THIS reader claimed. A reader may
// claim several (DataFusion bin-packs and sends a group), so it is a list.
type splitState struct {
	Ranges []splitRange
	Idx    int
	Cur    int64
	// Parallel to Ranges, and populated only for the batch-index fixture: the
	// split ordinal each claimed range came from, which is the base of the index
	// space that range emits into.
	Ordinals []int64
	// Batches emitted within the CURRENT split, reset at each boundary.
	EmittedInSplit int64
	// Set after the first batch of this reader's stream, which is the only one
	// the client reads cache control from.
	CacheAdvertised bool
}

// encodeSplitOrdinal packs (ordinal, lo, hi) for the batch-index fixture, whose
// redemption needs the split's position as well as its range.
func encodeSplitOrdinal(ordinal int64, r splitRange) []byte {
	out := make([]byte, 24)
	binary.LittleEndian.PutUint64(out[0:8], uint64(ordinal))
	binary.LittleEndian.PutUint64(out[8:16], uint64(r.Lo))
	binary.LittleEndian.PutUint64(out[16:24], uint64(r.Hi))
	return out
}

func decodeSplitOrdinal(payload []byte) (int64, splitRange, error) {
	if len(payload) != 24 {
		return 0, splitRange{}, fmt.Errorf(
			"batch-index split payload must be 24 bytes, got %d", len(payload))
	}
	return int64(binary.LittleEndian.Uint64(payload[0:8])), splitRange{
		Lo: int64(binary.LittleEndian.Uint64(payload[8:16])),
		Hi: int64(binary.LittleEndian.Uint64(payload[16:24])),
	}, nil
}

// metaBatchIndexKey is the per-batch tag a supports_batch_index function emits.
// The client enforces monotonicity per reader, so a fixture that restarted its
// numbering per split would be rejected — which is the contract greedy claiming
// depends on and the reason this is exercised at all.
const metaBatchIndexKey = "vgi_batch_index"

// --- the shared machinery -------------------------------------------------

type splitPlanner struct {
	name  string
	desc  string
	plan  func(args splitArgs) []splitRange
	rowAt func(r splitRange, i int64) int64

	// Optional hooks. Each exists because one fixture in the shared cross-SDK
	// suite needs it and the rest must be unaffected, so all of them are
	// zero-valued no-ops by default.

	// perPage > 0 enumerates the plan over several pages, cursoring on the page
	// index. Pages are disjoint by construction: each is a window of the same
	// range list. Disjointness is the worker's obligation and no client checks
	// it, so this is the well-behaved side of that contract.
	perPage int
	// catalogVersion pins the plan to a version the live catalog will not agree
	// with, which is the only way a stale token is reachable through SQL — the
	// framework owns the envelope, so a worker cannot mint a bad fingerprint or
	// clear a seal even deliberately.
	catalogVersion *int64
	// ttlSeconds declares a split-token lifetime, for the client-side TTL floor.
	ttlSeconds *int64
	// cacheTTL advertises cacheability on the FIRST batch of every reader's
	// stream. Every reader advertises the same value: a result is one entry with
	// one lifetime, and a per-split TTL would be decided by whichever reader
	// happened to arrive first.
	cacheTTL *int64
	// batchStride > 0 gives each split a slice of the batch-index space
	// (ordinal * stride). Ascending claims then make the index monotonic per
	// reader across split boundaries, which is exactly the contract that makes
	// greedy claiming safe.
	batchStride int64
}

func (f *splitPlanner) Name() string { return f.name }

func (f *splitPlanner) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:          f.desc,
		Stability:            vgi.StabilityConsistent,
		SupportsSplits:       true,
		SupportsBatchIndex:   f.batchStride > 0,
		SplitTokenTTLSeconds: f.ttlSeconds,
	}
}

func (f *splitPlanner) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(splitArgs{}) }

func (f *splitPlanner) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(splitOutputSchema)
}

// Plan divides the scan. Only the payload is set: the framework stamps the
// consistency anchor, the bind fingerprint and (where a key exists) the seal, so
// a fixture cannot accidentally mint a token that skips them.
func (f *splitPlanner) Plan(params *vgi.BindParams, req vgi.PlanRequestWire) (*vgi.PlanResult, error) {
	var args splitArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	ranges := f.plan(args)

	// Pagination: hand out one window per call, cursoring on the page index.
	// The range list is regenerable from the bind arguments alone, so the cursor
	// needs to carry nothing else.
	var nextCursors [][]byte
	base := 0
	if f.perPage > 0 {
		page := 0
		if len(req.Cursor) == 8 {
			page = int(binary.LittleEndian.Uint64(req.Cursor))
		}
		base = page * f.perPage
		lo := base
		if lo > len(ranges) {
			lo = len(ranges)
		}
		hi := lo + f.perPage
		if hi > len(ranges) {
			hi = len(ranges)
		}
		if base+f.perPage < len(ranges) {
			cur := make([]byte, 8)
			binary.LittleEndian.PutUint64(cur, uint64(page+1))
			nextCursors = [][]byte{cur}
		}
		ranges = ranges[lo:hi]
	}

	splits := make([]vgi.ScanSplit, 0, len(ranges))
	var totalRows int64
	for i, r := range ranges {
		rows := r.Hi - r.Lo
		totalRows += rows
		bytes := rows * 8
		payload := encodeSplit(r)
		if f.batchStride > 0 {
			// The ordinal is what this split's index space keys on, so it has to
			// survive into redemption — which means it belongs in the payload.
			payload = encodeSplitOrdinal(int64(base+i), r)
		}
		splits = append(splits, vgi.ScanSplit{
			Payload:        payload,
			EstimatedRows:  &rows,
			RowsExact:      true,
			EstimatedBytes: &bytes,
		})
	}
	nSplits := int64(len(ranges))
	result := &vgi.PlanResult{
		Splits:               splits,
		EstimatedTotalSplits: &nSplits,
		EstimatedTotalRows:   &totalRows,
		NextCursors:          nextCursors,
	}
	if f.catalogVersion != nil {
		result.CatalogVersion = *f.catalogVersion
	}
	return result, nil
}

// Redeem is the explicit opt-in: a worker that mints splits must be able to
// redeem them. The ranges are read off params.SplitPayloads in NewState, so
// there is nothing to do here beyond declaring the capability.
func (f *splitPlanner) Redeem(params *vgi.InitParams) error { return nil }

func (f *splitPlanner) NewState(params *vgi.ProcessParams) (*splitState, error) {
	// No tokens at all means the client stopped planning (vgi_split_scans off).
	// A split-only worker has no way to know what to read then, and failing here
	// is the point: quietly returning zero rows would be A DIFFERENT ANSWER to
	// the same query, which is worse than an error. Distinct from a plan that
	// legitimately produced ZERO splits — there the client never inits at all.
	if params.SplitPayloads == nil {
		return nil, fmt.Errorf(
			"%s is split-only but was initialized with no split tokens; "+
				"vgi_split_scans is probably off, and this function has no "+
				"primary/secondary path to fall back to", f.name)
	}
	ranges := make([]splitRange, 0, len(params.SplitPayloads))
	ordinals := make([]int64, 0, len(params.SplitPayloads))
	for _, payload := range params.SplitPayloads {
		if f.batchStride > 0 {
			ord, r, err := decodeSplitOrdinal(payload)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, r)
			ordinals = append(ordinals, ord)
			continue
		}
		r, err := decodeSplit(payload)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	state := &splitState{Ranges: ranges, Ordinals: ordinals}
	if len(ranges) > 0 {
		state.Cur = ranges[0].Lo
	}
	return state, nil
}

// Process emits one batch per call, walking THIS reader's claimed ranges in
// order. A zero-row range is stepped over rather than reported as end-of-stream.
func (f *splitPlanner) Process(ctx context.Context, params *vgi.ProcessParams, state *splitState, out *vgirpc.OutputCollector) error {
	const maxBatch = int64(1024)
	for state.Idx < len(state.Ranges) {
		r := state.Ranges[state.Idx]
		if state.Cur >= r.Hi {
			state.Idx++
			state.EmittedInSplit = 0
			if state.Idx < len(state.Ranges) {
				state.Cur = state.Ranges[state.Idx].Lo
			}
			continue
		}
		size := r.Hi - state.Cur
		if size > maxBatch {
			size = maxBatch
		}
		start := state.Cur
		state.Cur += size
		arr := vgi.BuildInt64Array(size, func(i int64) int64 { return f.rowAt(r, start+i) })
		defer arr.Release()

		md := map[string]string{}
		if f.cacheTTL != nil && !state.CacheAdvertised {
			// The FIRST batch of this reader's stream — the only one the client
			// reads freshness from. Every reader advertises the same value,
			// because the result is one entry with one lifetime and a per-split
			// TTL would be decided by whichever reader arrived first.
			state.CacheAdvertised = true
			md[vgi.CacheTTLKey] = strconv.FormatInt(*f.cacheTTL, 10)
		}
		if f.batchStride > 0 {
			ordinal := int64(state.Idx)
			if state.Idx < len(state.Ordinals) {
				ordinal = state.Ordinals[state.Idx]
			}
			md[metaBatchIndexKey] = strconv.FormatInt(
				ordinal*f.batchStride+state.EmittedInSplit, 10)
			state.EmittedInSplit++
		}
		if len(md) == 0 {
			return out.EmitArrays([]arrow.Array{arr}, size)
		}
		schema := out.ProcessSchema
		if schema == nil {
			schema = splitOutputSchema
		}
		batch := array.NewRecordBatch(schema, []arrow.Array{arr}, size)
		defer batch.Release()
		return out.EmitWithMetadata(batch, md)
	}
	// Every claimed range is drained: signal end-of-stream explicitly. Note this
	// is reached only when the reader's OWN claims are exhausted — a zero-row
	// range is stepped over by the loop above, never mistaken for EOS.
	return out.Finish()
}

// --- the fixtures ---------------------------------------------------------

func identityRow(_ splitRange, i int64) int64 { return i }

// NewSplitSequenceFunction is the parity twin: split_sequence(n) must equal
// sequence(n) row for row.
func NewSplitSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name:  "split_sequence",
		desc:  "Split-capable twin of sequence(n): 0..n-1 divided into `splits` ranges",
		plan:  func(a splitArgs) []splitRange { return splitRanges(a.N, a.Splits) },
		rowAt: identityRow,
	})
}

// NewSplitZeroFunction returns NO splits. Legal, and it must produce an empty
// result rather than a crash — a fully-pruned scan reaches exactly this.
func NewSplitZeroFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name:  "split_zero",
		desc:  "Returns zero splits: a legal empty result, not an error",
		plan:  func(a splitArgs) []splitRange { return nil },
		rowAt: identityRow,
	})
}

// NewSplitEmptyRangesFunction interleaves EMPTY splits between non-empty ones.
// This is the shape that silently truncates a scan if a reader mistakes an empty
// split for end-of-stream, and it is far likelier in practice than zero splits.
func NewSplitEmptyRangesFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name: "split_empty_ranges",
		desc: "Some splits yield zero rows; the scan must not end early",
		plan: func(a splitArgs) []splitRange {
			var out []splitRange
			for _, r := range splitRanges(a.N, a.Splits) {
				out = append(out, splitRange{Lo: r.Lo, Hi: r.Lo}) // empty
				out = append(out, r)
			}
			return out
		},
		rowAt: identityRow,
	})
}

// NewSplitSkewedFunction makes ONE split ~100x the rest, so greedy per-split
// claiming is distinguishable from static assignment: under greedy claiming the
// fast readers keep working while one reader owns the big split.
func NewSplitSkewedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name: "split_skewed",
		desc: "One split ~100x the others: exercises greedy claiming under skew",
		plan: func(a splitArgs) []splitRange {
			if a.N <= 0 || a.Splits <= 0 {
				return nil
			}
			// The first split takes ~99% of the rows; the rest divide the tail.
			head := a.N * 99 / 100
			out := []splitRange{{Lo: 0, Hi: head}}
			for _, r := range splitRanges(a.N-head, a.Splits-1) {
				out = append(out, splitRange{Lo: head + r.Lo, Hi: head + r.Hi})
			}
			return out
		},
		rowAt: identityRow,
	})
}

// NewSplitManyFunction returns far more splits than reader threads, which forces
// sequential re-init on a REUSED connection — the path where a split-init
// failure would otherwise pool a connection with an unanswered init in flight.
func NewSplitManyFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name: "split_many",
		desc: "Far more splits than threads: exercises greedy claiming and re-init",
		plan: func(a splitArgs) []splitRange {
			k := a.Splits
			if k <= 0 {
				k = 1000
			}
			return splitRanges(a.N, k)
		},
		rowAt: identityRow,
	})
}

// --- fixtures that exercise the CLIENT's split machinery -------------------

type splitFailArgs struct {
	N          int64 `vgi:"name=n,ge=0,doc=Number of rows to produce"`
	Splits     int64 `vgi:"name=splits,default=4,ge=0,doc=How many splits to divide the scan into"`
	FailAt     int64 `vgi:"name=fail_at,default=-1,doc=Split ordinal to fail on; -1 never fails"`
	FailInInit bool  `vgi:"name=fail_in_init,default=false,doc=Fail during the split's init rather than mid-stream"`
}

// SplitFailAtFunction fails on a chosen split, in either of the two places that
// matter. They are genuinely different failure paths, not variations:
//
//   - fail_in_init fails while REDEEMING the token, before any row is produced.
//     The client must not return that connection to the pool — the init request
//     is on the wire with no answer, so a later checkout would read this split's
//     init response as its own stream header: silent cross-query corruption on
//     the pool-enabled default.
//   - Otherwise it fails MID-STREAM, after emitting rows, so the capture is
//     genuinely partial when it dies. A partial result committed as complete is
//     the failure class the never-partial gate exists to prevent.
type SplitFailAtFunction struct{}

type splitFailState struct {
	Ranges   []splitRange
	Ordinals []int64
	Idx      int
	Cur      int64
	FailAt   int64
}

func (f *SplitFailAtFunction) Name() string { return "split_fail_at" }

func (f *SplitFailAtFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:    "Fails on a chosen split, at init or mid-stream",
		Stability:      vgi.StabilityConsistent,
		SupportsSplits: true,
	}
}

func (f *SplitFailAtFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(splitFailArgs{})
}

func (f *SplitFailAtFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(splitOutputSchema)
}

// encodeFail packs ordinal ‖ lo ‖ hi. The ordinal is what fail_at names, so the
// test says what it means regardless of how the rows happen to divide.
func encodeFail(ordinal int64, r splitRange) []byte {
	out := binary.LittleEndian.AppendUint64(nil, uint64(ordinal))
	out = binary.LittleEndian.AppendUint64(out, uint64(r.Lo))
	return binary.LittleEndian.AppendUint64(out, uint64(r.Hi))
}

func decodeFail(payload []byte) (int64, splitRange, error) {
	if len(payload) != 24 {
		return 0, splitRange{}, fmt.Errorf("fail payload must be 24 bytes, got %d", len(payload))
	}
	return int64(binary.LittleEndian.Uint64(payload[0:8])), splitRange{
		Lo: int64(binary.LittleEndian.Uint64(payload[8:16])),
		Hi: int64(binary.LittleEndian.Uint64(payload[16:24])),
	}, nil
}

func (f *SplitFailAtFunction) Plan(params *vgi.BindParams, req vgi.PlanRequestWire) (*vgi.PlanResult, error) {
	var args splitFailArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	ranges := splitRanges(args.N, args.Splits)
	splits := make([]vgi.ScanSplit, 0, len(ranges))
	for i, r := range ranges {
		rows := r.Hi - r.Lo
		splits = append(splits, vgi.ScanSplit{
			Payload:       encodeFail(int64(i), r),
			EstimatedRows: &rows,
			RowsExact:     true,
		})
	}
	n := int64(len(ranges))
	return &vgi.PlanResult{Splits: splits, EstimatedTotalSplits: &n}, nil
}

// Redeem is where the init-time failure lands, so the client's
// connection-poisoning path is exercised rather than the mid-stream one.
func (f *SplitFailAtFunction) Redeem(params *vgi.InitParams) error {
	var args splitFailArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return err
	}
	if !args.FailInInit {
		return nil
	}
	for _, payload := range params.SplitPayloads {
		ordinal, _, err := decodeFail(payload)
		if err != nil {
			return err
		}
		if ordinal == args.FailAt {
			return fmt.Errorf("split %d refuses to initialize (fixture)", ordinal)
		}
	}
	return nil
}

func (f *SplitFailAtFunction) NewState(params *vgi.ProcessParams) (*splitFailState, error) {
	if params.SplitPayloads == nil {
		return nil, fmt.Errorf("split_fail_at is split-only but was initialized with no split tokens")
	}
	var args splitFailArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	state := &splitFailState{FailAt: args.FailAt}
	for _, payload := range params.SplitPayloads {
		ordinal, r, err := decodeFail(payload)
		if err != nil {
			return nil, err
		}
		state.Ranges = append(state.Ranges, r)
		state.Ordinals = append(state.Ordinals, ordinal)
	}
	if len(state.Ranges) > 0 {
		state.Cur = state.Ranges[0].Lo
	}
	return state, nil
}

func (f *SplitFailAtFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *splitFailState, out *vgirpc.OutputCollector) error {
	for state.Idx < len(state.Ranges) {
		r := state.Ranges[state.Idx]
		if state.Cur >= r.Hi {
			state.Idx++
			if state.Idx < len(state.Ranges) {
				state.Cur = state.Ranges[state.Idx].Lo
			}
			continue
		}
		// Fail AFTER at least one row of this split has gone out, so the
		// never-partial gate is tested against a genuinely partial capture
		// rather than an empty one.
		if state.FailAt >= 0 && state.Ordinals[state.Idx] == state.FailAt && state.Cur > r.Lo {
			return fmt.Errorf("split %d failed mid-stream (fixture)", state.FailAt)
		}
		size := r.Hi - state.Cur
		if size > 8 {
			size = 8
		}
		start := state.Cur
		state.Cur += size
		arr := vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		defer arr.Release()
		return out.EmitArrays([]arrow.Array{arr}, size)
	}
	return out.Finish()
}

// NewSplitFailAtFunction registers the failure fixture.
func NewSplitFailAtFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitFailState](&SplitFailAtFunction{})
}

// SplitEndlessCursorFunction paginates forever: every plan page returns a cursor
// and never exhausts it.
//
// A worker can hang a client this way by accident as easily as on purpose, and
// the failure mode is the bad one: a client that stopped early would scan a
// PARTIAL enumeration and report it as the whole answer. The client must hit its
// page cap and throw an error naming it — never truncate and proceed.
type SplitEndlessCursorFunction struct{ splitPlanner }

func NewSplitEndlessCursorFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&SplitEndlessCursorFunction{splitPlanner{
		name:  "split_endless_cursor",
		desc:  "Paginates forever: the client must hit its page cap, not truncate",
		plan:  func(a splitArgs) []splitRange { return []splitRange{{Lo: 0, Hi: 1}} },
		rowAt: identityRow,
	}})
}

// Plan always hands back one split and a fresh cursor.
func (f *SplitEndlessCursorFunction) Plan(params *vgi.BindParams, req vgi.PlanRequestWire) (*vgi.PlanResult, error) {
	page := 0
	if req.Cursor != nil {
		page = len(req.Cursor)
	}
	next := make([]byte, page+1)
	for i := range next {
		next[i] = 'x'
	}
	return &vgi.PlanResult{
		Splits:      []vgi.ScanSplit{{Payload: encodeSplit(splitRange{Lo: 0, Hi: 1})}},
		NextCursors: [][]byte{next},
	}, nil
}

var splitEchoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "split_ordinal", Type: arrow.PrimitiveTypes.Int64},
	{Name: "saw_filters", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "n_projection", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type splitEchoArgs struct {
	Splits int64 `vgi:"name=splits,default=3,ge=1,doc=How many splits to report"`
}

// SplitEchoFiltersFunction reports, per split, what pushdown the PLAN call
// actually received.
//
// A row-count assertion cannot catch a pushdown regression — the rows are the
// same either way — so this fixture makes the pushdown itself the data. What it
// reports is recorded at PLAN time and baked into each split's payload, which is
// the claim under test: filters and projection must reach plan(), not merely
// reach the per-split init() afterwards.
type SplitEchoFiltersFunction struct{}

type splitEchoState struct {
	Ordinals []int64
	SawFilt  []bool
	NProj    []int64
	Done     bool
}

func (f *SplitEchoFiltersFunction) Name() string { return "split_echo_filters" }

func (f *SplitEchoFiltersFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:    "Reports the pushdown each split's plan() call saw",
		Stability:      vgi.StabilityConsistent,
		SupportsSplits: true,
		// FilterPushdown declares that this worker APPLIES the filter, so DuckDB
		// stops re-checking it above the scan. Declaring it while only reporting
		// the filter would be the "wrong answers if declared falsely" hazard in
		// miniature. AutoApplyFilters makes the declaration true.
		FilterPushdown:   true,
		AutoApplyFilters: true,
	}
}

func (f *SplitEchoFiltersFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(splitEchoArgs{})
}

func (f *SplitEchoFiltersFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(splitEchoSchema)
}

func (f *SplitEchoFiltersFunction) Plan(params *vgi.BindParams, req vgi.PlanRequestWire) (*vgi.PlanResult, error) {
	var args splitEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	sawFilters := int64(0)
	if req.PushdownFilters != nil {
		sawFilters = 1
	}
	nProj := int64(0)
	if req.ProjectionIDs != nil {
		nProj = int64(len(*req.ProjectionIDs))
	}
	splits := make([]vgi.ScanSplit, 0, args.Splits)
	for i := int64(0); i < args.Splits; i++ {
		payload := binary.LittleEndian.AppendUint64(nil, uint64(i))
		payload = binary.LittleEndian.AppendUint64(payload, uint64(sawFilters))
		payload = binary.LittleEndian.AppendUint64(payload, uint64(nProj))
		splits = append(splits, vgi.ScanSplit{Payload: payload})
	}
	n := args.Splits
	return &vgi.PlanResult{Splits: splits, EstimatedTotalSplits: &n}, nil
}

func (f *SplitEchoFiltersFunction) Redeem(params *vgi.InitParams) error { return nil }

func (f *SplitEchoFiltersFunction) NewState(params *vgi.ProcessParams) (*splitEchoState, error) {
	if params.SplitPayloads == nil {
		return nil, fmt.Errorf("split_echo_filters is split-only but was initialized with no split tokens")
	}
	state := &splitEchoState{}
	for _, payload := range params.SplitPayloads {
		if len(payload) != 24 {
			return nil, fmt.Errorf("echo payload must be 24 bytes, got %d", len(payload))
		}
		state.Ordinals = append(state.Ordinals, int64(binary.LittleEndian.Uint64(payload[0:8])))
		state.SawFilt = append(state.SawFilt, binary.LittleEndian.Uint64(payload[8:16]) != 0)
		state.NProj = append(state.NProj, int64(binary.LittleEndian.Uint64(payload[16:24])))
	}
	return state, nil
}

func (f *SplitEchoFiltersFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *splitEchoState, out *vgirpc.OutputCollector) error {
	if state.Done || len(state.Ordinals) == 0 {
		return out.Finish()
	}
	state.Done = true
	n := int64(len(state.Ordinals))
	ordinals := vgi.BuildInt64Array(n, func(i int64) int64 { return state.Ordinals[i] })
	defer ordinals.Release()
	nproj := vgi.BuildInt64Array(n, func(i int64) int64 { return state.NProj[i] })
	defer nproj.Release()

	b := array.NewBooleanBuilder(memory.NewGoAllocator())
	defer b.Release()
	for _, v := range state.SawFilt {
		b.Append(v)
	}
	saw := b.NewArray()
	defer saw.Release()

	return out.EmitArrays([]arrow.Array{ordinals, saw, nproj}, n)
}

// NewSplitEchoFiltersFunction registers the pushdown-reporting fixture.
func NewSplitEchoFiltersFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitEchoState](&SplitEchoFiltersFunction{})
}

// --- fixtures completing the cross-SDK set --------------------------------
//
// Each is the twin of a vgi-python fixture of the same name. The shared SQL
// suite runs unchanged against every SDK's worker, so a wire disagreement shows
// up as the same named test failing under one of them — which only works if the
// fixtures agree on behaviour, not merely on name.

// NewSplitPaginatedFunction enumerates its plan over several pages, each
// disjoint from the last. Pagination is how a worker keeps one plan response
// bounded when a scan has very many splits; what has to hold is that the pages
// compose, so each split appears exactly once across the whole enumeration.
//
// Disjointness is the worker's obligation and no client checks it — a dedup was
// tried and removed, because it needed a copy of every token, it compared token
// bytes and so could never fire on a keyed worker, and the most a client can do
// with a duplicate is refuse anyway. This is the well-behaved side of that.
func NewSplitPaginatedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name:    "split_paginated",
		desc:    "Plan enumerated over several disjoint pages",
		plan:    func(a splitArgs) []splitRange { return splitRanges(a.N, a.Splits) },
		rowAt:   identityRow,
		perPage: 4,
	})
}

// NewSplitStalePlanFunction pins its plan to a catalog version that has moved
// on. This is the only way a bad split token is reachable through SQL, and
// deliberately so: the framework owns the envelope, so a worker cannot mint a
// wrong fingerprint or clear a seal even on purpose. What it CAN do is plan
// against a snapshot that is no longer current — exactly the situation
// SPLIT_SNAPSHOT_EXPIRED names.
//
// The refusal must stay distinguishable from SPLIT_TOKEN_INVALID, because only
// this one means "re-run the query": re-planning mints a valid token, whereas
// re-running a wrongly-bound one just reproduces it.
func NewSplitStalePlanFunction() vgi.TableFunction {
	// Any value the live catalog will not report. The fixture catalog's version
	// is small, so a large constant is reliably "not current" without depending
	// on what that version happens to be.
	stale := int64(987654321)
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name:           "split_stale_plan",
		desc:           "Plans against a catalog version that is not the live one",
		plan:           func(a splitArgs) []splitRange { return splitRanges(a.N, a.Splits) },
		rowAt:          identityRow,
		catalogVersion: &stale,
	})
}

// NewSplitShortTtlFunction declares a split-token lifetime shorter than any
// client's scheduling horizon. An expired token is a failed query, not a
// degradation: nothing re-plans when one expires, because a distributed engine
// retries the serialized task it was handed and has no path back to the
// planner. So the only useful moment to notice a too-short lifetime is BEFORE
// the plan is issued.
//
// One second is unusable everywhere: even DuckDB, whose horizon is the shortest
// of any engine because it plans at execution start, can take longer than that
// to reach a split.
func NewSplitShortTtlFunction() vgi.TableFunction {
	ttl := int64(1)
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name:       "split_short_ttl",
		desc:       "Declares a 1s split-token TTL, below any client horizon",
		plan:       func(a splitArgs) []splitRange { return splitRanges(a.N, a.Splits) },
		rowAt:      identityRow,
		ttlSeconds: &ttl,
	})
}

// NewSplitBatchIndexFunction is split-capable AND supports_batch_index, which
// together are a contract. A batch index must be globally monotonic per reader,
// and greedy per-split claiming re-initializes the same connection for each
// split — so every split starts a fresh stream, and a worker that restarted its
// numbering per split would hand one reader a DECREASING index.
//
// What makes it work is that the client's claim counter hands each reader
// strictly ASCENDING split indices, so a worker deriving its index from the
// split's position in a globally-ordered space is monotonic by construction.
// That is the whole reason claiming is greedy rather than grouped, and it is NOT
// something multi-token init provides — a group's tokens carry no ordering.
//
// The stride bounds how many batches one split may emit before colliding with
// the next, and VGI_BATCH_INDEX_CAP bounds the product, so choosing a stride is
// really choosing cap / n_splits.
func NewSplitBatchIndexFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name:        "split_batch_index",
		desc:        "Split-capable with per-split batch_index space",
		plan:        func(a splitArgs) []splitRange { return splitRanges(a.N, a.Splits) },
		rowAt:       identityRow,
		batchStride: 1000,
	})
}

// NewSplitCacheableFunction makes a split scan's result cacheable, so the
// never-partial gate becomes assertable.
//
// The result cache knows nothing about splits, deliberately: its key describes
// the QUERY — identity, filters, projection, catalog version — while splits are
// how the rows were produced. What that makes testable is that a scan abandoned
// partway (a LIMIT satisfied early, or an error) commits NOTHING: storing what
// was captured would put a SUBSET under a key claiming to be the whole answer,
// and every later identical query would return missing rows with no error.
func NewSplitCacheableFunction() vgi.TableFunction {
	ttl := int64(300)
	return vgi.AsTableFunction[splitState](&splitPlanner{
		name:     "split_cacheable",
		desc:     "Split-capable and cacheable, for the never-partial gate",
		plan:     func(a splitArgs) []splitRange { return splitRanges(a.N, a.Splits) },
		rowAt:    identityRow,
		cacheTTL: &ttl,
	})
}

// renderFiltersCanonical is the CANONICAL cross-SDK rendering of a pushed-down
// filter set.
//
// Every SDK must produce this byte-for-byte, because the shared SQL suite
// asserts on the string. A language's own debug formatting cannot be used —
// Python's repr(PushdownFilters) is Python-shaped and no other SDK can
// reproduce it, so a test asserting it could only ever pass against that one
// worker, which defeats the point of a shared suite.
//
// So it renders from GetColumnBounds, which every SDK mirrors: for each filtered
// column in sorted order, col>=min and/or col<=max, joined by ",". Bounds are
// normalized to INCLUSIVE integers, because that is the only form every SDK can
// produce (Rust's ColumnBounds carries no inclusive flag at all). Values are
// included deliberately: without them a tightening Top-N filter and a loose one
// render identically and the test cannot tell them apart.
func renderFiltersCanonical(pf *vgi.PushdownFilters) string {
	if pf == nil || len(pf.Filters) == 0 {
		return "(none)"
	}
	seen := map[string]bool{}
	var cols []string
	var walk func(f vgi.Filter)
	walk = func(f vgi.Filter) {
		// Recursive: a compound predicate arrives as And([Constant, Constant]),
		// so collecting only top-level columns renders "(none)" for exactly the
		// multi-clause filters worth asserting on.
		switch t := f.(type) {
		case *vgi.AndFilter:
			for _, c := range t.Children {
				walk(c)
			}
		case *vgi.OrFilter:
			for _, c := range t.Children {
				walk(c)
			}
		default:
			if name := f.ColumnName(); name != "" && !seen[name] {
				seen[name] = true
				cols = append(cols, name)
			}
		}
	}
	for _, f := range pf.Filters {
		walk(f)
	}
	sort.Strings(cols)

	var parts []string
	for _, c := range cols {
		b := pf.GetColumnBounds(c)
		if b == nil {
			continue
		}
		if b.MinValue != nil {
			if v, ok := scalarInt64(b.MinValue); ok {
				if !b.MinInclusive {
					v++
				}
				parts = append(parts, fmt.Sprintf("%s>=%d", c, v))
			}
		}
		if b.MaxValue != nil {
			if v, ok := scalarInt64(b.MaxValue); ok {
				if !b.MaxInclusive {
					v--
				}
				parts = append(parts, fmt.Sprintf("%s<=%d", c, v))
			}
		}
	}
	if len(parts) == 0 {
		return "(none)"
	}
	return strings.Join(parts, ",")
}

// scalarInt64 integer-coerces a bound value. The canonical rendering is defined
// over integers, matching the other SDKs' integer-coerced bounds.
func scalarInt64(s scalar.Scalar) (int64, bool) {
	switch v := s.(type) {
	case *scalar.Int8:
		return int64(v.Value), true
	case *scalar.Int16:
		return int64(v.Value), true
	case *scalar.Int32:
		return int64(v.Value), true
	case *scalar.Int64:
		return v.Value, true
	case *scalar.Uint8:
		return int64(v.Value), true
	case *scalar.Uint16:
		return int64(v.Value), true
	case *scalar.Uint32:
		return int64(v.Value), true
	case *scalar.Uint64:
		return int64(v.Value), true
	case *scalar.Float32:
		return int64(v.Value), true
	case *scalar.Float64:
		return int64(v.Value), true
	}
	return 0, false
}

var splitDynFilterSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

// SplitDynamicFilterFunction echoes the DYNAMIC filter each tick carried, per
// split.
//
// A plan is built from STATIC filters only — join-key values are not known when
// the plan RPC fires, so they cannot prune the split SET. They arrive later, per
// tick, and prune WITHIN each split. Both halves have to keep working once a
// reader re-initializes the same connection per split: the tick filter state is
// a property of the connection, and a split that lost it would silently stop
// pruning.
//
// "Silently" is the operative word, and it is why this reports the filter as
// DATA rather than leaving the test to infer it from row counts. A scan that
// stopped receiving dynamic filters returns exactly the same rows — DuckDB
// re-checks the predicate above the scan — just after shipping more of them.
type SplitDynamicFilterFunction struct{ splitPlanner }

func NewSplitDynamicFilterFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitState](&SplitDynamicFilterFunction{splitPlanner{
		name:  "split_dynamic_filter",
		desc:  "Echoes the dynamic filter each tick carried, per split",
		plan:  func(a splitArgs) []splitRange { return splitRanges(a.N, a.Splits) },
		rowAt: identityRow,
	}})
}

func (f *SplitDynamicFilterFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        f.desc,
		Stability:          vgi.StabilityConsistent,
		SupportsSplits:     true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		ProjectionPushdown: true,
	}
}

func (f *SplitDynamicFilterFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(splitDynFilterSchema)
}

// Cardinality reports the row count, which decides which side of a join this
// lands on. Without it DuckDB assumes a default (large) cardinality and puts the
// scan on the BUILD side of a hash join — where no join-key IN filter is pushed
// into it, because the filter goes to the probe side. The scan then reads
// everything and DuckDB filters above it: right answers, no pushdown, and
// nothing in the result to say so. Nothing about splits causes that; it is the
// ordinary consequence of a table function declining to estimate itself.
func (f *SplitDynamicFilterFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args splitArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.N, Max: args.N}, nil
}

func (f *SplitDynamicFilterFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *splitState, out *vgirpc.OutputCollector) error {
	const maxBatch = int64(4)
	// Read the same way the Python and Rust twins do: the INIT request carries
	// both the serialized filters and the join keys, and MERGING them is what
	// produces the IN filter a join pushes down. CurrentPushdownFilters alone
	// carries the per-tick filters but not the join keys, so a join rendered as
	// "(none)" — the pushdown arrived and the fixture could not see it.
	merged := params.CurrentPushdownFilters
	if params.PushdownFilters != nil {
		if pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys); err == nil && pf != nil {
			merged = pf
		}
	}
	rendered := renderFiltersCanonical(merged)
	for state.Idx < len(state.Ranges) {
		r := state.Ranges[state.Idx]
		if state.Cur >= r.Hi {
			state.Idx++
			if state.Idx < len(state.Ranges) {
				state.Cur = state.Ranges[state.Idx].Lo
			}
			continue
		}
		size := r.Hi - state.Cur
		if size > maxBatch {
			size = maxBatch
		}
		start := state.Cur
		state.Cur += size

		nb := array.NewInt64Builder(memory.DefaultAllocator)
		defer nb.Release()
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		defer sb.Release()
		for i := int64(0); i < size; i++ {
			nb.Append(start + i)
			sb.Append(rendered)
		}
		na := nb.NewArray()
		defer na.Release()
		sa := sb.NewArray()
		defer sa.Release()
		// Emit exactly the columns the stream expects, chosen by NAME.
		//
		// EmitArrays builds the batch against ProcessSchema (or the stream schema
		// when that is unset), and which of the two is narrowed depends on how
		// projection was pushed — so neither "always emit both" nor "always emit
		// the projected set" is right on its own. Selecting by name is correct
		// under either, and fails loudly rather than silently mis-ordering if the
		// projection ever contains something unexpected.
		// Which schema the stream expects depends on how projection was pushed:
		// ProcessSchema when the framework installed a projecting interceptor,
		// otherwise the (already narrowed) OutputSchema. Reading the wrong one is
		// a "number of columns/fields mismatch" at the first projected query, so
		// both are consulted in order rather than either being assumed.
		schema := out.ProcessSchema
		if schema == nil {
			schema = params.OutputSchema
		}
		if schema == nil {
			schema = splitDynFilterSchema
		}
		cols := make([]arrow.Array, 0, len(schema.Fields()))
		for _, fld := range schema.Fields() {
			switch fld.Name {
			case "n":
				cols = append(cols, na)
			case "pushed_filters":
				cols = append(cols, sa)
			default:
				return fmt.Errorf("split_dynamic_filter: unexpected projected column %q", fld.Name)
			}
		}
		return out.EmitArrays(cols, size)
	}
	return out.Finish()
}

var splitPartCountries = []string{"US", "DE", "JP", "BR"}

var splitPartitionedFields = []arrow.Field{
	vgi.PartitionField("country", arrow.BinaryTypes.String, false),
	{Name: "sales", Type: arrow.PrimitiveTypes.Int64},
}

var splitPartitionedSchema = arrow.NewSchema(splitPartitionedFields, nil)

// Only the partition-ANNOTATED columns. Handing EmitPartitioned the whole field
// list made `sales` a partition key too, and under SINGLE_VALUE that is an error
// the moment two rows differ — which they always do here, by design.
var splitPartitionKeyFields = splitPartitionedFields[:1]

type splitPartArgs struct {
	RowsPerCountry int64 `vgi:"name=rows_per_country,default=5,ge=0,doc=Rows in each partition"`
}

type splitPartState struct {
	Indices []int64
	At      int
	Rows    int64
}

// SplitPartitionedFunction is one split per partition — the shape a partitioned
// table naturally takes.
//
// A partition and a split are different things that usually coincide: a
// partition is a property of the DATA (every row here shares a value), a split
// is a unit of WORK. A worker that already stores data per partition has its
// split boundaries handed to it, so this is the common case rather than a
// contrived one.
//
// What needs asserting is that the two survive each other. Splits are claimed
// greedily, in an order nobody chose, by readers that each end up holding
// several — so the association between a batch and the partition value it
// carries has to hold through re-init on a reused connection and across the
// boundary where one reader moves from one partition to the next. Losing it does
// not raise: it produces a GROUP BY that silently mixes partitions.
type SplitPartitionedFunction struct{}

func NewSplitPartitionedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[splitPartState](&SplitPartitionedFunction{})
}

func (f *SplitPartitionedFunction) Name() string { return "split_partitioned" }

func (f *SplitPartitionedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:    "One split per partition, with partition values on each batch",
		Stability:      vgi.StabilityConsistent,
		SupportsSplits: true,
		PartitionKind:  vgi.PartitionKindSingleValuePartitions,
	}
}

func (f *SplitPartitionedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(splitPartArgs{})
}

func (f *SplitPartitionedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(splitPartitionedSchema)
}

// Plan names each partition by INDEX, so a redemption reads the same partition
// however many times it runs and in whichever process.
func (f *SplitPartitionedFunction) Plan(params *vgi.BindParams, req vgi.PlanRequestWire) (*vgi.PlanResult, error) {
	splits := make([]vgi.ScanSplit, 0, len(splitPartCountries))
	for i := range splitPartCountries {
		payload := make([]byte, 8)
		binary.LittleEndian.PutUint64(payload, uint64(i))
		splits = append(splits, vgi.ScanSplit{Payload: payload})
	}
	n := int64(len(splits))
	return &vgi.PlanResult{Splits: splits, EstimatedTotalSplits: &n}, nil
}

func (f *SplitPartitionedFunction) Redeem(params *vgi.InitParams) error { return nil }

func (f *SplitPartitionedFunction) NewState(params *vgi.ProcessParams) (*splitPartState, error) {
	if params.SplitPayloads == nil {
		return nil, fmt.Errorf(
			"split_partitioned is split-only but was initialized with no split tokens")
	}
	var args splitPartArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	idxs := make([]int64, 0, len(params.SplitPayloads))
	for _, p := range params.SplitPayloads {
		if len(p) != 8 {
			return nil, fmt.Errorf("partition split payload must be 8 bytes, got %d", len(p))
		}
		idxs = append(idxs, int64(binary.LittleEndian.Uint64(p)))
	}
	return &splitPartState{Indices: idxs, Rows: args.RowsPerCountry}, nil
}

func (f *SplitPartitionedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *splitPartState, out *vgirpc.OutputCollector) error {
	// A partition with zero rows is STEPPED OVER, never reported as
	// end-of-stream — the same rule every split fixture follows, and here it is
	// reachable through rows_per_country := 0.
	for state.At < len(state.Indices) {
		ci := state.Indices[state.At]
		state.At++
		if state.Rows <= 0 || ci < 0 || int(ci) >= len(splitPartCountries) {
			continue
		}
		// Each partition's values are offset by its own index, so swapping two
		// splits' labels MOVES the per-partition sums. With identical values
		// everywhere a mislabelled partition would be invisible in the totals.
		base := ci * 100
		cb := array.NewStringBuilder(memory.DefaultAllocator)
		defer cb.Release()
		sb := array.NewInt64Builder(memory.DefaultAllocator)
		defer sb.Release()
		for i := int64(1); i <= state.Rows; i++ {
			cb.Append(splitPartCountries[ci])
			sb.Append(base + i)
		}
		ca := cb.NewArray()
		defer ca.Release()
		sa := sb.NewArray()
		defer sa.Release()
		batch := array.NewRecordBatch(splitPartitionedSchema, []arrow.Array{ca, sa}, state.Rows)
		defer batch.Release()
		// SINGLE_VALUE: min == max within the batch, which is what lets the
		// client read row 0 as the exact partition key.
		return vgi.EmitPartitioned(out, batch, splitPartitionKeyFields,
			vgi.PartitionKindSingleValuePartitions, nil)
	}
	return out.Finish()
}
