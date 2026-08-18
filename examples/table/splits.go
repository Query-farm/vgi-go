// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
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
}

// --- the shared machinery -------------------------------------------------

type splitPlanner struct {
	name  string
	desc  string
	plan  func(args splitArgs) []splitRange
	rowAt func(r splitRange, i int64) int64
}

func (f *splitPlanner) Name() string { return f.name }

func (f *splitPlanner) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:    f.desc,
		Stability:      vgi.StabilityConsistent,
		SupportsSplits: true,
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
	splits := make([]vgi.ScanSplit, 0, len(ranges))
	var totalRows int64
	for _, r := range ranges {
		rows := r.Hi - r.Lo
		totalRows += rows
		bytes := rows * 8
		splits = append(splits, vgi.ScanSplit{
			Payload:        encodeSplit(r),
			EstimatedRows:  &rows,
			RowsExact:      true,
			EstimatedBytes: &bytes,
		})
	}
	nSplits := int64(len(ranges))
	return &vgi.PlanResult{
		Splits:               splits,
		EstimatedTotalSplits: &nSplits,
		EstimatedTotalRows:   &totalRows,
	}, nil
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
	for _, payload := range params.SplitPayloads {
		r, err := decodeSplit(payload)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	state := &splitState{Ranges: ranges}
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
		return out.EmitArrays([]arrow.Array{arr}, size)
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
