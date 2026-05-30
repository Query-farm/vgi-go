// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// lateMatRowIDName is the rowid column name. The C++ extension resolves a pushed
// rowid filter to this name on the wire, so the worker matches it by name.
const lateMatRowIDName = "row_id"

// lateMatScramble is an odd multiplier (coprime with any reasonable count) used
// to turn the monotonic row index into a scattered ordering key, so a Top-N on
// `ord` yields scattered survivor rowids (driving the exact IN-list pushdown).
const lateMatScramble = 2654435761

const lateMatModulus = 1_000_000_007

// lateMatRowIDMetadata marks the rowid virtual column. Late materialization only
// activates when the output schema carries a column tagged is_row_id.
var lateMatRowIDMetadata = arrow.NewMetadata([]string{"is_row_id"}, []string{""})

// lateMaterializationOutputSchema mirrors vgi-python's LateMaterializationFunction:
// {row_id int64 [is_row_id], ord int64, payload utf8, pushed utf8}.
var lateMaterializationOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: lateMatRowIDName, Type: arrow.PrimitiveTypes.Int64, Metadata: lateMatRowIDMetadata},
	{Name: "ord", Type: arrow.PrimitiveTypes.Int64},
	{Name: "payload", Type: arrow.BinaryTypes.String},
	{Name: "pushed", Type: arrow.BinaryTypes.String},
}, nil)

// LateMaterializationFunction is a rowid-bearing generator that participates in
// DuckDB's late-materialization optimizer. When Meta.late_materialization is
// advertised and the table has a rowid virtual column, a TOP_N/LIMIT/SAMPLE over
// the scan is rewritten into a SEMI join on the rowid: a narrow ordering scan
// selects survivors, then the wide scan re-fetches their columns with the
// surviving rowids pushed down as a filter. The `pushed` witness column echoes
// the rowid filter the worker received, proving the pushdown reached it.
type LateMaterializationFunction struct{}

var _ vgi.TypedTableFunc[lateMaterializationState] = (*LateMaterializationFunction)(nil)

func (f *LateMaterializationFunction) Name() string { return "late_materialization" }

func (f *LateMaterializationFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:         "Rowid generator that participates in late materialization",
		Stability:           vgi.StabilityConsistent,
		ProjectionPushdown:  true,
		FilterPushdown:      true,
		AutoApplyFilters:    true,
		LateMaterialization: true,
		Categories:          []string{"generator", "diagnostic"},
	}
}

// lateMaterializationArgs is the typed argument schema for late_materialization().
type lateMaterializationArgs struct {
	Count         int64 `vgi:"pos=0,doc=Number of rows to generate"`
	BatchSize     int64 `vgi:"default=2048,doc=Batch size for output"`
	DupRowID      bool  `vgi:"default=false,doc=Emit a deliberately non-unique row_id (index // 2)"`
	NullOrdStride int64 `vgi:"default=0,doc=Emit NULL ord every Nth row (0 = never)"`
}

func (f *LateMaterializationFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(lateMaterializationArgs{})
}

func (f *LateMaterializationFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(lateMaterializationOutputSchema)
}

func (f *LateMaterializationFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

// lateMaterializationState carries the row-emission cursor plus the cached witness
// string. All fields are gob-serialized with the rest of the state so they survive
// the HTTP state-token round-trip (which deserializes state without re-running
// NewState) — in particular the witness observed from the init-time filters.
type lateMaterializationState struct {
	vgi.BatchState
	DupRowID      bool
	NullOrdStride int64
	Witness       string
}

func (f *LateMaterializationFunction) NewState(params *vgi.ProcessParams) (*lateMaterializationState, error) {
	var args lateMaterializationArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	// For the wide probe scan, the SEMI join's build side completes before the
	// scan inits, so the surviving rowid range arrives as a concrete filter on
	// the init-time pushdown filters. Process() additionally latches anything
	// that shows up per-tick.
	witness := lateMatNoFilterWitness
	if params.PushdownFilters != nil {
		pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
		if err == nil && pf != nil {
			witness = lateMatRowIDWitness(pf)
		}
	}

	return &lateMaterializationState{
		BatchState:    vgi.NewBatchState(args.Count, args.BatchSize),
		DupRowID:      args.DupRowID,
		NullOrdStride: args.NullOrdStride,
		Witness:       witness,
	}, nil
}

func (f *LateMaterializationFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *lateMaterializationState, out *vgirpc.OutputCollector) error {
	// The surviving-rowid filter from late materialization is pushed as a dynamic
	// filter (populated after the SEMI join's build side completes), so it surfaces
	// on CurrentPushdownFilters per tick. Latch a non-empty result; guard against a
	// transient empty tick clobbering a witness we already captured.
	if params.CurrentPushdownFilters != nil {
		tickWitness := lateMatRowIDWitness(params.CurrentPushdownFilters)
		if tickWitness != lateMatNoFilterWitness || state.Witness == lateMatNoFilterWitness {
			state.Witness = tickWitness
		}
	}

	projected := vgi.ProjectedColumns(params.ProjectionIDs, lateMaterializationOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains(lateMatRowIDName) {
			colMap[lateMatRowIDName] = vgi.BuildInt64Array(size, func(i int64) int64 {
				idx := start + i
				if state.DupRowID {
					return idx / 2
				}
				return idx
			})
		}
		if projected.Contains("ord") {
			colMap["ord"] = buildOrdArray(size, start, state.NullOrdStride)
		}
		if projected.Contains("payload") {
			colMap["payload"] = vgi.BuildStringArray(size, func(i int64) string {
				return fmt.Sprintf("payload_%d", start+i)
			})
		}
		if projected.Contains("pushed") {
			colMap["pushed"] = vgi.BuildStringArray(size, func(_ int64) string { return state.Witness })
		}
		return colMap, nil
	})
}

func NewLateMaterializationFunction() vgi.TableFunction {
	return vgi.AsTableFunction[lateMaterializationState](&LateMaterializationFunction{})
}

// scrambleOrd is a deterministic, scattered ordering key for a given row index.
func scrambleOrd(index int64) int64 {
	return (index * lateMatScramble) % lateMatModulus
}

// buildOrdArray builds the `ord` column for `size` rows starting at `start`,
// injecting a NULL every nullStride-th row (0 = never).
func buildOrdArray(size, start, nullStride int64) arrow.Array {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Reserve(int(size))
	for i := int64(0); i < size; i++ {
		idx := start + i
		if nullStride > 0 && idx%nullStride == 0 {
			b.AppendNull()
		} else {
			b.Append(scrambleOrd(idx))
		}
	}
	return b.NewArray()
}

// lateMatNoFilterWitness is the witness string when no rowid filter was received.
const lateMatNoFilterWitness = "rid:in=0;rng=none"

// lateMatRowIDWitness summarizes the rowid filter the worker received as a stable
// string matching vgi-python's _rowid_pushdown_witness:
//
//	rid:in=<n>        — total number of rowid IN-list (join-key) values.
//	rng=<lo>..<hi>    — min/max rowid range bounds, or "none" if absent.
func lateMatRowIDWitness(pf *vgi.PushdownFilters) string {
	if pf == nil {
		return lateMatNoFilterWitness
	}
	var inCount int
	var lo, hi *int64

	var walk func(f vgi.Filter)
	walk = func(f vgi.Filter) {
		switch ft := f.(type) {
		case *vgi.AndFilter:
			for _, c := range ft.Children {
				walk(c)
			}
		case *vgi.OrFilter:
			for _, c := range ft.Children {
				walk(c)
			}
		case *vgi.InFilter:
			if ft.ColumnName() == lateMatRowIDName {
				inCount += ft.Values.Len()
			}
		case *vgi.ConstantFilter:
			if ft.ColumnName() != lateMatRowIDName {
				return
			}
			v, ok := scalarToInt64(ft.Value)
			if !ok {
				return
			}
			switch ft.Op {
			case vgi.OpGT, vgi.OpGE:
				if lo == nil || v < *lo {
					lo = &v
				}
			case vgi.OpLT, vgi.OpLE:
				if hi == nil || v > *hi {
					hi = &v
				}
			case vgi.OpEQ:
				lo, hi = &v, &v
			}
		}
	}
	for _, f := range pf.Filters {
		walk(f)
	}

	rng := "none"
	if lo != nil || hi != nil {
		rng = boundString(lo) + ".." + boundString(hi)
	}
	return fmt.Sprintf("rid:in=%d;rng=%s", inCount, rng)
}

// boundString renders a range bound, mirroring Python's repr of an unset bound.
func boundString(v *int64) string {
	if v == nil {
		return "None"
	}
	return strconv.FormatInt(*v, 10)
}

// scalarToInt64 extracts an int64 from a (possibly narrower) integer scalar.
func scalarToInt64(s scalar.Scalar) (int64, bool) {
	if s == nil || !s.IsValid() {
		return 0, false
	}
	switch v := s.(type) {
	case *scalar.Int64:
		return v.Value, true
	case *scalar.Int32:
		return int64(v.Value), true
	case *scalar.Int16:
		return int64(v.Value), true
	case *scalar.Int8:
		return int64(v.Value), true
	case *scalar.Uint64:
		return int64(v.Value), true
	case *scalar.Uint32:
		return int64(v.Value), true
	case *scalar.Uint16:
		return int64(v.Value), true
	case *scalar.Uint8:
		return int64(v.Value), true
	}
	return 0, false
}
