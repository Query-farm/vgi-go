// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Split-based scan planning.
//
// A split names an independently redeemable unit of scan work, so a distributed
// engine can retry a task without re-reading or skipping rows. Naming the work
// rather than describing it is the point: "these three files at version 47"
// survives a retry, "rows 0-999 of whatever this returns now" does not.

// PlanRequestWire is the inner request record for table_function_plan.
//
// Field order and nullability are wire-significant — they must match
// vgi/protocol.py's TableFunctionPlanRequest exactly, or the extension rejects
// the response with a schema-mismatch error naming the field count.
type PlanRequestWire struct {
	BindCall       BindRequestWire `vgirpc:"bind_call"`
	BindOpaqueData []byte          `vgirpc:"bind_opaque_data,nullable"`

	// Pushdown, as it reached the scan. A plan is built from STATIC filters
	// only: join-key values land after planning, so they prune within a split
	// rather than deciding the split set.
	ProjectionIDs   *[]int64 `vgirpc:"projection_ids"`
	PushdownFilters []byte   `vgirpc:"pushdown_filters,nullable"`
	JoinKeys        [][]byte `vgirpc:"join_keys,nullable"`

	// RowLimit is a plain fetch limit. DuckDB cannot supply it —
	// TableFunctionInitInput carries no limit — so it is always nil from there;
	// DataFusion supplies it via TableProvider::scan(limit).
	RowLimit *int64 `vgirpc:"row_limit"`

	// TargetSplitBytes is the primary sizing lever: every engine is byte-driven.
	// MinSplits is the parallelism FLOOR — a small but expensive table still
	// needs one reader per thread. MaxSplitsPerResponse is a PAGINATION cap, not
	// a sizing hint; conflating the two produces wrongly-sized splits.
	TargetSplitBytes     *int64 `vgirpc:"target_split_bytes"`
	MinSplits            *int64 `vgirpc:"min_splits"`
	MaxSplitsPerResponse *int64 `vgirpc:"max_splits_per_response"`

	// Cursor names a place in the ENUMERATION of splits — not a place in the
	// data (that is StartPosition). Merging the two later would be a mess: a
	// cursor lives for one plan call, a position is checkpointed and must
	// survive restarts, upgrades and key rotation.
	Cursor []byte `vgirpc:"cursor,nullable"`

	// RefinedFilters narrows FUTURE splits only; splits already emitted under a
	// looser filter stay valid. FiltersComplete=false says more narrowing may
	// arrive, so a worker may hold splits back; true says stop waiting.
	RefinedFilters  []byte `vgirpc:"refined_filters,nullable"`
	FiltersComplete bool   `vgirpc:"filters_complete"`

	// The position range in the DATA. A null EndPosition means "as of now" — the
	// worker resolves the frontier and reports it back, which is latestOffset()
	// and planInputPartitions(start, end) in one call.
	StartPosition []byte `vgirpc:"start_position,nullable"`
	EndPosition   []byte `vgirpc:"end_position,nullable"`

	// Existing scan hints. A worker may return FEWER splits when a Top-N limit
	// is present.
	OrderByColumnName     *string  `vgirpc:"order_by_column_name"`
	OrderByDirection      *string  `vgirpc:"order_by_direction,enum"`
	OrderByNullOrder      *string  `vgirpc:"order_by_null_order,enum"`
	OrderByLimit          *int64   `vgirpc:"order_by_limit"`
	TablesamplePercentage *float64 `vgirpc:"tablesample_percentage"`
	TablesampleSeed       *int64   `vgirpc:"tablesample_seed"`
}

// PlanResponseWire is the reply record for table_function_plan.
//
// An EMPTY Splits list is legal and means "no work" — a fully-pruned scan
// reaches it, and the client must produce an empty result rather than an error.
type PlanResponseWire struct {
	Splits [][]byte `vgirpc:"splits"`

	// NextCursors is a list so a client can enumerate a large plan in PARALLEL.
	// That is only sound under a contract the worker must honour: the cursors in
	// one response MUST partition the remaining enumeration disjointly and
	// No client checks this. A dedup was tried and removed: it needed a set
	// holding a copy of every token (hundreds of MB on a large plan, paid by every
	// scan), it compared token bytes so it could never work on a keyed worker where
	// each mint uses a fresh nonce, and the most a client can do with a duplicate is
	// refuse anyway. Violating this returns DUPLICATE ROWS, silently.
	NextCursors [][]byte `vgirpc:"next_cursors,nullable"`

	ExecutionID    *[]byte `vgirpc:"execution_id"`
	InitOpaqueData []byte  `vgirpc:"init_opaque_data,nullable"`

	// MaxWorkers is NORMATIVE on redemption, not advisory.
	MaxWorkers *int64 `vgirpc:"max_workers"`

	// Totals answer CBO/estimateStatistics without forcing full enumeration.
	EstimatedTotalSplits *int64 `vgirpc:"estimated_total_splits"`
	EstimatedTotalRows   *int64 `vgirpc:"estimated_total_rows"`
	EstimatedTotalBytes  *int64 `vgirpc:"estimated_total_bytes"`

	// CatalogVersion is the counter that MOVES within an attach, so it is what a
	// plan is pinned to and what a stale token is detected against.
	// resolved_data_version is fixed at attach and would say nothing.
	CatalogVersion *int64 `vgirpc:"catalog_version"`

	// Scope names which anchor the tokens bind to: "catalog" or "transaction".
	Scope string `vgirpc:"scope"`

	// Locations is a hoisted host list; splits index into it by LocationIDs,
	// which keeps a large plan off the coordinator heap.
	Locations *[]string `vgirpc:"locations"`

	// Partitioning is serialized PartitionTransform records. NOT derivable from
	// a split's partition values: country=US does not say whether partitions are
	// identity(country) or bucket(16, user_id). Report NOTHING here unless every
	// split really is single-valued — a byte-sized plan that packs across
	// partition boundaries would otherwise produce silently wrong co-partitioned
	// join results.
	Partitioning [][]byte `vgirpc:"partitioning,nullable"`

	// SortOrder is ordering WITHIN each split, never a global claim across
	// splits. An engine that bin-packs several splits into one partition must
	// declare no ordering at all: concatenating K non-contiguous sorted runs is
	// not sorted, and a sort-elimination pass would then delete a needed sort.
	SortOrder [][]byte `vgirpc:"sort_order,nullable"`

	CacheMaxAgeSeconds *int64 `vgirpc:"cache_max_age_seconds"`

	// StartPosition is what the worker actually started from; EndPosition is the
	// data frontier resolved at plan time — checkpoint it and pass it back as
	// the next StartPosition.
	StartPosition []byte `vgirpc:"start_position,nullable"`
	EndPosition   []byte `vgirpc:"end_position,nullable"`
}

// ScanSplit is one unit of scan work.
//
// A worker sets Payload and nothing else; the framework stamps Token from it.
// The client sends the TOKEN back, never the raw payload — the payload lives
// inside the envelope and is unverifiable on its own.
type ScanSplit struct {
	Payload          []byte   `vgirpc:"payload"`
	Token            []byte   `vgirpc:"token"`
	EstimatedRows    *int64   `vgirpc:"estimated_rows"`
	RowsExact        bool     `vgirpc:"rows_exact"`
	EstimatedBytes   *int64   `vgirpc:"estimated_bytes"`
	PartitionBounds  *[]byte  `vgirpc:"partition_bounds"`
	ColumnStatistics *[]byte  `vgirpc:"column_statistics"`
	LocationIDs      *[]int64 `vgirpc:"location_ids"`
	StartPosition    *[]byte  `vgirpc:"start_position"`
	EndPosition      *[]byte  `vgirpc:"end_position"`
}

// PlanResult is what a Plan hook returns: Go-native splits plus the scan-wide
// metadata. The framework serializes each split and stamps its token — a worker
// sets ScanSplit.Payload and never touches ScanSplit.Token.
type PlanResult struct {
	Splits               []ScanSplit
	NextCursors          [][]byte
	ExecutionID          []byte
	InitOpaqueData       []byte
	MaxWorkers           int64
	EstimatedTotalSplits *int64
	EstimatedTotalRows   *int64
	EstimatedTotalBytes  *int64
	CatalogVersion       int64

	// Scope names which anchor the tokens bind to: "catalog" (the default) or
	// "transaction". A transaction-scoped plan is not cacheable and is not
	// redeemable after commit or rollback.
	Scope string
}

// TableFunctionWithPlan is implemented by table functions that divide their scan
// into splits. Implementing it is what opts a function into the split path.
type TableFunctionWithPlan interface {
	// Plan divides the scan into named units. Size them into comparable units of
	// work and honour TargetSplitBytes: the client claims splits greedily as
	// interchangeable units because it cannot see per-split cost, so wildly
	// uneven splits leave its makespan bounded by the largest one.
	//
	// An EMPTY split list is legal and means "no work" — a fully-pruned scan
	// reaches it, and the client produces an empty result rather than an error.
	Plan(params *BindParams, req PlanRequestWire) (*PlanResult, error)

	// Redeem is handed the verified payloads for the splits this init claimed,
	// on params.SplitPayloads. The framework has already opened and stripped the
	// envelope, so an unverified token never reaches here.
	//
	// Any state carried from planning to reading must live in cross-process
	// storage keyed by execution_id: the process that plans is, in the general
	// case, not the process that reads — and under a distributed engine it is not
	// even the same host.
	//
	// A split may be redeemed MORE THAN ONCE (recursive CTEs, re-collected
	// DataFrames, task retry) and may be ABANDONED mid-stream (LIMIT, TopK, an
	// empty join build side). Neither is an error.
	Redeem(params *InitParams) error
}

// SerializeScanSplit encodes one split as a single-row Arrow IPC batch, the
// same list<binary>-of-records shape ScanBranch uses. Field ORDER and types are
// wire-significant and must match vgi/protocol.py's ScanSplit exactly.
// SerializeScanSplit encodes one split as the single-row Arrow IPC batch that
// rides one entry of PlanResponse.splits.
//
// The schema comes from codegen (ScanSplitSchema) rather than being spelled out
// here. It used to be hand-written, and hand-written is how four SDKs ended up
// disagreeing about which columns were binary and which were large_binary — a
// disagreement the client surfaced as "the worker bypassed the framework".
func SerializeScanSplit(split *ScanSplit) ([]byte, error) {
	mem := memory.NewGoAllocator()

	bin := func(v []byte, null bool) arrow.Array {
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer b.Release()
		if null {
			b.AppendNull()
		} else {
			b.Append(v)
		}
		return b.NewArray()
	}
	nullableInt64 := func(v *int64) arrow.Array {
		b := array.NewInt64Builder(mem)
		defer b.Release()
		if v != nil {
			b.Append(*v)
		} else {
			b.AppendNull()
		}
		return b.NewArray()
	}

	boolBuilder := array.NewBooleanBuilder(mem)
	defer boolBuilder.Release()
	boolBuilder.Append(split.RowsExact)

	locBuilder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	defer locBuilder.Release()
	if split.LocationIDs != nil {
		locBuilder.Append(true)
		vb := locBuilder.ValueBuilder().(*array.Int64Builder)
		for _, id := range *split.LocationIDs {
			vb.Append(id)
		}
	} else {
		locBuilder.AppendNull()
	}

	deref := func(p *[]byte) ([]byte, bool) {
		if p == nil {
			return nil, true
		}
		return *p, false
	}
	pb, pbNull := deref(split.PartitionBounds)
	cs, csNull := deref(split.ColumnStatistics)
	sp, spNull := deref(split.StartPosition)
	ep, epNull := deref(split.EndPosition)

	cols := []arrow.Array{
		bin(split.Payload, false),
		bin(split.Token, false),
		nullableInt64(split.EstimatedRows),
		boolBuilder.NewArray(),
		nullableInt64(split.EstimatedBytes),
		bin(pb, pbNull),
		bin(cs, csNull),
		locBuilder.NewArray(),
		bin(sp, spNull),
		bin(ep, epNull),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	rec := array.NewRecord(generated.ScanSplitSchema, cols, 1)
	defer rec.Release()
	return SerializeRecordBatch(rec)
}

// splitPlannerFor finds the split hooks on a function, looking through any
// framework adapter wrapping it. A typed function is registered wrapped, so a
// bare type assertion on the registered value would never see the hooks the
// author wrote.
func splitPlannerFor(fn any) (TableFunctionWithPlan, bool) {
	for fn != nil {
		if p, ok := fn.(TableFunctionWithPlan); ok {
			return p, true
		}
		u, ok := fn.(interface{ InnerFunc() any })
		if !ok {
			return nil, false
		}
		fn = u.InnerFunc()
	}
	return nil, false
}
