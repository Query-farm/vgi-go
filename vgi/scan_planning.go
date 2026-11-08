// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// Protocol 3.2 filtered scan planning.
//
// A table with TableInfo.scan_planning = "filtered" receives the query's
// predicates in catalog_table_scan_branches_get. The worker's planning hook
// (WithAttachScanBranchesPlanHandler) gets them as *ScanPlanningFilters and may
// use them to return fewer branches or fewer files. Every predicate is
// advisory: the client re-applies all of them, so a worker may ignore any
// predicate it does not understand, but it must never drop a branch or file
// that could hold a matching row.
//
// The normative spec is vgi-python's docs/protocol/scan-planning.md.

const (
	// ScanPlanningBind resolves the table's scan redirect at bind time: the
	// 3.1 behaviour and the default.
	ScanPlanningBind = "bind"
	// ScanPlanningFiltered resolves it per scan, after the client has
	// collected the query's predicates on the table.
	ScanPlanningFiltered = "filtered"
)

// validateScanPlanning normalizes "" to ScanPlanningBind and refuses unknown
// values, and a filtered table that also inlines its scan function.
func validateScanPlanning(table, mode string, inlinesScanFunction bool) (string, error) {
	switch mode {
	case "":
		return ScanPlanningBind, nil
	case ScanPlanningBind:
		return mode, nil
	case ScanPlanningFiltered:
		if inlinesScanFunction {
			return "", fmt.Errorf("table %q: ScanPlanning %q cannot be combined with an inlined ScanFunction; "+
				"the scan redirect must be resolved per query with the query's filters", table, ScanPlanningFiltered)
		}
		return mode, nil
	}
	return "", fmt.Errorf("table %q: ScanPlanning must be %q or %q, got %q", table, ScanPlanningBind, ScanPlanningFiltered, mode)
}

// ScanPlanningFilters are the query predicates sent with a filtered
// catalog_table_scan_branches_get.
type ScanPlanningFilters struct {
	// Raw is the wire payload: an Arrow IPC stream holding one VGI Filter
	// Encoding v2 snapshot batch. Column indexes refer to the table's declared
	// columns (TableInfo.columns), in declaration order.
	Raw []byte

	tableColumns func() (*arrow.Schema, error)
	once         sync.Once
	decoded      *PushdownFilters
	err          error
}

// NewScanPlanningFilters wraps a raw pushdown_filters payload whose column
// indexes refer to tableColumns. The worker builds these for its planning hook;
// this constructor exists for tests and for code that plans outside the RPC.
func NewScanPlanningFilters(raw []byte, tableColumns *arrow.Schema) *ScanPlanningFilters {
	return &ScanPlanningFilters{Raw: raw, tableColumns: func() (*arrow.Schema, error) { return tableColumns, nil }}
}

// Decode decodes the predicates against the table's declared columns. The
// result is cached; the table's columns are resolved at most once, on first
// call. An undecodable payload is an error here, but since every predicate is
// advisory a hook may choose to ignore it and return every branch.
func (f *ScanPlanningFilters) Decode() (*PushdownFilters, error) {
	if f == nil {
		return nil, nil
	}
	f.once.Do(func() {
		var columns *arrow.Schema
		if f.tableColumns != nil {
			columns, f.err = f.tableColumns()
			if f.err != nil {
				f.err = fmt.Errorf("resolving the table's declared columns: %w", f.err)
				return
			}
		}
		batch, err := DeserializeRecordBatch(f.Raw)
		if err != nil {
			f.err = fmt.Errorf("pushdown_filters: %w", err)
			return
		}
		// The batch backs the decoded literals and value sets, so it stays
		// alive with the decoded filters. Spatial predicates decode as opaque
		// views (MayMatch keeps candidates for them): a planning hook has no
		// function metadata to gate them on, and they are advisory anyway.
		f.decoded, f.err = deserializeFiltersV2(batch, nil, nil, true, columns)
	})
	return f.decoded, f.err
}

// MayMatch reports false only when the predicates provably exclude every row
// of a candidate (a file, a partition, a branch) described by stats: every
// non-null value of a column lies within [Min, Max], HasNull=false means no
// NULLs, HasNotNull=false means entirely NULL. Use it (or MayMatchBranch, with
// the safe-by-default BranchColumnStatistics) to prune a manifest before
// returning branches.
// Columns, predicates or types it cannot reason about count as "may match", as
// does a payload Decode cannot read, so the answer errs towards keeping data.
func (f *ScanPlanningFilters) MayMatch(stats []ColumnStatistics) bool {
	pf, err := f.Decode()
	if err != nil || pf == nil {
		return true
	}
	return pf.MayMatch(stats)
}

// MayMatch reports false only when the filters provably exclude every row
// described by stats; see ScanPlanningFilters.MayMatch. Top-level filters are
// conjuncts, so one provably false filter excludes the candidate.
func (pf *PushdownFilters) MayMatch(stats []ColumnStatistics) bool {
	if pf == nil || len(stats) == 0 {
		return true
	}
	byName := make(map[string]*ColumnStatistics, len(stats))
	for i := range stats {
		byName[stats[i].ColumnName] = &stats[i]
	}
	for _, f := range pf.Filters {
		if filterExcludes(f, byName) {
			return false
		}
	}
	return true
}

// filterExcludes reports whether f is provably not satisfied by any row.
func filterExcludes(f Filter, stats map[string]*ColumnStatistics) bool {
	switch ft := f.(type) {
	case *AndFilter:
		for _, child := range ft.Children {
			if filterExcludes(child, stats) {
				return true
			}
		}
		return false
	case *OrFilter:
		if len(ft.Children) == 0 {
			return false
		}
		for _, child := range ft.Children {
			if !filterExcludes(child, stats) {
				return false
			}
		}
		return true
	}
	stat, ok := stats[f.ColumnName()]
	if !ok {
		return false
	}
	// Min/Max with HasNotNull=false contradicts itself (most likely Go's zero
	// value left unset); trust the bounds rather than the "entirely NULL" claim.
	contradictory := !stat.HasNotNull && (stat.Min != nil || stat.Max != nil)
	allNull := !stat.HasNotNull && !contradictory
	switch ft := f.(type) {
	case *IsNullFilter:
		return !stat.HasNull
	case *IsNotNullFilter:
		return allNull
	case *ConstantFilter:
		if allNull {
			// A comparison with NULL is never true.
			return true
		}
		return constantExcludes(ft, stat)
	case *InFilter:
		if allNull {
			return true
		}
		return inExcludes(ft, stat)
	}
	return false
}

func constantExcludes(f *ConstantFilter, stat *ColumnStatistics) bool {
	c, ok := scalarCmpValue(f.Value)
	if !ok {
		return false
	}
	lo, hasLo := statCmpValue(stat.Min, stat.Type)
	hi, hasHi := statCmpValue(stat.Max, stat.Type)
	// cmp(a, b) < 0 means a < b; ok=false means incomparable (keep).
	below := func(a, b cmpValue) bool { r, ok := compareCmp(a, b); return ok && r < 0 }
	atMost := func(a, b cmpValue) bool { r, ok := compareCmp(a, b); return ok && r <= 0 }
	switch f.Op {
	case OpEQ:
		return (hasLo && below(c, lo)) || (hasHi && below(hi, c))
	case OpLT: // col < c is false for every row when min >= c
		return hasLo && atMost(c, lo)
	case OpLE:
		return hasLo && below(c, lo)
	case OpGT: // col > c is false for every row when max <= c
		return hasHi && atMost(hi, c)
	case OpGE:
		return hasHi && below(hi, c)
	case OpNE: // false for every row when every non-null value equals c
		if !hasLo || !hasHi {
			return false
		}
		r1, ok1 := compareCmp(lo, c)
		r2, ok2 := compareCmp(hi, c)
		return ok1 && ok2 && r1 == 0 && r2 == 0
	}
	return false
}

func inExcludes(f *InFilter, stat *ColumnStatistics) bool {
	if f.Values == nil {
		return false
	}
	lo, hasLo := statCmpValue(stat.Min, stat.Type)
	hi, hasHi := statCmpValue(stat.Max, stat.Type)
	// Excluded when every non-null value lies provably outside [lo, hi]; an IN
	// list with no non-null value never matches at all.
	for i := 0; i < f.Values.Len(); i++ {
		if f.Values.IsNull(i) {
			continue
		}
		s, err := scalar.GetScalar(f.Values, i)
		if err != nil {
			return false
		}
		v, ok := scalarCmpValue(s)
		if !ok {
			return false
		}
		outside := false
		if hasLo {
			r, ok := compareCmp(v, lo)
			if !ok {
				return false
			}
			outside = r < 0
		}
		if hasHi && !outside {
			r, ok := compareCmp(v, hi)
			if !ok {
				return false
			}
			outside = r > 0
		}
		if !outside {
			return false
		}
	}
	return true
}

// cmpKind is the comparison domain of a value. Values of different domains
// are incomparable, which MayMatch treats as "may match".
type cmpKind int

const (
	cmpInt cmpKind = iota + 1
	cmpFloat
	cmpString
	cmpBytes
	cmpBool
	cmpDate // days since the epoch
	cmpTimestamp
)

type cmpValue struct {
	kind cmpKind
	i    int64
	f    float64
	s    string
	b    []byte
	t    time.Time
}

func compareCmp(a, b cmpValue) (int, bool) {
	order := func(lt, gt bool) int {
		switch {
		case lt:
			return -1
		case gt:
			return 1
		}
		return 0
	}
	numeric := func(k cmpKind) bool { return k == cmpInt || k == cmpFloat }
	if numeric(a.kind) && numeric(b.kind) {
		if a.kind == cmpInt && b.kind == cmpInt {
			return order(a.i < b.i, a.i > b.i), true
		}
		af, bf := a.f, b.f
		if a.kind == cmpInt {
			af = float64(a.i)
		}
		if b.kind == cmpInt {
			bf = float64(b.i)
		}
		if math.IsNaN(af) || math.IsNaN(bf) {
			return 0, false
		}
		return order(af < bf, af > bf), true
	}
	if a.kind != b.kind {
		return 0, false
	}
	switch a.kind {
	case cmpString:
		return order(a.s < b.s, a.s > b.s), true
	case cmpBytes:
		return bytes.Compare(a.b, b.b), true
	case cmpBool:
		return order(!a.i2b() && b.i2b(), a.i2b() && !b.i2b()), true
	case cmpDate:
		return order(a.i < b.i, a.i > b.i), true
	case cmpTimestamp:
		return a.t.Compare(b.t), true
	}
	return 0, false
}

func (v cmpValue) i2b() bool { return v.i != 0 }

const millisPerDay = 86_400_000

// scalarCmpValue maps a filter literal onto its comparison domain.
func scalarCmpValue(s scalar.Scalar) (cmpValue, bool) {
	if s == nil || !s.IsValid() {
		return cmpValue{}, false
	}
	switch v := s.(type) {
	case *scalar.Int8:
		return cmpValue{kind: cmpInt, i: int64(v.Value)}, true
	case *scalar.Int16:
		return cmpValue{kind: cmpInt, i: int64(v.Value)}, true
	case *scalar.Int32:
		return cmpValue{kind: cmpInt, i: int64(v.Value)}, true
	case *scalar.Int64:
		return cmpValue{kind: cmpInt, i: v.Value}, true
	case *scalar.Uint8:
		return cmpValue{kind: cmpInt, i: int64(v.Value)}, true
	case *scalar.Uint16:
		return cmpValue{kind: cmpInt, i: int64(v.Value)}, true
	case *scalar.Uint32:
		return cmpValue{kind: cmpInt, i: int64(v.Value)}, true
	case *scalar.Uint64:
		return uintCmp(v.Value), true
	case *scalar.Float32:
		return cmpValue{kind: cmpFloat, f: float64(v.Value)}, true
	case *scalar.Float64:
		return cmpValue{kind: cmpFloat, f: v.Value}, true
	case *scalar.String:
		return cmpValue{kind: cmpString, s: string(v.Value.Bytes())}, true
	case *scalar.LargeString:
		return cmpValue{kind: cmpString, s: string(v.Value.Bytes())}, true
	case *scalar.Binary:
		return cmpValue{kind: cmpBytes, b: v.Value.Bytes()}, true
	case *scalar.LargeBinary:
		return cmpValue{kind: cmpBytes, b: v.Value.Bytes()}, true
	case *scalar.Boolean:
		return boolCmp(v.Value), true
	case *scalar.Date32:
		return cmpValue{kind: cmpDate, i: int64(v.Value)}, true
	case *scalar.Date64:
		return cmpValue{kind: cmpDate, i: floorDiv(int64(v.Value), millisPerDay)}, true
	case *scalar.Timestamp:
		unit := v.Type.(*arrow.TimestampType).Unit
		return cmpValue{kind: cmpTimestamp, t: v.Value.ToTime(unit)}, true
	}
	return cmpValue{}, false
}

// statCmpValue maps a ColumnStatistics Min/Max (a Go value typed by t) onto
// its comparison domain. ok=false means the bound is absent or not understood.
func statCmpValue(v interface{}, t arrow.DataType) (cmpValue, bool) {
	if v == nil {
		return cmpValue{}, false
	}
	if t != nil {
		switch t.ID() {
		case arrow.DATE32, arrow.DATE64:
			return dateCmp(v, t.ID())
		case arrow.TIMESTAMP:
			return timestampCmp(v, t.(*arrow.TimestampType).Unit)
		case arrow.BINARY, arrow.LARGE_BINARY:
			switch x := v.(type) {
			case []byte:
				return cmpValue{kind: cmpBytes, b: x}, true
			case string:
				return cmpValue{kind: cmpBytes, b: []byte(x)}, true
			}
			return cmpValue{}, false
		}
	}
	switch x := v.(type) {
	case int:
		return cmpValue{kind: cmpInt, i: int64(x)}, true
	case int8:
		return cmpValue{kind: cmpInt, i: int64(x)}, true
	case int16:
		return cmpValue{kind: cmpInt, i: int64(x)}, true
	case int32:
		return cmpValue{kind: cmpInt, i: int64(x)}, true
	case int64:
		return cmpValue{kind: cmpInt, i: x}, true
	case uint:
		return uintCmp(uint64(x)), true
	case uint8:
		return cmpValue{kind: cmpInt, i: int64(x)}, true
	case uint16:
		return cmpValue{kind: cmpInt, i: int64(x)}, true
	case uint32:
		return cmpValue{kind: cmpInt, i: int64(x)}, true
	case uint64:
		return uintCmp(x), true
	case float32:
		return cmpValue{kind: cmpFloat, f: float64(x)}, true
	case float64:
		return cmpValue{kind: cmpFloat, f: x}, true
	case string:
		return cmpValue{kind: cmpString, s: x}, true
	case []byte:
		return cmpValue{kind: cmpBytes, b: x}, true
	case bool:
		return boolCmp(x), true
	case arrow.Date32:
		return cmpValue{kind: cmpDate, i: int64(x)}, true
	case time.Time:
		return cmpValue{kind: cmpTimestamp, t: x}, true
	}
	return cmpValue{}, false
}

func dateCmp(v interface{}, id arrow.Type) (cmpValue, bool) {
	switch x := v.(type) {
	case arrow.Date32:
		return cmpValue{kind: cmpDate, i: int64(x)}, true
	case arrow.Date64:
		return cmpValue{kind: cmpDate, i: floorDiv(int64(x), millisPerDay)}, true
	case time.Time:
		return cmpValue{kind: cmpDate, i: int64(arrow.Date32FromTime(x))}, true
	case int32, int, int64:
		n := toInt64(x)
		if id == arrow.DATE64 {
			n = floorDiv(n, millisPerDay)
		}
		return cmpValue{kind: cmpDate, i: n}, true
	}
	return cmpValue{}, false
}

func timestampCmp(v interface{}, unit arrow.TimeUnit) (cmpValue, bool) {
	switch x := v.(type) {
	case arrow.Timestamp:
		return cmpValue{kind: cmpTimestamp, t: x.ToTime(unit)}, true
	case int64:
		return cmpValue{kind: cmpTimestamp, t: arrow.Timestamp(x).ToTime(unit)}, true
	case time.Time:
		return cmpValue{kind: cmpTimestamp, t: x}, true
	}
	return cmpValue{}, false
}

func uintCmp(u uint64) cmpValue {
	if u > math.MaxInt64 {
		return cmpValue{kind: cmpFloat, f: float64(u)}
	}
	return cmpValue{kind: cmpInt, i: int64(u)}
}

func boolCmp(b bool) cmpValue {
	if b {
		return cmpValue{kind: cmpBool, i: 1}
	}
	return cmpValue{kind: cmpBool}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// BranchColumnStatistics is one column's statistics on a ScanBranch
// (protocol 3.2). Unlike ColumnStatistics, its zero value is the conservative
// claim, because a client prunes branches by these and an over-strong claim
// silently drops rows: leaving NoNulls false says the column may hold NULLs
// (has_null = true on the wire), leaving AllNull false says it may hold values
// (has_not_null = true). Set NoNulls or AllNull only when it is known.
type BranchColumnStatistics struct {
	ColumnName string
	// Type is the Arrow type of Min/Max (required if either is set). The
	// supported types are those SerializeColumnStatistics supports.
	Type arrow.DataType
	// Min / Max bound every non-null value of the column; nil = unbounded.
	Min, Max interface{}
	// NoNulls claims the column has no NULLs in this branch.
	NoNulls bool
	// AllNull claims the column is entirely NULL in this branch. It cannot be
	// combined with Min/Max or NoNulls.
	AllNull bool
}

// ColumnStatistics converts to the wire-level statistics record.
func (s BranchColumnStatistics) ColumnStatistics() ColumnStatistics {
	return ColumnStatistics{ColumnName: s.ColumnName, Type: s.Type, Min: s.Min, Max: s.Max,
		HasNull: !s.NoNulls, HasNotNull: !s.AllNull}
}

func (s BranchColumnStatistics) validate() error {
	if s.AllNull && (s.Min != nil || s.Max != nil) {
		return fmt.Errorf("column %q: AllNull cannot be combined with Min/Max", s.ColumnName)
	}
	if s.AllNull && s.NoNulls {
		return fmt.Errorf("column %q: AllNull and NoNulls contradict each other", s.ColumnName)
	}
	return nil
}

// BranchStatistics converts branch statistics to ColumnStatistics, e.g. for
// PushdownFilters.MayMatch.
func BranchStatistics(stats []BranchColumnStatistics) []ColumnStatistics {
	if stats == nil {
		return nil
	}
	out := make([]ColumnStatistics, len(stats))
	for i, s := range stats {
		out[i] = s.ColumnStatistics()
	}
	return out
}

// MayMatchBranch is MayMatch over branch statistics, so a worker can prune its
// candidates with the same values it puts on ScanBranch.ColumnStatistics.
func (f *ScanPlanningFilters) MayMatchBranch(stats []BranchColumnStatistics) bool {
	return f.MayMatch(BranchStatistics(stats))
}

// MayMatchBranch is MayMatch over branch statistics.
func (pf *PushdownFilters) MayMatchBranch(stats []BranchColumnStatistics) bool {
	return pf.MayMatch(BranchStatistics(stats))
}

// serializeBranchStatistics validates and encodes ScanBranch's protocol 3.2
// statistics fields. Nil ColumnStatistics encodes as an absent (null) column.
func serializeBranchStatistics(branch *ScanBranch) ([]byte, error) {
	if branch.RowCount != nil && *branch.RowCount < 0 {
		return nil, fmt.Errorf("ScanBranch.RowCount must be >= 0, got %d", *branch.RowCount)
	}
	if branch.ColumnStatistics == nil {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(branch.ColumnStatistics))
	for _, stat := range branch.ColumnStatistics {
		if _, dup := seen[stat.ColumnName]; dup {
			return nil, fmt.Errorf("ScanBranch.ColumnStatistics names column %q more than once", stat.ColumnName)
		}
		seen[stat.ColumnName] = struct{}{}
		if err := stat.validate(); err != nil {
			return nil, fmt.Errorf("ScanBranch.ColumnStatistics: %w", err)
		}
	}
	data, err := SerializeColumnStatistics(BranchStatistics(branch.ColumnStatistics), nil)
	if err != nil {
		return nil, fmt.Errorf("serializing branch column statistics: %w", err)
	}
	return data, nil
}

// declaredTableColumns resolves a table's declared columns the way
// catalog_table_get serves them, for decoding pushdown_filters.
// It is a function rather than a workerService method because every method of
// workerService must be a vgi.v3 method.
func declaredTableColumns(w workerService, req CatalogTableGetParams) (*arrow.Schema, error) {
	items, err := w.CatalogTableGet(context.Background(), nil, req)
	if err != nil {
		return nil, err
	}
	if len(items.Items) == 0 {
		return nil, fmt.Errorf("table %q not found", req.Name)
	}
	batch, err := DeserializeRecordBatch(items.Items[0])
	if err != nil {
		return nil, fmt.Errorf("reading TableInfo: %w", err)
	}
	defer batch.Release()
	indices := batch.Schema().FieldIndices("columns")
	if len(indices) != 1 || batch.NumRows() != 1 {
		return nil, fmt.Errorf("TableInfo for %q has no columns field", req.Name)
	}
	column, ok := batch.Column(indices[0]).(*array.Binary)
	if !ok || column.IsNull(0) || len(column.Value(0)) == 0 {
		return nil, fmt.Errorf("TableInfo for %q declares no columns", req.Name)
	}
	return DeserializeSchema(column.Value(0))
}
