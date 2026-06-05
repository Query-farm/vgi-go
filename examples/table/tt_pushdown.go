// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Time-travel + filter-pushdown fixtures backing
// test/sql/integration/table/time_travel_pushdown.test. They mirror
// vgi-python's vgi/_test_fixtures/table/tt_pushdown.py and prove a table can be
// partition-pruned (filter pushdown) AND time-travelled (AT (VERSION|TIMESTAMP))
// in the same query — for tables declared both ways:
//
//   - tt_pushdown_scan       — function-backed: reads the AT clause at init
//     (ProcessParams.AtUnit/AtValue), proving the framework threads AT onto the
//     bind request embedded in init.
//   - tt_pushdown_cols_scan  — columns-based: receives the resolved version as a
//     scan-function argument (the native columns-based AT mechanism, resolved in
//     catalog_table_scan_function_get).
//
// Both echo seen_version (the version they actually scanned) and pushed_filters
// (the SQL-like predicate DuckDB pushed down), so one query asserts both signals.

// TtPushdownOutputSchema is version-independent: only the row data changes per
// version, so the function-backed table stays inline-bound.
var TtPushdownOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "val", Type: arrow.PrimitiveTypes.Int64},
	{Name: "seen_version", Type: arrow.PrimitiveTypes.Int64},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

// ttVersionIDs are the per-version row ids (val = id*10). v2 is a strict
// superset of v1, so a row-count difference cleanly proves which version scanned.
var ttVersionIDs = map[int64][]int64{
	1: {1, 2, 3, 4, 5},
	2: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
}

// ttCurrentVersion is the default when there is no AT clause.
const ttCurrentVersion = 2

// ResolveTtVersion resolves an AT clause to one of this fixture's versions:
//   - nil            → current version (2)
//   - VERSION => n   → int(n), must be 1 or 2
//   - TIMESTAMP      → year <= 2020 → 1, else 2
func ResolveTtVersion(atUnit, atValue *string) (int64, error) {
	if atUnit == nil || *atUnit == "" {
		return ttCurrentVersion, nil
	}
	switch strings.ToUpper(*atUnit) {
	case "VERSION":
		if atValue == nil {
			return 0, fmt.Errorf("VERSION requires a value")
		}
		v, err := strconv.ParseInt(*atValue, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid version: %s", *atValue)
		}
		if _, ok := ttVersionIDs[v]; !ok {
			return 0, fmt.Errorf("Unknown version %d; valid: 1, 2", v)
		}
		return v, nil
	case "TIMESTAMP":
		if atValue == nil || len(*atValue) < 4 {
			return 0, fmt.Errorf("invalid timestamp: %v", atValue)
		}
		year, err := strconv.Atoi((*atValue)[:4])
		if err != nil {
			return 0, fmt.Errorf("invalid timestamp: %s", *atValue)
		}
		if year <= 2020 {
			return 1, nil
		}
		return 2, nil
	default:
		return 0, fmt.Errorf("Unsupported at_unit: %s", *atUnit)
	}
}

// ttPushedFiltersStr formats the init-time pushdown filters for the echo column.
func ttPushedFiltersStr(params *vgi.ProcessParams) string {
	if params.PushdownFilters == nil {
		return "(none)"
	}
	pf, err := vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
	if err != nil || pf == nil || len(pf.Filters) == 0 {
		return "(none)"
	}
	return formatFiltersInline(pf)
}

// ttState carries the resolved version, cached filter string, and emit cursor.
// seen_version / pushed_filters are serialized (not transient) so they survive
// the HTTP state-token rehydrate path, which deserializes state without
// re-running NewState.
type ttState struct {
	SeenVersion   int64
	PushedFilters string
	Done          bool
}

// emitTtVersion emits the single batch for state.SeenVersion, projected to the
// requested output schema.
func emitTtVersion(params *vgi.ProcessParams, state *ttState, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	ids := ttVersionIDs[state.SeenVersion]
	n := int64(len(ids))
	cols := make([]arrow.Array, 0, params.OutputSchema.NumFields())
	for _, f := range params.OutputSchema.Fields() {
		switch f.Name {
		case "id":
			cols = append(cols, vgi.BuildInt64Array(n, func(i int64) int64 { return ids[i] }))
		case "val":
			cols = append(cols, vgi.BuildInt64Array(n, func(i int64) int64 { return ids[i] * 10 }))
		case "seen_version":
			cols = append(cols, vgi.BuildInt64Array(n, func(_ int64) int64 { return state.SeenVersion }))
		case "pushed_filters":
			cols = append(cols, vgi.BuildStringArray(n, func(_ int64) string { return state.PushedFilters }))
		default:
			return fmt.Errorf("tt_pushdown: unexpected projected column %q", f.Name)
		}
	}
	out.Emit(array.NewRecordBatch(params.OutputSchema, cols, n))
	return nil
}

// ---------------------------------------------------------------------------
// tt_pushdown_scan — function-backed: reads AT at init.
// ---------------------------------------------------------------------------

// TimeTravelPushdownFunction is a function-backed time-travel + pushdown scan.
// The version comes from the AT clause (read at init), not from an argument.
type TimeTravelPushdownFunction struct{}

var _ vgi.TypedTableFunc[ttState] = (*TimeTravelPushdownFunction)(nil)

func (f *TimeTravelPushdownFunction) Name() string { return "tt_pushdown_scan" }

func (f *TimeTravelPushdownFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Function-backed time-travel + filter-pushdown scan (reads AT at init).",
		Stability:          vgi.StabilityConsistent,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		ProjectionPushdown: true,
		Categories:         []string{"generator", "diagnostic", "testing"},
	}
}

func (f *TimeTravelPushdownFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *TimeTravelPushdownFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(TtPushdownOutputSchema)
}

func (f *TimeTravelPushdownFunction) NewState(params *vgi.ProcessParams) (*ttState, error) {
	version, err := ResolveTtVersion(params.AtUnit, params.AtValue)
	if err != nil {
		return nil, err
	}
	return &ttState{SeenVersion: version, PushedFilters: ttPushedFiltersStr(params)}, nil
}

func (f *TimeTravelPushdownFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *ttState, out *vgirpc.OutputCollector) error {
	return emitTtVersion(params, state, out)
}

func NewTimeTravelPushdownFunction() vgi.TableFunction {
	return vgi.AsTableFunction[ttState](&TimeTravelPushdownFunction{})
}

// ---------------------------------------------------------------------------
// tt_pushdown_cols_scan — columns-based: version via scan-function argument.
// ---------------------------------------------------------------------------

// ttColsArgs is the typed argument schema: the resolved version injected by the
// worker's catalog_table_scan_function_get from the AT clause.
type ttColsArgs struct {
	Version int64 `vgi:"pos=0,doc=Resolved data version"`
}

// TtPushdownColsScanFunction is a columns-based time-travel + pushdown scan that
// receives the version as a scan-function argument.
type TtPushdownColsScanFunction struct{}

var _ vgi.TypedTableFunc[ttState] = (*TtPushdownColsScanFunction)(nil)

func (f *TtPushdownColsScanFunction) Name() string { return "tt_pushdown_cols_scan" }

func (f *TtPushdownColsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Columns-based time-travel + filter-pushdown scan (version via arg).",
		Stability:          vgi.StabilityConsistent,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		ProjectionPushdown: true,
		Categories:         []string{"generator", "diagnostic", "testing"},
	}
}

func (f *TtPushdownColsScanFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(ttColsArgs{})
}

func (f *TtPushdownColsScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(TtPushdownOutputSchema)
}

func (f *TtPushdownColsScanFunction) NewState(params *vgi.ProcessParams) (*ttState, error) {
	var args ttColsArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if args.Version == 0 {
		args.Version = ttCurrentVersion
	}
	return &ttState{SeenVersion: args.Version, PushedFilters: ttPushedFiltersStr(params)}, nil
}

func (f *TtPushdownColsScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *ttState, out *vgirpc.OutputCollector) error {
	return emitTtVersion(params, state, out)
}

func NewTtPushdownColsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[ttState](&TtPushdownColsScanFunction{})
}
