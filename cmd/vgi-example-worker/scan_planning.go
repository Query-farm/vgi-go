// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// Protocol 3.2 filtered scan planning fixtures
// (test/sql/integration/catalog/scan_planning_*.test), mirroring vgi-python's
// _planned_days_branches. Five days of data, one parquet file per day:
// ${VGI_TEST_BRANCH_DIR}/planned_days_<day>.parquet. The tests create the files
// for the first four days only: the 2026-10-05 file never exists, so any scan
// that fails to prune that day errors with "No files found" — a passing query
// proves the pruning happened.

var plannedDays = func() []time.Time {
	days := make([]time.Time, 5)
	for i := range days {
		days[i] = time.Date(2026, 10, i+1, 0, 0, 0, 0, time.UTC)
	}
	return days
}()

var plannedDaysColumns = arrow.NewSchema([]arrow.Field{
	{Name: "event_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

func plannedDayFile(day time.Time) string {
	return branchPath("planned_days_" + day.Format(time.DateOnly) + ".parquet")
}

func plannedDayStats(day time.Time) []vgi.BranchColumnStatistics {
	d := arrow.Date32FromTime(day)
	return []vgi.BranchColumnStatistics{{
		ColumnName: "event_date", Type: arrow.FixedWidthTypes.Date32,
		Min: d, Max: d, NoNulls: true,
	}}
}

// planned_events: one file per day (planned_events_<day>.parquet, written by
// scan_planning_predicates*.test), 10 rows each. Statistics cover event_date
// and ts, so predicates on either prune; every other column only reaches
// DuckDB. Mirrors vgi-python's _planned_events_branches.
var plannedEventsColumns = arrow.NewSchema([]arrow.Field{
	{Name: "event_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	{Name: "series", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "ticker", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
}, nil)

func plannedEventFile(day time.Time) string {
	return branchPath("planned_events_" + day.Format(time.DateOnly) + ".parquet")
}

// plannedEventStats: event_date is the day; ts spans 00:00-09:00 UTC.
func plannedEventStats(day time.Time) []vgi.BranchColumnStatistics {
	tsType := plannedEventsColumns.Field(1).Type
	start := arrow.Timestamp(day.UnixMicro())
	end := arrow.Timestamp(day.Add(9 * time.Hour).UnixMicro())
	return append(plannedDayStats(day), vgi.BranchColumnStatistics{
		ColumnName: "ts", Type: tsType, Min: start, Max: end, NoNulls: true,
	})
}

// describeFilter is a stable one-line description of a decoded pushdown
// filter, for test assertions; the same strings as vgi-python's fixture
// _describe_filter ("col OP", "And(...)", "col IsNull", "col V2Expression").
func describeFilter(f vgi.Filter) string {
	switch f := f.(type) {
	case *vgi.AndFilter:
		return "And(" + describeFilters(f.Children, ", ") + ")"
	case *vgi.OrFilter:
		return "Or(" + describeFilters(f.Children, ", ") + ")"
	case *vgi.ConstantFilter:
		return f.ColumnName() + " " + strings.ToUpper(string(f.Op))
	case *vgi.StructFilter:
		return f.ColumnName() + "." + f.ChildName + " " + describeFilter(f.ChildFilter)
	case *vgi.IsNullFilter:
		return f.ColumnName() + " IsNull"
	case *vgi.IsNotNullFilter:
		return f.ColumnName() + " IsNotNull"
	case *vgi.InFilter:
		return f.ColumnName() + " In"
	case *vgi.ExpressionFilter:
		return f.ColumnName() + " Expression"
	}
	// Any other v2 shape: vgi-python's V2ExpressionFilter.
	return f.ColumnName() + " V2Expression"
}

func describeFilters(filters []vgi.Filter, sep string) string {
	parts := make([]string, len(filters))
	for i, f := range filters {
		parts[i] = describeFilter(f)
	}
	return strings.Join(parts, sep)
}

// plannedEventsBranches answers every planning call for data.planned_events:
// one read_parquet branch over the day files the predicates may match. Each
// call writes planned_events_last_plan.json with the planned days and the
// sorted filter descriptions (null when the client sent no predicates), so a
// test can assert what reached the worker, including that a predicate it
// cannot use (LIKE, most functions) stayed with the client. Only
// scan_planning_predicates*.test query this table, in one serial group, so the
// record is never overwritten concurrently.
func plannedEventsBranches(filters *vgi.ScanPlanningFilters) (*vgi.ScanBranchesResult, error) {
	days, files := []string{}, []string{}
	for _, day := range plannedDays {
		if filters == nil || filters.MayMatchBranch(plannedEventStats(day)) {
			days = append(days, day.Format(time.DateOnly))
			files = append(files, plannedEventFile(day))
		}
	}
	var described []string
	if filters != nil {
		decoded, err := filters.Decode()
		if err != nil {
			return nil, err
		}
		described = []string{}
		if decoded != nil {
			for _, f := range decoded.Filters {
				described = append(described, describeFilter(f))
			}
		}
		sort.Strings(described)
	}
	record, err := json.Marshal(map[string]any{"days": days, "filters": described})
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(branchPath("planned_events_last_plan.json"), record, 0o644); err != nil {
		return nil, err
	}
	parquet := []string{"parquet"}
	if len(files) == 0 {
		return &vgi.ScanBranchesResult{RequiredExtensions: parquet}, nil
	}
	rows := int64(10 * len(files))
	return &vgi.ScanBranchesResult{
		Branches: []vgi.ScanBranch{{
			FunctionName:        "read_parquet",
			PositionalArguments: []vgi.ScanArg{{Value: files, Type: arrow.ListOf(arrow.BinaryTypes.String)}},
			RowCount:            &rows,
		}},
		RequiredExtensions: parquet,
	}, nil
}

func registerPlannedDaysTables(w *vgi.Worker) {
	// Worker-side pruning of per-day files with the query's predicates.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name: "planned_days", Columns: plannedDaysColumns, ScanPlanning: vgi.ScanPlanningFiltered,
		Comment: "Protocol 3.2: worker-side pruning of per-day files — scan_planning_filtered.test",
	})
	// One branch per day with event_date min/max; the worker ignores the
	// predicates and the client prunes by the branch statistics.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name: "planned_days_stats", Columns: plannedDaysColumns, ScanPlanning: vgi.ScanPlanningFiltered,
		Comment: "Protocol 3.2: client-side pruning by ScanBranch statistics — scan_planning_stats.test",
	})
	// branch_filter contradiction on a bind-time table: one branch per day
	// with branch_filter event_date = DATE '<day>'.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name: "planned_days_bf", Columns: plannedDaysColumns,
		Comment: "Protocol 3.2: pruning by branch_filter contradiction — scan_planning_branch_filter.test",
	})
	// Filtered planning over a mixed-type table: the predicates a real catalog
	// sees (string LIKE / functions / NULLs / timestamps / bools), most of
	// which the worker cannot use and DuckDB must still apply.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name: "planned_events", Columns: plannedEventsColumns, ScanPlanning: vgi.ScanPlanningFiltered,
		Comment: "Protocol 3.2: filtered planning, non-prunable predicates — scan_planning_predicates.test",
	})
}

// plannedDaysScanBranchesPlan answers catalog_table_scan_branches_get for the
// planned_days* and planned_events tables, with or without predicates; every
// other table falls through to multiBranchScanBranchesGet.
func plannedDaysScanBranchesPlan(_ []byte, schemaPath vgi.SchemaPath, name string, _, _ *string, filters *vgi.ScanPlanningFilters) (*vgi.ScanBranchesResult, bool, error) {
	name = strings.ToLower(name)
	if schemaPathIs(schemaPath, "data") && name == "planned_events" {
		result, err := plannedEventsBranches(filters)
		return result, err == nil, err
	}
	if !schemaPathIs(schemaPath, "data") || !strings.HasPrefix(name, "planned_days") {
		return nil, false, nil
	}
	readParquet := func(files ...string) vgi.ScanBranch {
		arg := vgi.ScanArg{Value: files[0], Type: arrow.BinaryTypes.String}
		if len(files) > 1 || name == "planned_days" {
			arg = vgi.ScanArg{Value: files, Type: arrow.ListOf(arrow.BinaryTypes.String)}
		}
		return vgi.ScanBranch{FunctionName: "read_parquet", PositionalArguments: []vgi.ScanArg{arg}}
	}
	parquet := []string{"parquet"}
	switch name {
	case "planned_days":
		var days, files []string
		for _, day := range plannedDays {
			if filters == nil || filters.MayMatchBranch(plannedDayStats(day)) {
				days = append(days, day.Format(time.DateOnly))
				files = append(files, plannedDayFile(day))
			}
		}
		// Record what a filtered planning call saw so tests can assert the
		// predicates arrived. Unfiltered calls (vgi_table_branches(), bind-time
		// planning) must not overwrite it: test files share the branch dir and
		// run in parallel.
		if filters != nil {
			record, err := json.Marshal(map[string]any{"received_filters": true, "days": append([]string{}, days...)})
			if err != nil {
				return nil, false, err
			}
			if err := os.WriteFile(branchPath("planned_days_last_plan.json"), record, 0o644); err != nil {
				return nil, false, err
			}
		}
		if len(files) == 0 {
			return &vgi.ScanBranchesResult{RequiredExtensions: parquet}, true, nil
		}
		branch := readParquet(files...)
		rows := int64(10 * len(files))
		branch.RowCount = &rows
		return &vgi.ScanBranchesResult{Branches: []vgi.ScanBranch{branch}, RequiredExtensions: parquet}, true, nil
	case "planned_days_stats":
		branches := make([]vgi.ScanBranch, 0, len(plannedDays))
		for _, day := range plannedDays {
			branch := readParquet(plannedDayFile(day))
			rows := int64(10)
			branch.RowCount = &rows
			branch.ColumnStatistics = plannedDayStats(day)
			branches = append(branches, branch)
		}
		return &vgi.ScanBranchesResult{Branches: branches, RequiredExtensions: parquet}, true, nil
	case "planned_days_bf":
		branches := make([]vgi.ScanBranch, 0, len(plannedDays))
		for _, day := range plannedDays {
			branch := readParquet(plannedDayFile(day))
			branch.BranchFilter = strPtr(fmt.Sprintf("event_date = DATE '%s'", day.Format(time.DateOnly)))
			branches = append(branches, branch)
		}
		return &vgi.ScanBranchesResult{Branches: branches, RequiredExtensions: parquet}, true, nil
	}
	return nil, false, fmt.Errorf("unknown planned_days fixture table: %s", name)
}
