// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"encoding/json"
	"fmt"
	"os"
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
}

// plannedDaysScanBranchesPlan answers catalog_table_scan_branches_get for the
// planned_days* tables, with or without predicates; every other table falls
// through to multiBranchScanBranchesGet.
func plannedDaysScanBranchesPlan(_ []byte, schemaPath vgi.SchemaPath, name string, _, _ *string, filters *vgi.ScanPlanningFilters) (*vgi.ScanBranchesResult, bool, error) {
	name = strings.ToLower(name)
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
		// Record what the worker saw so tests can assert the predicates arrived.
		record, err := json.Marshal(map[string]any{"received_filters": filters != nil, "days": append([]string{}, days...)})
		if err != nil {
			return nil, false, err
		}
		if err := os.WriteFile(branchPath("planned_days_last_plan.json"), record, 0o644); err != nil {
			return nil, false, err
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
