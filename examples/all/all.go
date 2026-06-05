// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package all provides RegisterAll, which registers every example function
// (scalars, tables, table-in-outs, table-bufferings, aggregates) onto a
// Worker. It mirrors vgi-python's vgi._test_fixtures.worker pattern so the
// inventory is reusable from fixture binaries and from documentation.
package all

import (
	"github.com/Query-farm/vgi-go/examples/aggregate"
	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/examples/schema_reconcile"
	"github.com/Query-farm/vgi-go/examples/table"
	table_in_out "github.com/Query-farm/vgi-go/examples/table_in_out"
	"github.com/Query-farm/vgi-go/vgi"
)

// RegisterAll registers every example function on w. Catalog tables, secret
// types, settings, and worker-option wiring are intentionally NOT registered
// here — those are bespoke per worker binary.
func RegisterAll(w *vgi.Worker) {
	registerScalars(w)
	registerTables(w)
	aggregate.RegisterAll(w)
	registerTableInOuts(w)
	schema_reconcile.RegisterAll(w)
}

func registerScalars(w *vgi.Worker) {
	w.RegisterScalar(scalar.NewAddValues())
	w.RegisterScalar(&scalar.AnyMixedIntFunction{})
	w.RegisterScalar(&scalar.AnyMixedStrFunction{})
	w.RegisterScalar(&scalar.BernoulliFunction{})
	w.RegisterScalar(&scalar.BinaryPacketFunction{})
	w.RegisterScalar(scalar.NewConcatValuesInt())
	w.RegisterScalar(scalar.NewConcatValuesStr())
	w.RegisterScalar(&scalar.ConditionalMessageFunction{})
	w.RegisterScalar(scalar.NewDouble())
	w.RegisterScalar(&scalar.FormatNumberDefaultFunction{})
	w.RegisterScalar(&scalar.FormatNumberPrecisionFunction{})
	w.RegisterScalar(&scalar.FormatNumberFullFunction{})
	w.RegisterScalar(&scalar.GeoCentroidFixedFunction{})
	w.RegisterScalar(&scalar.GeoCentroidListFunction{})
	w.RegisterScalar(&scalar.GeoCentroidStructFunction{})
	w.RegisterScalar(&scalar.GeoDistanceFixedFunction{})
	w.RegisterScalar(&scalar.GeoDistanceListFunction{})
	w.RegisterScalar(&scalar.GeoDistanceStructFunction{})
	w.RegisterScalar(&scalar.HashSeedFunction{})
	w.RegisterScalar(&scalar.MultiplyBySettingFunction{})
	w.RegisterScalar(scalar.NewMultiply())
	w.RegisterScalar(scalar.NewNullHandling())
	w.RegisterScalar(scalar.NewTypeInfoInt32Function())
	w.RegisterScalar(scalar.NewTypeInfoInt64Function())
	w.RegisterScalar(scalar.NewTypeInfoUint32Function())
	w.RegisterScalar(scalar.NewTypeInfoUint64Function())
	w.RegisterScalar(scalar.NewTypeInfoVarcharFunction())
	w.RegisterScalar(&scalar.PairTypeIntIntFunction{})
	w.RegisterScalar(&scalar.PairTypeStrStrFunction{})
	w.RegisterScalar(&scalar.PairTypeIntStrFunction{})
	w.RegisterScalar(&scalar.RandomBytesFunction{})
	w.RegisterScalar(&scalar.RandomIntFunction{})
	w.RegisterScalar(&scalar.ReturnSecretValueFunction{})
	w.RegisterScalar(&scalar.SmartFormatWidthFunction{})
	w.RegisterScalar(&scalar.SmartFormatPrefixFunction{})
	w.RegisterScalar(scalar.NewSumValues())
	w.RegisterScalar(&scalar.UnnestTensorFunction{})
	w.RegisterScalar(&scalar.UpperCaseFunction{})
	w.RegisterScalar(&scalar.WhoAmIFunction{})
}

func registerTables(w *vgi.Worker) {
	w.RegisterTable(table.NewConstantColumnsFunction())
	w.RegisterTable(table.NewFilterEchoFunction())
	w.RegisterTable(table.NewDictFilterEchoFunction())
	w.RegisterTable(table.NewValuePruneFunction())
	w.RegisterTable(table.NewLateMaterializationFunction())
	w.RegisterTable(table.NewFilterEchoPartitionedFunction())
	w.RegisterTable(table.NewOrderEchoFunction())
	w.RegisterTable(table.NewSampleEchoFunction())
	w.RegisterTable(table.NewSpatialFilterExampleFunction())
	w.RegisterTable(table.NewColorsScanFunction())
	w.RegisterTable(table.NewExpressionFilterTestFunction())
	w.RegisterTable(table.NewDoubleSequenceFunction())
	w.RegisterTable(table.NewDynamicFilterEchoFunction())
	w.RegisterTable(table.NewGeneratorExceptionFunction())
	w.RegisterTable(table.NewLoggingGeneratorFunction())
	w.RegisterTable(table.NewMakePairsIntFunction())
	w.RegisterTable(table.NewMakePairsMixedFunction())
	w.RegisterTable(table.NewMakePairsStrFunction())
	w.RegisterTable(table.NewMakeSeriesCountFunction())
	w.RegisterTable(table.NewMakeSeriesRangeFunction())
	w.RegisterTable(table.NewMakeSeriesStepFunction())
	w.RegisterTable(table.NewMakeSeriesCsvFunction())
	w.RegisterTable(table.NewMakeSeriesFloatStepFunction())
	w.RegisterTable(table.NewNamedParamsEchoFunction())
	w.RegisterTable(table.NewNestedSequenceFunction())
	w.RegisterTable(table.NewPartitionedSequenceFunction())
	w.RegisterTable(table.NewPartitionedBatchIndexFunction())
	w.RegisterTable(table.NewPartitionedBatchIndexMarkedFunction())
	w.RegisterTable(table.NewMissingBatchIndexTagFunction())
	w.RegisterTable(table.NewNonMonotoneBatchIndexFunction())
	w.RegisterTable(table.NewBatchIndexOverflowFunction())
	w.RegisterTable(table.NewCountryPartitionedSalesFunction())
	w.RegisterTable(table.NewRegionYearPartitionedFunction())
	w.RegisterTable(table.NewPartitionedWithExplicitOverrideFunction())
	w.RegisterTable(table.NewDisjointRangePartitionedFunction())
	w.RegisterTable(table.NewBrokenMissingPartitionValuesFunction())
	w.RegisterTable(table.NewBrokenPartitionMinNeqMaxFunction())
	w.RegisterTable(table.NewBrokenPartitionValuesNoAnnotationFunction())
	w.RegisterTable(table.NewBrokenPartitionColumnAbsentFromBatchFunction())
	w.RegisterTable(table.NewTxCachedValueFunction())
	w.RegisterTable(table.NewPartitionedFixedOrderFunction())
	w.RegisterTable(table.NewPartitionedPreservesOrderFunction())
	w.RegisterTable(table.NewPartitionedNoOrderGuaranteeFunction())
	w.RegisterTable(table.NewProfilingDemoFunction())
	w.RegisterTable(table.NewSlowCancellableFunction())
	// Scope projection-pushdown reproducer functions to the
	// ``projection_repro`` catalog only — they're invisible to the
	// ``example`` catalog's function listing (function_registration.test
	// asserts an exact 54-function count there).
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproStrictFunction())
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproFullSchemaFunction())
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproChunkedFunction())
	w.RegisterTableForCatalog("projection_repro", table.NewProjReproMultiWorkerFunction())
	w.RegisterTable(table.NewProjectedDataFunction())
	w.RegisterTable(table.NewRepeatValueIntFunction())
	w.RegisterTable(table.NewRowIdSequenceFunction())
	w.RegisterTable(table.NewRepeatValueStrFunction())
	w.RegisterTable(table.NewScopedSecretDemoFunction())
	w.RegisterTable(table.NewSecretDemoFunction())
	w.RegisterTable(table.NewSequenceFunction())
	w.RegisterTable(table.NewSettingsAwareFunction())
	w.RegisterTable(table.NewStructSettingsFunction())
	w.RegisterTable(table.NewTenThousandFunction())
	w.RegisterTable(table.NewVersionedDataFunction())
	w.RegisterTable(table.NewDepartmentsScanFunction())
	w.RegisterTable(table.NewEmployeesScanFunction())
	w.RegisterTable(table.NewProductsScanFunction())
	w.RegisterTable(table.NewProjectsScanFunction())
	// rff_* scan functions back the Tables exercised by the
	// required_field_filter_paths_*.test sqllogictest matrix.
	w.RegisterTable(table.NewRffSimpleScanFunction())
	w.RegisterTable(table.NewRffStructScanFunction())
	w.RegisterTable(table.NewRffNestedScanFunction())
	w.RegisterTable(table.NewRffMultiScanFunction())
	w.RegisterTable(table.NewRffNoneScanFunction())
	w.RegisterTable(table.NewRffRowidScanFunction())
	// filter_echo_table — catalog table echoing pushed-down filters
	// (filter_pushdown_through_view.test).
	w.RegisterTable(table.NewFilterEchoTableScanFunction())
	// Time travel + filter pushdown together (time_travel_pushdown.test).
	w.RegisterTable(table.NewTimeTravelPushdownFunction())
	w.RegisterTable(table.NewTtPushdownColsScanFunction())
	w.RegisterTable(table.NewVersionedConstraintsScanFunction())
}

func registerTableInOuts(w *vgi.Worker) {
	w.RegisterTableBuffering(&table_in_out.BufferInputFunction{})
	w.RegisterTableBuffering(&table_in_out.OrderedBufferInputFunction{})
	w.RegisterTableBuffering(&table_in_out.BatchIndexBufferInputFunction{})
	w.RegisterTableBuffering(&table_in_out.OrderedSourceFunction{})
	w.RegisterTableBuffering(&table_in_out.LargeStateFunction{})
	w.RegisterTableBuffering(&table_in_out.CrashOnProcessFunction{})
	w.RegisterTableBuffering(&table_in_out.CrashOnCombineFunction{})
	w.RegisterTableBuffering(&table_in_out.CrashOnFinalizeFunction{})
	w.RegisterTableBuffering(&table_in_out.HangOnProcessFunction{})
	w.RegisterTableBuffering(&table_in_out.SlowCancellableBufferingFunction{})
	w.RegisterTableBuffering(&table_in_out.EchoBufferingFunction{})
	w.RegisterTableBuffering(&table_in_out.BufferEmitWideFunction{})
	w.RegisterTableInOut(table_in_out.NewEchoWitnessFunction())
	w.RegisterTableInOut(table_in_out.NewDistributedSumFunction())
	w.RegisterTableInOut(table_in_out.NewEchoFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionFinalizeFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionProcessFunction())
	w.RegisterTableInOut(table_in_out.NewFilterBySettingFunction())
	w.RegisterTableInOut(table_in_out.NewRepeatInputsFunction())
	w.RegisterTableInOut(table_in_out.NewSlowCancellableInOutFunction())
	w.RegisterTableBuffering(&table_in_out.SumAllColumnsFunction{})
	w.RegisterTableInOut(table_in_out.NewUnnestTensorRowsFunction())
}
