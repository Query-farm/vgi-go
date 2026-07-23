// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package all provides RegisterAll, which registers every example function
// (scalars, tables, table-in-outs, table-bufferings, aggregates) onto a
// Worker. It mirrors vgi-python's vgi._test_fixtures.worker pattern so the
// inventory is reusable from fixture binaries and from documentation.
package all

import (
	"github.com/Query-farm/vgi-go/examples/aggregate"
	copy_from "github.com/Query-farm/vgi-go/examples/copy_from"
	copy_to "github.com/Query-farm/vgi-go/examples/copy_to"
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
	registerCopyFroms(w)
	registerCopyTos(w)
	schema_reconcile.RegisterAll(w)
}

func registerCopyFroms(w *vgi.Worker) {
	// Custom COPY ... FROM format reader (advertised via catalog_copy_from_formats
	// and registered as an ordinary producer-mode table function).
	w.RegisterCopyFrom(&copy_from.ExampleLinesCopyFromFunction{})
	// Reader that forwards a CREATE SECRET credential via the secret-bind hook.
	w.RegisterCopyFrom(&copy_from.SecretLinesCopyFromFunction{})
}

func registerCopyTos(w *vgi.Worker) {
	// Custom COPY ... TO format writers (advertised via catalog_copy_from_formats
	// with direction="to" and registered as table-buffering functions reusing the
	// Sink+Combine machinery). The ordered variant requests a single-thread sink.
	w.RegisterCopyTo(&copy_to.ExampleLinesCopyToFunction{})
	w.RegisterCopyTo(&copy_to.ExampleLinesOrderedCopyToFunction{})
	// Writer that forwards a CREATE SECRET credential via the secret-bind hook.
	w.RegisterCopyTo(&copy_to.SecretLinesCopyToFunction{})
}

func registerScalars(w *vgi.Worker) {
	w.RegisterScalar(scalar.NewAddValues())
	w.RegisterScalar(&scalar.AnyMixedIntFunction{})
	w.RegisterScalar(&scalar.AnyMixedStrFunction{})
	w.RegisterScalar(&scalar.BernoulliFunction{})
	w.RegisterScalar(&scalar.BinaryPacketFunction{})
	// Cacheable scalar fixtures (per-value memoization — scalar/per_value*.test).
	w.RegisterScalar(&scalar.CachedAddConstScalarFunction{})
	w.RegisterScalar(&scalar.CachedDoubleScalarFunction{})
	w.RegisterScalar(&scalar.CachedLabelScalarFunction{})
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
	w.RegisterScalar(scalar.NewQuerySeed())
	w.RegisterScalar(&scalar.RandomBytesFunction{})
	w.RegisterScalar(&scalar.RandomIntFunction{})
	w.RegisterScalar(&scalar.ReturnSecretValueFunction{})
	// Schema-disambiguation probe: one function name declared in two schemas of
	// the same catalog. A schema-qualified call must reach the implementation
	// the schema names, so each returns its own schema as a tag.
	w.RegisterScalar(&scalar.SameNameMainFunction{})
	w.RegisterScalarInSchema("data", &scalar.SameNameDataFunction{})
	w.RegisterScalar(&scalar.ScaleBySettingFunction{})
	w.RegisterScalar(&scalar.SecretFieldFunction{})
	w.RegisterScalar(&scalar.SmartFormatWidthFunction{})
	w.RegisterScalar(&scalar.SmartFormatPrefixFunction{})
	w.RegisterScalar(scalar.NewSumValues())
	w.RegisterScalar(&scalar.UnnestTensorFunction{})
	w.RegisterScalar(&scalar.UpperCaseFunction{})
	w.RegisterScalar(&scalar.PassthruFunction{})
	w.RegisterScalar(&scalar.CollatzStepsFunction{})
	w.RegisterScalar(&scalar.Sha256HexFunction{})
	w.RegisterScalar(scalar.NewHashRounds())
	w.RegisterScalar(&scalar.WhoAmIFunction{})
}

func registerTables(w *vgi.Worker) {
	w.RegisterTable(table.NewConstantColumnsFunction())
	w.RegisterTable(table.NewFilterEchoFunction())
	w.RegisterTable(table.NewFilteredColumnsEchoFunction())
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
	w.RegisterTable(table.NewTypedProbeFunction())
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
	w.RegisterTable(table.NewOverlappingRangePartitionedFunction())
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
	// Result-cache fixtures (advertise vgi.cache.* on the first emitted batch)
	// — see examples/table/cache.go and cache_advanced.go.
	w.RegisterTable(table.NewCacheableNumbersFunction())
	w.RegisterTable(table.NewCacheNonceFunction())
	w.RegisterTable(table.NewCacheNoStoreFunction())
	w.RegisterTable(table.NewCacheScopedTxnFunction())
	w.RegisterTable(table.NewCacheBigFunction())
	w.RegisterTable(table.NewCacheRevalidatableFunction())
	// cache_multicol exists only to back the ex.data.cache_multicol table, so
	// it stays out of the catalog's function listing (mirrors vgi-python).
	w.RegisterTableUnlisted(table.NewCacheMultiColFunction())
	w.RegisterTable(table.NewCacheWhoamiFunction())
	w.RegisterTable(table.NewCacheVersionedFunction())
	w.RegisterTable(table.NewCacheProjectionFunction())
	w.RegisterTable(table.NewCachePoisonFunction())
	// test_same_name_cached — one cacheable producer name homed in BOTH main and
	// data, each tagging its row with its own schema; the result-cache member of
	// the schema-disambiguation family (cache/same_name_schemas.test).
	w.RegisterTable(table.NewSameNameCachedFunction("main"))
	w.RegisterTableInSchema("data", table.NewSameNameCachedFunction("data"))
	w.RegisterTable(table.NewCacheExternalFailFunction())
	w.RegisterTable(table.NewCacheBenchFunction())
	w.RegisterTable(table.NewCacheParallelFunction())
	w.RegisterTable(table.NewCacheOrderedFunction())
	w.RegisterTable(table.NewCacheTypesFunction())
	w.RegisterTable(table.NewCacheFilteredFunction())
	w.RegisterTable(table.NewCachePartitionedFunction())
	w.RegisterTable(table.NewCachePartitionScopeFunction())
	w.RegisterTable(table.NewCachePartitionParallelFunction())
	w.RegisterTable(table.NewCachePartitionMultiColFunction())
	w.RegisterTable(table.NewCachePartitionProjFunction())
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
	w.RegisterTable(table.NewMultiSecretDemoFunction())
	w.RegisterTable(table.NewRepeatValueStrFunction())
	w.RegisterTable(table.NewScopedSecretDemoFunction())
	w.RegisterTable(table.NewSecretDemoFunction())
	w.RegisterTable(table.NewSequenceFunction())
	w.RegisterTable(table.NewSettingsAwareFunction())
	w.RegisterTable(table.NewStructSettingsFunction())
	w.RegisterTable(table.NewTenThousandFunction())
	w.RegisterTable(table.NewUnionVarargsFunction())
	w.RegisterTable(table.NewVersionedDataFunction())
	w.RegisterTable(table.NewDepartmentsScanFunction())
	w.RegisterTable(table.NewEmployeesScanFunction())
	w.RegisterTable(table.NewProductsScanFunction())
	w.RegisterTable(table.NewProjectsScanFunction())
	// rff_* scan functions back the Tables exercised by the
	// required_filters_*.test sqllogictest matrix.
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
	// sum_all_columns_simple_distributed: a global cross-substream combine
	// belongs on the buffered Sink+Combine+Source path (migrated from the
	// streaming table-in-out finish(states) model — see distributed_sum.go).
	w.RegisterTableBuffering(&table_in_out.DistributedSumFunction{})
	w.RegisterTableInOut(table_in_out.NewEchoFunction())
	// substream_partial_sum: per-substream partial sum at finalize (parallel
	// streaming finalize, Phase A/A4 — see table_in_out/parallel_finalize.test).
	w.RegisterTableInOut(table_in_out.NewSubstreamPartialSumFunction())
	// Blended ("UNNEST-style") fixtures — positional args ARE the per-row input
	// columns; one registration serves literal / column / LATERAL (see
	// table_in_out/blended.test). geo_encode is arity-overloaded (2 + 3
	// positional columns); row_sum is the VARARGS fixture; blended_drop the
	// 1->0 scan-mode edge case.
	w.RegisterTableInOut(table_in_out.NewGeoEncodeFunction())
	w.RegisterTableInOut(table_in_out.NewGeoEncode3Function())
	w.RegisterTableInOut(table_in_out.NewRowSumFunction())
	w.RegisterTableInOut(table_in_out.NewBlendedDropFunction())
	// Batched correlated-LATERAL fixtures (table_in_out/lateral_batch.test):
	// blended_explode carries per-output-row vgi_rpc.parent_row provenance,
	// projectable_blended exercises the projection fallback, and
	// hostile_provenance emits malformed provenance the extension must reject.
	w.RegisterTableInOut(table_in_out.NewBlendedExplodeFunction())
	w.RegisterTableInOut(table_in_out.NewProjectableBlendedFunction())
	w.RegisterTableInOut(table_in_out.NewHostileProvenanceFunction())
	// Exchange-mode result-cache fixtures (cache/exchange_*.test): cached_double
	// (blended LATERAL cache), cached_echo (classic streaming cache),
	// cached_sum_all (buffered cache), and the two always-revalidate (304)
	// fixtures (cache/exchange_revalidate.test).
	// Schema-disambiguation probes for the two exchange-mode shapes: one name
	// per shape declared in two schemas of the same catalog. Both bind through
	// VgiTableInOutBind, and the buffered pair additionally re-resolves on the
	// unary process/combine RPCs.
	w.RegisterTableInOut(table_in_out.NewSameNameTransformFunction("main"))
	w.RegisterTableInOutInSchema("data", table_in_out.NewSameNameTransformFunction("data"))
	w.RegisterTableBuffering(table_in_out.NewSameNameBufferedFunction("main"))
	w.RegisterTableBufferingInSchema("data", table_in_out.NewSameNameBufferedFunction("data"))
	w.RegisterTableInOut(table_in_out.NewCachedDoubleFunction())
	// cached_explode is the per-VALUE memo fixture: 1:0 / 1:1 / 1:N by input,
	// emitted with interleaved parents (cache/per_value_multi_batch.test,
	// cache/per_value_negative_memo.test).
	w.RegisterTableInOut(table_in_out.NewCachedExplodeFunction())
	w.RegisterTableInOut(table_in_out.NewCachedEchoFunction())
	w.RegisterTableInOut(table_in_out.NewCachedRevalidatingEchoFunction())
	w.RegisterTableInOut(table_in_out.NewCachedRevalidatingDoubleFunction())
	w.RegisterTableBuffering(&table_in_out.CachedSumAllColumnsFunction{})
	w.RegisterTableInOut(table_in_out.NewExceptionFinalizeFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionProcessFunction())
	w.RegisterTableInOut(table_in_out.NewFilterBySettingFunction())
	w.RegisterTableInOut(table_in_out.NewRepeatInputsFunction())
	w.RegisterTableInOut(table_in_out.NewSlowCancellableInOutFunction())
	w.RegisterTableBuffering(&table_in_out.SumAllColumnsFunction{})
	w.RegisterTableInOut(table_in_out.NewUnnestTensorRowsFunction())
	w.RegisterTableInOut(table_in_out.NewSecretInOutFunction())
}
