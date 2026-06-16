// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import "github.com/apache/arrow-go/v18/arrow"

// TypeBoundPredicate is a function that validates whether an Arrow DataType
// is acceptable for a given argument. Used with ArgSpec.TypeBound to constrain
// "any"-typed arguments to specific type families (e.g., numeric types only).
type TypeBoundPredicate func(arrow.DataType) bool

// FunctionType identifies the kind of VGI function.
type FunctionType string

const (
	FunctionTypeScalar         FunctionType = "scalar"
	FunctionTypeTable          FunctionType = "table"
	FunctionTypeAggregate      FunctionType = "aggregate"
	FunctionTypeTableBuffering FunctionType = "table_buffering"
)

// FunctionStability describes when function results may change.
type FunctionStability string

const (
	StabilityConsistent            FunctionStability = "CONSISTENT"
	StabilityVolatile              FunctionStability = "VOLATILE"
	StabilityConsistentWithinQuery FunctionStability = "CONSISTENT_WITHIN_QUERY"
)

// NullHandling describes how the function handles NULL inputs.
// These values are DuckDB wire-protocol constants and must not be changed.
type NullHandling string

const (
	// NullHandlingDefault tells DuckDB to skip calling the function for NULL
	// inputs and return NULL automatically (standard SQL behaviour).
	NullHandlingDefault NullHandling = "DEFAULT"

	// NullHandlingSpecial tells DuckDB to pass NULL values through to the
	// function, letting it decide what to return. Use this when the function
	// needs to distinguish NULL from non-NULL inputs.
	NullHandlingSpecial NullHandling = "SPECIAL"

	// NullHandlingReceiveNulls is a Go-friendly alias for NullHandlingSpecial.
	NullHandlingReceiveNulls = NullHandlingSpecial
)

// PartitionKind describes the partition shape a table function declares over
// its vgi.partition_column-annotated bind-schema fields. These values are
// DuckDB wire-protocol dictionary constants and must not be changed.
type PartitionKind string

const (
	// PartitionKindNotPartitioned is the default — the function declares no
	// partitioning over the annotated columns.
	PartitionKindNotPartitioned PartitionKind = "NOT_PARTITIONED"

	// PartitionKindSingleValuePartitions means each emitted chunk has exactly
	// one distinct value per partition column. Unlocks DuckDB's
	// PhysicalPartitionedAggregate for GROUP BY over those columns.
	PartitionKindSingleValuePartitions PartitionKind = "SINGLE_VALUE_PARTITIONS"

	// PartitionKindOverlappingPartitions means partitions overlap only at
	// boundaries. Wire-level declarable; DuckDB has no consumer today.
	PartitionKindOverlappingPartitions PartitionKind = "OVERLAPPING_PARTITIONS"

	// PartitionKindDisjointPartitions means partitions are pairwise disjoint.
	// Wire-level declarable; DuckDB has no consumer today.
	PartitionKindDisjointPartitions PartitionKind = "DISJOINT_PARTITIONS"
)

// OrderPreservation declares how a table function's output rows relate to its
// inputs. These values are DuckDB wire-protocol dictionary constants and must
// not be changed. The empty value leaves the field null (C++ extension default).
type OrderPreservation string

const (
	// OrderPreservationUnspecified leaves the field null — the C++ extension
	// picks its default.
	OrderPreservationUnspecified OrderPreservation = ""

	// OrderPreservationPreservesOrder: output rows are in the same order as
	// input rows (DuckDB INSERTION_ORDER).
	OrderPreservationPreservesOrder OrderPreservation = "PRESERVES_ORDER"

	// OrderPreservationNoOrderGuarantee: output order is undefined; DuckDB may
	// freely reorder (DuckDB NO_ORDER).
	OrderPreservationNoOrderGuarantee OrderPreservation = "NO_ORDER_GUARANTEE"

	// OrderPreservationFixedOrder: output is in a fixed mandatory order; DuckDB
	// serializes the pipeline to a single worker to preserve it (FIXED_ORDER).
	OrderPreservationFixedOrder OrderPreservation = "FIXED_ORDER"
)

// OrderDependence declares whether an aggregate's result depends on row order.
// Wire-protocol dictionary constants — must not be changed.
type OrderDependence string

const (
	// OrderDependenceDependent: result changes with row order (FIRST, LAST, LISTAGG).
	OrderDependenceDependent OrderDependence = "ORDER_DEPENDENT"

	// OrderDependenceNotDependent: result is order-independent (SUM, COUNT).
	OrderDependenceNotDependent OrderDependence = "NOT_ORDER_DEPENDENT"
)

// DistinctDependence declares whether a DISTINCT modifier changes an
// aggregate's result. Wire-protocol dictionary constants — must not be changed.
type DistinctDependence string

const (
	// DistinctDependenceDependent: DISTINCT changes the result (COUNT DISTINCT).
	DistinctDependenceDependent DistinctDependence = "DISTINCT_DEPENDENT"

	// DistinctDependenceNotDependent: DISTINCT has no effect (MAX, MIN).
	DistinctDependenceNotDependent DistinctDependence = "NOT_DISTINCT_DEPENDENT"
)

// Phase identifies the table-in-out init phase. Wire-protocol dictionary
// constants — must not be changed.
type Phase string

const (
	// PhaseInput is the streaming-input phase: the function processes input
	// chunks as they arrive.
	PhaseInput Phase = "INPUT"

	// PhaseFinalize is the end-of-stream phase: the function flushes any
	// accumulated state. Only reached for functions with HasFinalize set.
	PhaseFinalize Phase = "FINALIZE"

	// PhaseTableBuffering is the sink init phase for a TableBufferingFunction.
	// After it, traffic moves to the table_buffering_process / _combine RPCs.
	PhaseTableBuffering Phase = "TABLE_BUFFERING"

	// PhaseTableBufferingFinalize opens a producer-mode finalize stream for one
	// finalize_state_id of a TableBufferingFunction.
	PhaseTableBufferingFinalize Phase = "TABLE_BUFFERING_FINALIZE"
)

// OrderByDirection is the sort direction carried by an ORDER BY pushdown hint.
// Wire-protocol dictionary constants — must not be changed.
type OrderByDirection string

const (
	// OrderByAscending sorts smallest-first (SQL ASC).
	OrderByAscending OrderByDirection = "ASC"

	// OrderByDescending sorts largest-first (SQL DESC).
	OrderByDescending OrderByDirection = "DESC"
)

// OrderByNullOrder is the NULL placement carried by an ORDER BY pushdown hint.
// Wire-protocol dictionary constants — must not be changed.
type OrderByNullOrder string

const (
	// NullsFirst places NULL values before non-NULL values.
	NullsFirst OrderByNullOrder = "NULLS_FIRST"

	// NullsLast places NULL values after non-NULL values.
	NullsLast OrderByNullOrder = "NULLS_LAST"
)

// WriteOp identifies which DML operation a writable-table function lookup is
// for. Wire-protocol dictionary constants — must not be changed.
type WriteOp string

const (
	// WriteOpInsert is an INSERT-time function lookup.
	WriteOpInsert WriteOp = "insert"

	// WriteOpUpdate is an UPDATE-time function lookup.
	WriteOpUpdate WriteOp = "update"

	// WriteOpDelete is a DELETE-time function lookup.
	WriteOpDelete WriteOp = "delete"
)

// SecretRequirement describes a secret type that a function needs.
type SecretRequirement struct {
	SecretType string
	SecretName string // empty = not specified
	Scope      string // empty = not specified
}

// SecretLookup describes a scoped secret lookup request for two-phase bind.
type SecretLookup struct {
	SecretType string
	SecretName string
	Scope      string
}

// FunctionMetadata holds descriptive metadata about a function.
type FunctionMetadata struct {
	// Description is a human-readable description.
	Description string
	// Stability controls caching/optimization hints.
	Stability FunctionStability
	// NullHandling controls whether NULLs are passed to the function.
	NullHandling NullHandling
	// ProjectionPushdown indicates support for projection pushdown.
	ProjectionPushdown bool
	// FilterPushdown indicates support for filter pushdown.
	FilterPushdown bool
	// SamplingPushdown indicates support for TABLESAMPLE SYSTEM pushdown.
	SamplingPushdown bool
	// LateMaterialization advertises that the function participates in DuckDB's
	// late-materialization rewrite. DuckDB only honours this when the function
	// also exposes a rowid virtual column and supports projection + filter
	// pushdown; the rowid must be unique, deterministic, and snapshot-stable.
	LateMaterialization bool
	// SupportedExpressionFilters lists DuckDB expression names that the
	// function can absorb into its scan (e.g. "&&", "list_contains",
	// "starts_with"). Without this, DuckDB inserts a separate FILTER node.
	SupportedExpressionFilters []string
	// SupportsWindow indicates an aggregate also implements the windowed
	// callbacks (window_init, window, window_destructor). Ignored for
	// non-aggregate functions.
	SupportsWindow bool
	// StreamingPartitioned indicates an aggregate also implements the
	// streaming-partitioned protocol (aggregate_streaming_open/_chunk/_close).
	// DuckDB's optimizer can replace a LogicalWindow with the streaming
	// operator when the frame is cumulative. The function MUST also keep its
	// standard update/combine/finalize path for the fallback case.
	StreamingPartitioned bool
	// HasFinalize signals a TableInOut function whose Finalize method emits
	// meaningful batches. Defaults to false; set to true only for functions
	// that accumulate during Process and flush at end-of-stream. DuckDB
	// skips the FINALIZE phase RPC entirely when this is false — required
	// for LATERAL compatibility (avoids "FinalExecute not supported for
	// project_input").
	HasFinalize bool
	// OrderPreservation declares how a table function's output rows relate
	// to its inputs. Empty leaves the field null and uses the C++ extension
	// default. Maps to DuckDB's OrderPreservationType.
	OrderPreservation OrderPreservation
	// OrderDependent declares whether the aggregate result depends on the
	// row order. Empty defaults to NOT_ORDER_DEPENDENT.
	OrderDependent OrderDependence
	// DistinctDependent declares whether DISTINCT changes the result.
	// Empty defaults to NOT_DISTINCT_DEPENDENT.
	DistinctDependent DistinctDependence
	// AutoApplyFilters indicates the framework should auto-apply pushdown filters.
	AutoApplyFilters bool
	// Categories is a list of classification tags for the function.
	Categories []string
	// Examples lists usage examples surfaced in the catalog's FunctionInfo.
	// Each example carries SQL, a description, and an optional expected output.
	Examples []CatalogExample
	// ReturnType is the static return type for scalar functions.
	// When set, the catalog registers this concrete type instead of ANY.
	// Leave nil for functions with dynamic return types (resolved at bind time).
	ReturnType arrow.DataType
	// RequiredSecrets lists secret types the function needs at bind time.
	RequiredSecrets []SecretRequirement
	// SupportsBatchIndex opts a table function into per-batch vgi_batch_index
	// tagging (see EmitBatchIndex). The C++ extension enforces monotonicity.
	SupportsBatchIndex bool
	// PartitionKind declares the partition shape of a table function's output
	// (see PartitionField / EmitPartitioned). Empty = NOT_PARTITIONED.
	PartitionKind PartitionKind
	// SourceOrderDependent / SinkOrderDependent / RequiresInputBatchIndex are
	// table-buffering ordering hints (mirror the FunctionInfo fields).
	SourceOrderDependent    bool
	SinkOrderDependent      bool
	RequiresInputBatchIndex bool
}

// DefaultMetadata returns metadata with default values.
func DefaultMetadata() FunctionMetadata {
	return FunctionMetadata{
		Stability:    StabilityConsistent,
		NullHandling: NullHandlingDefault,
	}
}

// ArgSpec describes a single argument in a function's signature.
type ArgSpec struct {
	// Name is the argument name (empty for positional-only).
	Name string
	// Position is the positional index (0-based). -1 for named-only.
	Position int
	// ArrowType is the Arrow type string (e.g., "int64", "varchar", "any").
	ArrowType string
	// Doc is a documentation string.
	Doc string
	// IsConst is true for constant (scalar) parameters.
	IsConst bool
	// IsVarargs is true for variadic parameters.
	IsVarargs bool
	// HasDefault is true if the parameter has a default value.
	HasDefault bool
	// DefaultValue is the string representation of the default.
	DefaultValue string
	// ArrowDataType is an optional concrete Arrow DataType for the argument.
	// When set, it takes precedence over ArrowType for schema building.
	// Use this for complex types like structs where the string representation
	// is insufficient (e.g., arrow.StructOf(...) for typed struct params).
	ArrowDataType arrow.DataType
	// TypeBound is an optional slice of type predicates for "any"-typed arguments.
	// At bind time, the input schema field type must satisfy at least one predicate
	// (OR logic). Nil means no type constraint (any type is accepted).
	TypeBound []TypeBoundPredicate
}
