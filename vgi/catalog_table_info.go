// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"maps"
	"slices"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// TableInfo describes a table in the catalog for wire serialization.
type TableInfo struct {
	Constraints              [][]byte // canonical ConstraintInfo IPC, never synthesized from legacy fields
	DefinitionRevision       []byte
	Name                     string
	SchemaPath               []string
	Comment                  string
	Tags                     map[string]string
	Columns                  *arrow.Schema // serialized as IPC schema bytes
	NotNullConstraints       []int32
	UniqueConstraints        [][]int32
	CheckConstraints         []string
	PrimaryKeyConstraints    [][]int32
	ForeignKeyConstraints    [][]byte // each []byte is an IPC-serialized FK RecordBatch
	WriteResultModes         map[string]string
	SupportsColumnStatistics bool

	// Optional inlined function-discovery results. When populated (non-nil),
	// the C++ extension uses the cached bytes and skips the corresponding
	// catalog_table_{scan,insert,update,delete}_function_get RPC. Bytes are
	// the IPC payload from SerializeScanFunctionResult.
	ScanFunction   []byte
	InsertFunction []byte
	UpdateFunction []byte
	DeleteFunction []byte

	// Optional inlined cardinality. When set, the C++ extension uses these
	// directly and skips the table_function_cardinality RPC. Use nil to leave
	// the field unset (per-bind RPC continues to fire).
	CardinalityEstimate *int64
	CardinalityMax      *int64

	// Optional inlined column statistics. Bytes are the IPC payload from
	// SerializeColumnStatistics. When non-nil, the C++ extension skips the
	// per-bind catalog_table_column_statistics_get and the per-scan
	// table_function_statistics RPCs.
	ColumnStatistics []byte

	// Optional inlined bind result. Bytes are the IPC payload from a bind
	// response. When non-nil, the C++ extension threads it straight into
	// bind_data and skips the per-scan bind RPC.
	BindResult []byte

	// RequiredFilters is an AND of OR-groups of raw component paths.
	// Example: {{{"bbox", "xmin"}}, {{"ticker"}, {"cik"}}}.
	// Dots and quotes are literal. Prefix satisfaction operates on components.
	// Empty disables enforcement; the wire value is always an explicit list.
	RequiredFilters [][][]string
	Options         []byte
	Layout          []byte
}

var tableInfoSchema = generated.TableInfoSchema

// SerializeTableInfo serializes a TableInfo to IPC bytes.
func SerializeTableInfo(info *TableInfo) ([]byte, error) {
	if err := ValidateColumnMetadata(info.Columns); err != nil {
		return nil, err
	}
	if len(info.NotNullConstraints)+len(info.UniqueConstraints)+len(info.CheckConstraints)+len(info.PrimaryKeyConstraints)+len(info.ForeignKeyConstraints) > 0 {
		return nil, methodNotImplemented("legacy constraint metadata requires explicit canonical records")
	}
	mem := memory.NewGoAllocator()

	// comment
	commentBuilder := array.NewStringBuilder(mem)
	defer commentBuilder.Release()
	if info.Comment != "" {
		commentBuilder.Append(info.Comment)
	} else {
		commentBuilder.AppendNull()
	}

	// tags
	tagsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer tagsBuilder.Release()
	tagsBuilder.Append(true)
	if len(info.Tags) > 0 {
		kb := tagsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := tagsBuilder.ItemBuilder().(*array.StringBuilder)
		// Sorted, so the same record always encodes to the same bytes.
		for _, k := range slices.Sorted(maps.Keys(info.Tags)) {
			v := info.Tags[k]
			kb.Append(k)
			vb.Append(v)
		}
	}

	// name
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	// schema_path
	schemaPathBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer schemaPathBuilder.Release()
	appendSchemaPath(schemaPathBuilder, info.SchemaPath)

	// columns (serialized as IPC schema bytes)
	columnsBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer columnsBuilder.Release()
	if info.Columns != nil {
		colBytes, err := SerializeSchema(info.Columns)
		if err != nil {
			return nil, err
		}
		columnsBuilder.Append(colBytes)
	} else {
		columnsBuilder.Append([]byte{})
	}

	constraintsBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	defer constraintsBuilder.Release()
	constraintsBuilder.Append(true)
	for _, value := range info.Constraints {
		constraintsBuilder.ValueBuilder().(*array.BinaryBuilder).Append(value)
	}

	writeModesBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer writeModesBuilder.Release()
	writeModesBuilder.Append(true)
	writeModeKeys := make([]string, 0, len(info.WriteResultModes))
	for operation := range info.WriteResultModes {
		writeModeKeys = append(writeModeKeys, operation)
	}
	sort.Strings(writeModeKeys)
	writeModeKeyBuilder := writeModesBuilder.KeyBuilder().(*array.StringBuilder)
	writeModeValueBuilder := writeModesBuilder.ItemBuilder().(*array.StringBuilder)
	for _, operation := range writeModeKeys {
		writeModeKeyBuilder.Append(operation)
		writeModeValueBuilder.Append(info.WriteResultModes[operation])
	}

	scsBuilder := array.NewBooleanBuilder(mem)
	defer scsBuilder.Release()
	scsBuilder.Append(info.SupportsColumnStatistics)

	// Inlined function-discovery payloads. Schema declares these as
	// non-nullable binary, but the C++ extension treats both null and empty
	// bytes as "not present". Match vgi-python: write null when nil.
	appendOptBinary := func(b []byte) *array.Binary {
		bb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer bb.Release()
		if b == nil {
			bb.AppendNull()
		} else {
			bb.Append(b)
		}
		return bb.NewBinaryArray()
	}
	scanFunctionArr := appendOptBinary(info.ScanFunction)
	insertFunctionArr := appendOptBinary(info.InsertFunction)
	updateFunctionArr := appendOptBinary(info.UpdateFunction)
	deleteFunctionArr := appendOptBinary(info.DeleteFunction)
	columnStatsArr := appendOptBinary(info.ColumnStatistics)
	bindResultArr := appendOptBinary(info.BindResult)

	// cardinality_estimate / cardinality_max: nullable int64. The C++ extension
	// reads via `row[...].as<int64_t>()`, which yields std::nullopt for nulls.
	// Match vgi-python: write null when unset.
	cardEstBuilder := array.NewInt64Builder(mem)
	defer cardEstBuilder.Release()
	if info.CardinalityEstimate != nil {
		cardEstBuilder.Append(*info.CardinalityEstimate)
	} else {
		cardEstBuilder.AppendNull()
	}

	cardMaxBuilder := array.NewInt64Builder(mem)
	defer cardMaxBuilder.Release()
	if info.CardinalityMax != nil {
		cardMaxBuilder.Append(*info.CardinalityMax)
	} else {
		cardMaxBuilder.AppendNull()
	}

	// required_filters: non-null AND-of-OR groups of raw component paths.
	rfBuilder := array.NewListBuilder(mem, arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)))
	defer rfBuilder.Release()
	rfBuilder.Append(true)
	if len(info.RequiredFilters) > 0 {
		innerListBuilder := rfBuilder.ValueBuilder().(*array.ListBuilder)
		for _, group := range info.RequiredFilters {
			innerListBuilder.Append(true)
			pathBuilder := innerListBuilder.ValueBuilder().(*array.ListBuilder)
			innerVB := pathBuilder.ValueBuilder().(*array.StringBuilder)
			for _, p := range group {
				pathBuilder.Append(true)
				innerVB.AppendValues(p, nil)
			}
		}
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaPathBuilder.NewArray(),
		columnsBuilder.NewArray(),
		constraintsBuilder.NewArray(),
		appendOptBinary(info.DefinitionRevision),
		writeModesBuilder.NewArray(),
		scsBuilder.NewArray(),
		scanFunctionArr,
		insertFunctionArr,
		updateFunctionArr,
		deleteFunctionArr,
		cardEstBuilder.NewArray(),
		cardMaxBuilder.NewArray(),
		columnStatsArr,
		bindResultArr,
		rfBuilder.NewArray(),
		appendOptBinary(info.Options),
		appendOptBinary(info.Layout),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(tableInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(tableInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// serializeInlineBindResult produces the IPC bytes for an inlined bind result
// carrying a static output schema. It mirrors the payload a regular bind RPC
// would return (vgi-python's BindResponse(output_schema=...).serialize_to_bytes()):
// the given schema in output_schema, null opaque_data, and empty secret-lookup
// lists. Set on TableInfo.BindResult so the C++ extension threads it straight
// into bind_data and skips the per-scan bind RPC.
func serializeInlineBindResult(schema *arrow.Schema) ([]byte, error) {
	schemaBytes, err := SerializeSchema(schema)
	if err != nil {
		return nil, err
	}

	mem := memory.NewGoAllocator()

	outputBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer outputBuilder.Release()
	outputBuilder.Append(schemaBytes)

	opaqueBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer opaqueBuilder.Release()
	opaqueBuilder.AppendNull()

	// Three empty (non-null) string lists: lookup_secret_types/scopes/names.
	emptyList := func() arrow.Array {
		lb := array.NewListBuilder(mem, arrow.BinaryTypes.String)
		defer lb.Release()
		lb.Append(true)
		return lb.NewArray()
	}

	cols := []arrow.Array{
		outputBuilder.NewArray(),
		opaqueBuilder.NewArray(),
		emptyList(),
		emptyList(),
		emptyList(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(generated.BindResultSchema, cols, 1)
	defer batch.Release()

	return SerializeRecordBatch(batch)
}
