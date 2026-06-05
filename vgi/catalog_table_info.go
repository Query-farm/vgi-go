// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// TableInfo describes a table in the catalog for wire serialization.
type TableInfo struct {
	Name                     string
	SchemaName               string
	Comment                  string
	Tags                     map[string]string
	Columns                  *arrow.Schema // serialized as IPC schema bytes
	NotNullConstraints       []int32
	UniqueConstraints        [][]int32
	CheckConstraints         []string
	PrimaryKeyConstraints    [][]int32
	ForeignKeyConstraints    [][]byte // each []byte is an IPC-serialized FK RecordBatch
	SupportsInsert           bool
	SupportsUpdate           bool
	SupportsDelete           bool
	SupportsReturning        bool
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

	// RequiredFieldFilterPaths lists dotted-path column references that MUST
	// appear in a WHERE expression for any scan of this table (top-level names
	// like "country" or struct subfields like "bbox.xmin"). Empty (default)
	// means no enforcement. The C++ extension's optimizer pass consults this
	// list at bind time and throws BinderException listing any unsatisfied
	// paths. Satisfaction is prefix-based — a filter on a parent path satisfies
	// every required child path.
	RequiredFieldFilterPaths []string
}

var tableInfoSchema = generated.TableInfoSchema

// SerializeTableInfo serializes a TableInfo to IPC bytes.
func SerializeTableInfo(info *TableInfo) ([]byte, error) {
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
		for k, v := range info.Tags {
			kb.Append(k)
			vb.Append(v)
		}
	}

	// name
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	// schema_name
	schemaNameBuilder := array.NewStringBuilder(mem)
	defer schemaNameBuilder.Release()
	schemaNameBuilder.Append(info.SchemaName)

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

	// not_null_constraints
	notNullBuilder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
	defer notNullBuilder.Release()
	notNullBuilder.Append(true)
	if len(info.NotNullConstraints) > 0 {
		vb := notNullBuilder.ValueBuilder().(*array.Int32Builder)
		for _, idx := range info.NotNullConstraints {
			vb.Append(idx)
		}
	}

	// unique_constraints (list of list of int32)
	uniqueBuilder := array.NewListBuilder(mem, arrow.ListOf(arrow.PrimitiveTypes.Int32))
	defer uniqueBuilder.Release()
	uniqueBuilder.Append(true)
	if len(info.UniqueConstraints) > 0 {
		innerListBuilder := uniqueBuilder.ValueBuilder().(*array.ListBuilder)
		for _, group := range info.UniqueConstraints {
			innerListBuilder.Append(true)
			innerVB := innerListBuilder.ValueBuilder().(*array.Int32Builder)
			for _, idx := range group {
				innerVB.Append(idx)
			}
		}
	}

	// check_constraints
	checkBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer checkBuilder.Release()
	checkBuilder.Append(true)
	if len(info.CheckConstraints) > 0 {
		vb := checkBuilder.ValueBuilder().(*array.StringBuilder)
		for _, expr := range info.CheckConstraints {
			vb.Append(expr)
		}
	}

	// primary_key_constraints (list of list of int32)
	pkBuilder := array.NewListBuilder(mem, arrow.ListOf(arrow.PrimitiveTypes.Int32))
	defer pkBuilder.Release()
	pkBuilder.Append(true)
	if len(info.PrimaryKeyConstraints) > 0 {
		innerListBuilder := pkBuilder.ValueBuilder().(*array.ListBuilder)
		for _, group := range info.PrimaryKeyConstraints {
			innerListBuilder.Append(true)
			innerVB := innerListBuilder.ValueBuilder().(*array.Int32Builder)
			for _, idx := range group {
				innerVB.Append(idx)
			}
		}
	}

	// foreign_key_constraints (list of binary)
	fkBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	defer fkBuilder.Release()
	fkBuilder.Append(true)
	if len(info.ForeignKeyConstraints) > 0 {
		vb := fkBuilder.ValueBuilder().(*array.BinaryBuilder)
		for _, fkBytes := range info.ForeignKeyConstraints {
			vb.Append(fkBytes)
		}
	}

	siBuilder := array.NewBooleanBuilder(mem)
	defer siBuilder.Release()
	siBuilder.Append(info.SupportsInsert)

	suBuilder := array.NewBooleanBuilder(mem)
	defer suBuilder.Release()
	suBuilder.Append(info.SupportsUpdate)

	sdBuilder := array.NewBooleanBuilder(mem)
	defer sdBuilder.Release()
	sdBuilder.Append(info.SupportsDelete)

	srBuilder := array.NewBooleanBuilder(mem)
	defer srBuilder.Release()
	srBuilder.Append(info.SupportsReturning)

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

	// cardinality_estimate / cardinality_max: schema is non-nullable int64,
	// but the C++ extension reads via `row[...].as<int64_t>()` which yields
	// std::nullopt for nulls. Match vgi-python: write null when unset.
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

	// required_field_filter_paths (non-null list of string)
	rffpBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer rffpBuilder.Release()
	rffpBuilder.Append(true)
	if len(info.RequiredFieldFilterPaths) > 0 {
		vb := rffpBuilder.ValueBuilder().(*array.StringBuilder)
		for _, p := range info.RequiredFieldFilterPaths {
			vb.Append(p)
		}
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaNameBuilder.NewArray(),
		columnsBuilder.NewArray(),
		notNullBuilder.NewArray(),
		uniqueBuilder.NewArray(),
		checkBuilder.NewArray(),
		pkBuilder.NewArray(),
		fkBuilder.NewArray(),
		siBuilder.NewArray(),
		suBuilder.NewArray(),
		sdBuilder.NewArray(),
		srBuilder.NewArray(),
		scsBuilder.NewArray(),
		scanFunctionArr,
		insertFunctionArr,
		updateFunctionArr,
		deleteFunctionArr,
		cardEstBuilder.NewArray(),
		cardMaxBuilder.NewArray(),
		columnStatsArr,
		bindResultArr,
		rffpBuilder.NewArray(),
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
