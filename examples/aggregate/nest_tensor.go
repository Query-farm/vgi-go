// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================================
// nest_tensor — aggregate rows into a dense N-D tensor keyed by axis coords.
//
// Mirrors vgi-python's NestTensorFunction. Output schema:
//   struct {
//     tensor: list<list<...<value_type>>>  (N nested levels, one per axis)
//     axes:   struct { axis_1: list<T1>, ..., axis_N: list<TN> }
//   }
// ============================================================================

const nestTensorDefaultMaxCells = 10_000_000

func nestTensorMaxCells() (int64, error) {
	raw := os.Getenv("VGI_NEST_TENSOR_MAX_CELLS")
	if raw == "" {
		return nestTensorDefaultMaxCells, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("NestTensorError: nest_tensor: VGI_NEST_TENSOR_MAX_CELLS must be an integer, got %q", raw)
	}
	if v <= 0 {
		return 0, fmt.Errorf("NestTensorError: nest_tensor: VGI_NEST_TENSOR_MAX_CELLS must be positive")
	}
	return v, nil
}

// NestTensorState accumulates rows per group as an IPC-serialized RecordBatch
// with schema (value: V, axes: struct<axis_1: T1, ..., axis_N: TN>).
type NestTensorState struct {
	RowsIPC []byte
}

type NestTensorFunction struct{}

var _ vgi.AggregateFunction = (*NestTensorFunction)(nil)

func (NestTensorFunction) Name() string { return "nest_tensor" }

func (NestTensorFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Collect rows into a dense N-D tensor plus per-axis coordinates",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// nestTensorArgs is the typed argument schema for vgi_nest_tensor().
type nestTensorArgs struct {
	Value any `vgi:"pos=0,const=false,doc=Cell value"`
	Axes  any `vgi:"pos=1,const=false,doc=Axis coordinates (struct)"`
}

func (NestTensorFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(nestTensorArgs{})
}

func (NestTensorFunction) OnBind(params *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil || params.InputSchema.NumFields() < 2 {
		return nil, fmt.Errorf("NestTensorError: nest_tensor: expected 2 arguments (value, axes struct)")
	}
	valueType := params.InputSchema.Field(0).Type
	axesType := params.InputSchema.Field(1).Type
	structType, ok := axesType.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("NestTensorError: nest_tensor: second argument must be a struct, got %s", axesType)
	}
	if structType.NumFields() == 0 {
		return nil, fmt.Errorf("NestTensorError: nest_tensor: axes struct must have at least one field")
	}
	for _, f := range structType.Fields() {
		if err := validateCoordType(f.Name, f.Type); err != nil {
			return nil, err
		}
	}
	out := nestTensorOutputStruct(valueType, structType)
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "result", Type: out, Nullable: true}}, nil),
	}, nil
}

func validateCoordType(name string, t arrow.DataType) error {
	switch t.ID() {
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return fmt.Errorf("NestTensorError: nest_tensor: axis '%s' has floating-point type %s; floats are not supported as coord types (NaN breaks equality)", name, t)
	case arrow.STRUCT, arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST, arrow.MAP:
		return fmt.Errorf("NestTensorError: nest_tensor: axis '%s' has nested type %s; only scalar coord types are supported", name, t)
	}
	return nil
}

func nestTensorOutputStruct(valueType arrow.DataType, axes *arrow.StructType) *arrow.StructType {
	tensorType := arrow.DataType(valueType)
	for i := 0; i < axes.NumFields(); i++ {
		tensorType = arrow.ListOf(tensorType)
	}
	axesOutFields := make([]arrow.Field, axes.NumFields())
	for i, f := range axes.Fields() {
		axesOutFields[i] = arrow.Field{Name: f.Name, Type: arrow.ListOf(f.Type), Nullable: true}
	}
	return arrow.StructOf(
		arrow.Field{Name: "tensor", Type: tensorType, Nullable: true},
		arrow.Field{Name: "axes", Type: arrow.StructOf(axesOutFields...), Nullable: true},
	)
}

func (NestTensorFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &NestTensorState{}
}

func (NestTensorFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, p *vgi.AggregateProcessParams) error {
	if len(columns) < 2 {
		return fmt.Errorf("NestTensorError: nest_tensor: expected 2 columns, got %d", len(columns))
	}
	value := columns[0]
	axes, ok := columns[1].(*array.Struct)
	if !ok {
		return fmt.Errorf("NestTensorError: nest_tensor: axes argument must be a struct array, got %T", columns[1])
	}
	axesType := axes.DataType().(*arrow.StructType)
	n := gids.Len()

	// Partition rows by group, validating null/duplicate coords within the batch.
	perGroup := map[int64][]int{}
	perGroupSeen := map[int64]map[string]bool{}
	for i := 0; i < n; i++ {
		gid := gids.At(i)
		if axes.IsNull(i) {
			continue
		}
		coordKey, coordDesc, err := buildCoordKey(axes, axesType, i, gid)
		if err != nil {
			return err
		}
		seen := perGroupSeen[gid]
		if seen == nil {
			seen = map[string]bool{}
			perGroupSeen[gid] = seen
		}
		if seen[coordKey] {
			return fmt.Errorf("NestTensorError: nest_tensor: duplicate coordinate %s in group %d", coordDesc, gid)
		}
		seen[coordKey] = true
		perGroup[gid] = append(perGroup[gid], i)
	}
	if len(perGroup) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: value.DataType(), Nullable: true},
		{Name: "axes", Type: axesType, Nullable: true},
	}, nil)

	for gid, rowIdx := range perGroup {
		indices := buildInt64IndexArray(mem, rowIdx)
		valSlice, err := takeArray(value, indices)
		if err != nil {
			indices.Release()
			return fmt.Errorf("NestTensorError: nest_tensor: take value slice: %w", err)
		}
		axesSlice, err := takeArray(axes, indices)
		if err != nil {
			indices.Release()
			valSlice.Release()
			return fmt.Errorf("NestTensorError: nest_tensor: take axes slice: %w", err)
		}
		indices.Release()
		rb := array.NewRecordBatch(schema, []arrow.Array{valSlice, axesSlice}, int64(len(rowIdx)))
		valSlice.Release()
		axesSlice.Release()

		state := vgi.EnsureState(states, gid, func() *NestTensorState { return &NestTensorState{} })
		merged, err := appendBatchToState(state.RowsIPC, rb)
		rb.Release()
		if err != nil {
			return err
		}
		state.RowsIPC = merged
	}
	return nil
}

func (NestTensorFunction) Combine(src, tgt interface{}, p *vgi.AggregateProcessParams) (interface{}, error) {
	s := src.(*NestTensorState)
	t := tgt.(*NestTensorState)
	if len(s.RowsIPC) == 0 {
		return t, nil
	}
	if len(t.RowsIPC) == 0 {
		return &NestTensorState{RowsIPC: append([]byte(nil), s.RowsIPC...)}, nil
	}
	// Merge: deserialize target batch, then append source batches to it.
	sBatches, err := readAllBatches(s.RowsIPC)
	if err != nil {
		return nil, err
	}
	defer releaseBatches(sBatches)
	tBatches, err := readAllBatches(t.RowsIPC)
	if err != nil {
		return nil, err
	}
	defer releaseBatches(tBatches)
	if len(tBatches) == 0 {
		return &NestTensorState{RowsIPC: append([]byte(nil), s.RowsIPC...)}, nil
	}
	combined, err := serializeBatches(tBatches[0].Schema(), append(append([]arrow.RecordBatch{}, tBatches...), sBatches...))
	if err != nil {
		return nil, err
	}
	return &NestTensorState{RowsIPC: combined}, nil
}

func (NestTensorFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	outSchema := p.OutputSchema
	outField := outSchema.Field(0)
	outStruct, ok := outField.Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("nest_tensor.finalize: expected struct output, got %s", outField.Type)
	}
	tensorField, _ := outStruct.FieldByName("tensor")
	axesOutField, _ := outStruct.FieldByName("axes")
	axesOutType := axesOutField.Type.(*arrow.StructType)
	axisNames := make([]string, axesOutType.NumFields())
	axisTypes := make([]arrow.DataType, axesOutType.NumFields())
	for i, f := range axesOutType.Fields() {
		axisNames[i] = f.Name
		axisTypes[i] = f.Type.(*arrow.ListType).Elem()
	}
	// valueType is the innermost element of the nested tensor list.
	valueType := tensorField.Type
	for i := 0; i < len(axisNames); i++ {
		valueType = valueType.(*arrow.ListType).Elem()
	}

	maxCells, err := nestTensorMaxCells()
	if err != nil {
		return nil, err
	}

	structBuilder := array.NewStructBuilder(mem, outStruct)
	defer structBuilder.Release()
	tensorBuilder := structBuilder.FieldBuilder(0)
	axesBuilder := structBuilder.FieldBuilder(1).(*array.StructBuilder)
	// Per-axis field builders: outer list<inner>.
	axisListBuilders := make([]*array.ListBuilder, len(axisNames))
	for i := range axisNames {
		axisListBuilders[i] = axesBuilder.FieldBuilder(i).(*array.ListBuilder)
	}

	for _, gid := range gids {
		structBuilder.Append(true)
		st, _ := states[gid].(*NestTensorState)
		var table []arrow.RecordBatch
		if st != nil && len(st.RowsIPC) > 0 {
			table, err = readAllBatches(st.RowsIPC)
			if err != nil {
				return nil, err
			}
		}
		// Empty group — zero-shape tensor + empty axes lists.
		if len(table) == 0 {
			if err := appendEmptyTensor(tensorBuilder, tensorField.Type); err != nil {
				return nil, err
			}
			for _, lb := range axisListBuilders {
				lb.Append(true)
			}
			continue
		}
		err = materialiseGroup(tensorBuilder, axisListBuilders, axisNames, axisTypes, table, gid, maxCells)
		releaseBatches(table)
		if err != nil {
			return nil, err
		}
		axesBuilder.Append(true)
	}

	col := structBuilder.NewArray()
	defer col.Release()
	return array.NewRecordBatch(outSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// materialiseGroup consumes one group's accumulated (value, axes) rows and
// appends a single tensor cell and matching per-axis coordinate lists.
func materialiseGroup(tensorBuilder array.Builder, axisListBuilders []*array.ListBuilder,
	axisNames []string, axisTypes []arrow.DataType, table []arrow.RecordBatch, gid int64, maxCells int64) error {
	// Collect distinct sorted coord values per axis, plus a flat row list of
	// (value_index, coord_indices).
	type rowEntry struct {
		batchIdx int
		rowIdx   int
		coords   []int // index into distinct-sorted axis values
	}
	perAxisDistinct := make([]map[string]int, len(axisNames)) // encoded-key → distinct idx (pre-sort)
	perAxisKeys := make([][]string, len(axisNames))           // ordered distinct encoded keys
	axisFieldArrays := make([][]arrow.Array, len(axisNames))  // per-batch axis field array
	for a := range axisNames {
		perAxisDistinct[a] = map[string]int{}
	}

	totalRows := 0
	for _, b := range table {
		sa := b.Column(1).(*array.Struct)
		for a, name := range axisNames {
			idx := findStructFieldByName(sa.DataType().(*arrow.StructType), name)
			axisFieldArrays[a] = append(axisFieldArrays[a], sa.Field(idx))
		}
		totalRows += int(b.NumRows())
	}
	// Assign raw distinct keys then sort them.
	entries := make([]rowEntry, 0, totalRows)
	for bi, b := range table {
		n := int(b.NumRows())
		for ri := 0; ri < n; ri++ {
			re := rowEntry{batchIdx: bi, rowIdx: ri, coords: make([]int, len(axisNames))}
			for a := range axisNames {
				key := scalarKey(axisFieldArrays[a][bi], ri)
				if _, ok := perAxisDistinct[a][key]; !ok {
					perAxisDistinct[a][key] = -1 // placeholder; assigned after sort
					perAxisKeys[a] = append(perAxisKeys[a], key)
				}
			}
			entries = append(entries, re)
		}
	}

	// Sort each axis's distinct keys and compute ordered index map.
	perAxisSortedKeys := make([][]string, len(axisNames))
	perAxisGetScalar := make([]func(key string) (int, int), len(axisNames)) // batch, row of first occurrence
	for a := range axisNames {
		// Record first-occurrence (batch,row) per key to drive the per-axis output.
		first := map[string][2]int{}
		// Walk entries in encounter order for deterministic first-occurrence.
		for bi, arr := range axisFieldArrays[a] {
			for ri := 0; ri < arr.Len(); ri++ {
				key := scalarKey(arr, ri)
				if _, ok := first[key]; !ok {
					first[key] = [2]int{bi, ri}
				}
			}
		}
		sorted := append([]string(nil), perAxisKeys[a]...)
		sort.Slice(sorted, func(i, j int) bool {
			bi, ri := first[sorted[i]][0], first[sorted[i]][1]
			bj, rj := first[sorted[j]][0], first[sorted[j]][1]
			return compareScalar(axisFieldArrays[a][bi], ri, axisFieldArrays[a][bj], rj) < 0
		})
		perAxisSortedKeys[a] = sorted
		idxOf := map[string]int{}
		for i, k := range sorted {
			idxOf[k] = i
		}
		perAxisDistinct[a] = idxOf
		perAxisGetScalar[a] = func(key string) (int, int) { p := first[key]; return p[0], p[1] }
	}

	// Fill coords on each entry.
	eIdx := 0
	for bi, b := range table {
		n := int(b.NumRows())
		for ri := 0; ri < n; ri++ {
			for a := range axisNames {
				entries[eIdx].coords[a] = perAxisDistinct[a][scalarKey(axisFieldArrays[a][bi], ri)]
			}
			eIdx++
		}
	}

	// Dimensions check.
	shape := make([]int, len(axisNames))
	var total int64 = 1
	for a := range axisNames {
		shape[a] = len(perAxisSortedKeys[a])
		total *= int64(shape[a])
	}
	if total > maxCells {
		return fmt.Errorf("NestTensorError: nest_tensor: tensor has %d cells (shape %v) exceeds VGI_NEST_TENSOR_MAX_CELLS=%d (group %d)", total, shape, maxCells, gid)
	}

	// flatIdx(coords) assuming row-major order on axes.
	flatIndex := func(coords []int) int {
		idx := 0
		for a := range coords {
			idx = idx*shape[a] + coords[a]
		}
		return idx
	}

	// Track which flat cells have been filled; build dense row layout for value column.
	values := make([]arrow.RecordBatch, 0, 1)
	_ = values
	valueMap := make([]*valueRef, total)
	// Also remember duplicate detection across batches (parallel partitions).
	for i, re := range entries {
		_ = i
		idx := flatIndex(re.coords)
		if valueMap[idx] != nil {
			// Cross-partition duplicate.
			coord := map[string]string{}
			for a, name := range axisNames {
				coord[name] = perAxisSortedKeys[a][re.coords[a]]
			}
			return fmt.Errorf("NestTensorError: nest_tensor: duplicate coordinate (group %d, cell %v)", gid, coord)
		}
		valueMap[idx] = &valueRef{batchIdx: re.batchIdx, rowIdx: re.rowIdx}
	}

	// Emit nested list structure. The tensorBuilder is an arbitrarily-nested
	// ListBuilder; we build by walking recursively.
	if err := buildNestedTensor(tensorBuilder, shape, 0, func(flat int) valueRef {
		if valueMap[flat] == nil {
			return valueRef{batchIdx: -1}
		}
		return *valueMap[flat]
	}, table, 1); err != nil {
		return err
	}

	// Emit axes lists: for each axis, append list of distinct values in sorted order.
	for a := range axisNames {
		lb := axisListBuilders[a]
		lb.Append(true)
		vb := lb.ValueBuilder()
		for _, key := range perAxisSortedKeys[a] {
			bi, ri := perAxisGetScalar[a](key)
			if err := appendScalarFromArray(vb, axisFieldArrays[a][bi], ri); err != nil {
				return fmt.Errorf("NestTensorError: nest_tensor: build axis '%s': %w", axisNames[a], err)
			}
		}
	}
	return nil
}

// valueRef points to a single cell in the accumulated (value, axes) batches.
type valueRef struct {
	batchIdx int
	rowIdx   int
}

// buildNestedTensor walks the shape dimensions recursively, appending either
// a sub-list or a leaf value via lookup(flat). tensorColIdx is the column index
// in the per-batch record-batch holding the value column (column 0).
func buildNestedTensor(b array.Builder, shape []int, depth int, lookup func(int) valueRef, table []arrow.RecordBatch, _ int) error {
	// Compute stride for this level.
	stride := 1
	for i := depth + 1; i < len(shape); i++ {
		stride *= shape[i]
	}
	if depth == len(shape) {
		// Leaf value — single scalar.
		vr := lookup(0)
		if vr.batchIdx < 0 {
			b.AppendNull()
			return nil
		}
		return appendScalarFromArray(b, table[vr.batchIdx].Column(0), vr.rowIdx)
	}
	lb, ok := b.(*array.ListBuilder)
	if !ok {
		return fmt.Errorf("NestTensorError: nest_tensor: expected list builder at depth %d, got %T", depth, b)
	}
	lb.Append(true)
	vb := lb.ValueBuilder()
	for i := 0; i < shape[depth]; i++ {
		// Offset view: cells in [i*stride, i*stride+stride).
		base := i * stride
		if err := buildNestedTensor(vb, shape, depth+1, func(flat int) valueRef { return lookup(base + flat) }, table, 0); err != nil {
			return err
		}
	}
	return nil
}

// appendEmptyTensor appends a deep zero-shape nested-list value (all empty
// lists all the way to innermost depth).
func appendEmptyTensor(b array.Builder, t arrow.DataType) error {
	lt, ok := t.(*arrow.ListType)
	if !ok {
		b.AppendNull()
		return nil
	}
	lb := b.(*array.ListBuilder)
	lb.Append(true)
	_ = lt
	return nil
}

// appendScalarFromArray copies a single scalar value at arr[idx] into the
// given builder. Supports the scalar types allowed as nest_tensor values.
func appendScalarFromArray(b array.Builder, arr arrow.Array, idx int) error {
	if arr.IsNull(idx) {
		b.AppendNull()
		return nil
	}
	switch a := arr.(type) {
	case *array.Boolean:
		b.(*array.BooleanBuilder).Append(a.Value(idx))
	case *array.Int8:
		b.(*array.Int8Builder).Append(a.Value(idx))
	case *array.Int16:
		b.(*array.Int16Builder).Append(a.Value(idx))
	case *array.Int32:
		b.(*array.Int32Builder).Append(a.Value(idx))
	case *array.Int64:
		b.(*array.Int64Builder).Append(a.Value(idx))
	case *array.Uint8:
		b.(*array.Uint8Builder).Append(a.Value(idx))
	case *array.Uint16:
		b.(*array.Uint16Builder).Append(a.Value(idx))
	case *array.Uint32:
		b.(*array.Uint32Builder).Append(a.Value(idx))
	case *array.Uint64:
		b.(*array.Uint64Builder).Append(a.Value(idx))
	case *array.Float32:
		b.(*array.Float32Builder).Append(a.Value(idx))
	case *array.Float64:
		b.(*array.Float64Builder).Append(a.Value(idx))
	case *array.String:
		b.(*array.StringBuilder).Append(a.Value(idx))
	case *array.Binary:
		b.(*array.BinaryBuilder).Append(a.Value(idx))
	case *array.Date32:
		b.(*array.Date32Builder).Append(a.Value(idx))
	case *array.Date64:
		b.(*array.Date64Builder).Append(a.Value(idx))
	case *array.Timestamp:
		b.(*array.TimestampBuilder).Append(a.Value(idx))
	default:
		return fmt.Errorf("NestTensorError: nest_tensor: unsupported scalar type %s", arr.DataType())
	}
	return nil
}

// scalarKey returns a string key suitable for equality/dedup on scalar cells.
// Uses a type-prefixed format so e.g. int 1 != string "1".
func scalarKey(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return "\x00null"
	}
	switch a := arr.(type) {
	case *array.Boolean:
		if a.Value(idx) {
			return "b:1"
		}
		return "b:0"
	case *array.Int8:
		return "i:" + strconv.FormatInt(int64(a.Value(idx)), 10)
	case *array.Int16:
		return "i:" + strconv.FormatInt(int64(a.Value(idx)), 10)
	case *array.Int32:
		return "i:" + strconv.FormatInt(int64(a.Value(idx)), 10)
	case *array.Int64:
		return "i:" + strconv.FormatInt(a.Value(idx), 10)
	case *array.Uint8:
		return "u:" + strconv.FormatUint(uint64(a.Value(idx)), 10)
	case *array.Uint16:
		return "u:" + strconv.FormatUint(uint64(a.Value(idx)), 10)
	case *array.Uint32:
		return "u:" + strconv.FormatUint(uint64(a.Value(idx)), 10)
	case *array.Uint64:
		return "u:" + strconv.FormatUint(a.Value(idx), 10)
	case *array.String:
		return "s:" + a.Value(idx)
	case *array.Binary:
		return "B:" + string(a.Value(idx))
	case *array.Date32:
		return "d32:" + strconv.FormatInt(int64(a.Value(idx)), 10)
	case *array.Date64:
		return "d64:" + strconv.FormatInt(int64(a.Value(idx)), 10)
	case *array.Timestamp:
		return "ts:" + strconv.FormatInt(int64(a.Value(idx)), 10)
	default:
		return "?:" + fmt.Sprintf("%v", arr.GetOneForMarshal(idx))
	}
}

// compareScalar orders scalar values for axis sorting. Returns <0, 0, >0.
func compareScalar(a arrow.Array, ai int, b arrow.Array, bi int) int {
	// Both arrays have the same Arrow type because they come from the same
	// axis field.
	switch ax := a.(type) {
	case *array.Int8:
		bx := b.(*array.Int8)
		return cmpInt64(int64(ax.Value(ai)), int64(bx.Value(bi)))
	case *array.Int16:
		bx := b.(*array.Int16)
		return cmpInt64(int64(ax.Value(ai)), int64(bx.Value(bi)))
	case *array.Int32:
		bx := b.(*array.Int32)
		return cmpInt64(int64(ax.Value(ai)), int64(bx.Value(bi)))
	case *array.Int64:
		bx := b.(*array.Int64)
		return cmpInt64(ax.Value(ai), bx.Value(bi))
	case *array.Uint8:
		bx := b.(*array.Uint8)
		return cmpUint64(uint64(ax.Value(ai)), uint64(bx.Value(bi)))
	case *array.Uint16:
		bx := b.(*array.Uint16)
		return cmpUint64(uint64(ax.Value(ai)), uint64(bx.Value(bi)))
	case *array.Uint32:
		bx := b.(*array.Uint32)
		return cmpUint64(uint64(ax.Value(ai)), uint64(bx.Value(bi)))
	case *array.Uint64:
		bx := b.(*array.Uint64)
		return cmpUint64(ax.Value(ai), bx.Value(bi))
	case *array.Boolean:
		av, bv := ax.Value(ai), b.(*array.Boolean).Value(bi)
		switch {
		case av == bv:
			return 0
		case !av:
			return -1
		default:
			return 1
		}
	case *array.String:
		bv := b.(*array.String).Value(bi)
		av := ax.Value(ai)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case *array.Binary:
		return bytes.Compare(ax.Value(ai), b.(*array.Binary).Value(bi))
	case *array.Date32:
		return cmpInt64(int64(ax.Value(ai)), int64(b.(*array.Date32).Value(bi)))
	case *array.Date64:
		return cmpInt64(int64(ax.Value(ai)), int64(b.(*array.Date64).Value(bi)))
	case *array.Timestamp:
		return cmpInt64(int64(ax.Value(ai)), int64(b.(*array.Timestamp).Value(bi)))
	}
	return 0
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// buildCoordKey returns a string key uniquely identifying an axis coordinate
// and a human-readable description for error messages.
func buildCoordKey(axes *array.Struct, axesType *arrow.StructType, row int, gid int64) (string, string, error) {
	var key bytes.Buffer
	var desc bytes.Buffer
	desc.WriteByte('{')
	for i := 0; i < axesType.NumFields(); i++ {
		name := axesType.Field(i).Name
		col := axes.Field(i)
		if col.IsNull(row) {
			return "", "", fmt.Errorf("NestTensorError: nest_tensor: null coord value for axis '%s' at row %d (group %d)", name, row, gid)
		}
		if i > 0 {
			key.WriteByte('|')
			desc.WriteString(", ")
		}
		key.WriteString(name)
		key.WriteByte('=')
		key.WriteString(scalarKey(col, row))
		desc.WriteString(name)
		desc.WriteByte('=')
		desc.WriteString(scalarKey(col, row))
	}
	desc.WriteByte('}')
	return key.String(), desc.String(), nil
}

// ---------------------------------------------------------------------------
// Batch serialization helpers for state storage.
// ---------------------------------------------------------------------------

func appendBatchToState(prior []byte, batch arrow.RecordBatch) ([]byte, error) {
	priorBatches, err := readAllBatches(prior)
	if err != nil {
		return nil, err
	}
	defer releaseBatches(priorBatches)
	return serializeBatches(batch.Schema(), append(priorBatches, batch))
}

func serializeBatches(schema *arrow.Schema, batches []arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	for _, b := range batches {
		if err := w.Write(b); err != nil {
			return nil, fmt.Errorf("NestTensorError: nest_tensor: serialize batch: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readAllBatches(data []byte) ([]arrow.RecordBatch, error) {
	if len(data) == 0 {
		return nil, nil
	}
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("NestTensorError: nest_tensor: read batches: %w", err)
	}
	defer r.Release()
	var out []arrow.RecordBatch
	for r.Next() {
		b := r.RecordBatch()
		b.Retain()
		out = append(out, b)
	}
	if err := r.Err(); err != nil {
		releaseBatches(out)
		return nil, err
	}
	return out, nil
}

func releaseBatches(bs []arrow.RecordBatch) {
	for _, b := range bs {
		b.Release()
	}
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

func findStructFieldByName(t *arrow.StructType, name string) int {
	for i, f := range t.Fields() {
		if f.Name == name {
			return i
		}
	}
	return -1
}

func buildInt64IndexArray(mem memory.Allocator, idx []int) arrow.Array {
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, i := range idx {
		b.Append(int64(i))
	}
	return b.NewArray()
}

// takeArray is a lightweight analogue of pyarrow's Array.take: returns a new
// array containing src at the positions given in idx (an Int64 array).
func takeArray(src arrow.Array, idx arrow.Array) (arrow.Array, error) {
	mem := memory.NewGoAllocator()
	b := array.NewBuilder(mem, src.DataType())
	defer b.Release()
	ii := idx.(*array.Int64)
	for i := 0; i < ii.Len(); i++ {
		row := int(ii.Value(i))
		if err := appendOneValue(b, src, row); err != nil {
			return nil, err
		}
	}
	return b.NewArray(), nil
}

// appendOneValue is the nested-aware counterpart of appendScalarFromArray; it
// handles struct arrays in addition to scalars, which is required for taking
// rows from the axes struct column.
func appendOneValue(b array.Builder, src arrow.Array, row int) error {
	if src.IsNull(row) {
		b.AppendNull()
		return nil
	}
	if sa, ok := src.(*array.Struct); ok {
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i := 0; i < sa.NumField(); i++ {
			if err := appendOneValue(sb.FieldBuilder(i), sa.Field(i), row); err != nil {
				return err
			}
		}
		return nil
	}
	return appendScalarFromArray(b, src, row)
}
