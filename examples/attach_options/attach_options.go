// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package attach_options implements the reference attach-options example
// worker: it declares one ATTACH option of every supported Arrow/DuckDB
// type and exposes an echo_attach_options() function that round-trips the
// attached values via attach_id.
//
// Mirrors vgi-python's vgi/examples/attach_options.py so the shared
// integration test (test/sql/integration/attach/attach_options_echo.test)
// runs against both implementations.
package attach_options

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

const (
	CatalogName     = "attach_options"
	attachIDSepByte = 0x00
	uuidBytes       = 16
)

// OptionDef describes one declared attach option.
type OptionDef struct {
	Name        string
	Description string
	Type        arrow.DataType
	// Default is a single-element arrow.Array carrying the default value.
	Default arrow.Array
}

// DeclaredOptions lists every option this worker accepts — one per supported
// type. Built at package init so the arrays can be shared by the spec
// serializer, the echo schema, and the default merge.
var DeclaredOptions []OptionDef

// echoSchema is one column per declared option, in declaration order.
var echoSchema *arrow.Schema

func init() {
	mem := memory.NewGoAllocator()

	var opts []OptionDef
	add := func(name, desc string, dt arrow.DataType, arr arrow.Array) {
		opts = append(opts, OptionDef{Name: name, Description: desc, Type: dt, Default: arr})
	}

	// bool
	{
		b := array.NewBooleanBuilder(mem)
		b.Append(true)
		add("opt_bool", "Boolean option", &arrow.BooleanType{}, b.NewArray())
		b.Release()
	}

	// int8..int64
	{
		b := array.NewInt8Builder(mem)
		b.Append(-8)
		add("opt_int8", "int8", arrow.PrimitiveTypes.Int8, b.NewArray())
		b.Release()
	}
	{
		b := array.NewInt16Builder(mem)
		b.Append(-16)
		add("opt_int16", "int16", arrow.PrimitiveTypes.Int16, b.NewArray())
		b.Release()
	}
	{
		b := array.NewInt32Builder(mem)
		b.Append(-32)
		add("opt_int32", "int32", arrow.PrimitiveTypes.Int32, b.NewArray())
		b.Release()
	}
	{
		b := array.NewInt64Builder(mem)
		b.Append(-64)
		add("opt_int64", "int64", arrow.PrimitiveTypes.Int64, b.NewArray())
		b.Release()
	}

	// uint8..uint64
	{
		b := array.NewUint8Builder(mem)
		b.Append(8)
		add("opt_uint8", "uint8", arrow.PrimitiveTypes.Uint8, b.NewArray())
		b.Release()
	}
	{
		b := array.NewUint16Builder(mem)
		b.Append(16)
		add("opt_uint16", "uint16", arrow.PrimitiveTypes.Uint16, b.NewArray())
		b.Release()
	}
	{
		b := array.NewUint32Builder(mem)
		b.Append(32)
		add("opt_uint32", "uint32", arrow.PrimitiveTypes.Uint32, b.NewArray())
		b.Release()
	}
	{
		b := array.NewUint64Builder(mem)
		b.Append(64)
		add("opt_uint64", "uint64", arrow.PrimitiveTypes.Uint64, b.NewArray())
		b.Release()
	}

	// float32/float64
	{
		b := array.NewFloat32Builder(mem)
		b.Append(1.5)
		add("opt_float32", "float32", arrow.PrimitiveTypes.Float32, b.NewArray())
		b.Release()
	}
	{
		b := array.NewFloat64Builder(mem)
		b.Append(2.5)
		add("opt_float64", "float64", arrow.PrimitiveTypes.Float64, b.NewArray())
		b.Release()
	}

	// string
	{
		b := array.NewStringBuilder(mem)
		b.Append("hello")
		add("opt_string", "UTF-8 string", arrow.BinaryTypes.String, b.NewArray())
		b.Release()
	}

	// blob
	{
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		b.Append([]byte{0x00, 0x01, 0x02})
		add("opt_blob", "Binary blob", arrow.BinaryTypes.Binary, b.NewArray())
		b.Release()
	}

	// date32 (days since epoch)
	{
		dt := &arrow.Date32Type{}
		b := array.NewDate32Builder(mem)
		days := int32(time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC).Unix() / 86400)
		b.Append(arrow.Date32(days))
		add("opt_date", "Date", dt, b.NewArray())
		b.Release()
	}

	// time64[us]
	{
		dt := &arrow.Time64Type{Unit: arrow.Microsecond}
		b := array.NewTime64Builder(mem, dt)
		// 12:34:56 → (12*3600 + 34*60 + 56) seconds = 45296 seconds → microseconds
		us := int64(45296) * 1_000_000
		b.Append(arrow.Time64(us))
		add("opt_time", "Time of day", dt, b.NewArray())
		b.Release()
	}

	// timestamp[us]
	{
		dt := &arrow.TimestampType{Unit: arrow.Microsecond}
		b := array.NewTimestampBuilder(mem, dt)
		t := time.Date(2026, 4, 24, 12, 34, 56, 0, time.UTC)
		b.Append(arrow.Timestamp(t.UnixMicro()))
		add("opt_timestamp", "Naive timestamp", dt, b.NewArray())
		b.Release()
	}

	// timestamp[us, UTC]
	{
		dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		b := array.NewTimestampBuilder(mem, dt)
		t := time.Date(2026, 4, 24, 12, 34, 56, 0, time.UTC)
		b.Append(arrow.Timestamp(t.UnixMicro()))
		add("opt_timestamp_tz", "Timestamp with UTC tz", dt, b.NewArray())
		b.Release()
	}

	// decimal128(18, 4) -- default 123.4500 → scaled int = 1234500
	{
		dt := &arrow.Decimal128Type{Precision: 18, Scale: 4}
		b := array.NewDecimal128Builder(mem, dt)
		b.Append(decimal128.FromI64(1234500))
		add("opt_decimal", "Decimal(18,4)", dt, b.NewArray())
		b.Release()
	}

	// list[int64] default [1,2,3]
	{
		dt := arrow.ListOf(arrow.PrimitiveTypes.Int64)
		lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
		vb := lb.ValueBuilder().(*array.Int64Builder)
		lb.Append(true)
		vb.AppendValues([]int64{1, 2, 3}, nil)
		add("opt_list", "List of int64", dt, lb.NewArray())
		lb.Release()
	}

	// struct{a:int64, b:string} default {a:1, b:"x"}
	{
		dt := arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
		)
		sb := array.NewStructBuilder(mem, dt)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int64Builder).Append(1)
		sb.FieldBuilder(1).(*array.StringBuilder).Append("x")
		add("opt_struct", "Struct", dt, sb.NewArray())
		sb.Release()
	}

	DeclaredOptions = opts

	fields := make([]arrow.Field, len(opts))
	for i, o := range opts {
		fields[i] = arrow.Field{Name: o.Name, Type: o.Type}
	}
	echoSchema = arrow.NewSchema(fields, nil)
}

// AttachOptionSpecs returns the worker's declared option specs, each with a
// single-row default batch (column name "value"). Pass to
// vgi.WithAttachOptions.
func AttachOptionSpecs() []vgi.AttachOptionSpec {
	out := make([]vgi.AttachOptionSpec, 0, len(DeclaredOptions))
	for _, o := range DeclaredOptions {
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: o.Type}}, nil)
		o.Default.Retain()
		batch := array.NewRecordBatch(schema, []arrow.Array{o.Default}, 1)
		out = append(out, vgi.AttachOptionSpec{
			Name:         o.Name,
			Description:  o.Description,
			Type:         o.Type,
			DefaultBatch: batch,
		})
	}
	return out
}

// mergeOptionsToEchoBatch builds a single-row RecordBatch matching echoSchema
// by overlaying user-provided option values onto the declared defaults.
func mergeOptionsToEchoBatch(userBatch arrow.RecordBatch) (arrow.RecordBatch, error) {
	userCols := map[string]arrow.Array{}
	if userBatch != nil && userBatch.NumRows() > 0 {
		for i := 0; i < int(userBatch.NumCols()); i++ {
			userCols[userBatch.ColumnName(i)] = userBatch.Column(i)
		}
	}

	fields := make([]arrow.Field, len(DeclaredOptions))
	cols := make([]arrow.Array, len(DeclaredOptions))
	for i, o := range DeclaredOptions {
		if ua, ok := userCols[o.Name]; ok && ua.Len() > 0 && !ua.IsNull(0) {
			ua.Retain()
			cols[i] = array.NewSlice(ua, 0, 1)
			ua.Release()
		} else {
			o.Default.Retain()
			cols[i] = o.Default
		}
		// Use the actual column type (not the declared one) so differences
		// in nested-field nullability between the user batch and the
		// declared schema don't trip NewRecordBatch's strict type check.
		fields[i] = arrow.Field{Name: o.Name, Type: cols[i].DataType()}
	}

	schema := arrow.NewSchema(fields, nil)
	batch := array.NewRecordBatch(schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return batch, nil
}

// EncodeAttachID: uuid (16 bytes) || 0x00 || ipc(batch).
func EncodeAttachID(optionsBytes []byte) ([]byte, error) {
	var userBatch arrow.RecordBatch
	if len(optionsBytes) > 0 {
		b, err := vgi.DeserializeRecordBatch(optionsBytes)
		if err != nil {
			return nil, fmt.Errorf("deserializing attach options batch: %w", err)
		}
		defer b.Release()
		userBatch = b
	}

	merged, err := mergeOptionsToEchoBatch(userBatch)
	if err != nil {
		return nil, err
	}
	defer merged.Release()

	ipcBytes, err := vgi.SerializeRecordBatch(merged)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	id := uuid.New()
	buf.Write(id[:])
	buf.WriteByte(attachIDSepByte)
	buf.Write(ipcBytes)
	return buf.Bytes(), nil
}

// DecodeAttachID recovers the single-row echo batch stashed in attach_id.
func DecodeAttachID(attachID []byte) (arrow.RecordBatch, error) {
	if len(attachID) <= uuidBytes+1 || attachID[uuidBytes] != attachIDSepByte {
		return nil, fmt.Errorf("attach_id does not carry options payload")
	}
	ipcBytes := attachID[uuidBytes+1:]
	return vgi.DeserializeRecordBatch(ipcBytes)
}

// ---------------------------------------------------------------------------
// echo_attach_options table function
// ---------------------------------------------------------------------------

// EchoAttachOptionsFunction emits the single-row record batch carried in
// attach_id.
type EchoAttachOptionsFunction struct{}

var _ vgi.TypedTableFunc[echoState] = (*EchoAttachOptionsFunction)(nil)

func (f *EchoAttachOptionsFunction) Name() string { return "echo_attach_options" }

func (f *EchoAttachOptionsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Echo the attach-time option values carried in attach_id",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *EchoAttachOptionsFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *EchoAttachOptionsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.AttachID == nil {
		return nil, fmt.Errorf("echo_attach_options requires an attach_id")
	}
	batch, err := DecodeAttachID(params.AttachID)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	ipc, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return nil, err
	}
	return &vgi.BindResponse{
		OutputSchema: batch.Schema(),
		OpaqueData:   ipc,
	}, nil
}

// OnInit forwards the bind-phase opaque data so Process can reach it via
// params.InitOpaqueData.
func (f *EchoAttachOptionsFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1, OpaqueData: params.BindOpaqueData}, nil
}

func (f *EchoAttachOptionsFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	one := int64(1)
	return &vgi.TableCardinality{Estimate: one, Max: one}, nil
}

type echoState struct {
	Emitted bool
}

func (f *EchoAttachOptionsFunction) NewState(params *vgi.ProcessParams) (*echoState, error) {
	return &echoState{}, nil
}

func (f *EchoAttachOptionsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *echoState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		out.Finish()
		return nil
	}
	if len(params.InitOpaqueData) == 0 {
		return fmt.Errorf("echo_attach_options: missing opaque data")
	}
	batch, err := vgi.DeserializeRecordBatch(params.InitOpaqueData)
	if err != nil {
		return err
	}
	defer batch.Release()
	out.Emit(batch)
	state.Emitted = true
	return nil
}

// NewEchoAttachOptionsFunction returns the registerable TableFunction.
func NewEchoAttachOptionsFunction() vgi.TableFunction {
	return vgi.AsTableFunction[echoState](&EchoAttachOptionsFunction{})
}
