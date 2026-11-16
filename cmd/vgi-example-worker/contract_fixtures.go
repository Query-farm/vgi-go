// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The example.contract schema: cross-SDK fixtures for the table_only scan
// visibility, finalize-only INSERT counts and decimal-width write contracts
// (vgi test/sql/integration/visibility/example_table_only.test and
// write_contract/example_{insert_finalize,decimal_width}.test). Every scan
// and write function backs its table only (table_only). Nothing is stored:
// the insert functions validate, count and discard.

const contractSchema = "contract"

// contractCountNS is the storage namespace of contract.notes' per-call row
// counter, keyed by execution_id.
var contractCountNS = []byte("contract_notes_insert")

var contractCountKey = []byte("count")

var contractGoodColumns = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	vgi.RowIDField("rowid", arrow.PrimitiveTypes.Int64),
}, nil)

var contractRangedColumns = arrow.NewSchema([]arrow.Field{
	{Name: "who", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

var contractNotesColumns = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

var contractAmountsColumns = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "d32", Type: &arrow.Decimal32Type{Precision: 7, Scale: 2}, Nullable: true},
	{Name: "d64", Type: &arrow.Decimal64Type{Precision: 15, Scale: 3}, Nullable: true},
	{Name: "d128", Type: &arrow.Decimal128Type{Precision: 9, Scale: 2}, Nullable: true},
}, nil)

func registerContractFixtures(w *vgi.Worker) {
	w.RegisterCatalogTable(contractSchema, vgi.CatalogTable{
		Name:     "good",
		Columns:  contractGoodColumns,
		Function: vgi.AsTableFunction[contractScanState](contractGoodScan{}),
	})
	w.RegisterCatalogTable(contractSchema, vgi.CatalogTable{
		Name:     "ranged",
		Columns:  contractRangedColumns,
		Function: vgi.AsTableFunction[contractScanState](contractRangedScan{}),
	})
	notNull, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{
		ID: "notes_id_not_null", SchemaPath: []string{contractSchema}, Table: "notes",
		Kind: vgi.ConstraintNotNull, Columns: [][]string{{"id"}},
		Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
	})
	if err != nil {
		panic(err)
	}
	w.RegisterCatalogTable(contractSchema, vgi.CatalogTable{
		Name:             "notes",
		Columns:          contractNotesColumns,
		Function:         vgi.AsTableFunction[contractScanState](contractNotesScan{}),
		InsertFunction:   vgi.AsTableInOutFunction[contractWriteState](contractNotesInsert{}),
		WriteResultModes: map[vgi.WriteOp]vgi.WriteResultMode{vgi.WriteOpInsert: vgi.WriteResultCount},
		Constraints:      [][]byte{notNull},
	})
	w.RegisterCatalogTable(contractSchema, vgi.CatalogTable{
		Name:             "amounts",
		Columns:          contractAmountsColumns,
		Function:         vgi.AsTableFunction[contractScanState](contractAmountsScan{}),
		InsertFunction:   vgi.AsTableInOutFunction[contractWriteState](contractAmountsInsert{}),
		WriteResultModes: map[vgi.WriteOp]vgi.WriteResultMode{vgi.WriteOpInsert: vgi.WriteResultRows},
	})
}

type contractScanState struct{ Done bool }

type contractWriteState struct{}

func contractVolatile(desc string) vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: desc, Stability: vgi.StabilityVolatile}
}

// contractEmitOnce emits batch on the first call and finishes on the next.
func contractEmitOnce(state *contractScanState, out *vgirpc.OutputCollector, build func() (arrow.RecordBatch, error)) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	batch, err := build()
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

// newContractBatch assembles a batch and drops the builders' column refs.
func newContractBatch(schema *arrow.Schema, cols []arrow.Array, rows int64) arrow.RecordBatch {
	batch := array.NewRecordBatch(schema, cols, rows)
	for _, c := range cols {
		c.Release()
	}
	return batch
}

// ---------------------------------------------------------------------------
// contract.good: table_only scan with a row id and projection pushdown
// ---------------------------------------------------------------------------

type contractGoodScan struct{}

func (contractGoodScan) Name() string { return "contract_good_scan" }
func (contractGoodScan) Metadata() vgi.FunctionMetadata {
	m := contractVolatile("Scan contract.good")
	m.ProjectionPushdown = true
	return m
}
func (contractGoodScan) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (contractGoodScan) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(contractGoodColumns)
}
func (contractGoodScan) NewState(*vgi.ProcessParams) (*contractScanState, error) {
	return &contractScanState{}, nil
}
func (contractGoodScan) Process(_ context.Context, p *vgi.ProcessParams, st *contractScanState, out *vgirpc.OutputCollector) error {
	return contractEmitOnce(st, out, func() (arrow.RecordBatch, error) {
		ids := []int64{1, 2, 3}
		labels := []string{"a", "b", "c"}
		schema := p.OutputSchema
		if schema == nil {
			schema = contractGoodColumns
		}
		cols := make([]arrow.Array, 0, schema.NumFields())
		for _, f := range schema.Fields() {
			switch f.Name {
			case "id", "rowid":
				b := array.NewInt64Builder(memory.DefaultAllocator)
				b.AppendValues(ids, nil)
				cols = append(cols, b.NewArray())
				b.Release()
			case "label":
				b := array.NewStringBuilder(memory.DefaultAllocator)
				b.AppendValues(labels, nil)
				cols = append(cols, b.NewArray())
				b.Release()
			default:
				for _, c := range cols {
					c.Release()
				}
				return nil, fmt.Errorf("contract: good has no column %q", f.Name)
			}
		}
		return newContractBatch(schema, cols, int64(len(ids))), nil
	})
}

// ---------------------------------------------------------------------------
// contract.ranged: a table_only scan named exactly "range"
// ---------------------------------------------------------------------------

type contractRangedScan struct{}

func (contractRangedScan) Name() string { return "range" }
func (contractRangedScan) Metadata() vgi.FunctionMetadata {
	return contractVolatile("Scan contract.ranged")
}
func (contractRangedScan) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (contractRangedScan) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(contractRangedColumns)
}
func (contractRangedScan) NewState(*vgi.ProcessParams) (*contractScanState, error) {
	return &contractScanState{}, nil
}
func (contractRangedScan) Process(_ context.Context, _ *vgi.ProcessParams, st *contractScanState, out *vgirpc.OutputCollector) error {
	return contractEmitOnce(st, out, func() (arrow.RecordBatch, error) {
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.Append("vgi example worker")
		return newContractBatch(contractRangedColumns, []arrow.Array{b.NewArray()}, 1), nil
	})
}

// ---------------------------------------------------------------------------
// contract.notes: INSERT whose count comes from finalize only
// ---------------------------------------------------------------------------

type contractNotesScan struct{}

func (contractNotesScan) Name() string { return "contract_notes_scan" }
func (contractNotesScan) Metadata() vgi.FunctionMetadata {
	return contractVolatile("Scan contract.notes (always empty)")
}
func (contractNotesScan) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (contractNotesScan) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(contractNotesColumns)
}
func (contractNotesScan) NewState(*vgi.ProcessParams) (*contractScanState, error) {
	return &contractScanState{}, nil
}
func (contractNotesScan) Process(_ context.Context, _ *vgi.ProcessParams, _ *contractScanState, out *vgirpc.OutputCollector) error {
	return out.Finish()
}

func contractWriteArgs(doc string) []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: doc},
		vgi.WriteOptionsArgSpec(),
	}
}

// contractNotesInsert validates and counts the new rows, emitting a 0-row
// count batch per input batch; Finalize reports the call's total. The count
// lives in execution-scoped shared storage (keyed by execution_id), since
// Finalize gets a fresh state and pooled worker processes may serve one
// statement.
type contractNotesInsert struct{}

func (contractNotesInsert) Name() string { return "contract_notes_insert" }
func (contractNotesInsert) Metadata() vgi.FunctionMetadata {
	m := contractVolatile("INSERT INTO contract.notes")
	m.HasFinalize = true
	return m
}
func (contractNotesInsert) ArgumentSpecs() []vgi.ArgSpec { return contractWriteArgs("the new rows") }
func (contractNotesInsert) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	schema, err := vgi.WriteResultSchema(vgi.WriteResultCount, nil)
	if err != nil {
		return nil, err
	}
	return vgi.BindSchema(schema)
}
func (contractNotesInsert) NewState(*vgi.ProcessParams) (*contractWriteState, error) {
	return &contractWriteState{}, nil
}
func (contractNotesInsert) Process(_ context.Context, p *vgi.ProcessParams, _ *contractWriteState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	for _, f := range batch.Schema().Fields() {
		if !f.Nullable {
			return fmt.Errorf("contract: notes_insert input field %s is non-nullable", f.Name)
		}
	}
	idx := batch.Schema().FieldIndices("id")
	if len(idx) == 0 || batch.Column(idx[0]).NullN() > 0 {
		return fmt.Errorf("contract: notes.id required")
	}
	store, err := p.Storage.AttachStore(p.ExecutionID)
	if err != nil {
		return err
	}
	if _, err := store.CounterAdd(contractCountNS, contractCountKey, batch.NumRows()); err != nil {
		return err
	}
	schema, _ := vgi.WriteResultSchema(vgi.WriteResultCount, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	return out.Emit(newContractBatch(schema, []arrow.Array{b.NewArray()}, 0))
}
func (contractNotesInsert) Finalize(_ context.Context, p *vgi.ProcessParams, _ *contractWriteState) ([]arrow.RecordBatch, error) {
	store, err := p.Storage.AttachStore(p.ExecutionID)
	if err != nil {
		return nil, err
	}
	total, err := store.CounterGet(contractCountNS, contractCountKey)
	if err != nil {
		return nil, err
	}
	if err := store.CounterDelete(contractCountNS, contractCountKey); err != nil {
		return nil, err
	}
	return []arrow.RecordBatch{vgi.WriteCountBatch(total)}, nil
}

// ---------------------------------------------------------------------------
// contract.amounts: decimal32 / decimal64 / decimal128 at declared widths
// ---------------------------------------------------------------------------

type contractAmountsScan struct{}

func (contractAmountsScan) Name() string { return "contract_amounts_scan" }
func (contractAmountsScan) Metadata() vgi.FunctionMetadata {
	return contractVolatile("Scan contract.amounts")
}
func (contractAmountsScan) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (contractAmountsScan) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(contractAmountsColumns)
}
func (contractAmountsScan) NewState(*vgi.ProcessParams) (*contractScanState, error) {
	return &contractScanState{}, nil
}
func (contractAmountsScan) Process(_ context.Context, _ *vgi.ProcessParams, st *contractScanState, out *vgirpc.OutputCollector) error {
	return contractEmitOnce(st, out, func() (arrow.RecordBatch, error) {
		mem := memory.DefaultAllocator
		fields := contractAmountsColumns.Fields()

		ids := array.NewInt64Builder(mem)
		defer ids.Release()
		ids.AppendValues([]int64{1, 2, 3}, nil)

		// Unscaled values: d32 scale 2, d64 scale 3, d128 scale 2.
		d32 := array.NewDecimal32Builder(mem, fields[1].Type.(*arrow.Decimal32Type))
		defer d32.Release()
		d32.Append(decimal.Decimal32(1234567))
		d32.Append(decimal.Decimal32(-9999999))
		d32.AppendNull()

		d64 := array.NewDecimal64Builder(mem, fields[2].Type.(*arrow.Decimal64Type))
		defer d64.Release()
		d64.Append(decimal.Decimal64(-123456789012345))
		d64.Append(decimal.Decimal64(999999999999999))
		d64.Append(decimal.Decimal64(1))

		d128 := array.NewDecimal128Builder(mem, fields[3].Type.(*arrow.Decimal128Type))
		defer d128.Release()
		d128.Append(decimal128.FromI64(123456789))
		d128.Append(decimal128.FromI64(-999999999))
		d128.AppendNull()

		cols := []arrow.Array{ids.NewArray(), d32.NewArray(), d64.NewArray(), d128.NewArray()}
		return newContractBatch(contractAmountsColumns, cols, 3), nil
	})
}

// contractAmountsInsert checks every column arrives at exactly its declared
// Arrow type (bit width included), then returns the count or, for
// RETURNING, the received rows.
type contractAmountsInsert struct{}

func (contractAmountsInsert) Name() string { return "contract_amounts_insert" }
func (contractAmountsInsert) Metadata() vgi.FunctionMetadata {
	return contractVolatile("INSERT INTO contract.amounts")
}
func (contractAmountsInsert) ArgumentSpecs() []vgi.ArgSpec {
	return contractWriteArgs("the new rows")
}
func (contractAmountsInsert) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	opts, err := vgi.ParseWriteOptions(p.Args)
	if err != nil {
		return nil, err
	}
	schema, err := vgi.WriteResultSchema(opts.ResultMode, contractAmountsColumns)
	if err != nil {
		return nil, err
	}
	return vgi.BindSchema(schema)
}
func (contractAmountsInsert) NewState(*vgi.ProcessParams) (*contractWriteState, error) {
	return &contractWriteState{}, nil
}
func (contractAmountsInsert) Process(_ context.Context, p *vgi.ProcessParams, _ *contractWriteState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	cols := make([]arrow.Array, contractAmountsColumns.NumFields())
	for i, declared := range contractAmountsColumns.Fields() {
		idx := batch.Schema().FieldIndices(declared.Name)
		if len(idx) == 0 {
			return fmt.Errorf("contract: amounts.%s missing from the input", declared.Name)
		}
		got := batch.Schema().Field(idx[0]).Type
		if !arrow.TypeEqual(got, declared.Type) {
			return fmt.Errorf("contract: amounts.%s arrived as %s, declared %s", declared.Name, got, declared.Type)
		}
		cols[i] = batch.Column(idx[0])
	}
	opts, err := vgi.ParseWriteOptions(p.Args)
	if err != nil {
		return err
	}
	switch opts.ResultMode {
	case vgi.WriteResultCount:
		return out.Emit(vgi.WriteCountBatch(batch.NumRows()))
	case vgi.WriteResultRows:
		// The received columns, already at the declared types.
		return out.Emit(array.NewRecordBatch(contractAmountsColumns, cols, batch.NumRows()))
	default:
		return fmt.Errorf("contract: amounts does not support result mode %q", opts.ResultMode)
	}
}
func (contractAmountsInsert) Finalize(context.Context, *vgi.ProcessParams, *contractWriteState) ([]arrow.RecordBatch, error) {
	return nil, nil
}
