// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package writable_tx

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var memAlloc = memory.DefaultAllocator

// scanState is the one-shot state of the fixture's table functions.
type scanState struct{ Done bool }

// writeState is the (empty) state of the table in-out functions.
type writeState struct{}

func volatile(desc string) vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: desc, Stability: vgi.StabilityVolatile}
}

// emitOnce emits rows (name-keyed) in schema once, then finishes.
func emitOnce(state *scanState, out *vgirpc.OutputCollector, schema *arrow.Schema, rows func() ([]map[string]any, error)) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	data, err := rows()
	if err != nil {
		return err
	}
	bb := vgi.NewBatchBuilder(schema)
	defer bb.Release()
	for _, r := range data {
		if err := bb.AppendRow(r); err != nil {
			return err
		}
	}
	batch, err := bb.Build()
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

func itemRow(it Item) map[string]any {
	row := map[string]any{"id": it.ID, "name": it.Name, "rowid": int64(it.ID)}
	if it.Qty != nil {
		row["qty"] = *it.Qty
	}
	return row
}

// txHexOrNil is the lowercase hex of a transaction id, or nil when none.
func txHexOrNil(tx []byte) any {
	if len(tx) == 0 {
		return nil
	}
	return txHex(tx)
}

// ---------------------------------------------------------------------------
// items / labels scans
// ---------------------------------------------------------------------------

// itemsScan scans main.items with projection pushdown (DuckDB asks for the
// rowid virtual column only for UPDATE). Inside a transaction it shows that
// transaction's staged rows (read-your-writes).
type itemsScan struct{}

func (itemsScan) Name() string { return "items_scan" }
func (itemsScan) Metadata() vgi.FunctionMetadata {
	m := volatile("Scan main.items (staged rows of the current transaction included)")
	m.ProjectionPushdown = true
	return m
}
func (itemsScan) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (itemsScan) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(itemsSchema)
}
func (itemsScan) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (itemsScan) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		var rows []map[string]any
		err := withState(decodeAttach(p.AttachScope).Token, false, func(s *state) error {
			for _, it := range s.visibleItems(p.TransactionOpaqueData) {
				rows = append(rows, itemRow(it))
			}
			return nil
		})
		return rows, err
	})
}

// labelsScan scans the read-only main.labels.
type labelsScan struct{}

func (labelsScan) Name() string { return "labels_scan" }
func (labelsScan) Metadata() vgi.FunctionMetadata {
	m := volatile("Scan main.labels")
	m.ProjectionPushdown = true
	return m
}
func (labelsScan) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (labelsScan) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(labelsSchema)
}
func (labelsScan) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (labelsScan) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		var rows []map[string]any
		err := withState(decodeAttach(p.AttachScope).Token, false, func(s *state) error {
			for _, l := range s.Labels {
				row := map[string]any{"id": l.ID}
				if l.Label != nil {
					row["label"] = *l.Label
				}
				rows = append(rows, row)
			}
			return nil
		})
		return rows, err
	})
}

// ---------------------------------------------------------------------------
// items_update: the table's UpdateFunction
// ---------------------------------------------------------------------------

// itemsUpdate receives the rowid plus the SET columns of each updated row.
// Non-transactional ATTACHes apply immediately; a transactional ATTACH stages
// the new images under the transaction id until the commit hook. It keeps the
// ENFORCED constraints (NOT NULL name, primary key id) the catalog promised by
// negotiating vgi.constraint.enforce.v1.
type itemsUpdate struct{}

func (itemsUpdate) Name() string                   { return "items_update" }
func (itemsUpdate) Metadata() vgi.FunctionMetadata { return volatile("UPDATE main.items") }
func (itemsUpdate) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "rowid plus the SET columns"},
		vgi.WriteOptionsArgSpec(),
	}
}
func (itemsUpdate) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	opts, err := vgi.ParseWriteOptions(p.Args)
	if err != nil {
		return nil, err
	}
	schema, err := vgi.WriteResultSchema(opts.ResultMode, itemsUserSchema)
	if err != nil {
		return nil, err
	}
	return vgi.BindSchema(schema)
}
func (itemsUpdate) OnInit(*vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (itemsUpdate) NewState(*vgi.ProcessParams) (*writeState, error) { return &writeState{}, nil }
func (itemsUpdate) Process(_ context.Context, p *vgi.ProcessParams, _ *writeState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	opts, err := vgi.ParseWriteOptions(p.Args)
	if err != nil {
		return err
	}
	tx := p.TransactionOpaqueData
	changes := make([]map[string]any, batch.NumRows())
	for i := range changes {
		changes[i] = map[string]any{}
	}
	for c, f := range batch.Schema().Fields() {
		col := batch.Column(c)
		for i := range changes {
			changes[i][f.Name] = cell(col, i)
		}
	}
	var olds, news []map[string]any
	err = withState(decodeAttach(p.AttachScope).Token, true, func(s *state) error {
		current := map[int32]Item{}
		for _, it := range s.visibleItems(tx) {
			current[it.ID] = it
		}
		images := map[int32]Item{}
		for _, ch := range changes {
			rowid, ok := ch["rowid"].(int64)
			if !ok {
				return fmt.Errorf("txfix: update input has no rowid")
			}
			old, ok := current[int32(rowid)]
			if !ok {
				return fmt.Errorf("txfix: no row with rowid %d", rowid)
			}
			img := old
			if v, set := ch["id"]; set {
				if id, ok := v.(int32); !ok || id != old.ID {
					return fmt.Errorf("txfix: id is not updatable")
				}
			}
			if v, set := ch["name"]; set {
				name, ok := v.(string)
				if !ok {
					return fmt.Errorf("txfix: name must not be NULL")
				}
				img.Name = name
			}
			if v, set := ch["qty"]; set {
				if v == nil {
					img.Qty = nil
				} else {
					img.Qty = i64(v.(int64))
				}
			}
			if len(tx) == 0 && img.Qty != nil && *img.Qty < 0 {
				return fmt.Errorf("txfix: qty must be >= 0")
			}
			images[img.ID] = img
			current[img.ID] = img
			olds = append(olds, itemRow(old))
			news = append(news, itemRow(img))
		}
		if len(tx) == 0 {
			s.applyItems(images)
			return nil
		}
		key := txHex(tx)
		if s.Pending[key] == nil {
			s.Pending[key] = map[int32]Item{}
		}
		for id, img := range images {
			s.Pending[key][id] = img
		}
		return nil
	})
	if err != nil {
		return err
	}
	result, err := vgi.WriteResultBatch(opts.ResultMode, itemsUserSchema, olds, news)
	if err != nil {
		return err
	}
	return out.Emit(result)
}
func (itemsUpdate) Finalize(context.Context, *vgi.ProcessParams, *writeState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// cell reads one value of the update input as a Go scalar (nil for NULL).
func cell(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(i)
	case *array.Int32:
		return c.Value(i)
	case *array.String:
		return c.Value(i)
	}
	return col.ValueStr(i)
}

// ---------------------------------------------------------------------------
// reset / whoami / current_tx / events
// ---------------------------------------------------------------------------

type resetFn struct{}

var resetSchema = arrow.NewSchema([]arrow.Field{{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}}, nil)

func (resetFn) Name() string { return "reset" }
func (resetFn) Metadata() vgi.FunctionMetadata {
	return volatile("Restore the initial rows; clear staged changes and events")
}
func (resetFn) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (resetFn) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(resetSchema)
}
func (resetFn) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (resetFn) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		err := withState(decodeAttach(p.AttachScope).Token, true, func(s *state) error {
			*s = *initialState()
			return nil
		})
		return []map[string]any{{"ok": true}}, err
	})
}

type whoamiFn struct{}

var whoamiSchema = arrow.NewSchema([]arrow.Field{
	{Name: "token", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "transactional", Type: arrow.FixedWidthTypes.Boolean},
}, nil)

func (whoamiFn) Name() string { return "whoami" }
func (whoamiFn) Metadata() vgi.FunctionMetadata {
	return volatile("The ATTACH options as this function sees them")
}
func (whoamiFn) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (whoamiFn) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(whoamiSchema)
}
func (whoamiFn) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (whoamiFn) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		info := decodeAttach(p.AttachScope)
		row := map[string]any{"transactional": info.Transactional}
		if info.Token != nil {
			row["token"] = *info.Token
		}
		return []map[string]any{row}, nil
	})
}

type currentTxFn struct{}

var currentTxSchema = arrow.NewSchema([]arrow.Field{
	{Name: "in_tx", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "tx_id", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func (currentTxFn) Name() string { return "current_tx" }
func (currentTxFn) Metadata() vgi.FunctionMetadata {
	return volatile("The transaction id this call received at process time")
}
func (currentTxFn) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (currentTxFn) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(currentTxSchema)
}
func (currentTxFn) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (currentTxFn) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		tx := p.TransactionOpaqueData
		return []map[string]any{{"in_tx": len(tx) > 0, "tx_id": txHexOrNil(tx)}}, nil
	})
}

type eventsFn struct{}

var eventsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "seq", Type: arrow.PrimitiveTypes.Int64},
	{Name: "kind", Type: arrow.BinaryTypes.String},
	{Name: "tx_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "ok", Type: arrow.FixedWidthTypes.Boolean},
}, nil)

func (eventsFn) Name() string { return "events" }
func (eventsFn) Metadata() vgi.FunctionMetadata {
	return volatile("The transaction event log, in order")
}
func (eventsFn) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (eventsFn) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(eventsSchema)
}
func (eventsFn) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (eventsFn) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		var rows []map[string]any
		err := withState(decodeAttach(p.AttachScope).Token, false, func(s *state) error {
			for _, e := range s.Events {
				rows = append(rows, map[string]any{"seq": e.Seq, "kind": e.Kind, "tx_id": e.TxID, "ok": e.OK})
			}
			return nil
		})
		return rows, err
	})
}

// ---------------------------------------------------------------------------
// tag_tx(TABLE): pass-through plus the process-time transaction id
// ---------------------------------------------------------------------------

type tagTx struct{}

func (tagTx) Name() string { return "tag_tx" }
func (tagTx) Metadata() vgi.FunctionMetadata {
	return volatile("Pass every input column through and add the process-time transaction id")
}
func (tagTx) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"}}
}
func (tagTx) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	fields := append(append([]arrow.Field{}, p.InputSchema.Fields()...), arrow.Field{Name: "tx_id", Type: arrow.BinaryTypes.String, Nullable: true})
	return vgi.BindSchema(arrow.NewSchema(fields, nil))
}
func (tagTx) NewState(*vgi.ProcessParams) (*writeState, error) { return &writeState{}, nil }
func (tagTx) Process(_ context.Context, p *vgi.ProcessParams, _ *writeState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	b := array.NewStringBuilder(memAlloc)
	defer b.Release()
	tx := p.TransactionOpaqueData
	for i := int64(0); i < batch.NumRows(); i++ {
		if len(tx) == 0 {
			b.AppendNull()
		} else {
			b.Append(txHex(tx))
		}
	}
	tag := b.NewArray()
	defer tag.Release()
	cols := append(append([]arrow.Array{}, batch.Columns()...), tag)
	return out.Emit(array.NewRecordBatch(p.OutputSchema, cols, batch.NumRows()))
}
func (tagTx) Finalize(context.Context, *vgi.ProcessParams, *writeState) ([]arrow.RecordBatch, error) {
	return nil, nil
}
