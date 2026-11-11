// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package writable_tx

import (
	"bytes"
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
// rowid virtual column only for UPDATE and DELETE). Inside a transaction it
// shows that transaction's staged rows (read-your-writes). It reads the state
// once, at its first process call, and emits every row in that one batch: a
// snapshot taken at scan start, so INSERT INTO items SELECT ... FROM items
// never sees the rows it inserts.
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
		err := withState(tokenOf(p.AttachOptions()), false, func(s *state) error {
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
		err := withState(tokenOf(p.AttachOptions()), false, func(s *state) error {
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
// items_insert / items_update / items_delete: the table's write functions
// ---------------------------------------------------------------------------
//
// All three are declared on the table descriptor (so the SDK lists them
// table_only) and set HasFinalize. Each validates every row of a batch before
// applying any. A non-transactional ATTACH applies a batch immediately (a
// later batch failing leaves the earlier ones applied); a transactional one
// stages it under the transaction id until the commit hook, which refuses (and
// discards) a transaction that would store a negative qty. Every call logs
// one writes() entry: created by its first applied batch, counting the rows
// it wrote, and marked finalized by its finalize.

// writeState is a write call's per-stream state: the number of batches it
// applied. The call's writes() entry is keyed by its execution id instead
// (see Write), because Finalize starts from a fresh NewState.
type writeState struct{ Batches int64 }

// writeMetadata is a write function's metadata: volatile, with a finalize.
func writeMetadata(desc string) vgi.FunctionMetadata {
	m := volatile(desc)
	m.HasFinalize = true
	return m
}

// writeArgs are a write function's arguments: its input table and the
// write_options DuckDB sends.
func writeArgs(doc string) []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: doc},
		vgi.WriteOptionsArgSpec(),
	}
}

// bindWrite binds the result schema of the statement's write_options mode.
func bindWrite(p *vgi.BindParams) (*vgi.BindResponse, error) {
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

// oneWorker keeps a write call on one worker thread, so its batches apply in
// order and its writes() entry is one.
func oneWorker(*vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

// batchRows decodes a write function's input batch into name-keyed rows
// (nil for NULL).
func batchRows(batch arrow.RecordBatch) []map[string]any {
	rows := make([]map[string]any, batch.NumRows())
	for i := range rows {
		rows[i] = map[string]any{}
	}
	for c, f := range batch.Schema().Fields() {
		col := batch.Column(c)
		for i := range rows {
			rows[i][f.Name] = cell(col, i)
		}
	}
	return rows
}

// applyWrite runs fn on the attach's state partition, then logs the batch's
// counted rows on the call's writes() entry. fn returns that count.
func applyWrite(p *vgi.ProcessParams, st *writeState, op string, fn func(s *state) (int64, error)) error {
	return withState(tokenOf(p.AttachOptions()), true, func(s *state) error {
		n, err := fn(s)
		if err != nil {
			return err
		}
		s.recordWrite(p.ExecutionID, op, p.TransactionOpaqueData, n, false)
		st.Batches++
		return nil
	})
}

// finalizeWrite marks the call's writes() entry finalized.
func finalizeWrite(p *vgi.ProcessParams, op string) ([]arrow.RecordBatch, error) {
	err := withState(tokenOf(p.AttachOptions()), true, func(s *state) error {
		s.recordWrite(p.ExecutionID, op, p.TransactionOpaqueData, 0, true)
		return nil
	})
	return nil, err
}

// emitWriteResult emits the statement's result for the batch's before/after
// images.
func emitWriteResult(p *vgi.ProcessParams, out *vgirpc.OutputCollector, olds, news []map[string]any) error {
	opts, err := vgi.ParseWriteOptions(p.Args)
	if err != nil {
		return err
	}
	result, err := vgi.WriteResultBatch(opts.ResultMode, itemsUserSchema, olds, news)
	if err != nil {
		return err
	}
	return out.Emit(result)
}

// putOrStage applies images now (no transaction) or stages them under tx.
func putOrStage(s *state, tx []byte, images []Item) {
	if len(tx) == 0 {
		m := make(map[int32]Item, len(images))
		for _, img := range images {
			m[img.ID] = img
		}
		s.putItems(m)
		return
	}
	for _, img := range images {
		s.stage(tx, img)
	}
}

// qtyRefused is the immediate qty check of a non-transactional write (a
// transactional one is checked at commit).
func qtyRefused(tx []byte, img Item) error {
	if len(tx) == 0 && img.Qty != nil && *img.Qty < 0 {
		return fmt.Errorf("txfix: qty must be >= 0")
	}
	return nil
}

// itemsInsert receives the new rows in the table's user columns (DuckDB has
// filled qty's DEFAULT 0). It keeps the ENFORCED constraints: an id already
// visible (committed, staged by this transaction, or earlier in this
// statement) fails with "txfix: duplicate id <id>", a NULL name with
// "txfix: name required".
type itemsInsert struct{}

func (itemsInsert) Name() string                   { return "items_insert" }
func (itemsInsert) Metadata() vgi.FunctionMetadata { return writeMetadata("INSERT INTO main.items") }
func (itemsInsert) ArgumentSpecs() []vgi.ArgSpec   { return writeArgs("the new rows") }
func (itemsInsert) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	return bindWrite(p)
}
func (itemsInsert) OnInit(p *vgi.InitParams) (*vgi.GlobalInitResponse, error) { return oneWorker(p) }
func (itemsInsert) NewState(*vgi.ProcessParams) (*writeState, error)          { return &writeState{}, nil }
func (itemsInsert) Process(_ context.Context, p *vgi.ProcessParams, st *writeState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	tx := p.TransactionOpaqueData
	rows := batchRows(batch)
	var news []map[string]any
	err := applyWrite(p, st, "insert", func(s *state) (int64, error) {
		taken := map[int32]bool{}
		for _, it := range s.visibleItems(tx) {
			taken[it.ID] = true
		}
		images := make([]Item, 0, len(rows))
		for _, r := range rows {
			id, ok := r["id"].(int32)
			if !ok {
				return 0, fmt.Errorf("txfix: id required")
			}
			if taken[id] {
				return 0, fmt.Errorf("txfix: duplicate id %d", id)
			}
			name, ok := r["name"].(string)
			if !ok {
				return 0, fmt.Errorf("txfix: name required")
			}
			img := Item{ID: id, Name: name}
			if q, ok := r["qty"].(int64); ok {
				img.Qty = i64(q)
			}
			if err := qtyRefused(tx, img); err != nil {
				return 0, err
			}
			taken[id] = true
			images = append(images, img)
		}
		putOrStage(s, tx, images)
		for _, img := range images {
			news = append(news, itemRow(img))
		}
		return int64(len(images)), nil
	})
	if err != nil {
		return err
	}
	return emitWriteResult(p, out, make([]map[string]any, len(news)), news)
}
func (itemsInsert) Finalize(_ context.Context, p *vgi.ProcessParams, _ *writeState) ([]arrow.RecordBatch, error) {
	return finalizeWrite(p, "insert")
}

// itemsUpdate receives the rowid plus the SET columns of each updated row
// (every column with RETURNING). It keeps the ENFORCED constraints (NOT NULL
// name, primary key id) the catalog promised by negotiating
// vgi.constraint.enforce.v1. Updating a row this transaction inserted edits
// the staged insert.
type itemsUpdate struct{}

func (itemsUpdate) Name() string                   { return "items_update" }
func (itemsUpdate) Metadata() vgi.FunctionMetadata { return writeMetadata("UPDATE main.items") }
func (itemsUpdate) ArgumentSpecs() []vgi.ArgSpec   { return writeArgs("rowid plus the SET columns") }
func (itemsUpdate) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	return bindWrite(p)
}
func (itemsUpdate) OnInit(p *vgi.InitParams) (*vgi.GlobalInitResponse, error) { return oneWorker(p) }
func (itemsUpdate) NewState(*vgi.ProcessParams) (*writeState, error)          { return &writeState{}, nil }
func (itemsUpdate) Process(_ context.Context, p *vgi.ProcessParams, st *writeState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	tx := p.TransactionOpaqueData
	changes := batchRows(batch)
	var olds, news []map[string]any
	err := applyWrite(p, st, "update", func(s *state) (int64, error) {
		current := map[int32]Item{}
		for _, it := range s.visibleItems(tx) {
			current[it.ID] = it
		}
		images := make([]Item, 0, len(changes))
		for _, ch := range changes {
			rowid, ok := ch["rowid"].(int64)
			if !ok {
				return 0, fmt.Errorf("txfix: update input has no rowid")
			}
			old, ok := current[int32(rowid)]
			if !ok {
				return 0, fmt.Errorf("txfix: no row with rowid %d", rowid)
			}
			img := old
			if v, set := ch["id"]; set {
				if id, ok := v.(int32); !ok || id != old.ID {
					return 0, fmt.Errorf("txfix: id is not updatable")
				}
			}
			if v, set := ch["name"]; set {
				name, ok := v.(string)
				if !ok {
					return 0, fmt.Errorf("txfix: name must not be NULL")
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
			if err := qtyRefused(tx, img); err != nil {
				return 0, err
			}
			current[img.ID] = img
			images = append(images, img)
			olds = append(olds, itemRow(old))
			news = append(news, itemRow(img))
		}
		putOrStage(s, tx, images)
		return int64(len(images)), nil
	})
	if err != nil {
		return err
	}
	return emitWriteResult(p, out, olds, news)
}
func (itemsUpdate) Finalize(_ context.Context, p *vgi.ProcessParams, _ *writeState) ([]arrow.RecordBatch, error) {
	return finalizeWrite(p, "update")
}

// itemsDelete receives the rowid (= id) of each deleted row. An id that is
// not (or no longer) visible is a no-op and is not counted, so a repeated
// rowid counts once. Deleting a row this transaction inserted drops the
// staged insert.
type itemsDelete struct{}

func (itemsDelete) Name() string                   { return "items_delete" }
func (itemsDelete) Metadata() vgi.FunctionMetadata { return writeMetadata("DELETE FROM main.items") }
func (itemsDelete) ArgumentSpecs() []vgi.ArgSpec   { return writeArgs("rowid of each deleted row") }
func (itemsDelete) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	return bindWrite(p)
}
func (itemsDelete) OnInit(p *vgi.InitParams) (*vgi.GlobalInitResponse, error) { return oneWorker(p) }
func (itemsDelete) NewState(*vgi.ProcessParams) (*writeState, error)          { return &writeState{}, nil }
func (itemsDelete) Process(_ context.Context, p *vgi.ProcessParams, st *writeState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	tx := p.TransactionOpaqueData
	rows := batchRows(batch)
	var olds []map[string]any
	err := applyWrite(p, st, "delete", func(s *state) (int64, error) {
		current := map[int32]Item{}
		for _, it := range s.visibleItems(tx) {
			current[it.ID] = it
		}
		gone := map[int32]bool{}
		for _, r := range rows {
			rowid, ok := r["rowid"].(int64)
			if !ok {
				return 0, fmt.Errorf("txfix: delete input has no rowid")
			}
			old, ok := current[int32(rowid)]
			if !ok {
				continue
			}
			delete(current, old.ID)
			gone[old.ID] = true
			olds = append(olds, itemRow(old))
		}
		if len(tx) == 0 {
			s.deleteItems(gone)
		} else {
			for id := range gone {
				s.stageDelete(tx, id)
			}
		}
		return int64(len(olds)), nil
	})
	if err != nil {
		return err
	}
	return emitWriteResult(p, out, olds, make([]map[string]any, len(olds)))
}
func (itemsDelete) Finalize(_ context.Context, p *vgi.ProcessParams, _ *writeState) ([]arrow.RecordBatch, error) {
	return finalizeWrite(p, "delete")
}

// cell reads one value of a write function's input as a Go scalar (nil for
// NULL).
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
// reset / whoami / current_tx / events / writes
// ---------------------------------------------------------------------------

type resetFn struct{}

var resetSchema = arrow.NewSchema([]arrow.Field{{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}}, nil)

func (resetFn) Name() string { return "reset" }
func (resetFn) Metadata() vgi.FunctionMetadata {
	return volatile("Restore the initial rows; clear staged changes, events and writes")
}
func (resetFn) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (resetFn) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(resetSchema)
}
func (resetFn) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (resetFn) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		err := withState(tokenOf(p.AttachOptions()), true, func(s *state) error {
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
	{Name: "token_in_attach_data", Type: arrow.FixedWidthTypes.Boolean},
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
		opts := p.AttachOptions()
		token := tokenOf(opts)
		// Conformance: the token must never be in the attach bytes the SDK
		// hands the catalog.
		inAttach := token != nil && bytes.Contains(p.AttachScope, []byte(*token))
		row := map[string]any{"transactional": transactionalOf(opts), "token_in_attach_data": inAttach}
		if token != nil {
			row["token"] = *token
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
		err := withState(tokenOf(p.AttachOptions()), false, func(s *state) error {
			for _, e := range s.Events {
				rows = append(rows, map[string]any{"seq": e.Seq, "kind": e.Kind, "tx_id": e.TxID, "ok": e.OK})
			}
			return nil
		})
		return rows, err
	})
}

type writesFn struct{}

var writesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "seq", Type: arrow.PrimitiveTypes.Int64},
	{Name: "op", Type: arrow.BinaryTypes.String},
	{Name: "tx_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "rows", Type: arrow.PrimitiveTypes.Int64},
	{Name: "finalized", Type: arrow.FixedWidthTypes.Boolean},
}, nil)

func (writesFn) Name() string { return "writes" }
func (writesFn) Metadata() vgi.FunctionMetadata {
	return volatile("The write-call log: one row per call of a write function, in order")
}
func (writesFn) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (writesFn) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(writesSchema)
}
func (writesFn) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (writesFn) Process(_ context.Context, p *vgi.ProcessParams, st *scanState, out *vgirpc.OutputCollector) error {
	return emitOnce(st, out, p.OutputSchema, func() ([]map[string]any, error) {
		var rows []map[string]any
		err := withState(tokenOf(p.AttachOptions()), false, func(s *state) error {
			for _, wr := range s.Writes {
				row := map[string]any{"seq": wr.Seq, "op": wr.Op, "rows": wr.Rows, "finalized": wr.Finalized}
				if wr.TxID != "" {
					row["tx_id"] = wr.TxID
				}
				rows = append(rows, row)
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
