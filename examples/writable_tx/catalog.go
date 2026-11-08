// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package writable_tx is the cross-SDK "txfix" fixture worker: a writable
// table declared on its CatalogTable descriptor, per-ATTACH transactions with
// worker transaction hooks, and a secret ATTACH option that must reach every
// function. It is built only with vgi-go's public API (CatalogTable write
// functions, AttachDecision.SupportsTransactions, WithTransactionHooks,
// ProcessParams.TransactionOpaqueData, AttachOptions for the secret token and
// the write-result helpers), and backs
// the vgi repo's test/sql/integration/writable_tx/*.test files (gated on
// VGI_WRITABLE_TX_FIXTURES and VGI_WRITABLE_TX_WORKER). The contract is the
// vgi repo's docs/writable_tx_fixtures.md.
package writable_tx

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// CatalogName is the catalog the tests ATTACH.
const CatalogName = "txfix"

// tokenOf is the ATTACH's secret token, read only through the SDK's
// attach-options accessor (the SDK carries it encrypted in the attach value;
// it is never in this catalog's attach bytes). Nil when absent.
func tokenOf(opts *vgi.AttachOptions) *string {
	if v, ok := opts.String("token"); ok {
		return &v
	}
	return nil
}

// transactionalOf is the ATTACH's transactional option (default false).
func transactionalOf(opts *vgi.AttachOptions) bool {
	v, _ := opts.Bool("transactional")
	return v
}

// attachOptions declares the ATTACH options: transactional (default false)
// and the secret token.
func attachOptions() []vgi.AttachOptionSpec {
	b := array.NewBooleanBuilder(memAlloc)
	defer b.Release()
	b.Append(false)
	def := b.NewArray()
	defer def.Release()
	defBatch := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.FixedWidthTypes.Boolean}}, nil), []arrow.Array{def}, 1)
	return []vgi.AttachOptionSpec{
		{Name: "transactional", Description: "Report supports_transactions for this ATTACH", Type: arrow.FixedWidthTypes.Boolean, DefaultBatch: defBatch},
		{Name: "token", Description: "Access token (redacted by DuckDB)", Type: arrow.BinaryTypes.String, Secret: true},
	}
}

// decideAttach makes supports_transactions follow the transactional option
// for this ATTACH only. It stores nothing in the attach value: the options,
// the secret token included, reach functions and hooks through the SDK.
func decideAttach(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
	transactional := false
	if req.Options != nil && len(*req.Options) > 0 {
		batch, err := vgi.DeserializeRecordBatch(*req.Options)
		if err != nil {
			return nil, fmt.Errorf("txfix: attach options: %w", err)
		}
		defer batch.Release()
		if idx := batch.Schema().FieldIndices("transactional"); len(idx) > 0 && batch.NumRows() > 0 {
			if c, ok := batch.Column(idx[0]).(*array.Boolean); ok && !c.IsNull(0) {
				transactional = c.Value(0)
			}
		}
	}
	return &vgi.AttachDecision{SupportsTransactions: &transactional}, nil
}

// transactionHooks stage-and-commit the transactional ATTACH's writes.
func transactionHooks() vgi.TransactionHooks {
	return vgi.TransactionHooks{
		Begin: func(_ context.Context, tx vgi.TransactionHookContext) ([]byte, error) {
			if !transactionalOf(tx.AttachOptions()) {
				return nil, nil
			}
			id := make([]byte, 16)
			if _, err := rand.Read(id); err != nil {
				return nil, err
			}
			return id, withState(tokenOf(tx.AttachOptions()), true, func(s *state) error {
				s.record("begin", id, true)
				return nil
			})
		},
		Commit: func(_ context.Context, tx vgi.TransactionHookContext) error {
			return withState(tokenOf(tx.AttachOptions()), true, func(s *state) error {
				key := txHex(tx.TransactionOpaqueData)
				staged := s.Pending[key]
				delete(s.Pending, key)
				if bad := stagedViolation(staged); bad != nil {
					s.record("commit", tx.TransactionOpaqueData, false)
					return fmt.Errorf("txfix: commit refused: qty %d for id %d", *bad.Qty, bad.ID)
				}
				s.applyItems(staged)
				s.record("commit", tx.TransactionOpaqueData, true)
				return nil
			})
		},
		Rollback: func(_ context.Context, tx vgi.TransactionHookContext) error {
			return withState(tokenOf(tx.AttachOptions()), true, func(s *state) error {
				delete(s.Pending, txHex(tx.TransactionOpaqueData))
				s.record("rollback", tx.TransactionOpaqueData, true)
				return nil
			})
		},
	}
}

// itemsUserSchema is main.items' user columns; itemsSchema adds the row id.
var itemsUserSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "qty", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

var itemsSchema = arrow.NewSchema(append(append([]arrow.Field{}, itemsUserSchema.Fields()...), vgi.RowIDField("rowid", arrow.PrimitiveTypes.Int64)), nil)

var labelsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// constraints builds the ENFORCED records of a table: NOT NULL columns and a
// primary key on id.
func constraints(table string, notNull ...string) [][]byte {
	var out [][]byte
	add := func(spec vgi.ConstraintRecordSpec) {
		spec.SchemaPath, spec.Table = []string{"main"}, table
		spec.Enforcement, spec.Validation = vgi.ConstraintEnforced, vgi.ConstraintAllRows
		rec, err := vgi.ConstraintRecord(spec)
		if err != nil {
			panic(err)
		}
		out = append(out, rec)
	}
	for _, col := range notNull {
		add(vgi.ConstraintRecordSpec{ID: table + ":" + col + ":not_null", Kind: vgi.ConstraintNotNull, Columns: [][]string{{col}}})
	}
	add(vgi.ConstraintRecordSpec{ID: table + ":pk", Kind: vgi.ConstraintPrimaryKey, Columns: [][]string{{"id"}}})
	return out
}

// Options returns the worker options of the txfix catalog.
func Options() []vgi.WorkerOption {
	return []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithAttachOptions(attachOptions()...),
		vgi.WithAttachValidator(decideAttach),
		vgi.WithTransactionHooks(transactionHooks()),
	}
}

// Register declares the tables and functions on w.
func Register(w *vgi.Worker) {
	w.RegisterCatalogTable("main", vgi.CatalogTable{
		Name:           "items",
		Comment:        "Writable through items_update; transactional ATTACHes stage changes until COMMIT",
		Columns:        itemsSchema,
		Function:       vgi.AsTableFunction[scanState](itemsScan{}),
		UpdateFunction: vgi.AsTableInOutFunction[writeState](itemsUpdate{}),
		WriteResultModes: map[vgi.WriteOp]vgi.WriteResultMode{
			vgi.WriteOpUpdate: vgi.WriteResultChanges,
		},
		Constraints: constraints("items", "id", "name"),
	})
	w.RegisterCatalogTable("main", vgi.CatalogTable{
		Name:        "labels",
		Comment:     "Read-only",
		Columns:     labelsSchema,
		Function:    vgi.AsTableFunction[scanState](labelsScan{}),
		Constraints: constraints("labels", "id"),
	})
	w.RegisterTable(vgi.AsTableFunction[scanState](resetFn{}))
	w.RegisterTable(vgi.AsTableFunction[scanState](whoamiFn{}))
	w.RegisterTable(vgi.AsTableFunction[scanState](currentTxFn{}))
	w.RegisterTable(vgi.AsTableFunction[scanState](eventsFn{}))
	w.RegisterTableInOut(vgi.AsTableInOutFunction[writeState](tagTx{}))
}
