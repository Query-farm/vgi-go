// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/google/uuid"
)

// TransactionHookContext is what a transaction hook receives: the plaintext
// attach and transaction values of the transaction's ATTACH, plus the call.
//
// Both values are plaintext on every transport: on HTTP, where the SDK seals
// attach_opaque_data and transaction_opaque_data on the wire, the SDK opens
// them before the hook runs, exactly as it does for functions
// (BindParams.TransactionOpaqueData, InitParams.TransactionOpaqueData,
// ProcessParams.TransactionOpaqueData). The transaction id a hook sees is
// therefore byte-for-byte the id the functions of that transaction see.
type TransactionHookContext struct {
	// AttachOpaqueData is the catalog's own attach bytes for the ATTACH the
	// transaction runs on (the value an AttachValidator put in
	// AttachDecision.AttachOpaqueData, or the catalog name by default), with
	// the framework's shard UUID stripped. The same bytes functions see as
	// ProcessParams.AttachScope.
	AttachOpaqueData []byte
	// TransactionOpaqueData is the plaintext transaction id: the value the
	// Begin hook (or the SDK's default) minted. Nil in the Begin hook.
	TransactionOpaqueData []byte
	// Call is the RPC call context (auth, client logging). Nil when the hook
	// is invoked outside an RPC, e.g. from a unit test.
	Call *vgirpc.CallContext

	attachOptions *AttachOptions
}

// AttachOptions returns the options of the transaction's ATTACH, secret ones
// included (decrypted by the SDK). See ProcessParams.AttachOptions.
func (c TransactionHookContext) AttachOptions() *AttachOptions { return c.attachOptions }

// TransactionHooks are the worker's callbacks for DuckDB's BEGIN / COMMIT /
// ROLLBACK on an ATTACH whose catalog_attach result reported
// supports_transactions (WithSupportsTransactions, or per ATTACH with
// AttachDecision.SupportsTransactions). Every hook is optional.
//
// Mirrors the TypeScript SDK's CatalogInterface.transactionBegin /
// transactionCommit / transactionRollback.
type TransactionHooks struct {
	// Begin mints the transaction id for catalog_transaction_begin. The bytes
	// it returns are what every function of the transaction receives as
	// TransactionOpaqueData and what Commit/Rollback receive (sealed for the
	// caller on HTTP, so a client cannot forge or replay one). Returning nil
	// bytes and a nil error starts no worker-side transaction (functions see
	// no transaction id). An error fails the BEGIN with its message.
	//
	// When Begin is nil, the SDK mints a random 16-byte id.
	Begin func(ctx context.Context, tx TransactionHookContext) ([]byte, error)
	// Commit runs for catalog_transaction_commit. An error fails the COMMIT
	// and its message reaches the user (DuckDB then rolls the transaction
	// back on its side; Rollback is not called for it).
	Commit func(ctx context.Context, tx TransactionHookContext) error
	// Rollback runs for catalog_transaction_rollback. An error is reported
	// to DuckDB.
	Rollback func(ctx context.Context, tx TransactionHookContext) error
}

// WithTransactionHooks installs the worker's transaction hooks. The SDK's own
// transaction-scoped storage (BindParams.TransactionStorage) is still cleared
// when a transaction ends, whether or not the Commit hook fails.
//
//	vgi.WithTransactionHooks(vgi.TransactionHooks{
//		Commit: func(ctx context.Context, tx vgi.TransactionHookContext) error {
//			return store.Commit(tx.TransactionOpaqueData) // error fails COMMIT
//		},
//		Rollback: func(ctx context.Context, tx vgi.TransactionHookContext) error {
//			return store.Discard(tx.TransactionOpaqueData)
//		},
//	})
//
// Hooks run for the catalogs this worker serves itself (the default catalog,
// its aliases and RegisterWritableCatalog catalogs; tell them apart by
// TransactionHookContext.AttachOpaqueData). Catalogs routed elsewhere
// (RegisterSubCatalog, memory catalogs) are served by their own handlers.
func WithTransactionHooks(h TransactionHooks) WorkerOption {
	return func(w *Worker) {
		w.transactionHooks = h
	}
}

// mayBeginTransactions reports whether catalog_transaction_begin should mint a
// transaction: the worker advertises transactions on every ATTACH, or an
// AttachValidator / Begin hook can opt individual ATTACHes in. DuckDB only
// calls begin on an ATTACH whose result reported supports_transactions.
func (w *Worker) mayBeginTransactions() bool {
	return w.supportsTransactions || w.attachValidator != nil || w.transactionHooks.Begin != nil
}

// beginTransaction mints a transaction id for an opened attach value.
func (w *Worker) beginTransaction(ctx context.Context, cc *vgirpc.CallContext, attach []byte) ([]byte, error) {
	if w.transactionHooks.Begin != nil {
		tx, err := w.transactionHooks.Begin(ctx, TransactionHookContext{AttachOpaqueData: attach, Call: cc,
			attachOptions: contextAttachOptions(cc)})
		return tx, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), id[:]...), nil
}

// endTransaction runs the commit or rollback hook, then clears the SDK's
// transaction-scoped storage. The hook's error is returned (failing the
// COMMIT/ROLLBACK); storage is cleared either way, since DuckDB ends the
// transaction regardless.
func (w *Worker) endTransaction(ctx context.Context, cc *vgirpc.CallContext, attach, tx []byte, commit bool) error {
	hook := w.transactionHooks.Rollback
	if commit {
		hook = w.transactionHooks.Commit
	}
	var err error
	if hook != nil {
		err = hook(ctx, TransactionHookContext{AttachOpaqueData: attach, TransactionOpaqueData: tx, Call: cc,
			attachOptions: contextAttachOptions(cc)})
	}
	w.clearTransactionState(tx)
	return err
}
