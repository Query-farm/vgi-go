// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// BindParams holds the parameters available during the bind phase.
type BindParams struct {
	// FunctionName is the name of the function being bound.
	FunctionName string
	// FunctionType is the type of the function.
	FunctionType FunctionType
	// Args are the parsed function arguments.
	Args *Arguments
	// InputSchema is the input table schema (nil for table functions).
	InputSchema *arrow.Schema
	// Settings is a map of DuckDB setting names to their scalar values.
	Settings map[string]interface{}
	// Secrets is a map of secret names to their value maps.
	Secrets map[string]map[string]interface{}
	// AttachOpaqueData is the catalog attachment identifier.
	AttachOpaqueData []byte
	// TransactionOpaqueData is the transaction identifier.
	TransactionOpaqueData []byte
	// ResolvedSecretsProvided is true on the second phase of a two-phase bind,
	// indicating that scoped secrets have been resolved and are in Secrets.
	ResolvedSecretsProvided bool
	// AtUnit/AtValue carry the AT (TIMESTAMP|VERSION ...) time-travel clause for
	// this scan, threaded onto the bind request embedded in init. Both nil when
	// the scan has no AT clause. For function-backed tables this is the only
	// place the per-scan AT is visible (the actual on_bind RPC runs once at
	// attach with no AT), so read it at NewState via ProcessParams.AtUnit/AtValue.
	AtUnit  *string
	AtValue *string
	// CopyFrom carries the COPY ... FROM context when this bind opens a
	// COPY-FROM scan (nil otherwise). A CopyFromFunction's OnBind reads its
	// ExpectedSchema here. Mirrors Python's BindParams.bind_call.copy_from.
	CopyFrom *CopyFromContext
	// Auth is the authentication context for the current request.
	// Always non-nil; unauthenticated requests receive vgirpc.Anonymous().
	Auth *vgirpc.AuthContext

	// txBackend is the worker's shared storage backend, injected so OnBind can
	// reach transaction-scoped state via TransactionStorage(). Unexported.
	txBackend FunctionStorage
}

// TransactionStorage is a per-transaction key/value view over the worker's
// shared storage, scoped to BindParams.TransactionOpaqueData. Returns nil when
// the bind is not running inside a transaction (no caching possible).
func (p *BindParams) TransactionStorage() *TransactionStorage {
	if p.txBackend == nil || len(p.TransactionOpaqueData) == 0 {
		return nil
	}
	return &TransactionStorage{back: p.txBackend, txID: p.TransactionOpaqueData}
}

// AttachStore returns an attach-scoped key/value store bound to this bind's
// AttachOpaqueData (the per-ATTACH plaintext). It persists across queries, so
// OnBind can read/pin per-collection state that Process/Combine will later see
// through ProcessParams.AttachScope. Errors if the backend lacks
// AttachStateStorage or the bind has no attach context.
func (p *BindParams) AttachStore() (*AttachStore, error) {
	return newAttachStore(p.txBackend, p.AttachOpaqueData)
}

// TransactionStorage caches values per (transaction_opaque_data, key) via the
// worker's FunctionStorage transaction-state table.
type TransactionStorage struct {
	back FunctionStorage
	txID []byte
}

// GetOne returns the stored value for key, or (nil, nil) if absent.
func (t *TransactionStorage) GetOne(key []byte) ([]byte, error) {
	vals, err := t.back.TransactionStateGet(t.txID, [][]byte{key})
	if err != nil || len(vals) == 0 {
		return nil, err
	}
	return vals[0], nil
}

// PutOne stores value under key for this transaction.
func (t *TransactionStorage) PutOne(key, value []byte) error {
	return t.back.TransactionStatePut(t.txID, []TransactionStateItem{{Key: key, Value: value}})
}

// BindResponse is returned by a function's OnBind method.
type BindResponse struct {
	// OutputSchema is the Arrow schema for the function's output.
	OutputSchema *arrow.Schema
	// OpaqueData is optional opaque data passed to the init phase.
	OpaqueData []byte
	// SecretScopeRequest, when non-nil, signals a two-phase bind scope request.
	// The extension will resolve scoped secrets and re-call bind with
	// ResolvedSecretsProvided=true and the resolved secrets in Secrets.
	SecretScopeRequest []SecretLookup
}
