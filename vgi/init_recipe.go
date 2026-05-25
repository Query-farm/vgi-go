// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

// InitRecipe carries all serializable data needed to reconstruct
// ProcessParams after a state token round-trip through HTTP transport.
// It captures the raw IPC bytes from the init request so that the
// rehydration path can replay the same bind/init logic without the
// original wire message.
type InitRecipe struct {
	BindCall          BindRequestWire
	OutputSchemaIPC   []byte
	FunctionName      string
	FunctionType      FunctionType
	ProjectionIDs     []int32
	ExecutionID       []byte
	BindOpaqueData    []byte
	InitOpaqueData    []byte
	PushdownFilterIPC []byte
	Phase             Phase
	IsSecondary       bool
	// ShardKey is the per-attach Durable Object routing key (att-<hex uuid>),
	// derived once at init from the unwrapped attach UUID and carried through
	// serialization so a rehydrated process/finalize turn routes storage to the
	// same DO without re-opening the auth-scoped seal. "" for non-attach paths.
	ShardKey string
}
