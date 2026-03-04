// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

// InitRecipe carries all serializable data needed to reconstruct
// ProcessParams after a state token round-trip through HTTP transport.
// It captures the raw IPC bytes from the init request so that the
// rehydration path can replay the same bind/init logic without the
// original wire message.
type InitRecipe struct {
	BindCallIPC       []byte
	OutputSchemaIPC   []byte
	FunctionName      string
	FunctionType      string
	ProjectionIDs     []int32
	ExecutionID       []byte
	BindOpaqueData    []byte
	InitOpaqueData    []byte
	PushdownFilterIPC []byte
	Phase             string
	IsSecondary       bool
}
