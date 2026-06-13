// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import "github.com/Query-farm/vgi-rpc-go/vgirpc"

func init() {
	vgirpc.RegisterStateType(&ScalarExchangeState{})
	vgirpc.RegisterStateType(&TableProducerState{})
	vgirpc.RegisterStateType(&TableInOutExchangeState{})
	vgirpc.RegisterStateType(&FinalizeProducerState{})
	vgirpc.RegisterStateType(InitRecipe{})
	vgirpc.RegisterStateType(map[string]interface{}{})
	vgirpc.RegisterStateType(SecretRequirement{})
	vgirpc.RegisterStateType(SecretLookup{})
	vgirpc.RegisterStateType(&writableScanState{})
	vgirpc.RegisterStateType(&writableMutateState{})
}
