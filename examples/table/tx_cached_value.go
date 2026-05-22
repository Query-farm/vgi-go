// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TxCachedValueFunction returns a single-row table whose value is cached per
// (transaction_opaque_data, key) via transaction storage. On a cache hit the
// stored value is returned and the new seed ignored; on a miss (or with no
// transaction) the seed is used. The resolved value ships from OnBind to
// Process via opaque_data. Mirrors vgi-python's TxCachedValueFunction.
type TxCachedValueFunction struct{}

var _ vgi.TypedTableFunc[txCachedState] = (*TxCachedValueFunction)(nil)

type txCachedArgs struct {
	Key  string `vgi:"pos=0,doc=Cache key, scoped to the current transaction"`
	Seed int64  `vgi:"pos=1,doc=Value to cache on first call; ignored on cache hit"`
}

type txCachedState struct {
	Value   int64
	Emitted bool
}

func txStorageKey(userKey string) []byte {
	return []byte("vgi-fixture:tx_cached_value:" + userKey)
}

func (f *TxCachedValueFunction) Name() string { return "tx_cached_value" }
func (f *TxCachedValueFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Return a value cached per (transaction_opaque_data, key) via transaction storage.",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"test", "transaction-storage"},
	}
}
func (f *TxCachedValueFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(txCachedArgs{})
}
func (f *TxCachedValueFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args txCachedArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	value := args.Seed
	if ts := params.TransactionStorage(); ts != nil {
		key := txStorageKey(args.Key)
		cached, err := ts.GetOne(key)
		if err != nil {
			return nil, err
		}
		if len(cached) == 8 {
			value = int64(binary.BigEndian.Uint64(cached))
		} else {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(value))
			if err := ts.PutOne(key, buf); err != nil {
				return nil, err
			}
		}
	}
	opaque := make([]byte, 8)
	binary.BigEndian.PutUint64(opaque, uint64(value))
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil),
		OpaqueData:   opaque,
	}, nil
}
func (f *TxCachedValueFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: 1, Max: 1}, nil
}
func (f *TxCachedValueFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1, OpaqueData: params.BindOpaqueData}, nil
}
func (f *TxCachedValueFunction) NewState(params *vgi.ProcessParams) (*txCachedState, error) {
	st := &txCachedState{}
	if len(params.InitOpaqueData) == 8 {
		st.Value = int64(binary.BigEndian.Uint64(params.InitOpaqueData))
	}
	return st, nil
}
func (f *TxCachedValueFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *txCachedState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(state.Value)
	arr := b.NewArray()
	defer arr.Release()
	return out.Emit(array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1))
}

// NewTxCachedValueFunction wraps the function for registration.
func NewTxCachedValueFunction() vgi.TableFunction {
	return vgi.AsTableFunction[txCachedState](&TxCachedValueFunction{})
}
