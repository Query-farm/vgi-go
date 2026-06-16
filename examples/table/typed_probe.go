// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// TypedProbeFunction exercises the SDK's typed const-argument binding and the
// typed column builders cross-language. Const args cover the less-common Arrow
// scalar getters — TIMESTAMP (time.Time → GetScalarTime), INTERVAL
// (time.Duration → GetScalarDuration), BLOB ([]byte → GetScalarBytes) and
// UBIGINT (uint64). Each arg declares a default so calling typed_probe(n) with
// no consts drives the assignDefault path; passing them drives assignScalar.
// The output echoes the bound values into uint64 / double / blob / int64
// columns, exercising BuildUint64Array / BuildFloat64Array / BuildBinaryArray /
// BuildInt64Array. Values are echoed in normalized integer/byte form so the
// Go and Python fixtures produce byte-identical results for the shared test.
type TypedProbeFunction struct{}

var _ vgi.TypedTableFunc[typedProbeState] = (*TypedProbeFunction)(nil)

func (f *TypedProbeFunction) Name() string { return "typed_probe" }

func (f *TypedProbeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Echoes typed const args (timestamp/interval/blob/ubigint) into typed columns",
		Stability:   vgi.StabilityConsistent,
	}
}

// typedProbeArgs declares one const arg per less-common scalar type, each with
// a default so the const can be omitted in SQL.
type typedProbeArgs struct {
	N    int64         `vgi:"pos=0,doc=Number of rows to emit"`
	TS   time.Time     `vgi:"name=ts,default=2026-01-02T03:04:05Z,doc=Timestamp const (TIMESTAMPTZ)"`
	IV   time.Duration `vgi:"name=iv,default=1500ms,doc=Interval const (INTERVAL)"`
	Blob []byte        `vgi:"name=blob,default=vgi,doc=Blob const (BLOB)"`
	UB   uint64        `vgi:"name=ub,default=9,doc=Unsigned const (UBIGINT)"`
	F    float64       `vgi:"name=f,default=2.5,doc=Float const (DOUBLE)"`
}

func (f *TypedProbeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(typedProbeArgs{})
}

var typedProbeOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "idx", Type: arrow.PrimitiveTypes.Uint64},
	{Name: "ts_us", Type: arrow.PrimitiveTypes.Int64},
	{Name: "iv_ms", Type: arrow.PrimitiveTypes.Int64},
	{Name: "payload", Type: arrow.BinaryTypes.Binary},
	{Name: "ub", Type: arrow.PrimitiveTypes.Uint64},
	{Name: "f", Type: arrow.PrimitiveTypes.Float64},
}, nil)

func (f *TypedProbeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(typedProbeOutputSchema)
}

func (f *TypedProbeFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type typedProbeState struct {
	vgi.BatchState
	tsUS    int64
	ivMS    int64
	payload []byte
	ub      uint64
	f       float64
}

func (f *TypedProbeFunction) NewState(params *vgi.ProcessParams) (*typedProbeState, error) {
	var args typedProbeArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &typedProbeState{
		BatchState: vgi.NewBatchState(args.N, args.N),
		tsUS:       args.TS.UnixMicro(),
		ivMS:       args.IV.Milliseconds(),
		payload:    args.Blob,
		ub:         args.UB,
		f:          args.F,
	}, nil
}

func (f *TypedProbeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *typedProbeState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildUint64Array(size, func(i int64) uint64 { return uint64(start + i) }),
			vgi.BuildInt64Array(size, func(i int64) int64 { return state.tsUS }),
			vgi.BuildInt64Array(size, func(i int64) int64 { return state.ivMS }),
			vgi.BuildBinaryArray(size, func(i int64) []byte { return state.payload }),
			vgi.BuildUint64Array(size, func(i int64) uint64 { return state.ub }),
			vgi.BuildFloat64Array(size, func(i int64) float64 { return state.f + float64(start+i) }),
		}, nil
	})
}

func NewTypedProbeFunction() vgi.TableFunction {
	return vgi.AsTableFunction[typedProbeState](&TypedProbeFunction{})
}
