// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Compute-ladder + wire-probe scalar fixtures for the boundary-amortization
// benchmark (mirrors the Python/Rust bench_ladder fixtures):
//   passthru      identity string  -> zero-compute pure wire probe
//   collatz_steps int64 -> int64    -> data-dependent CPU loop
//   sha256_hex    string -> string  -> fixed moderate compute
//   hash_rounds   string,K -> string-> K rounds of SHA-256 (compute knob)

package scalar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// PassthruFunction returns the input string unchanged (zero-compute wire probe).
type PassthruFunction struct{}

func (*PassthruFunction) Name() string { return "passthru" }
func (*PassthruFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns the input string unchanged (zero-compute wire probe)",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}
func (*PassthruFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "value", Position: 0, ArrowType: "varchar", Doc: "String value"}}
}
func (*PassthruFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}
func (*PassthruFunction) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	get := vgi.StringAccessor(batch.Column(0))
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(_ arrow.Array, i int) string { return get(i) })
}

// CollatzStepsFunction returns the number of Collatz (3n+1) steps for each integer.
type CollatzStepsFunction struct{}

func (*CollatzStepsFunction) Name() string { return "collatz_steps" }
func (*CollatzStepsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Number of Collatz (3n+1) steps to reach 1",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}
func (*CollatzStepsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "value", Position: 0, ArrowType: "int64", Doc: "Positive integer"}}
}
func (*CollatzStepsFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}
func (*CollatzStepsFunction) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	get := vgi.Int64Accessor(batch.Column(0))
	return vgi.MapColumn(params, batch, 0, array.NewInt64Builder,
		func(_ arrow.Array, i int) int64 {
			n := get(i)
			if n <= 0 {
				return 0
			}
			var steps int64
			for n != 1 {
				if n&1 == 0 {
					n /= 2
				} else {
					n = 3*n + 1
				}
				steps++
			}
			return steps
		})
}

// Sha256HexFunction returns the lowercase hex SHA-256 of each UTF-8 string.
type Sha256HexFunction struct{}

func (*Sha256HexFunction) Name() string { return "sha256_hex" }
func (*Sha256HexFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Lowercase hex SHA-256 of the UTF-8 string",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}
func (*Sha256HexFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "value", Position: 0, ArrowType: "varchar", Doc: "String to hash"}}
}
func (*Sha256HexFunction) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}
func (*Sha256HexFunction) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	get := vgi.StringAccessor(batch.Column(0))
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(_ arrow.Array, i int) string {
			h := sha256.Sum256([]byte(get(i)))
			return hex.EncodeToString(h[:])
		})
}

// HashRoundsFunction applies SHA-256 `rounds` times (a const compute knob).
type HashRoundsFunction struct{}

type hashRoundsArgs struct {
	Value  *array.String `vgi:"pos=0,const=false,doc=String to stretch"`
	Rounds int64         `vgi:"pos=1,doc=Number of SHA-256 rounds"`
}

func (*HashRoundsFunction) Name() string { return "hash_rounds" }
func (*HashRoundsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Apply SHA-256 `rounds` times (key-stretching); rounds is a const compute knob",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}
func (*HashRoundsFunction) OnBindTyped(_ *hashRoundsArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}
func (*HashRoundsFunction) ProcessTyped(_ context.Context, args *hashRoundsArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	k := int(args.Rounds)
	if k < 0 {
		k = 0
	}
	get := vgi.StringAccessor(batch.Column(0))
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(_ arrow.Array, i int) string {
			buf := []byte(get(i))
			for j := 0; j < k; j++ {
				s := sha256.Sum256(buf)
				buf = s[:]
			}
			return hex.EncodeToString(buf)
		})
}

// NewHashRounds wraps the typed hash_rounds function (const arg via struct tags).
func NewHashRounds() vgi.ScalarFunction {
	return vgi.AsScalarFunction[hashRoundsArgs](&HashRoundsFunction{})
}
