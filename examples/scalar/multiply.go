// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"unsafe"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// MultiplyFunction multiplies a value by a constant factor.
type MultiplyFunction struct{}

type multiplyArgs struct {
	Value  *array.Int64 `vgi:"pos=0,const=false,doc=Integer value to multiply"`
	Factor int64        `vgi:"pos=1,doc=Multiplication factor"`
}

func (*MultiplyFunction) Name() string { return "multiply" }

func (*MultiplyFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Multiplies a value by a constant factor",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}

func (*MultiplyFunction) OnBindTyped(_ *multiplyArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (*MultiplyFunction) ProcessTyped(_ context.Context, args *multiplyArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	// Fast path: write the multiply result directly into a pooled
	// arrow.Buffer, skip Int64Builder entirely. The previous path was
	//
	//   out := make([]int64, n)        // 16 KB Go-heap allocation
	//   for i, v := range src { ... }  // multiply into out
	//   bldr.AppendValues(out, nil)    // alloc arrow.Buffer + memcpy out into it
	//
	// which paid a 16 KB Go-heap allocation AND a 16 KB memcpy per batch
	// on top of the actual compute. Here we reserve a buffer from the
	// pooled allocator and write through it directly via unsafe.Slice,
	// matching the Java direct-ArrowBuf pattern. The validity bitmap is
	// propagated by slicing the input's bitmap buffer (an aliased view —
	// no copy — that the output array Retain()s).
	n := int(batch.NumRows())
	src := args.Value.Int64Values()
	factor := args.Factor

	// Allocate the data buffer (n * 8 bytes) from the shared pool and
	// view it as a []int64 so the multiply loop writes through the same
	// memory that ends up in the output ArrayData.
	dataBuf := memory.NewResizableBuffer(sharedPooledAllocator)
	dataBuf.Resize(n * arrow.Int64SizeBytes)
	dst := unsafe.Slice((*int64)(unsafe.Pointer(&dataBuf.Bytes()[0])), n)
	for i, v := range src {
		dst[i] = v * factor
	}

	// Propagate the input validity bitmap by slicing it (Arrow's bitmap
	// buffers are bit-aligned at byte 0, so a same-offset view is correct
	// for any batch produced by vgi-rpc's IPC reader). We Retain() it
	// because NewData will Release() on tear-down, balancing the input's
	// own ownership.
	var validityBuf *memory.Buffer
	nullCount := args.Value.NullN()
	if nullCount > 0 {
		validityBuf = args.Value.Data().Buffers()[0]
		if validityBuf != nil {
			validityBuf.Retain()
		}
	}

	// Build the ArrayData -> Int64 -> RecordBatch chain. NewData retains
	// each non-nil buffer once, so we drop our own refcounts after handing
	// ownership over and let arr.Release() (deferred) free everything
	// back to the pool when the framework releases the returned batch.
	data := array.NewData(arrow.PrimitiveTypes.Int64, n,
		[]*memory.Buffer{validityBuf, dataBuf}, nil, nullCount, 0)
	dataBuf.Release()
	if validityBuf != nil {
		validityBuf.Release()
	}
	arr := array.NewInt64Data(data)
	data.Release()
	defer arr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, int64(n)), nil
}

// NewMultiply returns the registration-ready ScalarFunction.
func NewMultiply() vgi.ScalarFunction {
	return vgi.AsScalarFunction[multiplyArgs](&MultiplyFunction{})
}
