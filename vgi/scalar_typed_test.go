// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type addArgs struct {
	A int64 `vgi:"pos=0,doc=Left operand"`
	B int64 `vgi:"pos=1,doc=Right operand"`
}

type addImpl struct{}

func (addImpl) Name() string               { return "add_const" }
func (addImpl) Metadata() FunctionMetadata { return FunctionMetadata{Description: "add"} }

func (addImpl) OnBindTyped(args *addArgs, _ *BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (addImpl) ProcessTyped(_ context.Context, args *addArgs, _ *ProcessParams, _ arrow.RecordBatch) (arrow.RecordBatch, error) {
	return nil, nil
}

func TestAsScalarFunction_DerivesSpecs(t *testing.T) {
	fn := AsScalarFunction[addArgs](addImpl{})
	if fn.Name() != "add_const" {
		t.Fatalf("name: %q", fn.Name())
	}
	specs := fn.ArgumentSpecs()
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}
	if specs[0].Name != "a" || specs[1].Name != "b" {
		t.Fatalf("expected names a, b; got %q, %q", specs[0].Name, specs[1].Name)
	}
	if specs[0].Position != 0 || specs[1].Position != 1 {
		t.Fatalf("positions: %d, %d", specs[0].Position, specs[1].Position)
	}
}

// columnArgs uses non-const ("column") args — BindArgs leaves them zero;
// the typed adapter must not error on column-arg-only structs.
type columnArgs struct {
	X any `vgi:"pos=0,const=false,bound=addable"`
	Y any `vgi:"pos=1,const=false,bound=addable"`
}

type colImpl struct{}

func (colImpl) Name() string               { return "col_op" }
func (colImpl) Metadata() FunctionMetadata { return FunctionMetadata{} }

func (colImpl) OnBindTyped(*columnArgs, *BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil))
}

func (colImpl) ProcessTyped(context.Context, *columnArgs, *ProcessParams, arrow.RecordBatch) (arrow.RecordBatch, error) {
	return nil, nil
}

func TestAsScalarFunction_ColumnArgs(t *testing.T) {
	fn := AsScalarFunction[columnArgs](colImpl{})
	specs := fn.ArgumentSpecs()
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}
	if specs[0].IsConst || specs[1].IsConst {
		t.Fatalf("expected column args (IsConst=false), got %+v", specs)
	}
	if len(specs[0].TypeBound) == 0 {
		t.Fatalf("expected addable bound on x, got %+v", specs[0])
	}
}

// captureArgs holds arrow.Array column-arg fields; the adapter should fill
// them from the batch at Process time.
type captureArgs struct {
	A arrow.Array `vgi:"pos=0,const=false"`
	B arrow.Array `vgi:"pos=1,const=false"`
}

type captureImpl struct {
	saw *captureArgs
}

func (captureImpl) Name() string               { return "capture" }
func (captureImpl) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (captureImpl) OnBindTyped(_ *captureArgs, _ *BindParams) (*BindResponse, error) {
	return nil, nil
}
func (c *captureImpl) ProcessTyped(_ context.Context, a *captureArgs, _ *ProcessParams, _ arrow.RecordBatch) (arrow.RecordBatch, error) {
	c.saw = &captureArgs{A: a.A, B: a.B}
	return nil, nil
}

func TestAsScalarFunction_BindsColumnsFromBatch(t *testing.T) {
	impl := &captureImpl{}
	fn := AsScalarFunction[captureArgs](impl)

	mem := memory.NewGoAllocator()
	ab := array.NewInt64Builder(mem)
	ab.AppendValues([]int64{10, 20}, nil)
	a := ab.NewArray()
	defer a.Release()

	bb := array.NewInt64Builder(mem)
	bb.AppendValues([]int64{1, 2}, nil)
	b := bb.NewArray()
	defer b.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{a, b}, 2)
	defer batch.Release()

	if _, err := fn.Process(context.Background(), &ProcessParams{Args: &Arguments{}}, batch); err != nil {
		t.Fatal(err)
	}
	if impl.saw == nil {
		t.Fatal("ProcessTyped not called")
	}
	if impl.saw.A == nil || impl.saw.B == nil {
		t.Fatalf("expected both column args populated, got A=%v B=%v", impl.saw.A, impl.saw.B)
	}
	if got := impl.saw.A.(*array.Int64).Value(0); got != 10 {
		t.Errorf("A[0]=%d, want 10", got)
	}
	if got := impl.saw.B.(*array.Int64).Value(1); got != 2 {
		t.Errorf("B[1]=%d, want 2", got)
	}
}

// varargsArgs declares []arrow.Array varargs — the adapter should slurp every
// remaining batch column into the slice.
type varargsArgs struct {
	Vals []arrow.Array `vgi:"pos=0,const=false,varargs,type=int64"`
}

type varargsImpl struct{ saw []arrow.Array }

func (varargsImpl) Name() string               { return "vararg" }
func (varargsImpl) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (varargsImpl) OnBindTyped(_ *varargsArgs, _ *BindParams) (*BindResponse, error) {
	return nil, nil
}
func (v *varargsImpl) ProcessTyped(_ context.Context, a *varargsArgs, _ *ProcessParams, _ arrow.RecordBatch) (arrow.RecordBatch, error) {
	v.saw = a.Vals
	return nil, nil
}

// concreteArgs uses the concrete *array.Int64 type for one column. The
// framework should (a) infer int64 ArrowType from the Go type and (b)
// type-assert the column at Process time so the body can call Value(i) directly.
type concreteArgs struct {
	Col *array.Int64 `vgi:"pos=0,const=false,doc=Int column"`
}

type concreteImpl struct{ saw *array.Int64 }

func (concreteImpl) Name() string               { return "concrete" }
func (concreteImpl) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (concreteImpl) OnBindTyped(_ *concreteArgs, _ *BindParams) (*BindResponse, error) {
	return nil, nil
}
func (c *concreteImpl) ProcessTyped(_ context.Context, a *concreteArgs, _ *ProcessParams, _ arrow.RecordBatch) (arrow.RecordBatch, error) {
	c.saw = a.Col
	return nil, nil
}

func TestAsScalarFunction_ConcreteArrayType_InfersSpec(t *testing.T) {
	fn := AsScalarFunction[concreteArgs](&concreteImpl{})
	specs := fn.ArgumentSpecs()
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	if specs[0].ArrowType != "int64" {
		t.Errorf("ArrowType: got %q, want int64", specs[0].ArrowType)
	}
	if specs[0].ArrowDataType == nil || !arrow.TypeEqual(specs[0].ArrowDataType, arrow.PrimitiveTypes.Int64) {
		t.Errorf("ArrowDataType: got %v, want int64", specs[0].ArrowDataType)
	}
}

func TestAsScalarFunction_ConcreteArrayType_BindsAndAsserts(t *testing.T) {
	impl := &concreteImpl{}
	fn := AsScalarFunction[concreteArgs](impl)

	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	b.AppendValues([]int64{42, 43}, nil)
	col := b.NewArray()
	defer col.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{col}, 2)
	defer batch.Release()

	if _, err := fn.Process(context.Background(), &ProcessParams{Args: &Arguments{}}, batch); err != nil {
		t.Fatal(err)
	}
	if impl.saw == nil {
		t.Fatal("ProcessTyped not called")
	}
	if impl.saw.Value(0) != 42 || impl.saw.Value(1) != 43 {
		t.Errorf("unexpected values: %v %v", impl.saw.Value(0), impl.saw.Value(1))
	}
}

// Concrete-type mismatch path: reflect.Set panics; the dispatch recovery in
// protocol.go (see RecoverPanic) turns it into a WorkerPanicError. Confirm
// that the panic actually happens — the recovery is tested separately in
// errors_test.go.
func TestAsScalarFunction_ConcreteArrayType_PanicsOnMismatch(t *testing.T) {
	fn := AsScalarFunction[concreteArgs](&concreteImpl{})

	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem) // wrong type — function declares *array.Int64
	b.AppendValues([]float64{1.0}, nil)
	col := b.NewArray()
	defer col.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float64}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{col}, 1)
	defer batch.Release()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on column type mismatch")
		}
	}()
	_, _ = fn.Process(context.Background(), &ProcessParams{Args: &Arguments{}}, batch)
}

func TestAsScalarFunction_BindsVarargsColumns(t *testing.T) {
	impl := &varargsImpl{}
	fn := AsScalarFunction[varargsArgs](impl)

	mem := memory.NewGoAllocator()
	var arrs []arrow.Array
	for i := 0; i < 3; i++ {
		bld := array.NewInt64Builder(mem)
		bld.Append(int64(i))
		arr := bld.NewArray()
		defer arr.Release()
		arrs = append(arrs, arr)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
		{Name: "y", Type: arrow.PrimitiveTypes.Int64},
		{Name: "z", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	batch := array.NewRecordBatch(schema, arrs, 1)
	defer batch.Release()

	if _, err := fn.Process(context.Background(), &ProcessParams{Args: &Arguments{}}, batch); err != nil {
		t.Fatal(err)
	}
	if len(impl.saw) != 3 {
		t.Fatalf("expected 3 vararg columns, got %d", len(impl.saw))
	}
}
