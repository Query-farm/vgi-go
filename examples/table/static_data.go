// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ---------------------------------------------------------------------------
// departments_scan
// ---------------------------------------------------------------------------

var DepartmentsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "budget", Type: arrow.PrimitiveTypes.Float64},
}, nil)

type DepartmentsScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*DepartmentsScanFunction)(nil)

func (f *DepartmentsScanFunction) Name() string { return "departments_scan" }

func (f *DepartmentsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns department data",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *DepartmentsScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *DepartmentsScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(DepartmentsSchema)
}

type staticDone struct{ Done bool }

func (f *DepartmentsScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *DepartmentsScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	ids := vgi.BuildInt64Array(3, func(i int64) int64 { return i + 1 })
	names := vgi.BuildStringArray(3, func(i int64) string {
		return []string{"Engineering", "Sales", "HR"}[i]
	})
	budgets := vgi.BuildFloat64Array(3, func(i int64) float64 {
		return []float64{500000.0, 300000.0, 200000.0}[i]
	})

	batch := array.NewRecordBatch(DepartmentsSchema, []arrow.Array{ids, names, budgets}, 3)
	out.Emit(batch)
	return nil
}

func NewDepartmentsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&DepartmentsScanFunction{})
}

// ---------------------------------------------------------------------------
// employees_scan
// ---------------------------------------------------------------------------

var EmployeesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "email", Type: arrow.BinaryTypes.String},
	{Name: "department_id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type EmployeesScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*EmployeesScanFunction)(nil)

func (f *EmployeesScanFunction) Name() string { return "employees_scan" }

func (f *EmployeesScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns employee data",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *EmployeesScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *EmployeesScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(EmployeesSchema)
}

func (f *EmployeesScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *EmployeesScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	ids := vgi.BuildInt64Array(5, func(i int64) int64 { return i + 1 })
	names := vgi.BuildStringArray(5, func(i int64) string {
		return []string{"Alice", "Bob", "Carol", "Dave", "Eve"}[i]
	})
	emails := vgi.BuildStringArray(5, func(i int64) string {
		return []string{"alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com", "eve@co.com"}[i]
	})
	deptIDs := vgi.BuildInt64Array(5, func(i int64) int64 {
		return []int64{1, 1, 2, 2, 3}[i]
	})

	batch := array.NewRecordBatch(EmployeesSchema, []arrow.Array{ids, names, emails, deptIDs}, 5)
	out.Emit(batch)
	return nil
}

func NewEmployeesScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&EmployeesScanFunction{})
}

// ---------------------------------------------------------------------------
// projects_scan
// ---------------------------------------------------------------------------

var ProjectsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "department_id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "project_code", Type: arrow.BinaryTypes.String},
	{Name: "title", Type: arrow.BinaryTypes.String},
}, nil)

type ProjectsScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*ProjectsScanFunction)(nil)

func (f *ProjectsScanFunction) Name() string { return "projects_scan" }

func (f *ProjectsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns project data",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *ProjectsScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *ProjectsScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(ProjectsSchema)
}

func (f *ProjectsScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *ProjectsScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	deptIDs := vgi.BuildInt64Array(3, func(i int64) int64 {
		return []int64{1, 1, 2}[i]
	})
	codes := vgi.BuildStringArray(3, func(i int64) string {
		return []string{"P001", "P002", "P003"}[i]
	})
	titles := vgi.BuildStringArray(3, func(i int64) string {
		return []string{"Backend API", "Frontend UI", "Sales Portal"}[i]
	})

	batch := array.NewRecordBatch(ProjectsSchema, []arrow.Array{deptIDs, codes, titles}, 3)
	out.Emit(batch)
	return nil
}

func NewProjectsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&ProjectsScanFunction{})
}

// ---------------------------------------------------------------------------
// products_scan
// ---------------------------------------------------------------------------

var ProductsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
	{Name: "price", Type: arrow.PrimitiveTypes.Float64},
}, nil)

type ProductsScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*ProductsScanFunction)(nil)

func (f *ProductsScanFunction) Name() string { return "products_scan" }

func (f *ProductsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns product data",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *ProductsScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *ProductsScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(ProductsSchema)
}

func (f *ProductsScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *ProductsScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	ids := vgi.BuildInt64Array(3, func(i int64) int64 { return i + 1 })
	names := vgi.BuildStringArray(3, func(i int64) string {
		return []string{"Widget", "Gadget", "Doohickey"}[i]
	})
	quantities := vgi.BuildInt64Array(3, func(i int64) int64 {
		return []int64{100, 50, 200}[i]
	})
	prices := vgi.BuildFloat64Array(3, func(i int64) float64 {
		return []float64{9.99, 24.99, 4.99}[i]
	})

	batch := array.NewRecordBatch(ProductsSchema, []arrow.Array{ids, names, quantities, prices}, 3)
	out.Emit(batch)
	return nil
}

func NewProductsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&ProductsScanFunction{})
}
