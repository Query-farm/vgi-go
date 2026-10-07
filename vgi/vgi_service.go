// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// workerService is the vgi.v2 service a Worker hosts. The protocol surface --
// every method's params and result types, the UNIMPLEMENTED default, the
// registration table -- is generated (vgi_service_gen.go); this type supplies
// the methods the SDK implements. The catalog_* methods live beside the
// catalog (catalog.go); the rest forward to the Worker's handlers below.
//
// Embedding unimplementedVgiService is what makes the surface whole: a vgi.v2
// method this type does not define answers UNIMPLEMENTED rather than being
// absent, and a method added to the protocol appears on regeneration.
// TestVgiV2UnimplementedSet pins which methods those are.
type workerService struct {
	unimplementedVgiService
	*Worker
}

// vgiService returns the Worker's vgi.v2 service.
func (w *Worker) vgiService() workerService {
	return workerService{Worker: w}
}

// AggregateBind serves vgi.v2 aggregate_bind.
func (w workerService) AggregateBind(ctx context.Context, cc *vgirpc.CallContext, req AggregateBindRequestWire) (AggregateBindResponseWire, error) {
	return w.handleAggregateBind(ctx, cc, req)
}

// AggregateCombine serves vgi.v2 aggregate_combine.
func (w workerService) AggregateCombine(ctx context.Context, cc *vgirpc.CallContext, req AggregateCombineRequestWire) (AggregateCombineResponseWire, error) {
	return w.handleAggregateCombine(ctx, cc, req)
}

// AggregateDestructor serves vgi.v2 aggregate_destructor.
func (w workerService) AggregateDestructor(ctx context.Context, cc *vgirpc.CallContext, req AggregateDestructorRequestWire) (AggregateDestructorResponseWire, error) {
	return w.handleAggregateDestructor(ctx, cc, req)
}

// AggregateFinalize serves vgi.v2 aggregate_finalize.
func (w workerService) AggregateFinalize(ctx context.Context, cc *vgirpc.CallContext, req AggregateFinalizeRequestWire) (AggregateFinalizeResponseWire, error) {
	return w.handleAggregateFinalize(ctx, cc, req)
}

// AggregateStreamingChunk serves vgi.v2 aggregate_streaming_chunk.
func (w workerService) AggregateStreamingChunk(ctx context.Context, cc *vgirpc.CallContext, req AggregateStreamingChunkRequestWire) (AggregateStreamingChunkResponseWire, error) {
	return w.handleAggregateStreamingChunk(ctx, cc, req)
}

// AggregateStreamingClose serves vgi.v2 aggregate_streaming_close.
func (w workerService) AggregateStreamingClose(ctx context.Context, cc *vgirpc.CallContext, req AggregateStreamingCloseRequestWire) (AggregateStreamingCloseResponseWire, error) {
	return w.handleAggregateStreamingClose(ctx, cc, req)
}

// AggregateStreamingOpen serves vgi.v2 aggregate_streaming_open.
func (w workerService) AggregateStreamingOpen(ctx context.Context, cc *vgirpc.CallContext, req AggregateStreamingOpenRequestWire) (AggregateStreamingOpenResponseWire, error) {
	return w.handleAggregateStreamingOpen(ctx, cc, req)
}

// AggregateUpdate serves vgi.v2 aggregate_update.
func (w workerService) AggregateUpdate(ctx context.Context, cc *vgirpc.CallContext, req AggregateUpdateRequestWire) (AggregateUpdateResponseWire, error) {
	return w.handleAggregateUpdate(ctx, cc, req)
}

// AggregateWindow serves vgi.v2 aggregate_window.
func (w workerService) AggregateWindow(ctx context.Context, cc *vgirpc.CallContext, req AggregateWindowRequestWire) (AggregateWindowResponseWire, error) {
	return w.handleAggregateWindow(ctx, cc, req)
}

// AggregateWindowBatch serves vgi.v2 aggregate_window_batch.
func (w workerService) AggregateWindowBatch(ctx context.Context, cc *vgirpc.CallContext, req AggregateWindowBatchRequestWire) (AggregateWindowBatchResponseWire, error) {
	return w.handleAggregateWindowBatch(ctx, cc, req)
}

// AggregateWindowDestructor serves vgi.v2 aggregate_window_destructor.
func (w workerService) AggregateWindowDestructor(ctx context.Context, cc *vgirpc.CallContext, req AggregateWindowDestructorRequestWire) (AggregateWindowDestructorResponseWire, error) {
	return w.handleAggregateWindowDestructor(ctx, cc, req)
}

// AggregateWindowInit serves vgi.v2 aggregate_window_init.
func (w workerService) AggregateWindowInit(ctx context.Context, cc *vgirpc.CallContext, req AggregateWindowInitRequestWire) (AggregateWindowInitResponseWire, error) {
	return w.handleAggregateWindowInit(ctx, cc, req)
}

// Bind serves vgi.v2 bind.
func (w workerService) Bind(ctx context.Context, cc *vgirpc.CallContext, req BindRequestWire) (BindResponseWire, error) {
	return w.handleBind(ctx, cc, req)
}

// Init serves vgi.v2 init.
func (w workerService) Init(ctx context.Context, cc *vgirpc.CallContext, req InitRequestWire) (*vgirpc.StreamResult, error) {
	return w.handleInit(ctx, cc, req)
}

// TableBufferingCombine serves vgi.v2 table_buffering_combine.
func (w workerService) TableBufferingCombine(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingCombineRequestWire) (TableBufferingCombineResponseWire, error) {
	return w.handleTableBufferingCombine(ctx, cc, req)
}

// TableBufferingDestructor serves vgi.v2 table_buffering_destructor.
func (w workerService) TableBufferingDestructor(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingDestructorRequestWire) (TableBufferingDestructorResponseWire, error) {
	return w.handleTableBufferingDestructor(ctx, cc, req)
}

// TableBufferingProcess serves vgi.v2 table_buffering_process.
func (w workerService) TableBufferingProcess(ctx context.Context, cc *vgirpc.CallContext, req TableBufferingProcessRequestWire) (TableBufferingProcessResponseWire, error) {
	return w.handleTableBufferingProcess(ctx, cc, req)
}

// TableFunctionCardinality serves vgi.v2 table_function_cardinality.
func (w workerService) TableFunctionCardinality(ctx context.Context, cc *vgirpc.CallContext, req CardinalityRequestWire) (TableCardinality, error) {
	return w.handleCardinality(ctx, cc, req)
}

// TableFunctionDynamicToString serves vgi.v2 table_function_dynamic_to_string.
func (w workerService) TableFunctionDynamicToString(ctx context.Context, cc *vgirpc.CallContext, req TableFunctionDynamicToStringRequestWire) (TableFunctionDynamicToStringResponseWire, error) {
	return w.handleTableFunctionDynamicToString(ctx, cc, req)
}

// TableFunctionPlan serves vgi.v2 table_function_plan.
func (w workerService) TableFunctionPlan(ctx context.Context, cc *vgirpc.CallContext, req PlanRequestWire) (PlanResponseWire, error) {
	return w.handlePlan(ctx, cc, req)
}

// TableFunctionStatistics serves vgi.v2 table_function_statistics.
func (w workerService) TableFunctionStatistics(ctx context.Context, cc *vgirpc.CallContext, req CardinalityRequestWire) (*[]byte, error) {
	return w.handleTableFunctionStatistics(ctx, cc, req)
}

// IndexCreateRequestWire is the inner request record for catalog_index_create
// (generated.IndexCreateRequestSchema), carried wrapped in a `request` column.
type IndexCreateRequestWire struct {
	AttachOpaqueData      []byte            `vgirpc:"attach_opaque_data"`
	SchemaPath            []string          `vgirpc:"schema_path"`
	Name                  string            `vgirpc:"name"`
	TableName             string            `vgirpc:"table_name"`
	IndexType             string            `vgirpc:"index_type"`
	ConstraintType        string            `vgirpc:"constraint_type,enum"`
	Expressions           []string          `vgirpc:"expressions"`
	OnConflict            string            `vgirpc:"on_conflict,enum"`
	Options               map[string]string `vgirpc:"options"`
	TransactionOpaqueData *[]byte           `vgirpc:"transaction_opaque_data"`
}

// methodNotImplemented is the answer of every vgi.v2 method this SDK hosts but
// does not implement: code UNIMPLEMENTED, kind method_not_implemented.
func methodNotImplemented(method string) error {
	return &vgirpc.MethodNotImplementedError{
		Method:  method,
		Message: method + " is not implemented by this worker",
	}
}
