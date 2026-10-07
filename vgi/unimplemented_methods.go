// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// vgi.v2 is hosted whole. The protocol is the unit of optionality: a worker
// that hosts vgi.v2 registers every one of its methods with the reference's
// exact params, result and header schemas, so vgi_rpc.Reflection.v1 reports
// the same vgi.v2 protocol hash from every SDK (TestVgiV2ProtocolHash pins it).
//
// The methods below are part of vgi.v2 but this SDK does not implement them.
// They are registered anyway and answer every call with UNIMPLEMENTED /
// method_not_implemented -- never a silent success -- so a client sees the
// same surface everywhere and learns of the gap from the call, not from a
// missing entry in a description.

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

// VgiRpcParamsSchema advertises the wrapped protocol shape for catalog_index_create.
func (IndexCreateRequestWire) VgiRpcParamsSchema() *arrow.Schema {
	return generated.CatalogIndexCreateParamsSchema
}

// IndexDropRequestWire is for catalog_index_drop.
type IndexDropRequestWire struct {
	AttachOpaqueData      []byte   `vgirpc:"attach_opaque_data"`
	SchemaPath            []string `vgirpc:"schema_path"`
	Name                  string   `vgirpc:"name"`
	IgnoreNotFound        bool     `vgirpc:"ignore_not_found"`
	Cascade               bool     `vgirpc:"cascade"`
	TransactionOpaqueData *[]byte  `vgirpc:"transaction_opaque_data"`
}

// IndexGetRequestWire is for catalog_index_get.
type IndexGetRequestWire struct {
	AttachOpaqueData      []byte   `vgirpc:"attach_opaque_data"`
	SchemaPath            []string `vgirpc:"schema_path"`
	Name                  string   `vgirpc:"name"`
	TransactionOpaqueData *[]byte  `vgirpc:"transaction_opaque_data"`
}

// TableColumnCommentSetRequestWire is for catalog_table_column_comment_set.
type TableColumnCommentSetRequestWire struct {
	AttachOpaqueData      []byte   `vgirpc:"attach_opaque_data"`
	SchemaPath            []string `vgirpc:"schema_path"`
	Name                  string   `vgirpc:"name"`
	ColumnName            string   `vgirpc:"column_name"`
	Comment               *string  `vgirpc:"comment"`
	IgnoreNotFound        bool     `vgirpc:"ignore_not_found"`
	TransactionOpaqueData *[]byte  `vgirpc:"transaction_opaque_data"`
}

// methodNotImplemented is the answer of every vgi.v2 method this SDK hosts but
// does not implement: code UNIMPLEMENTED, kind method_not_implemented.
func methodNotImplemented(method string) error {
	return &vgirpc.MethodNotImplementedError{
		Method:  method,
		Message: method + " is not implemented by this worker",
	}
}

// unimplementedUnary registers a vgi.v2 unary method whose every call fails
// with methodNotImplemented.
func unimplementedUnary[P any, R any](s *vgirpc.Server, name string) {
	vgirpc.Unary[P, R](s, name, func(context.Context, *vgirpc.CallContext, P) (R, error) {
		var zero R
		return zero, methodNotImplemented(name)
	})
}

// unimplementedUnaryVoid is unimplementedUnary for a method with no result.
func unimplementedUnaryVoid[P any](s *vgirpc.Server, name string) {
	vgirpc.UnaryVoid[P](s, name, func(context.Context, *vgirpc.CallContext, P) error {
		return methodNotImplemented(name)
	})
}

// registerUnimplementedMethods registers the vgi.v2 methods this SDK hosts
// only to refuse.
func registerUnimplementedMethods(s *vgirpc.Server) {
	unimplementedUnaryVoid[IndexCreateRequestWire](s, "catalog_index_create")
	unimplementedUnaryVoid[IndexDropRequestWire](s, "catalog_index_drop")
	unimplementedUnary[IndexGetRequestWire, ItemsResponseWire](s, "catalog_index_get")
	unimplementedUnary[SchemaContentsRequestWire, ItemsResponseWire](s, "catalog_schema_contents_indexes")
	unimplementedUnaryVoid[TableColumnCommentSetRequestWire](s, "catalog_table_column_comment_set")
}
