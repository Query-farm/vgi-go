// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// CopyFromContext is the COPY ... FROM context threaded onto a bind/init when a
// COPY-FROM scan is opened. Mirrors Python's vgi.protocol.CopyFromContext.
//
// Present (non-nil on BindParams/ProcessParams) only when the scan was opened by
// a COPY ... FROM statement against a custom format. The COPY options arrive
// through the function's normal arguments (params.Args), not here.
type CopyFromContext struct {
	// Format is the FORMAT name resolved at COPY bind time.
	Format string
	// FilePath is the source path from the COPY ... FROM 'path' statement.
	FilePath string
	// ExpectedSchema is the COPY target's schema (column names + types, in target
	// order). The reader must emit batches whose schema matches this exactly —
	// DuckDB inserts no cast between the scan and the INSERT.
	ExpectedSchema *arrow.Schema
}

// CopyFromDirectionFrom is the only COPY direction supported today. "to" is
// reserved for a future COPY ... TO.
const CopyFromDirectionFrom = "from"

// CopyFromFunction is the interface a worker implements to serve a custom
// COPY ... FROM format. Mirrors vgi-python's CopyFromFunction base class.
//
// A CopyFromFunction is, mechanically, an ordinary producer-mode table function
// (RegisterCopyFrom wraps it as one so it reuses the whole table bind/init/scan
// path). What makes it a COPY format is that CopyFromFormat() returns the SQL
// FORMAT identifier and the worker advertises it via catalog_copy_from_formats.
//
//   - Name() is the handler's registered function name (also visible in
//     duckdb_functions like any table function).
//   - CopyFromFormat() is the bare SQL FORMAT identifier (the VGI extension
//     scopes it by the attach alias, e.g. "acme.<format>").
//   - The COPY options are declared via ArgumentSpecs() (the file_path is
//     supplied by COPY, never as an option) and read in Read via params.Args.
//   - Read parses the source and emits Arrow batches matching expectedSchema.
type CopyFromFunction interface {
	// Name returns the handler's registered function name.
	Name() string
	// Metadata returns descriptive metadata (description/categories/tags).
	Metadata() FunctionMetadata
	// ArgumentSpecs returns the COPY option specifications.
	ArgumentSpecs() []ArgSpec
	// CopyFromFormat returns the SQL FORMAT identifier users type.
	CopyFromFormat() string
	// Read parses the source at path and emits Arrow batches via out whose
	// schema matches expectedSchema exactly. out.Finish() is called by the
	// framework after Read returns.
	Read(ctx context.Context, params *ProcessParams, path string, expectedSchema *arrow.Schema, out *vgirpc.OutputCollector) error
}

// CopyFromCommenter is an optional interface; when implemented, the returned
// comment is surfaced by vgi_copy_formats(). Mirrors COPY_FROM_COMMENT.
type CopyFromCommenter interface {
	CopyFromComment() string
}

// copyFromFormatRecord is the worker-side record advertised via
// catalog_copy_from_formats. Mirrors CopyFromFormatInfo on the wire.
type copyFromFormatRecord struct {
	formatName  string
	handler     string
	comment     string // "" = no comment (encoded as null)
	direction   string
	description string
	tags        map[string]string
	argSpecs    []ArgSpec
}

// copyFromState is the single-shot read guard for the producer-mode adapter.
// Exported field so it gob-encodes for HTTP rehydration.
type copyFromState struct {
	Done bool
}

// copyFromAdapter wraps a CopyFromFunction as a TypedTableFunc so it reuses the
// table bind/init/scan machinery. Mirrors Python's CopyFromFunction.on_bind /
// process.
type copyFromAdapter struct {
	inner CopyFromFunction
}

var _ TypedTableFunc[copyFromState] = (*copyFromAdapter)(nil)

func (a *copyFromAdapter) Name() string               { return a.inner.Name() }
func (a *copyFromAdapter) Metadata() FunctionMetadata { return a.inner.Metadata() }
func (a *copyFromAdapter) ArgumentSpecs() []ArgSpec   { return a.inner.ArgumentSpecs() }

// OnBind binds the output schema to the COPY target's schema. DuckDB forces the
// scan's output types to the target table's columns, so a COPY-FROM reader must
// produce exactly the expected schema.
func (a *copyFromAdapter) OnBind(params *BindParams) (*BindResponse, error) {
	cf := params.CopyFrom
	if cf == nil {
		return nil, fmt.Errorf(
			"%s is a COPY FROM format reader; invoke it via COPY <table> FROM '<path>' (FORMAT %s), not as a table function",
			a.inner.Name(), a.inner.CopyFromFormat())
	}
	if cf.ExpectedSchema == nil {
		return nil, fmt.Errorf("%s: COPY FROM context is missing the expected target schema", a.inner.Name())
	}
	return BindSchema(cf.ExpectedSchema)
}

// NewState allocates the single-shot read guard.
func (a *copyFromAdapter) NewState(params *ProcessParams) (*copyFromState, error) {
	return &copyFromState{}, nil
}

// Process drives Read once, then finishes the stream.
func (a *copyFromAdapter) Process(ctx context.Context, params *ProcessParams, state *copyFromState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	cf := params.CopyFrom
	if cf == nil { // defended at bind; producer mode always carries the context
		return fmt.Errorf("%s: missing COPY FROM context at process time", a.inner.Name())
	}
	if err := a.inner.Read(ctx, params, cf.FilePath, params.OutputSchema, out); err != nil {
		return err
	}
	state.Done = true
	return out.Finish()
}

// RegisterCopyFrom registers a custom COPY ... FROM format. The function is
// registered as an ordinary producer-mode table function (so it appears in
// duckdb_functions and reuses the table scan path) AND advertised via
// catalog_copy_from_formats so the VGI extension registers a DuckDB CopyFunction
// for it. Mirrors vgi-python registering a CopyFromFunction subclass in the
// catalog's function list.
func (w *Worker) RegisterCopyFrom(f CopyFromFunction) {
	w.RegisterTable(AsTableFunction[copyFromState](&copyFromAdapter{inner: f}))

	meta := f.Metadata()
	comment := ""
	if c, ok := f.(CopyFromCommenter); ok {
		comment = c.CopyFromComment()
	}
	w.copyFromFormats = append(w.copyFromFormats, copyFromFormatRecord{
		formatName:  f.CopyFromFormat(),
		handler:     f.Name(),
		comment:     comment,
		direction:   CopyFromDirectionFrom,
		description: meta.Description,
		tags:        meta.Tags,
		argSpecs:    f.ArgumentSpecs(),
	})
}
