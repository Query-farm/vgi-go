// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// CopyToContext is the COPY ... TO context threaded onto a bind/init when a
// COPY-TO sink is opened. Mirrors Python's vgi.protocol.CopyToContext.
//
// Present (non-nil on BindParams/ProcessParams) only when the bind/init was
// opened by a COPY ... TO statement against a custom format. The COPY options
// arrive through the function's normal arguments (params.Args), not here; the
// source columns ride params.OutputSchema / the bind input schema.
type CopyToContext struct {
	// Format is the FORMAT name resolved at COPY bind time.
	Format string
	// FilePath is the destination path from the COPY ... TO 'path' statement.
	FilePath string
}

// CopyToDirectionTo is the COPY direction marker for write formats. (The
// discovery RPC is named catalog_copy_from_formats for historical reasons but
// returns all directions.)
const CopyToDirectionTo = "to"

// CopyToFunction is the interface a worker implements to serve a custom
// COPY ... TO format. Mirrors vgi-python's CopyToFunction base class.
//
// Mechanically a CopyToFunction is a buffered (Sink+Combine) function with NO
// Source phase: RegisterCopyTo wraps it as a TableBufferingFunction so it reuses
// the table_buffering_process / table_buffering_combine machinery on both sides.
//
//   - Write is called once per input batch (the buffered process() step, fanned
//     out across DuckDB's sink threads / per-thread workers). Persist the batch
//     to an execution_id-scoped shard via params.Storage (cross-process safe).
//   - Close is called exactly once on the coordinator worker (the buffered
//     combine() step, driven by DuckDB's once-only copy_to_finalize). Read the
//     shards back and perform the terminal write+flush+close of the destination.
//
// There is no finalize/drain phase, so the destination MUST be fully written and
// closed inside Close — a writer that forgets leaves a silent partial file.
//
// Cross-process invariant: Write and Close may run on different worker processes
// (pool rotation / HTTP). Any shard state Close needs MUST live in
// execution_id-scoped storage (params.Storage), not in per-instance fields.
//
//   - Name() is the handler's registered function name (the TableBufferingFunction
//     name; visible in duckdb_functions like any table-buffering function).
//   - CopyToFormat() is the bare SQL FORMAT identifier (the VGI extension scopes
//     it by the attach alias, e.g. "acme.<format>").
//   - The COPY options are declared via ArgumentSpecs() (the file_path is supplied
//     by COPY, never as an option) and read in Write/Close via params.Args.
//   - To require source order, return Metadata with SinkOrderDependent=true;
//     RegisterCopyTo surfaces ordered=true, which the extension maps to a
//     single-thread sink.
type CopyToFunction interface {
	// Name returns the handler's registered function name.
	Name() string
	// Metadata returns descriptive metadata (description/categories/tags). Set
	// SinkOrderDependent=true to request a single-thread, source-ordered sink.
	Metadata() FunctionMetadata
	// ArgumentSpecs returns the COPY option specifications.
	ArgumentSpecs() []ArgSpec
	// CopyToFormat returns the SQL FORMAT identifier users type.
	CopyToFormat() string
	// Write persists one input batch to an execution-scoped shard (called once
	// per sink batch). params.CopyTo carries the destination format + path.
	Write(ctx context.Context, params *ProcessParams, batch arrow.RecordBatch) error
	// Close reads every shard back and performs the terminal write + close of
	// the destination, exactly once. Called even for an empty COPY (zero rows).
	Close(ctx context.Context, params *ProcessParams) error
}

// CopyToCommenter is an optional interface; when implemented, the returned
// comment is surfaced by vgi_copy_formats(). Mirrors COPY_TO_COMMENT.
type CopyToCommenter interface {
	CopyToComment() string
}

// CopyToSecretProvider is an optional interface a CopyToFunction may implement to
// forward CREATE SECRET credentials for secret-backed cloud writes
// (S3/GCS/HTTP/…). Mirrors Python's CopyToFunction.on_secrets.
//
// SecretLookups is the COPY-TO secret-bind hook: it is called during bind (only
// on the first pass, before any secrets are resolved) and returns the secrets to
// resolve — typically scoped by the destination path (params.CopyTo.FilePath).
// The framework's two-phase secret bind resolves each lookup from the caller's
// SecretManager and surfaces the resolved values on params.Secrets at Write/Close
// time. Returning nil/empty requests nothing, so a writer that never touched
// credentials is unaffected.
type CopyToSecretProvider interface {
	SecretLookups(params *BindParams) []SecretLookup
}

// copyToAdapter wraps a CopyToFunction as a TableBufferingFunction so it reuses
// the buffered process/combine machinery. Mirrors Python's CopyToFunction
// process()/combine() (final methods over write()/close()).
type copyToAdapter struct {
	inner CopyToFunction
}

var _ TableBufferingFunction = (*copyToAdapter)(nil)

func (a *copyToAdapter) Name() string               { return a.inner.Name() }
func (a *copyToAdapter) Metadata() FunctionMetadata { return a.inner.Metadata() }
func (a *copyToAdapter) ArgumentSpecs() []ArgSpec   { return a.inner.ArgumentSpecs() }

// OnBind: a sink produces no rows — bind to an empty output schema. Mirrors
// Python's CopyToFunction.on_bind. If the writer implements CopyToSecretProvider,
// its requested secret lookups are forwarded on the first bind pass so the
// two-phase secret bind resolves them (the resolved values reach Write/Close via
// params.Secrets).
func (a *copyToAdapter) OnBind(params *BindParams) (*BindResponse, error) {
	if sp, ok := a.inner.(CopyToSecretProvider); ok && !params.ResolvedSecretsProvided {
		if lookups := sp.SecretLookups(params); len(lookups) > 0 {
			return &BindResponse{SecretScopeRequest: lookups}, nil
		}
	}
	return BindSchema(arrow.NewSchema([]arrow.Field{}, nil))
}

// Process sinks one input batch (→ Write) and returns the execution_id bucket so
// all of a query's batches land in one bucket, mirroring Python.
func (a *copyToAdapter) Process(ctx context.Context, params *ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	if params.CopyTo == nil {
		return nil, fmt.Errorf(
			"%s is a COPY TO format writer; invoke it via COPY <source> TO '<path>' (FORMAT %s)",
			a.inner.Name(), a.inner.CopyToFormat())
	}
	if err := a.inner.Write(ctx, params, batch); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

// Combine performs the terminal write (→ Close) once on the coordinator and
// returns an empty finalize list — the COPY-TO path never drains output.
func (a *copyToAdapter) Combine(ctx context.Context, params *ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	if params.CopyTo == nil {
		return nil, fmt.Errorf(
			"%s is a COPY TO format writer; invoke it via COPY <source> TO '<path>' (FORMAT %s)",
			a.inner.Name(), a.inner.CopyToFormat())
	}
	if err := a.inner.Close(ctx, params); err != nil {
		return nil, err
	}
	return [][]byte{}, nil
}

// Finalize is never invoked on the COPY-TO path (Combine returns no finalize
// ids). Present to satisfy the TableBufferingFunction interface.
func (a *copyToAdapter) Finalize(ctx context.Context, params *ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// RegisterCopyTo registers a custom COPY ... TO format. The function is
// registered as a table-buffering function (so init/process/combine reuse the
// buffered Sink+Combine path) AND advertised via catalog_copy_from_formats with
// direction="to" so the VGI extension registers a DuckDB CopyFunction for it.
// Mirrors vgi-python registering a CopyToFunction subclass in the catalog's
// function list.
func (w *Worker) RegisterCopyTo(f CopyToFunction) {
	w.RegisterTableBuffering(&copyToAdapter{inner: f})

	meta := f.Metadata()
	comment := ""
	if c, ok := f.(CopyToCommenter); ok {
		comment = c.CopyToComment()
	}
	w.copyFromFormats = append(w.copyFromFormats, copyFromFormatRecord{
		formatName:  f.CopyToFormat(),
		handler:     f.Name(),
		comment:     comment,
		direction:   CopyToDirectionTo,
		description: meta.Description,
		tags:        meta.Tags,
		argSpecs:    f.ArgumentSpecs(),
		ordered:     meta.SinkOrderDependent,
	})
}
