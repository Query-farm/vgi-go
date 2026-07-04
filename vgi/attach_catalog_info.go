// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// AttachCatalogInfo is a companion catalog the client should ATTACH when this
// VGI catalog attaches (lakehouse federation). It is IPC-serialized into
// CatalogAttachResult.attach_catalogs; the C++ VGI extension attaches each entry
// at VGI-attach time so multi-branch catalog-table branches (and direct queries)
// can resolve tables in a companion DuckLake / Iceberg / Postgres / DuckDB.
type AttachCatalogInfo struct {
	// Alias is the ATTACH alias; also the SourceCatalog a catalog-table
	// ScanBranch references. Namespace it by your catalog identity so two workers
	// don't both claim the same name (collisions are rejected, never merged).
	Alias string
	// Target is the ATTACH target — a path or DSN, e.g.
	// "ducklake:sqlite:/data/meta.sqlite" or "postgres:dbname=... host=...".
	Target string
	// DBType is the DuckDB db type (e.g. "ducklake", "postgres"). Empty => the
	// extension infers it from the Target scheme prefix.
	DBType string
	// Options are extra ATTACH options forwarded verbatim (e.g. DuckLake DATA_PATH).
	Options map[string]string
	// Hidden attaches the companion excluded from duckdb_databases() (still
	// resolvable by qualified name and by branches).
	Hidden bool
	// Required: when true, a failure to attach fails the whole VGI ATTACH; when
	// false it is logged and skipped.
	Required bool
	// SecretRef optionally names a credential to inject into the companion's
	// ATTACH options (opt-in on the client via attach_companion_secrets).
	SecretRef string
}

var attachCatalogInfoSchema = generated.AttachCatalogInfoSchema

// SerializeAttachCatalogInfo serializes one AttachCatalogInfo to IPC bytes
// matching AttachCatalogInfoSchema (alias, target, db_type, options, hidden,
// required, secret_ref).
func SerializeAttachCatalogInfo(info AttachCatalogInfo) ([]byte, error) {
	mem := memory.NewGoAllocator()

	aliasBuilder := array.NewStringBuilder(mem)
	defer aliasBuilder.Release()
	aliasBuilder.Append(info.Alias)

	targetBuilder := array.NewStringBuilder(mem)
	defer targetBuilder.Release()
	targetBuilder.Append(info.Target)

	dbTypeBuilder := array.NewStringBuilder(mem)
	defer dbTypeBuilder.Release()
	dbTypeBuilder.Append(info.DBType)

	// options (map<string,string>)
	optionsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer optionsBuilder.Release()
	optionsBuilder.Append(true)
	if len(info.Options) > 0 {
		kb := optionsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := optionsBuilder.ItemBuilder().(*array.StringBuilder)
		for k, v := range info.Options {
			kb.Append(k)
			vb.Append(v)
		}
	}

	hiddenBuilder := array.NewBooleanBuilder(mem)
	defer hiddenBuilder.Release()
	hiddenBuilder.Append(info.Hidden)

	requiredBuilder := array.NewBooleanBuilder(mem)
	defer requiredBuilder.Release()
	requiredBuilder.Append(info.Required)

	secretRefBuilder := array.NewStringBuilder(mem)
	defer secretRefBuilder.Release()
	secretRefBuilder.Append(info.SecretRef)

	cols := []arrow.Array{
		aliasBuilder.NewArray(),
		targetBuilder.NewArray(),
		dbTypeBuilder.NewArray(),
		optionsBuilder.NewArray(),
		hiddenBuilder.NewArray(),
		requiredBuilder.NewArray(),
		secretRefBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(attachCatalogInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	wtr := ipc.NewWriter(&buf, ipc.WithSchema(attachCatalogInfoSchema))
	if err := wtr.Write(batch); err != nil {
		return nil, err
	}
	if err := wtr.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
