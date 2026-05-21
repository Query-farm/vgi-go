// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// CatalogDataVersionRelease is one published data version of a catalog. It
// mirrors vgi-python's CatalogDataVersionRelease. Entries on
// CatalogInfo.Releases must be newest-first and unique by Version.
type CatalogDataVersionRelease struct {
	// Version is the concrete published version (e.g. "1.0.0"), not a spec.
	Version string
	// ReleasedAt is the release date (UTC). Nil when the worker doesn't track
	// dates.
	ReleasedAt *time.Time
	// Summary is a one-line human summary; empty string when unknown.
	Summary string
	// NotesURL optionally links to detailed notes for this release.
	NotesURL *string
}

// CatalogInfo is the discovery record returned by catalog_catalogs.
type CatalogInfo struct {
	Name                  string
	ImplementationVersion *string
	DataVersionSpec       *string
	// AttachOptionSpecs holds pre-serialized AttachOptionSpec records (one per
	// declared ATTACH-time option). Surfaced to DuckDB via vgi_catalogs() so
	// the extension can validate ATTACH options before attach.
	AttachOptionSpecs [][]byte
	// Releases lists the concrete published data versions, newest-first. Empty
	// when the worker doesn't track release history.
	Releases []CatalogDataVersionRelease
	// SourceURL points at where this worker's code lives (repo, build, docs).
	// Nil when the worker doesn't advertise a source location.
	SourceURL *string
}

var catalogInfoSchema = generated.CatalogInfoSchema

// SerializeCatalogInfo serializes a CatalogInfo to IPC bytes.
func SerializeCatalogInfo(info *CatalogInfo) ([]byte, error) {
	mem := memory.NewGoAllocator()

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	implBuilder := array.NewStringBuilder(mem)
	defer implBuilder.Release()
	if info.ImplementationVersion != nil {
		implBuilder.Append(*info.ImplementationVersion)
	} else {
		implBuilder.AppendNull()
	}

	dvsBuilder := array.NewStringBuilder(mem)
	defer dvsBuilder.Release()
	if info.DataVersionSpec != nil {
		dvsBuilder.Append(*info.DataVersionSpec)
	} else {
		dvsBuilder.AppendNull()
	}

	aosBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	defer aosBuilder.Release()
	aosBuilder.Append(true)
	aosVb := aosBuilder.ValueBuilder().(*array.BinaryBuilder)
	for _, spec := range info.AttachOptionSpecs {
		aosVb.Append(spec)
	}

	// releases: list<struct<version, released_at, summary, notes_url>>.
	// Field order and types must match generated.CatalogInfoSchema.
	relStructType := arrow.StructOf(
		arrow.Field{Name: "version", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "released_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		arrow.Field{Name: "summary", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "notes_url", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	relBuilder := array.NewListBuilder(mem, relStructType)
	defer relBuilder.Release()
	relBuilder.Append(true)
	relStruct := relBuilder.ValueBuilder().(*array.StructBuilder)
	for _, r := range info.Releases {
		relStruct.Append(true)
		relStruct.FieldBuilder(0).(*array.StringBuilder).Append(r.Version)
		tsBuilder := relStruct.FieldBuilder(1).(*array.TimestampBuilder)
		if r.ReleasedAt != nil {
			ts, err := arrow.TimestampFromTime(*r.ReleasedAt, arrow.Microsecond)
			if err != nil {
				return nil, err
			}
			tsBuilder.Append(ts)
		} else {
			tsBuilder.AppendNull()
		}
		relStruct.FieldBuilder(2).(*array.StringBuilder).Append(r.Summary)
		notesBuilder := relStruct.FieldBuilder(3).(*array.StringBuilder)
		if r.NotesURL != nil {
			notesBuilder.Append(*r.NotesURL)
		} else {
			notesBuilder.AppendNull()
		}
	}

	srcBuilder := array.NewStringBuilder(mem)
	defer srcBuilder.Release()
	if info.SourceURL != nil {
		srcBuilder.Append(*info.SourceURL)
	} else {
		srcBuilder.AppendNull()
	}

	cols := []arrow.Array{
		nameBuilder.NewArray(),
		implBuilder.NewArray(),
		dvsBuilder.NewArray(),
		aosBuilder.NewArray(),
		relBuilder.NewArray(),
		srcBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(catalogInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(catalogInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
