// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// WritableCatalog is a name-bound writable catalog hosted by a Worker.
// It owns a per-catalog set of schemas and tables that can be created,
// dropped, and mutated via DDL/DML over the VGI protocol.
//
// Storage is currently in-memory; use a fresh worker process for an
// empty starting state. Cross-process state sharing (analogous to the
// SQLite-backed aggregate storage) is a follow-up.
type WritableCatalog struct {
	// Name is the SQL-visible catalog name (must match ATTACH '<name>').
	Name string
	// Comment is an optional human-readable description.
	Comment string

	mu               sync.Mutex
	attachOpaqueData []byte
	version          int64
	// schemas keyed by schema name (lower-case canonical form).
	schemas map[string]*writableSchema
	// store persists schemas/tables/rows across worker processes.
	store *writableStore
}

type writableSchema struct {
	name    string
	comment string
	tables  map[string]*writableTable
}

type writableTable struct {
	name    string
	schema  *arrow.Schema
	comment string
	// rows is the in-memory row store; each row is a map of column name
	// to Go value (nil for NULL).
	rows []map[string]interface{}
	// constraints captured at CREATE TABLE time.
	notNull       []string
	primaryKey    [][]string
	unique        [][]string
	check         []string
	foreignKey    []ForeignKeyConstraint
	defaults      map[string]any
	columnComment map[string]string
}

// NewWritableCatalog builds an empty writable catalog with one default
// schema "main". The catalog uses a SQLite-backed store so DuckDB-spawned
// worker subprocesses see the same state.
func NewWritableCatalog(name string) *WritableCatalog {
	c := &WritableCatalog{
		Name:    name,
		version: 1,
		schemas: map[string]*writableSchema{},
		store:   newWritableStore(),
	}
	c.schemas["main"] = &writableSchema{name: "main", tables: map[string]*writableTable{}}
	// Ensure "main" is persisted so other processes can see it.
	_ = c.store.schemaUpsert(name, "main", "")
	return c
}

// AttachOpaqueData returns the attach_opaque_data assigned to this catalog after first attach.
func (c *WritableCatalog) AttachOpaqueData() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, len(c.attachOpaqueData))
	copy(out, c.attachOpaqueData)
	return out
}
