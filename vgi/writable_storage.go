// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
)

// writableStore is a SQLite-backed persistence layer that mirrors the
// in-memory writableSchema/writableTable/row data so DuckDB-spawned worker
// subprocesses see the same writable catalog state. Same rationale and
// same SQLite file as the aggregate state store.
//
// Schema:
//
//	wc_schema(catalog, name PK, comment, created_version)
//	wc_table(catalog, schema_name, name PK, schema_ipc, meta_blob, comment)
//	wc_row(catalog, schema_name, table_name, row_id INTEGER PK auto, data_blob)
//
// `meta_blob` is a gob-encoded writableTableMeta capturing not-null /
// PK / unique / check / FK / defaults / column comments — keeps the
// table row narrow.
type writableStore struct {
	mu      sync.Mutex
	db      *sql.DB
	once    sync.Once
	openErr error
}

func newWritableStore() *writableStore { return &writableStore{} }

func (s *writableStore) ensureOpen() error {
	s.once.Do(func() {
		// Use the shared FunctionStorage SQLite path so a single file holds
		// all worker state (aggregate, execution, and writable-catalog
		// tables). The writable store keeps its own *sql.DB handle since
		// its schema doesn't go through the FunctionStorage interface,
		// but SQLite's WAL handles the cross-connection coordination.
		path := defaultSQLitePath()
		dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)&_pragma=synchronous(NORMAL)"
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			s.openErr = fmt.Errorf("open sqlite: %w", err)
			return
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS wc_schema (
    catalog TEXT NOT NULL,
    name TEXT NOT NULL,
    comment TEXT,
    PRIMARY KEY (catalog, name)
);
CREATE TABLE IF NOT EXISTS wc_table (
    catalog TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    name TEXT NOT NULL,
    schema_ipc BLOB NOT NULL,
    meta_blob BLOB NOT NULL,
    comment TEXT,
    PRIMARY KEY (catalog, schema_name, name)
);
CREATE TABLE IF NOT EXISTS wc_row (
    catalog TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_id INTEGER NOT NULL,
    data_blob BLOB NOT NULL,
    PRIMARY KEY (catalog, schema_name, table_name, row_id)
);
CREATE INDEX IF NOT EXISTS wc_row_lookup ON wc_row(catalog, schema_name, table_name);`)
		if err != nil {
			s.openErr = fmt.Errorf("create wc schema: %w", err)
			return
		}
		s.db = db
	})
	return s.openErr
}

// writableTableMeta is the gob-serializable per-table metadata.
type writableTableMeta struct {
	NotNull       []string
	PrimaryKey    [][]string
	Unique        [][]string
	Check         []string
	ForeignKey    []ForeignKeyConstraint
	Defaults      map[string][]byte // gob-encoded defaultValue per column
	ColumnComment map[string]string
}

func encodeTableMeta(t *writableTable) []byte {
	defaults := map[string][]byte{}
	for k, v := range t.defaults {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		// gob.Register the concrete type once; for our usage the values
		// are int64/float64/string/bool/Sql.
		_ = enc.Encode(&v)
		defaults[k] = buf.Bytes()
	}
	m := writableTableMeta{
		NotNull: t.notNull, PrimaryKey: t.primaryKey, Unique: t.unique,
		Check: t.check, ForeignKey: t.foreignKey, Defaults: defaults,
		ColumnComment: t.columnComment,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m); err != nil {
		panic(fmt.Sprintf("encodeTableMeta: %v", err))
	}
	return buf.Bytes()
}

func decodeTableMeta(data []byte) (*writableTableMeta, error) {
	m := &writableTableMeta{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(m); err != nil {
		return nil, fmt.Errorf("decodeTableMeta: %w", err)
	}
	return m, nil
}

func (m *writableTableMeta) toDefaults() map[string]any {
	if len(m.Defaults) == 0 {
		return nil
	}
	out := map[string]any{}
	for k, b := range m.Defaults {
		var v any
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&v); err == nil {
			out[k] = v
		}
	}
	return out
}

func init() {
	gob.Register(Sql(""))
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(true)
	gob.Register("")
}

// schemaUpsert writes a schema record. Returns existing comment if found.
func (s *writableStore) schemaUpsert(catalog, name, comment string) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO wc_schema(catalog, name, comment) VALUES(?, ?, ?)
		 ON CONFLICT(catalog, name) DO UPDATE SET comment=excluded.comment`,
		catalog, strings.ToLower(name), comment)
	return err
}

func (s *writableStore) schemaExists(catalog, name string) (bool, error) {
	if err := s.ensureOpen(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	err := s.db.QueryRow(`SELECT 1 FROM wc_schema WHERE catalog=? AND name=?`, catalog, strings.ToLower(name)).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (s *writableStore) schemaDrop(catalog, name string, cascade bool) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if cascade {
		if _, err := tx.Exec(`DELETE FROM wc_row WHERE catalog=? AND schema_name=?`, catalog, strings.ToLower(name)); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.Exec(`DELETE FROM wc_table WHERE catalog=? AND schema_name=?`, catalog, strings.ToLower(name)); err != nil {
			tx.Rollback()
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM wc_schema WHERE catalog=? AND name=?`, catalog, strings.ToLower(name)); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *writableStore) schemaList(catalog string) ([]struct{ Name, Comment string }, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT name, COALESCE(comment, '') FROM wc_schema WHERE catalog=?`, catalog)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []struct{ Name, Comment string }
	for rows.Next() {
		var r struct{ Name, Comment string }
		if err := rows.Scan(&r.Name, &r.Comment); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// tableUpsert writes a table definition record.
func (s *writableStore) tableUpsert(catalog, schemaName string, t *writableTable) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	schemaIPC, err := SerializeSchema(t.schema)
	if err != nil {
		return err
	}
	meta := encodeTableMeta(t)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.db.Exec(
		`INSERT INTO wc_table(catalog, schema_name, name, schema_ipc, meta_blob, comment) VALUES(?, ?, ?, ?, ?, ?)
		 ON CONFLICT(catalog, schema_name, name) DO UPDATE SET schema_ipc=excluded.schema_ipc, meta_blob=excluded.meta_blob, comment=excluded.comment`,
		catalog, strings.ToLower(schemaName), strings.ToLower(t.name), schemaIPC, meta, t.comment)
	return err
}

func (s *writableStore) tableDrop(catalog, schemaName, tableName string) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM wc_row WHERE catalog=? AND schema_name=? AND table_name=?`, catalog, strings.ToLower(schemaName), strings.ToLower(tableName)); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`DELETE FROM wc_table WHERE catalog=? AND schema_name=? AND name=?`, catalog, strings.ToLower(schemaName), strings.ToLower(tableName)); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// tableLoad fetches a table definition and rehydrates it (without rows).
func (s *writableStore) tableLoad(catalog, schemaName, tableName string) (*writableTable, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var schemaIPC, metaBlob []byte
	var comment string
	err := s.db.QueryRow(
		`SELECT schema_ipc, meta_blob, COALESCE(comment, '') FROM wc_table WHERE catalog=? AND schema_name=? AND name=?`,
		catalog, strings.ToLower(schemaName), strings.ToLower(tableName),
	).Scan(&schemaIPC, &metaBlob, &comment)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	schema, err := DeserializeSchema(schemaIPC)
	if err != nil {
		return nil, err
	}
	meta, err := decodeTableMeta(metaBlob)
	if err != nil {
		return nil, err
	}
	return &writableTable{
		name: tableName, schema: schema, comment: comment,
		notNull: meta.NotNull, primaryKey: meta.PrimaryKey, unique: meta.Unique,
		check: meta.Check, foreignKey: meta.ForeignKey,
		defaults: meta.toDefaults(), columnComment: meta.ColumnComment,
	}, nil
}

func (s *writableStore) tableList(catalog, schemaName string) ([]string, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT name FROM wc_table WHERE catalog=? AND schema_name=?`, catalog, strings.ToLower(schemaName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

// rowsAppend writes new rows, returning the next row_id base.
func (s *writableStore) rowsAppend(catalog, schemaName, tableName string, rows []map[string]interface{}) (int64, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	var maxID sql.NullInt64
	err = tx.QueryRow(
		`SELECT MAX(row_id) FROM wc_row WHERE catalog=? AND schema_name=? AND table_name=?`,
		catalog, strings.ToLower(schemaName), strings.ToLower(tableName),
	).Scan(&maxID)
	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return 0, err
	}
	next := int64(0)
	if maxID.Valid {
		next = maxID.Int64 + 1
	}
	stmt, err := tx.Prepare(`INSERT INTO wc_row(catalog, schema_name, table_name, row_id, data_blob) VALUES(?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer stmt.Close()
	for i, r := range rows {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(r); err != nil {
			tx.Rollback()
			return 0, err
		}
		if _, err := stmt.Exec(catalog, strings.ToLower(schemaName), strings.ToLower(tableName), next+int64(i), buf.Bytes()); err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	return next, tx.Commit()
}

// rowsScan returns all rows ordered by row_id, attaching the row_id under
// rowIDFieldName.
func (s *writableStore) rowsScan(catalog, schemaName, tableName string) ([]map[string]interface{}, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(
		`SELECT row_id, data_blob FROM wc_row WHERE catalog=? AND schema_name=? AND table_name=? ORDER BY row_id`,
		catalog, strings.ToLower(schemaName), strings.ToLower(tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var rid int64
		var blob []byte
		if err := rows.Scan(&rid, &blob); err != nil {
			return nil, err
		}
		var r map[string]interface{}
		if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&r); err != nil {
			return nil, err
		}
		if r == nil {
			r = map[string]interface{}{}
		}
		r[rowIDFieldName] = rid
		out = append(out, r)
	}
	return out, nil
}

// rowsCount returns the number of rows for cardinality estimates.
func (s *writableStore) rowsCount(catalog, schemaName, tableName string) (int64, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM wc_row WHERE catalog=? AND schema_name=? AND table_name=?`,
		catalog, strings.ToLower(schemaName), strings.ToLower(tableName)).Scan(&n)
	return n, err
}

// rowsUpdate replaces specified columns in rows identified by row_id.
func (s *writableStore) rowsUpdate(catalog, schemaName, tableName string, updates []map[string]interface{}) (int64, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	loadStmt, err := tx.Prepare(`SELECT data_blob FROM wc_row WHERE catalog=? AND schema_name=? AND table_name=? AND row_id=?`)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer loadStmt.Close()
	saveStmt, err := tx.Prepare(`UPDATE wc_row SET data_blob=? WHERE catalog=? AND schema_name=? AND table_name=? AND row_id=?`)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer saveStmt.Close()
	count := int64(0)
	for _, u := range updates {
		ridV, ok := u[rowIDFieldName]
		if !ok {
			continue
		}
		rid := toInt64(ridV)
		var blob []byte
		err := loadStmt.QueryRow(catalog, strings.ToLower(schemaName), strings.ToLower(tableName), rid).Scan(&blob)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			tx.Rollback()
			return 0, err
		}
		var existing map[string]interface{}
		if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&existing); err != nil {
			tx.Rollback()
			return 0, err
		}
		if existing == nil {
			existing = map[string]interface{}{}
		}
		for k, v := range u {
			if k == rowIDFieldName {
				continue
			}
			existing[k] = v
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(existing); err != nil {
			tx.Rollback()
			return 0, err
		}
		if _, err := saveStmt.Exec(buf.Bytes(), catalog, strings.ToLower(schemaName), strings.ToLower(tableName), rid); err != nil {
			tx.Rollback()
			return 0, err
		}
		count++
	}
	return count, tx.Commit()
}

// rowsDelete removes rows by row_id.
func (s *writableStore) rowsDelete(catalog, schemaName, tableName string, rowIDs []int64) (int64, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	if len(rowIDs) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(`DELETE FROM wc_row WHERE catalog=? AND schema_name=? AND table_name=? AND row_id=?`)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer stmt.Close()
	count := int64(0)
	for _, rid := range rowIDs {
		res, err := stmt.Exec(catalog, strings.ToLower(schemaName), strings.ToLower(tableName), rid)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
		if affected, _ := res.RowsAffected(); affected > 0 {
			count++
		}
	}
	return count, tx.Commit()
}
