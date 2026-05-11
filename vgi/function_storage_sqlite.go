// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// SQLite-backed FunctionStorage
//
// One shared SQLite database covers every method group on FunctionStorage.
// Tables key by execution_id (or transaction_id) so a single DB is safe for
// many concurrent invocations — matching the vgi-python SQLite backend and
// making cloud backends (Cloudflare DO etc.) straightforward to add against
// the same interface.
// ---------------------------------------------------------------------------

// SQLiteStorageOptions tunes a SQLite-backed FunctionStorage.
type SQLiteStorageOptions struct {
	// Path is the SQLite database file path. Empty defaults to
	// $TMPDIR/vgi-storage-<pid>.db. Use ":memory:" or "file::memory:?cache=shared"
	// for in-process testing.
	Path string

	// CleanupSampleRate is the probability that a per-call operation
	// opportunistically prunes old rows (matching vgi-python's 1% sample).
	// Set to 0 to disable.
	CleanupSampleRate float64

	// CleanupMaxAge is the maximum age of rows kept by opportunistic
	// cleanup. Default: 24h.
	CleanupMaxAge time.Duration
}

// sqliteStorage implements FunctionStorage against a single SQLite database.
type sqliteStorage struct {
	db                *sql.DB
	mu                sync.Mutex
	rng               *rand.Rand
	cleanupSampleRate float64
	cleanupMaxAge     time.Duration
}

// NewSQLiteStorage opens (or creates) a SQLite-backed FunctionStorage. Safe
// for concurrent use; multiple workers in the same process share one *DB.
func NewSQLiteStorage(opts SQLiteStorageOptions) (FunctionStorage, error) {
	path := opts.Path
	if path == "" {
		dir := os.TempDir()
		path = filepath.Join(dir, fmt.Sprintf("vgi-storage-%d.db", os.Getpid()))
	}
	dsn := path
	if path != ":memory:" {
		// WAL + busy-timeout play well with cross-process access; mirrors
		// the Python defaults.
		dsn = path + "?_journal=WAL&_busy_timeout=5000"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite storage at %q: %w", path, err)
	}
	if err := initSQLiteSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	rate := opts.CleanupSampleRate
	if rate == 0 {
		rate = 0.01
	}
	maxAge := opts.CleanupMaxAge
	if maxAge == 0 {
		maxAge = 24 * time.Hour
	}
	return &sqliteStorage{
		db:                db,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		cleanupSampleRate: rate,
		cleanupMaxAge:     maxAge,
	}, nil
}

// initSQLiteSchema creates the eight backing tables. Idempotent.
func initSQLiteSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS worker_state (
			execution_id BLOB NOT NULL,
			worker_id INTEGER NOT NULL,
			state_data BLOB NOT NULL,
			created_at REAL DEFAULT (julianday('now')),
			PRIMARY KEY (execution_id, worker_id)
		)`,
		`CREATE TABLE IF NOT EXISTS scan_worker_state (
			execution_id BLOB NOT NULL,
			stream_id BLOB NOT NULL,
			state_data BLOB NOT NULL,
			created_at REAL DEFAULT (julianday('now')),
			PRIMARY KEY (execution_id, stream_id)
		)`,
		`CREATE TABLE IF NOT EXISTS work_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			execution_id BLOB NOT NULL,
			work_item BLOB NOT NULL,
			created_at REAL DEFAULT (julianday('now'))
		)`,
		`CREATE INDEX IF NOT EXISTS idx_work_queue_exec ON work_queue(execution_id)`,
		`CREATE TABLE IF NOT EXISTS invocation_registry (
			execution_id BLOB PRIMARY KEY,
			created_at REAL DEFAULT (julianday('now'))
		)`,
		`CREATE TABLE IF NOT EXISTS aggregate_state (
			execution_id BLOB NOT NULL,
			group_id INTEGER NOT NULL,
			state_data BLOB NOT NULL,
			created_at REAL DEFAULT (julianday('now')),
			PRIMARY KEY (execution_id, group_id)
		)`,
		`CREATE TABLE IF NOT EXISTS aggregate_const_args (
			execution_id BLOB NOT NULL,
			function_name TEXT NOT NULL,
			args BLOB NOT NULL,
			created_at REAL DEFAULT (julianday('now')),
			PRIMARY KEY (execution_id, function_name)
		)`,
		`CREATE TABLE IF NOT EXISTS aggregate_window_partitions (
			execution_id BLOB NOT NULL,
			partition_id INTEGER NOT NULL,
			payload BLOB NOT NULL,
			created_at REAL DEFAULT (julianday('now')),
			PRIMARY KEY (execution_id, partition_id)
		)`,
		`CREATE TABLE IF NOT EXISTS transaction_state (
			transaction_id BLOB NOT NULL,
			key BLOB NOT NULL,
			value BLOB NOT NULL,
			created_at REAL DEFAULT (julianday('now')),
			PRIMARY KEY (transaction_id, key)
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("initializing SQLite schema: %w", err)
		}
	}
	return nil
}

// maybeCleanup opportunistically prunes old rows. Called from mutation paths.
// Holds no lock — the caller must not be holding s.mu.
func (s *sqliteStorage) maybeCleanup() {
	s.mu.Lock()
	r := s.rng.Float64()
	s.mu.Unlock()
	if r < s.cleanupSampleRate {
		_, _ = s.CleanupOldEntries(s.cleanupMaxAge)
	}
}

// ---------------------------------------------------------------------------
// Worker state
// ---------------------------------------------------------------------------

func (s *sqliteStorage) WorkerPut(executionID []byte, workerID int64, state []byte) error {
	s.maybeCleanup()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO worker_state (execution_id, worker_id, state_data, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, workerID, state,
	)
	return err
}

func (s *sqliteStorage) WorkerCollect(executionID []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(
		`SELECT state_data FROM worker_state WHERE execution_id = ? ORDER BY worker_id`,
		executionID,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	var states [][]byte
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return nil, err
		}
		states = append(states, data)
	}
	rows.Close()
	if _, err := tx.Exec(`DELETE FROM worker_state WHERE execution_id = ?`, executionID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return states, tx.Commit()
}

func (s *sqliteStorage) WorkerScan(executionID []byte) ([]WorkerStateEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(
		`SELECT worker_id, state_data FROM worker_state WHERE execution_id = ? ORDER BY worker_id`,
		executionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkerStateEntry
	for rows.Next() {
		var e WorkerStateEntry
		if err := rows.Scan(&e.WorkerID, &e.State); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Scan-worker state
// ---------------------------------------------------------------------------

func (s *sqliteStorage) ScanWorkerPut(executionID, streamID, state []byte) error {
	s.maybeCleanup()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO scan_worker_state (execution_id, stream_id, state_data, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, streamID, state,
	)
	return err
}

func (s *sqliteStorage) ScanWorkerScan(executionID []byte) ([]ScanWorkerStateEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(
		`SELECT stream_id, state_data FROM scan_worker_state WHERE execution_id = ?`,
		executionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScanWorkerStateEntry
	for rows.Next() {
		var e ScanWorkerStateEntry
		if err := rows.Scan(&e.StreamID, &e.State); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Work queue
// ---------------------------------------------------------------------------

func (s *sqliteStorage) QueuePush(executionID []byte, items [][]byte) (int, error) {
	s.maybeCleanup()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(
		`INSERT OR IGNORE INTO invocation_registry (execution_id) VALUES (?)`,
		executionID,
	); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	count := 0
	if len(items) > 0 {
		stmt, err := tx.Prepare(`INSERT INTO work_queue (execution_id, work_item) VALUES (?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(executionID, item); err != nil {
				_ = tx.Rollback()
				return 0, err
			}
			count++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *sqliteStorage) QueuePop(executionID []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	// Reject unknown invocations.
	var registered int
	err = tx.QueryRow(
		`SELECT 1 FROM invocation_registry WHERE execution_id = ?`,
		executionID,
	).Scan(&registered)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return nil, ErrUnknownInvocation
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	var id int64
	var item []byte
	err = tx.QueryRow(
		`SELECT id, work_item FROM work_queue WHERE execution_id = ? ORDER BY id LIMIT 1`,
		executionID,
	).Scan(&id, &item)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return nil, nil // empty queue
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if _, err := tx.Exec(`DELETE FROM work_queue WHERE id = ?`, id); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return item, tx.Commit()
}

func (s *sqliteStorage) QueueClear(executionID []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(`DELETE FROM work_queue WHERE execution_id = ?`, executionID)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	n, _ := res.RowsAffected()
	if _, err := tx.Exec(`DELETE FROM invocation_registry WHERE execution_id = ?`, executionID); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	return int(n), tx.Commit()
}

// ---------------------------------------------------------------------------
// Aggregate state
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AggregateStateGet(executionID []byte, groupIDs []int64) ([]AggregateStateEntry, error) {
	out := make([]AggregateStateEntry, len(groupIDs))
	if len(groupIDs) == 0 {
		return out, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, err := s.db.Prepare(
		`SELECT state_data FROM aggregate_state WHERE execution_id = ? AND group_id = ?`,
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	for i, gid := range groupIDs {
		var data []byte
		err := stmt.QueryRow(executionID, gid).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			continue // leave zero-value
		}
		if err != nil {
			return nil, err
		}
		out[i] = AggregateStateEntry{GroupID: gid, State: data}
	}
	return out, nil
}

func (s *sqliteStorage) AggregateStatePut(executionID []byte, entries []AggregateStateEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.maybeCleanup()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(
		`INSERT OR REPLACE INTO aggregate_state (execution_id, group_id, state_data, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, e := range entries {
		if _, err := stmt.Exec(executionID, e.GroupID, e.State); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStorage) AggregateStateClear(executionID []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM aggregate_state WHERE execution_id = ?`, executionID)
	return err
}

// ---------------------------------------------------------------------------
// Aggregate const args (Go-specific)
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AggregateConstArgsPut(executionID []byte, functionName string, args []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO aggregate_const_args (execution_id, function_name, args, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, functionName, args,
	)
	return err
}

func (s *sqliteStorage) AggregateConstArgsGet(executionID []byte, functionName string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var data []byte
	err := s.db.QueryRow(
		`SELECT args FROM aggregate_const_args WHERE execution_id = ? AND function_name = ?`,
		executionID, functionName,
	).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return data, err
}

// ---------------------------------------------------------------------------
// Aggregate window partition
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AggregateWindowPartitionPut(executionID []byte, partitionID int64, data []byte) error {
	s.maybeCleanup()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO aggregate_window_partitions (execution_id, partition_id, payload, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, partitionID, data,
	)
	return err
}

func (s *sqliteStorage) AggregateWindowPartitionGet(executionID []byte, partitionID int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var data []byte
	err := s.db.QueryRow(
		`SELECT payload FROM aggregate_window_partitions WHERE execution_id = ? AND partition_id = ?`,
		executionID, partitionID,
	).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return data, err
}

func (s *sqliteStorage) AggregateWindowPartitionDelete(executionID []byte, partitionID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`DELETE FROM aggregate_window_partitions WHERE execution_id = ? AND partition_id = ?`,
		executionID, partitionID,
	)
	return err
}

func (s *sqliteStorage) AggregateWindowPartitionClear(executionID []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`DELETE FROM aggregate_window_partitions WHERE execution_id = ?`,
		executionID,
	)
	return err
}

// ---------------------------------------------------------------------------
// Transaction state
// ---------------------------------------------------------------------------

func (s *sqliteStorage) TransactionStateGet(transactionID []byte, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, err := s.db.Prepare(
		`SELECT value FROM transaction_state WHERE transaction_id = ? AND key = ?`,
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	for i, k := range keys {
		var v []byte
		err := stmt.QueryRow(transactionID, k).Scan(&v)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (s *sqliteStorage) TransactionStatePut(transactionID []byte, items []TransactionStateItem) error {
	if len(items) == 0 {
		return nil
	}
	s.maybeCleanup()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(
		`INSERT OR REPLACE INTO transaction_state (transaction_id, key, value, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, it := range items {
		if _, err := stmt.Exec(transactionID, it.Key, it.Value); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStorage) TransactionStateClear(transactionID []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM transaction_state WHERE transaction_id = ?`, transactionID)
	return err
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

func (s *sqliteStorage) CleanupOldEntries(maxAge time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// julianday math: 1 day = 1.0; convert maxAge to days.
	days := maxAge.Hours() / 24.0
	total := 0
	for _, table := range []string{
		"worker_state",
		"scan_worker_state",
		"work_queue",
		"invocation_registry",
		"aggregate_state",
		"aggregate_const_args",
		"aggregate_window_partitions",
		"transaction_state",
	} {
		res, err := s.db.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE created_at < julianday('now') - ?`, table),
			days,
		)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += int(n)
	}
	return total, nil
}

func (s *sqliteStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}
