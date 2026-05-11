// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
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
// Concurrency is handled entirely by database/sql + SQLite WAL:
//   - Within-process: MaxOpenConns(1) serializes operations through one
//     connection. database/sql queues callers transparently — no Go-level
//     mutex needed.
//   - Cross-process: WAL mode + busy_timeout=30000 lets multiple worker
//     subprocesses share the file. The per-conn busy_timeout means writers
//     wait up to 30s for the file lock before returning SQLITE_BUSY.
type sqliteStorage struct {
	db                *sql.DB
	cleanupSampleRate float64
	cleanupMaxAge     time.Duration
}

// NewSQLiteStorage opens (or creates) a SQLite-backed FunctionStorage. Safe
// for concurrent use across goroutines and across processes (WAL mode +
// busy_timeout): when DuckDB spawns subprocess workers for one execution,
// every subprocess opens the same database file and sees the others' rows.
//
// The default path is fixed per-user (not per-process) so worker subprocesses
// of a parallel scan share state. Override with opts.Path when the caller
// wants isolation (e.g. tests passing ":memory:").
func NewSQLiteStorage(opts SQLiteStorageOptions) (FunctionStorage, error) {
	path := opts.Path
	if path == "" {
		path = defaultSQLitePath()
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite storage at %q: %w", path, err)
	}
	// One connection per process: writes serialize through it (matches
	// Python's per-thread-connection model — each Go process is the unit
	// of write serialization, with WAL coordinating across processes).
	// Multiple readers within the same process don't need pool depth
	// because reads complete fast and busy_timeout absorbs collisions.
	db.SetMaxOpenConns(1)
	if path != ":memory:" {
		// Mirrors vgi-python's per-connection pragmas — these are what
		// make multi-process write contention tolerable:
		//
		//   journal_mode=WAL      cross-process reader/writer concurrency
		//   synchronous=NORMAL    skip fsync on every commit (fsync at
		//                         WAL checkpoint only) — dominant write-
		//                         throughput win
		//   busy_timeout=30000    wait up to 30s for a lock before
		//                         returning SQLITE_BUSY; matches Python
		//   temp_store=MEMORY     small temp materializations stay in RAM
		//   cache_size=-65536     64 MB page cache per connection
		//
		// With MaxOpenConns(1) the pool has at most one connection, so
		// these pragmas apply to every operation that follows.
		for _, p := range []string{
			"PRAGMA journal_mode=WAL",
			"PRAGMA synchronous=NORMAL",
			"PRAGMA busy_timeout=30000",
			"PRAGMA temp_store=MEMORY",
			"PRAGMA cache_size=-65536",
		} {
			if _, err := db.Exec(p); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("applying %q: %w", p, err)
			}
		}
	}
	if err := initSQLiteSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	// Opportunistic cleanup is opt-in: at 0 by default. Tests and short-
	// lived workers don't need it; long-running deployments should set
	// 0.01 (matches vgi-python's per-call sample rate) or call
	// CleanupOldEntries explicitly on a timer.
	rate := opts.CleanupSampleRate
	maxAge := opts.CleanupMaxAge
	if maxAge == 0 {
		maxAge = 24 * time.Hour
	}
	return &sqliteStorage{
		db:                db,
		cleanupSampleRate: rate,
		cleanupMaxAge:     maxAge,
	}, nil
}

// defaultSQLitePath returns a per-user, per-machine stable path for the
// FunctionStorage SQLite database. Honors XDG_STATE_HOME, falling back to
// ~/.local/state/vgi/storage.db on Unix or %LOCALAPPDATA%/vgi/storage.db on
// Windows. Mirrors vgi-python's _get_default_db_path semantics. The path
// is created if absent.
func defaultSQLitePath() string {
	var base string
	if v := os.Getenv("XDG_STATE_HOME"); v != "" {
		base = v
	} else if v := os.Getenv("LOCALAPPDATA"); v != "" { // Windows
		base = v
	} else if home, err := os.UserHomeDir(); err == nil {
		base = filepath.Join(home, ".local", "state")
	} else {
		base = os.TempDir()
	}
	dir := filepath.Join(base, "vgi")
	_ = os.MkdirAll(dir, 0o755)
	return filepath.Join(dir, "storage.db")
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

// maybeCleanup opportunistically prunes old rows on write paths. Uses
// math/rand/v2's goroutine-safe top-level Float64. No-op when the sample
// rate is 0 (the default).
func (s *sqliteStorage) maybeCleanup() {
	if s.cleanupSampleRate <= 0 {
		return
	}
	if rand.Float64() < s.cleanupSampleRate {
		_, _ = s.CleanupOldEntries(s.cleanupMaxAge)
	}
}

// ---------------------------------------------------------------------------
// Worker state
// ---------------------------------------------------------------------------

func (s *sqliteStorage) WorkerPut(executionID []byte, workerID int64, state []byte) error {
	s.maybeCleanup()
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO worker_state (execution_id, worker_id, state_data, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, workerID, state,
	)
	return err
}

func (s *sqliteStorage) WorkerCollect(executionID []byte) ([][]byte, error) {
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
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO scan_worker_state (execution_id, stream_id, state_data, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, streamID, state,
	)
	return err
}

func (s *sqliteStorage) ScanWorkerScan(executionID []byte) ([]ScanWorkerStateEntry, error) {
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
	// Fast path: single DELETE ... RETURNING claims the next item atomically
	// without a separate transaction. Holds the writer lock only for the
	// duration of one statement — short enough that even busy file-level
	// contention across worker subprocesses fits inside busy_timeout.
	var item []byte
	err := s.db.QueryRow(
		`DELETE FROM work_queue
		 WHERE id = (
		     SELECT id FROM work_queue
		     WHERE execution_id = ?
		     ORDER BY id ASC LIMIT 1
		 )
		 RETURNING work_item`,
		executionID,
	).Scan(&item)
	if err == nil {
		return item, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	// No row returned — disambiguate "empty queue" from "unknown invocation".
	// This second query only fires when the queue is exhausted, so it doesn't
	// add cost to the steady-state pop path.
	var registered int
	err = s.db.QueryRow(
		`SELECT 1 FROM invocation_registry WHERE execution_id = ?`,
		executionID,
	).Scan(&registered)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUnknownInvocation
	}
	if err != nil {
		return nil, err
	}
	return nil, nil // registered but empty
}

func (s *sqliteStorage) QueueClear(executionID []byte) (int, error) {
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
	_, err := s.db.Exec(`DELETE FROM aggregate_state WHERE execution_id = ?`, executionID)
	return err
}

// ---------------------------------------------------------------------------
// Aggregate const args (Go-specific)
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AggregateConstArgsPut(executionID []byte, functionName string, args []byte) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO aggregate_const_args (execution_id, function_name, args, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, functionName, args,
	)
	return err
}

func (s *sqliteStorage) AggregateConstArgsGet(executionID []byte, functionName string) ([]byte, error) {
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
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO aggregate_window_partitions (execution_id, partition_id, payload, created_at)
		 VALUES (?, ?, ?, julianday('now'))`,
		executionID, partitionID, data,
	)
	return err
}

func (s *sqliteStorage) AggregateWindowPartitionGet(executionID []byte, partitionID int64) ([]byte, error) {
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
	_, err := s.db.Exec(
		`DELETE FROM aggregate_window_partitions WHERE execution_id = ? AND partition_id = ?`,
		executionID, partitionID,
	)
	return err
}

func (s *sqliteStorage) AggregateWindowPartitionClear(executionID []byte) error {
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
	_, err := s.db.Exec(`DELETE FROM transaction_state WHERE transaction_id = ?`, transactionID)
	return err
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

func (s *sqliteStorage) CleanupOldEntries(maxAge time.Duration) (int, error) {
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
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}
