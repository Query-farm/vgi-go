// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// SQLite-backed FunctionStorage
//
// Every method group maps onto the three unified tables shared by all backends
// (the Cloudflare DO adds an HTTP-idempotency column layer on top):
//
//	function_state      composite-key K/V over (scope_id, ns, key). Worker,
//	                    scan-worker, aggregate, window-partition, const-args and
//	                    transaction state all live here under a reserved ns.
//	function_state_log  append-only log keyed by (scope_id, ns, key); the
//	                    AUTOINCREMENT id is the scan cursor.
//	work_queue          FIFO work items, destructive pop (no registration).
//
// scope_id holds either execution_id or transaction_opaque_data (caller's
// choice). The local tier carries none of the DO's idempotency machinery
// (no last_attempt_id / drained_* / attempt_id) and no created_at: a single
// SQLite connection per process has no network retries to dedup.
// ---------------------------------------------------------------------------

// Reserved namespaces — identical to the Cloudflare DO client's mapping so both
// backends share one schema and one mental model.
var (
	nsWorker     = []byte("worker")
	nsScanWorker = []byte("scan_worker")
	nsAgg        = []byte("agg")
	nsAggConst   = []byte("agg_const")
	nsWin        = []byte("win")
	nsTxn        = []byte("txn")
	nsLog        = []byte("log")
)

// int64Key encodes an int64 worker/group/partition id as an 8-byte big-endian
// state key (matching the DO client).
func int64Key(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func int64FromKey(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// SQLiteStorageOptions tunes a SQLite-backed FunctionStorage.
type SQLiteStorageOptions struct {
	// Path is the SQLite database file path. Empty defaults to a per-user,
	// per-machine state path. Use ":memory:" for the in-process tier.
	Path string
}

// sqliteStorage implements FunctionStorage against a single SQLite database.
// Concurrency is handled entirely by database/sql + SQLite WAL:
//   - Within-process: MaxOpenConns(1) serializes operations through one
//     connection. database/sql queues callers transparently.
//   - Cross-process: WAL mode + busy_timeout=30000 lets multiple worker
//     subprocesses share the file.
type sqliteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage opens (or creates) a SQLite-backed FunctionStorage. Safe for
// concurrent use across goroutines and across processes (WAL + busy_timeout):
// when DuckDB spawns subprocess workers for one execution, every subprocess
// opens the same database file and sees the others' rows.
func NewSQLiteStorage(opts SQLiteStorageOptions) (FunctionStorage, error) {
	path := opts.Path
	if path == "" {
		path = defaultSQLitePath()
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite storage at %q: %w", path, err)
	}
	// One connection per process: writes serialize through it (matches Python's
	// per-thread-connection model), with WAL coordinating across processes.
	db.SetMaxOpenConns(1)
	if path != ":memory:" {
		// Per-connection pragmas, matching vgi-python / vgi-typescript / vgi-java.
		// busy_timeout MUST be set before journal_mode=WAL: switching the journal
		// mode briefly needs an exclusive lock, and when several fresh workers
		// start concurrently (e.g. a pool=false late-materialization scan spawns a
		// worker per acquire) one would otherwise fail immediately with "database
		// is locked" instead of waiting.
		for _, p := range []string{
			"PRAGMA busy_timeout=30000",
			"PRAGMA journal_mode=WAL",
			"PRAGMA synchronous=NORMAL",
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
	return &sqliteStorage{db: db}, nil
}

// defaultSQLitePath returns a per-user, per-machine stable path for the
// FunctionStorage SQLite database. Honors XDG_STATE_HOME, falling back to
// ~/.local/state/vgi/storage.db on Unix or %LOCALAPPDATA%/vgi/storage.db on
// Windows. The path is created if absent.
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

// initSQLiteSchema creates the three unified tables. Idempotent. Drops any
// legacy split-schema or idempotency-column tables left over from an older
// on-disk DB (all this state is ephemeral in-progress worker state).
func initSQLiteSchema(db *sql.DB) error {
	for _, dead := range []string{
		"worker_state", "scan_worker_state", "invocation_registry",
		"aggregate_state", "aggregate_const_args", "aggregate_window_partitions",
		"transaction_state", "state_log", "global_state_storage",
	} {
		if _, err := db.Exec(`DROP TABLE IF EXISTS ` + dead); err != nil {
			return fmt.Errorf("dropping legacy table %s: %w", dead, err)
		}
	}
	// Drop a stale function_state / _log / work_queue carrying a created_at or
	// idempotency column (older schema), so the CREATEs below recreate the
	// minimal shape.
	for table, staleCol := range map[string]string{
		"function_state":     "created_at",
		"function_state_log": "created_at",
		"work_queue":         "created_at",
	} {
		if columnExists(db, table, staleCol) {
			if _, err := db.Exec(`DROP TABLE IF EXISTS ` + table); err != nil {
				return fmt.Errorf("dropping stale %s: %w", table, err)
			}
		}
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS work_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			execution_id BLOB NOT NULL,
			work_item BLOB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_work_queue_execution ON work_queue(execution_id, id)`,
		`CREATE TABLE IF NOT EXISTS function_state (
			scope_id BLOB NOT NULL,
			ns BLOB NOT NULL,
			key BLOB NOT NULL,
			value BLOB NOT NULL,
			PRIMARY KEY (scope_id, ns, key)
		)`,
		`CREATE TABLE IF NOT EXISTS function_state_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scope_id BLOB NOT NULL,
			ns BLOB NOT NULL,
			key BLOB NOT NULL,
			value BLOB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_function_state_log_lookup
			ON function_state_log(scope_id, ns, key, id)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("initializing SQLite schema: %w", err)
		}
	}
	return nil
}

func columnExists(db *sql.DB, table, col string) bool {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false
		}
		if name == col {
			return true
		}
	}
	return false
}

// --- Internal unified helpers over function_state -------------------------

func (s *sqliteStorage) statePut(scope, ns, key, value []byte) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO function_state (scope_id, ns, key, value) VALUES (?, ?, ?, ?)`,
		scope, ns, key, value,
	)
	return err
}

func (s *sqliteStorage) stateGetOne(scope, ns, key []byte) ([]byte, error) {
	var v []byte
	err := s.db.QueryRow(
		`SELECT value FROM function_state WHERE scope_id = ? AND ns = ? AND key = ?`,
		scope, ns, key,
	).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// stateDrain reads and deletes every (key, value) in (scope, ns), ordered by
// key, in one transaction.
func (s *sqliteStorage) stateDrain(scope, ns []byte) ([][2][]byte, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(
		`SELECT key, value FROM function_state WHERE scope_id = ? AND ns = ? ORDER BY key`,
		scope, ns,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	var out [][2][]byte
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return nil, err
		}
		out = append(out, [2][]byte{k, v})
	}
	rows.Close()
	if _, err := tx.Exec(`DELETE FROM function_state WHERE scope_id = ? AND ns = ?`, scope, ns); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return out, tx.Commit()
}

// stateScan returns every (key, value) in (scope, ns), ordered by key.
func (s *sqliteStorage) stateScan(scope, ns []byte) ([][2][]byte, error) {
	rows, err := s.db.Query(
		`SELECT key, value FROM function_state WHERE scope_id = ? AND ns = ? ORDER BY key`,
		scope, ns,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2][]byte
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out = append(out, [2][]byte{k, v})
	}
	return out, rows.Err()
}

func (s *sqliteStorage) stateDeleteNS(scope, ns []byte) error {
	_, err := s.db.Exec(`DELETE FROM function_state WHERE scope_id = ? AND ns = ?`, scope, ns)
	return err
}

func (s *sqliteStorage) stateDeleteKey(scope, ns, key []byte) error {
	_, err := s.db.Exec(
		`DELETE FROM function_state WHERE scope_id = ? AND ns = ? AND key = ?`, scope, ns, key)
	return err
}

// ---------------------------------------------------------------------------
// Worker state  (ns=worker, key=int64(worker_id))
// ---------------------------------------------------------------------------

func (s *sqliteStorage) WorkerPut(executionID []byte, workerID int64, state []byte) error {
	return s.statePut(executionID, nsWorker, int64Key(workerID), state)
}

func (s *sqliteStorage) WorkerCollect(executionID []byte) ([][]byte, error) {
	rows, err := s.stateDrain(executionID, nsWorker)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(rows))
	for i, kv := range rows {
		out[i] = kv[1]
	}
	return out, nil
}

func (s *sqliteStorage) WorkerScan(executionID []byte) ([]WorkerStateEntry, error) {
	rows, err := s.stateScan(executionID, nsWorker)
	if err != nil {
		return nil, err
	}
	out := make([]WorkerStateEntry, len(rows))
	for i, kv := range rows {
		out[i] = WorkerStateEntry{WorkerID: int64FromKey(kv[0]), State: kv[1]}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Scan-worker state  (ns=scan_worker, key=stream_id)
// ---------------------------------------------------------------------------

func (s *sqliteStorage) ScanWorkerPut(executionID, streamID, state []byte) error {
	return s.statePut(executionID, nsScanWorker, streamID, state)
}

func (s *sqliteStorage) ScanWorkerScan(executionID []byte) ([]ScanWorkerStateEntry, error) {
	rows, err := s.stateScan(executionID, nsScanWorker)
	if err != nil {
		return nil, err
	}
	out := make([]ScanWorkerStateEntry, len(rows))
	for i, kv := range rows {
		out[i] = ScanWorkerStateEntry{StreamID: kv[0], State: kv[1]}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Work queue  (no registration: pop on empty/unknown returns nil, nil)
// ---------------------------------------------------------------------------

func (s *sqliteStorage) QueuePush(executionID []byte, items [][]byte) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(`INSERT INTO work_queue (execution_id, work_item) VALUES (?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	defer stmt.Close()
	count := 0
	for _, item := range items {
		if _, err := stmt.Exec(executionID, item); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		count++
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *sqliteStorage) QueuePop(executionID []byte) ([]byte, error) {
	// Single DELETE ... RETURNING claims the next item atomically. An empty or
	// never-pushed queue both return (nil, nil) — there is no registration,
	// matching the Cloudflare DO.
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
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *sqliteStorage) QueueClear(executionID []byte) (int, error) {
	res, err := s.db.Exec(`DELETE FROM work_queue WHERE execution_id = ?`, executionID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ---------------------------------------------------------------------------
// Aggregate state  (ns=agg, key=int64(group_id))
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AggregateStateGet(executionID []byte, groupIDs []int64) ([]AggregateStateEntry, error) {
	out := make([]AggregateStateEntry, len(groupIDs))
	for i, gid := range groupIDs {
		v, err := s.stateGetOne(executionID, nsAgg, int64Key(gid))
		if err != nil {
			return nil, err
		}
		if v != nil {
			out[i] = AggregateStateEntry{GroupID: gid, State: v}
		}
	}
	return out, nil
}

func (s *sqliteStorage) AggregateStatePut(executionID []byte, entries []AggregateStateEntry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(
		`INSERT OR REPLACE INTO function_state (scope_id, ns, key, value) VALUES (?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, e := range entries {
		if _, err := stmt.Exec(executionID, nsAgg, int64Key(e.GroupID), e.State); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStorage) AggregateStateClear(executionID []byte) error {
	return s.stateDeleteNS(executionID, nsAgg)
}

// ---------------------------------------------------------------------------
// Aggregate const args  (ns=agg_const, key=function_name)
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AggregateConstArgsPut(executionID []byte, functionName string, args []byte) error {
	return s.statePut(executionID, nsAggConst, []byte(functionName), args)
}

func (s *sqliteStorage) AggregateConstArgsGet(executionID []byte, functionName string) ([]byte, error) {
	return s.stateGetOne(executionID, nsAggConst, []byte(functionName))
}

// ---------------------------------------------------------------------------
// Aggregate window partition  (ns=win, key=int64(partition_id))
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AggregateWindowPartitionPut(executionID []byte, partitionID int64, data []byte) error {
	return s.statePut(executionID, nsWin, int64Key(partitionID), data)
}

func (s *sqliteStorage) AggregateWindowPartitionGet(executionID []byte, partitionID int64) ([]byte, error) {
	return s.stateGetOne(executionID, nsWin, int64Key(partitionID))
}

func (s *sqliteStorage) AggregateWindowPartitionDelete(executionID []byte, partitionID int64) error {
	return s.stateDeleteKey(executionID, nsWin, int64Key(partitionID))
}

func (s *sqliteStorage) AggregateWindowPartitionClear(executionID []byte) error {
	return s.stateDeleteNS(executionID, nsWin)
}

// ---------------------------------------------------------------------------
// Transaction state  (scope=transaction_opaque_data, ns=txn)
// ---------------------------------------------------------------------------

func (s *sqliteStorage) TransactionStateGet(transactionOpaqueData []byte, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	for i, k := range keys {
		v, err := s.stateGetOne(transactionOpaqueData, nsTxn, k)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (s *sqliteStorage) TransactionStatePut(transactionOpaqueData []byte, items []TransactionStateItem) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(
		`INSERT OR REPLACE INTO function_state (scope_id, ns, key, value) VALUES (?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, it := range items {
		if _, err := stmt.Exec(transactionOpaqueData, nsTxn, it.Key, it.Value); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStorage) TransactionStateClear(transactionOpaqueData []byte) error {
	return s.stateDeleteNS(transactionOpaqueData, nsTxn)
}

// ---------------------------------------------------------------------------
// State log  (ns=log; append-only, keyed)
// ---------------------------------------------------------------------------

// StateAppend appends a value to the (executionID, key) log and returns the new
// monotonic log id.
func (s *sqliteStorage) StateAppend(executionID, key, value []byte) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO function_state_log (scope_id, ns, key, value) VALUES (?, ?, ?, ?)`,
		executionID, nsLog, key, value,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// StateLogScan returns log entries for (executionID, key) with id > afterID,
// ordered by id. afterID = -1 reads from the start. limit <= 0 means no limit.
func (s *sqliteStorage) StateLogScan(executionID, key []byte, afterID int64, limit int) ([]StateLogEntry, error) {
	q := `SELECT id, value FROM function_state_log
	      WHERE scope_id = ? AND ns = ? AND key = ? AND id > ? ORDER BY id`
	args := []any{executionID, nsLog, key, afterID}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StateLogEntry
	for rows.Next() {
		var e StateLogEntry
		if err := rows.Scan(&e.ID, &e.Value); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// StateLogClear removes all state-log rows for an execution_id.
func (s *sqliteStorage) StateLogClear(executionID []byte) error {
	_, err := s.db.Exec(`DELETE FROM function_state_log WHERE scope_id = ? AND ns = ?`, executionID, nsLog)
	return err
}

// ---------------------------------------------------------------------------
// Attach state  (scope=attach_opaque_data, caller-chosen ns; persistent)
//
// Reuses the function_state K/V table. The scope is the per-ATTACH plaintext
// (random per attach), so it never collides with execution_id / transaction
// scopes. Ordered scans fall out of the table's (scope_id, ns, key) primary
// key. accumulate-style collections use this for cross-query persistence.
// ---------------------------------------------------------------------------

func (s *sqliteStorage) AttachStatePut(scope, ns, key, value []byte) error {
	return s.statePut(scope, ns, key, value)
}

func (s *sqliteStorage) AttachStateGet(scope, ns, key []byte) ([]byte, error) {
	return s.stateGetOne(scope, ns, key)
}

func (s *sqliteStorage) AttachStateScan(scope, ns []byte) ([]AttachStateKV, error) {
	rows, err := s.stateScan(scope, ns)
	if err != nil {
		return nil, err
	}
	out := make([]AttachStateKV, len(rows))
	for i, kv := range rows {
		out[i] = AttachStateKV{Key: kv[0], Value: kv[1]}
	}
	return out, nil
}

func (s *sqliteStorage) AttachStateDeleteKey(scope, ns, key []byte) error {
	return s.stateDeleteKey(scope, ns, key)
}

func (s *sqliteStorage) AttachStateDeleteNS(scope, ns []byte) error {
	return s.stateDeleteNS(scope, ns)
}

// ---------------------------------------------------------------------------

func (s *sqliteStorage) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}
