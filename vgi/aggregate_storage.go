// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	_ "modernc.org/sqlite"
)

// aggregateStorage is the cross-process state backing for aggregate
// functions. DuckDB's parallel-aggregate model spawns multiple worker
// processes per ATTACH and expects state to flow across them
// (worker_A.update → worker_B.combine), so an in-memory map per process
// would lose data — see vgi-python's FunctionStorage (sqlite-backed) for
// the same rationale.
//
// Implementation: a single SQLite file at $VGI_GO_AGGREGATE_DB or, by
// default, $XDG_STATE_HOME/vgi/vgi_storage_go.db (matching vgi-python's
// platformdirs convention). All worker processes for a user share the
// file; SQLite's file locking handles concurrent writes.
type aggregateStorage struct {
	mu       sync.Mutex
	db       *sql.DB
	openErr  error
	openOnce sync.Once
	dbPath   string
}

func newAggregateStorage() *aggregateStorage {
	return &aggregateStorage{}
}

func defaultAggregateDBPath() string {
	if env := os.Getenv("VGI_GO_AGGREGATE_DB"); env != "" {
		return env
	}
	var base string
	if runtime.GOOS == "darwin" {
		base = filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "vgi-go")
	} else if dir := os.Getenv("XDG_STATE_HOME"); dir != "" {
		base = filepath.Join(dir, "vgi-go")
	} else {
		base = filepath.Join(os.Getenv("HOME"), ".local", "state", "vgi-go")
	}
	return filepath.Join(base, "aggregate_storage.db")
}

func (s *aggregateStorage) ensureOpen() error {
	s.openOnce.Do(func() {
		path := defaultAggregateDBPath()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			s.openErr = fmt.Errorf("create db directory: %w", err)
			return
		}
		// Use shared cache + busy_timeout for cross-process serialization.
		dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)&_pragma=synchronous(NORMAL)"
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			s.openErr = fmt.Errorf("open sqlite: %w", err)
			return
		}
		// Limit pool — SQLite is best with serialized writes.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		schema := `
CREATE TABLE IF NOT EXISTS agg_state (
    execution_id BLOB NOT NULL,
    function_name TEXT NOT NULL,
    group_id INTEGER NOT NULL,
    state BLOB NOT NULL,
    PRIMARY KEY (execution_id, function_name, group_id)
);
CREATE TABLE IF NOT EXISTS agg_const_args (
    execution_id BLOB NOT NULL,
    function_name TEXT NOT NULL,
    args BLOB NOT NULL,
    PRIMARY KEY (execution_id, function_name)
);
CREATE TABLE IF NOT EXISTS agg_window_partition (
    execution_id BLOB NOT NULL,
    function_name TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (execution_id, function_name, partition_id)
);`
		if _, err := db.Exec(schema); err != nil {
			s.openErr = fmt.Errorf("create schema: %w", err)
			return
		}
		s.db = db
		s.dbPath = path
	})
	return s.openErr
}

// stateBucket is a transactional view of the SQLite-backed storage scoped
// to (function_name, execution_id). Methods take a closure to keep the
// SQLite handles short-lived.
type stateBucket struct {
	storage      *aggregateStorage
	functionName string
	executionID  []byte
}

func (s *aggregateStorage) bucket(funcName string, execID []byte) *stateBucket {
	return &stateBucket{storage: s, functionName: funcName, executionID: execID}
}

// loadStates fetches all states for the given group_ids in one query.
// Returns a map; gids absent from the result are not yet stored.
func (b *stateBucket) loadStates(gids []int64) (map[int64][]byte, error) {
	if err := b.storage.ensureOpen(); err != nil {
		return nil, err
	}
	if len(gids) == 0 {
		return map[int64][]byte{}, nil
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	out := make(map[int64][]byte, len(gids))
	// One query per gid — keeps the SQL simple; small N in practice.
	stmt, err := b.storage.db.Prepare(`SELECT state FROM agg_state WHERE execution_id = ? AND function_name = ? AND group_id = ?`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	for _, gid := range gids {
		var data []byte
		err := stmt.QueryRow(b.executionID, b.functionName, gid).Scan(&data)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[gid] = data
	}
	return out, nil
}

// saveStates writes (gid, bytes) pairs in a single transaction.
func (b *stateBucket) saveStates(states map[int64][]byte) error {
	if err := b.storage.ensureOpen(); err != nil {
		return err
	}
	if len(states) == 0 {
		return nil
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	tx, err := b.storage.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO agg_state(execution_id, function_name, group_id, state) VALUES(?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	for gid, data := range states {
		if _, err := stmt.Exec(b.executionID, b.functionName, gid, data); err != nil {
			stmt.Close()
			tx.Rollback()
			return err
		}
	}
	stmt.Close()
	return tx.Commit()
}

func (b *stateBucket) clear() error {
	if err := b.storage.ensureOpen(); err != nil {
		return err
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	tx, err := b.storage.db.Begin()
	if err != nil {
		return err
	}
	for _, q := range []string{
		`DELETE FROM agg_state WHERE execution_id = ? AND function_name = ?`,
		`DELETE FROM agg_const_args WHERE execution_id = ? AND function_name = ?`,
		`DELETE FROM agg_window_partition WHERE execution_id = ? AND function_name = ?`,
	} {
		if _, err := tx.Exec(q, b.executionID, b.functionName); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (b *stateBucket) putConstArgs(args []byte) error {
	if err := b.storage.ensureOpen(); err != nil {
		return err
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	_, err := b.storage.db.Exec(
		`INSERT OR REPLACE INTO agg_const_args(execution_id, function_name, args) VALUES(?, ?, ?)`,
		b.executionID, b.functionName, args,
	)
	return err
}

func (b *stateBucket) getConstArgs() ([]byte, error) {
	if err := b.storage.ensureOpen(); err != nil {
		return nil, err
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	var data []byte
	err := b.storage.db.QueryRow(
		`SELECT args FROM agg_const_args WHERE execution_id = ? AND function_name = ?`,
		b.executionID, b.functionName,
	).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return data, err
}

func (b *stateBucket) putWindowPartition(partitionID int64, payload []byte) error {
	if err := b.storage.ensureOpen(); err != nil {
		return err
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	_, err := b.storage.db.Exec(
		`INSERT OR REPLACE INTO agg_window_partition(execution_id, function_name, partition_id, payload) VALUES(?, ?, ?, ?)`,
		b.executionID, b.functionName, partitionID, payload,
	)
	return err
}

func (b *stateBucket) getWindowPartition(partitionID int64) ([]byte, error) {
	if err := b.storage.ensureOpen(); err != nil {
		return nil, err
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	var payload []byte
	err := b.storage.db.QueryRow(
		`SELECT payload FROM agg_window_partition WHERE execution_id = ? AND function_name = ? AND partition_id = ?`,
		b.executionID, b.functionName, partitionID,
	).Scan(&payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return payload, err
}

func (b *stateBucket) deleteWindowPartition(partitionID int64) error {
	if err := b.storage.ensureOpen(); err != nil {
		return err
	}
	b.storage.mu.Lock()
	defer b.storage.mu.Unlock()
	_, err := b.storage.db.Exec(
		`DELETE FROM agg_window_partition WHERE execution_id = ? AND function_name = ? AND partition_id = ?`,
		b.executionID, b.functionName, partitionID,
	)
	return err
}
