// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	_ "modernc.org/sqlite"
)

var errStorageNotInitialized = errors.New("storage: database not initialized")

// ExecutionStorage provides shared storage for function executions across phases.
// Uses SQLite for cross-process coordination (DuckDB spawns separate worker
// processes for max_workers > 1). The SQLite database path is deterministic
// from the execution ID so all workers for the same execution share state.
type ExecutionStorage struct {
	mu          sync.Mutex
	executionID []byte
	db          *sql.DB
	dbPath      string
}

// NewExecutionStorage creates a new empty execution storage.
func NewExecutionStorage() *ExecutionStorage {
	return &ExecutionStorage{}
}

// SetExecutionID sets the execution ID and opens/creates the shared SQLite database.
func (s *ExecutionStorage) SetExecutionID(execID []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.executionID = execID
	hexID := hex.EncodeToString(execID)

	// Create a deterministic path so all workers for the same execution share the DB
	dir := filepath.Join(os.TempDir(), "vgi-storage")
	os.MkdirAll(dir, 0755)
	s.dbPath = filepath.Join(dir, fmt.Sprintf("%s.db", hexID))

	db, err := sql.Open("sqlite", s.dbPath)
	if err != nil {
		return fmt.Errorf("storage: failed to open database %s: %w", s.dbPath, err)
	}

	// Set connection pool to 1 to avoid multiple connections from same process
	db.SetMaxOpenConns(1)

	// Enable WAL mode and set busy timeout for concurrent access
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		slog.Warn("storage: WAL mode failed", "err", err)
	}
	_, err = db.Exec("PRAGMA busy_timeout=5000")
	if err != nil {
		slog.Warn("storage: busy_timeout failed", "err", err)
	}

	s.db = db

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS work_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			work_item BLOB NOT NULL
		);
		CREATE TABLE IF NOT EXISTS worker_state (
			worker_pid INTEGER PRIMARY KEY,
			state_data BLOB NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("storage: create tables failed: %w", err)
	}
	return nil
}

// QueuePush inserts work items into the shared SQLite queue.
func (s *ExecutionStorage) QueuePush(items [][]byte) error {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return errStorageNotInitialized
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("storage: begin tx failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO work_queue (work_item) VALUES (?)")
	if err != nil {
		return fmt.Errorf("storage: prepare statement failed: %w", err)
	}
	defer stmt.Close()

	for _, item := range items {
		if _, err := stmt.Exec(item); err != nil {
			return fmt.Errorf("storage: insert work item failed: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit tx failed: %w", err)
	}
	return nil
}

// QueuePop atomically removes and returns the next work item.
// Returns (nil, nil) if the queue is empty.
func (s *ExecutionStorage) QueuePop() ([]byte, error) {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return nil, errStorageNotInitialized
	}

	var data []byte
	err := db.QueryRow(`
		DELETE FROM work_queue
		WHERE id = (SELECT id FROM work_queue ORDER BY id ASC LIMIT 1)
		RETURNING work_item
	`).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: queue pop failed: %w", err)
	}
	return data, nil
}

// QueuePushBatches serializes and pushes record batches to the queue.
func (s *ExecutionStorage) QueuePushBatches(batches []arrow.RecordBatch) error {
	items := make([][]byte, 0, len(batches))
	for i, batch := range batches {
		data, err := SerializeRecordBatch(batch)
		if err != nil {
			return fmt.Errorf("storage: serializing batch %d: %w", i, err)
		}
		items = append(items, data)
	}
	return s.QueuePush(items)
}

// QueuePopBatch removes, deserializes, and returns the first batch from the queue.
// Returns (nil, nil) if the queue is empty.
func (s *ExecutionStorage) QueuePopBatch() (arrow.RecordBatch, error) {
	data, err := s.QueuePop()
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	batch, err := DeserializeRecordBatch(data)
	if err != nil {
		return nil, fmt.Errorf("storage: deserializing batch: %w", err)
	}
	return batch, nil
}

// Put stores a value keyed by the current worker PID.
// Each worker stores exactly one value (upsert semantics).
func (s *ExecutionStorage) Put(data []byte) error {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return errStorageNotInitialized
	}

	pid := os.Getpid()
	_, err := db.Exec(
		"INSERT OR REPLACE INTO worker_state (worker_pid, state_data) VALUES (?, ?)",
		pid, data,
	)
	if err != nil {
		return fmt.Errorf("storage: put worker state failed (pid %d): %w", pid, err)
	}
	return nil
}

// Collect returns all stored worker values and removes them.
func (s *ExecutionStorage) Collect() ([][]byte, error) {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return nil, errStorageNotInitialized
	}

	rows, err := db.Query("DELETE FROM worker_state RETURNING state_data")
	if err != nil {
		return nil, fmt.Errorf("storage: collect query failed: %w", err)
	}
	defer rows.Close()

	var result [][]byte
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("storage: collect scan failed: %w", err)
		}
		result = append(result, data)
	}
	return result, nil
}

// Cleanup closes the database and removes the file.
func (s *ExecutionStorage) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
	if s.dbPath != "" {
		os.Remove(s.dbPath)
		os.Remove(s.dbPath + "-wal")
		os.Remove(s.dbPath + "-shm")
	}
}
