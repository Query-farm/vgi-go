// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	_ "modernc.org/sqlite"
)

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
func (s *ExecutionStorage) SetExecutionID(execID []byte) {
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
		slog.Error("storage: failed to open database", "path", s.dbPath, "err", err)
		return
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
		slog.Error("storage: create tables failed", "err", err)
	}
}

// QueuePush inserts work items into the shared SQLite queue.
func (s *ExecutionStorage) QueuePush(items [][]byte) {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return
	}

	tx, err := db.Begin()
	if err != nil {
		slog.Error("storage: begin tx failed", "op", "queue_push", "err", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO work_queue (work_item) VALUES (?)")
	if err != nil {
		slog.Error("storage: prepare statement failed", "op", "queue_push", "err", err)
		return
	}
	defer stmt.Close()

	for _, item := range items {
		if _, err := stmt.Exec(item); err != nil {
			slog.Error("storage: insert work item failed", "op", "queue_push", "err", err)
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Error("storage: commit tx failed", "op", "queue_push", "err", err)
	}
}

// QueuePop atomically removes and returns the next work item, or nil if empty.
func (s *ExecutionStorage) QueuePop() []byte {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return nil
	}

	var data []byte
	err := db.QueryRow(`
		DELETE FROM work_queue
		WHERE id = (SELECT id FROM work_queue ORDER BY id ASC LIMIT 1)
		RETURNING work_item
	`).Scan(&data)
	if err != nil {
		return nil // empty queue or error
	}
	return data
}

// QueuePushBatches serializes and pushes record batches to the queue.
func (s *ExecutionStorage) QueuePushBatches(batches []arrow.RecordBatch) {
	items := make([][]byte, 0, len(batches))
	for _, batch := range batches {
		data, err := SerializeRecordBatch(batch)
		if err == nil {
			items = append(items, data)
		}
	}
	s.QueuePush(items)
}

// QueuePopBatch removes, deserializes, and returns the first batch from the queue.
// Returns nil if the queue is empty.
func (s *ExecutionStorage) QueuePopBatch() arrow.RecordBatch {
	data := s.QueuePop()
	if data == nil {
		return nil
	}
	batch, err := DeserializeRecordBatch(data)
	if err != nil {
		return nil
	}
	return batch
}

// Put stores a value keyed by the current worker PID.
// Each worker stores exactly one value (upsert semantics).
func (s *ExecutionStorage) Put(data []byte) {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return
	}

	pid := os.Getpid()
	_, err := db.Exec(
		"INSERT OR REPLACE INTO worker_state (worker_pid, state_data) VALUES (?, ?)",
		pid, data,
	)
	if err != nil {
		slog.Error("storage: put worker state failed", "pid", pid, "err", err)
	}
}

// Collect returns all stored worker values and removes them.
func (s *ExecutionStorage) Collect() [][]byte {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()

	if db == nil {
		return nil
	}

	rows, err := db.Query("DELETE FROM worker_state RETURNING state_data")
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result [][]byte
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err == nil {
			result = append(result, data)
		}
	}
	return result
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
