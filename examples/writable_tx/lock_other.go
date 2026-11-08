// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//go:build !unix

package writable_tx

import "sync"

var processLock sync.Mutex

// lockFile falls back to a process-local lock where flock is unavailable;
// the fixture's multi-process lanes run on unix only.
func lockFile(string) (func(), error) {
	processLock.Lock()
	return processLock.Unlock, nil
}
