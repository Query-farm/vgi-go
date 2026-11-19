// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package cfdo

import "time"

// SetSleep replaces the backoff sleep (tests only).
func SetSleep(s *Storage, f func(time.Duration)) { s.sleep = f }

// BackoffDelay exposes the jittered backoff for retry n (tests only).
func BackoffDelay(s *Storage, n int) time.Duration { return s.backoffDelay(n) }
