// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package cfdo_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi/storage/cfdo"
)

// A transient 5xx is retried after a jittered, growing backoff, and every
// attempt of one logical call carries the same attempt_id (so the DO's replay
// detection makes the retry idempotent).
func TestPostRetriesWithBackoffAndStableAttemptID(t *testing.T) {
	var mu sync.Mutex
	var attemptIDs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		attemptIDs = append(attemptIDs, body["attempt_id"].(string))
		n := len(attemptIDs)
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"overloaded"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ordinal":7}`))
	}))
	defer srv.Close()
	s, err := cfdo.NewStorage(cfdo.Options{URL: srv.URL, HTTPClient: srv.Client(), PostAttempts: 3,
		RetryBaseDelay: 40 * time.Millisecond, RetryMaxDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var sleeps []time.Duration
	cfdo.SetSleep(s, func(d time.Duration) { sleeps = append(sleeps, d) })
	id, err := s.StateAppend([]byte("exec"), []byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("append after two 503s: %v", err)
	}
	if id != 7 {
		t.Fatalf("ordinal = %d, want 7", id)
	}
	if len(attemptIDs) != 3 {
		t.Fatalf("server saw %d attempts, want 3", len(attemptIDs))
	}
	if attemptIDs[0] != attemptIDs[1] || attemptIDs[1] != attemptIDs[2] {
		t.Fatalf("attempt_id changed across retries of one call: %v", attemptIDs)
	}
	if len(sleeps) != 2 {
		t.Fatalf("slept %d times, want 2 (before each retry, never before the first try)", len(sleeps))
	}
	if sleeps[0] >= 40*time.Millisecond || sleeps[1] >= 80*time.Millisecond {
		t.Fatalf("backoff past its ceiling: %v", sleeps)
	}
}

// The jitter is spread over [0, ceiling) and the ceiling doubles up to the cap.
func TestBackoffDelayJitterAndCap(t *testing.T) {
	s, err := cfdo.NewStorage(cfdo.Options{URL: "http://unused", RetryBaseDelay: 10 * time.Millisecond, RetryMaxDelay: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[time.Duration]bool{}
	for range 200 {
		d := cfdo.BackoffDelay(s, 1)
		if d < 0 || d >= 10*time.Millisecond {
			t.Fatalf("retry 1 delay %v outside [0, 10ms)", d)
		}
		seen[d] = true
		if d := cfdo.BackoffDelay(s, 10); d >= 50*time.Millisecond {
			t.Fatalf("retry 10 delay %v past the 50ms cap", d)
		}
	}
	if len(seen) < 50 {
		t.Fatalf("only %d distinct delays in 200 draws: no jitter", len(seen))
	}
}

// A 4xx is not retried.
func TestPostDoesNotRetryClientErrors(t *testing.T) {
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad"}`))
	}))
	defer srv.Close()
	s, _ := cfdo.NewStorage(cfdo.Options{URL: srv.URL, HTTPClient: srv.Client()})
	cfdo.SetSleep(s, func(time.Duration) { t.Fatal("slept before a non-retryable error") })
	if _, err := s.StateAppend([]byte("e"), []byte("k"), []byte("v")); err == nil {
		t.Fatal("400 accepted")
	}
	if n != 1 {
		t.Fatalf("400 sent %d times, want 1", n)
	}
}

// An unbounded StateLogScan pages by after_id instead of asking for the whole
// log in one response.
func TestStateLogScanPages(t *testing.T) {
	srv := newMockServer(t)
	defer srv.Close()
	base, err := cfdo.NewStorage(cfdo.Options{URL: srv.URL, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	s := base.ForShard(testShardKey)
	ls := s.(*cfdo.Storage)
	const n = 2500
	for i := range n {
		if _, err := ls.StateAppend([]byte("exec"), []byte("k"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	all, err := ls.StateLogScan([]byte("exec"), []byte("k"), -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != n {
		t.Fatalf("unbounded scan returned %d rows, want %d", len(all), n)
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID <= all[i-1].ID {
			t.Fatal("rows out of order across pages")
		}
	}
	some, err := ls.StateLogScan([]byte("exec"), []byte("k"), all[10].ID, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(some) != 1500 || some[0].ID != all[11].ID {
		t.Fatalf("bounded scan returned %d rows starting at %d", len(some), some[0].ID)
	}
}
