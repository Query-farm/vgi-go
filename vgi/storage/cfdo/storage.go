// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package cfdo implements vgi.FunctionStorage backed by a Cloudflare Worker +
// Durable Object. The DO runs SQLite internally, providing the same semantics
// as the local SQLite backend but accessible over HTTP from any platform —
// useful for HTTP-mode VGI workers running on edge platforms (Cloud Run,
// Fly.io, Cloudflare Workers themselves) where local disk isn't a good fit
// for cross-process state.
//
// Mirrors vgi-python's vgi/function_storage_cf_do.py wire-for-wire so the
// same DO Worker can serve both Go and Python clients.
//
//	storage, err := cfdo.NewStorage(cfdo.Options{
//	    URL:   "https://vgi-storage.myaccount.workers.dev",
//	    Token: os.Getenv("VGI_CF_DO_TOKEN"),
//	})
//	if err != nil { ... }
//	defer storage.Close()
//
// Or from environment:
//
//	storage, err := cfdo.FromEnv()  // reads VGI_CF_DO_URL / VGI_CF_DO_TOKEN
//
// Once a worker selects this backend (typically via VGI_WORKER_SHARED_STORAGE
// = cloudflare-do once the env-driven selector lands), the framework's
// ExecutionStorage and aggregateStorage transparently route through it.
package cfdo

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
)

// Options configures a CF DO storage client.
type Options struct {
	// URL is the base URL of the Cloudflare Worker (e.g.
	// "https://vgi-storage.myaccount.workers.dev"). Trailing slashes are
	// stripped.
	URL string

	// Token is an optional bearer token sent as the Authorization header.
	Token string

	// HTTPClient overrides the default *http.Client. Useful for tests
	// (httptest.NewServer + custom Transport) and for callers that want
	// to share a pool. nil → a default client with sensible timeouts.
	HTTPClient *http.Client

	// PostAttempts is the maximum number of attempts per request. Default 3.
	// Transient transport errors and 5xx responses are retried; 4xx
	// responses are not (the request itself is wrong).
	PostAttempts int
}

// Storage implements vgi.FunctionStorage against a Cloudflare Worker DO.
// Aggregate state and window partitions are intentionally not supported on
// this backend — the underlying DO Worker doesn't expose those endpoints,
// matching the vgi-python CF DO client. Local SQLite remains the right
// backend for aggregate-heavy workloads.
type Storage struct {
	baseURL      string
	token        string
	client       *http.Client
	postAttempts int
}

// NewStorage constructs a CF DO storage client from explicit options.
func NewStorage(opts Options) (*Storage, error) {
	if opts.URL == "" {
		return nil, errors.New("cfdo: URL is required")
	}
	c := opts.HTTPClient
	if c == nil {
		c = &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     30 * time.Second,
			},
		}
	}
	attempts := opts.PostAttempts
	if attempts <= 0 {
		attempts = 3
	}
	return &Storage{
		baseURL:      strings.TrimRight(opts.URL, "/"),
		token:        opts.Token,
		client:       c,
		postAttempts: attempts,
	}, nil
}

// FromEnv constructs a Storage from environment variables, matching
// vgi-python's FunctionStorageCfDo.from_env(). Required: VGI_CF_DO_URL.
// Optional: VGI_CF_DO_TOKEN.
func FromEnv() (*Storage, error) {
	url := os.Getenv("VGI_CF_DO_URL")
	if url == "" {
		return nil, errors.New(
			"cfdo: VGI_CF_DO_URL is required when VGI_WORKER_SHARED_STORAGE=cloudflare-do",
		)
	}
	return NewStorage(Options{
		URL:   url,
		Token: os.Getenv("VGI_CF_DO_TOKEN"),
	})
}

// Close releases the underlying HTTP transport. Safe to call multiple times.
func (s *Storage) Close() error {
	if s.client != nil {
		if t, ok := s.client.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
		s.client = nil
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP POST machinery
// ---------------------------------------------------------------------------

// post sends a JSON POST to endpoint and unmarshals the response into out.
// Retries on transport errors and 5xx; raises typed errors on 4xx.
func (s *Storage) post(endpoint string, body any, out any) error {
	if s.client == nil {
		return errors.New("cfdo: storage is closed")
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("cfdo: encoding %s body: %w", endpoint, err)
	}
	url := s.baseURL + "/" + endpoint
	var lastErr error
	for attempt := 0; attempt < s.postAttempts; attempt++ {
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("cfdo: building %s request: %w", endpoint, err)
		}
		req.Header.Set("Content-Type", "application/json")
		if s.token != "" {
			req.Header.Set("Authorization", "Bearer "+s.token)
		}

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("cfdo: %s transport error: %w", endpoint, err)
			continue
		}

		respBody, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("cfdo: %s read body: %w", endpoint, readErr)
			continue
		}

		// Try to parse the response as JSON. Non-JSON 5xx pages are
		// treated as transient.
		var parsed map[string]json.RawMessage
		jsonErr := json.Unmarshal(respBody, &parsed)

		// 401: authentication failure (non-retryable).
		if resp.StatusCode == http.StatusUnauthorized {
			msg := "unauthorized"
			if jsonErr == nil {
				if raw, ok := parsed["error"]; ok {
					var s string
					_ = json.Unmarshal(raw, &s)
					if s != "" {
						msg = s
					}
				}
			}
			return fmt.Errorf("cfdo: %s authentication failed: %s", endpoint, msg)
		}

		// 404 + error: "unknown_invocation" → ErrUnknownInvocation.
		if resp.StatusCode == http.StatusNotFound && jsonErr == nil {
			if raw, ok := parsed["error"]; ok {
				var s string
				_ = json.Unmarshal(raw, &s)
				if s == "unknown_invocation" {
					return vgi.ErrUnknownInvocation
				}
			}
		}

		// 5xx or non-JSON: retryable.
		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			lastErr = fmt.Errorf(
				"cfdo: %s server error %d: %s",
				endpoint, resp.StatusCode, truncBody(respBody),
			)
			continue
		}
		if jsonErr != nil {
			lastErr = fmt.Errorf(
				"cfdo: %s non-JSON response (status=%d): %s",
				endpoint, resp.StatusCode, truncBody(respBody),
			)
			continue
		}

		// Other 4xx: non-retryable.
		if resp.StatusCode >= 400 {
			return fmt.Errorf(
				"cfdo: %s error %d: %s",
				endpoint, resp.StatusCode, string(respBody),
			)
		}

		if out != nil {
			if err := json.Unmarshal(respBody, out); err != nil {
				return fmt.Errorf("cfdo: %s decoding response: %w", endpoint, err)
			}
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("cfdo: %s exhausted %d attempts", endpoint, s.postAttempts)
	}
	return lastErr
}

func truncBody(b []byte) string {
	if len(b) > 200 {
		return string(b[:200])
	}
	return string(b)
}

// b64 is a convenience for the b64-everything wire format.
func b64(p []byte) string {
	return base64.StdEncoding.EncodeToString(p)
}

func unb64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

// ---------------------------------------------------------------------------
// Worker state
// ---------------------------------------------------------------------------

func (s *Storage) WorkerPut(executionID []byte, workerID int64, state []byte) error {
	return s.post("worker_put", map[string]any{
		"execution_id": b64(executionID),
		"worker_id":    workerID,
		"state":        b64(state),
	}, nil)
}

func (s *Storage) WorkerCollect(executionID []byte) ([][]byte, error) {
	var resp struct {
		States []string `json:"states"`
	}
	if err := s.post("worker_collect", map[string]any{
		"execution_id": b64(executionID),
	}, &resp); err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(resp.States))
	for _, s := range resp.States {
		b, err := unb64(s)
		if err != nil {
			return nil, fmt.Errorf("cfdo: worker_collect decode state: %w", err)
		}
		out = append(out, b)
	}
	return out, nil
}

func (s *Storage) WorkerScan(executionID []byte) ([]vgi.WorkerStateEntry, error) {
	var resp struct {
		Rows []struct {
			WorkerID int64  `json:"worker_id"`
			State    string `json:"state"`
		} `json:"rows"`
	}
	if err := s.post("worker_scan", map[string]any{
		"execution_id": b64(executionID),
	}, &resp); err != nil {
		return nil, err
	}
	out := make([]vgi.WorkerStateEntry, 0, len(resp.Rows))
	for _, r := range resp.Rows {
		state, err := unb64(r.State)
		if err != nil {
			return nil, fmt.Errorf("cfdo: worker_scan decode state: %w", err)
		}
		out = append(out, vgi.WorkerStateEntry{WorkerID: r.WorkerID, State: state})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Scan-worker state
// ---------------------------------------------------------------------------

func (s *Storage) ScanWorkerPut(executionID, streamID, state []byte) error {
	return s.post("scan_worker_put", map[string]any{
		"execution_id": b64(executionID),
		"stream_id":    b64(streamID),
		"state":        b64(state),
	}, nil)
}

func (s *Storage) ScanWorkerScan(executionID []byte) ([]vgi.ScanWorkerStateEntry, error) {
	var resp struct {
		Rows []struct {
			StreamID string `json:"stream_id"`
			State    string `json:"state"`
		} `json:"rows"`
	}
	if err := s.post("scan_worker_scan", map[string]any{
		"execution_id": b64(executionID),
	}, &resp); err != nil {
		return nil, err
	}
	out := make([]vgi.ScanWorkerStateEntry, 0, len(resp.Rows))
	for _, r := range resp.Rows {
		sid, err := unb64(r.StreamID)
		if err != nil {
			return nil, fmt.Errorf("cfdo: scan_worker_scan decode stream_id: %w", err)
		}
		state, err := unb64(r.State)
		if err != nil {
			return nil, fmt.Errorf("cfdo: scan_worker_scan decode state: %w", err)
		}
		out = append(out, vgi.ScanWorkerStateEntry{StreamID: sid, State: state})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Work queue
// ---------------------------------------------------------------------------

func (s *Storage) QueuePush(executionID []byte, items [][]byte) (int, error) {
	enc := make([]string, len(items))
	for i, item := range items {
		enc[i] = b64(item)
	}
	var resp struct {
		Count int `json:"count"`
	}
	if err := s.post("queue_push", map[string]any{
		"execution_id": b64(executionID),
		"items":        enc,
	}, &resp); err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (s *Storage) QueuePop(executionID []byte) ([]byte, error) {
	var resp struct {
		Item *string `json:"item"` // nullable on the wire
	}
	if err := s.post("queue_pop", map[string]any{
		"execution_id": b64(executionID),
	}, &resp); err != nil {
		return nil, err
	}
	if resp.Item == nil {
		return nil, nil // registered but empty
	}
	return unb64(*resp.Item)
}

func (s *Storage) QueueClear(executionID []byte) (int, error) {
	var resp struct {
		Cleared int `json:"cleared"`
	}
	if err := s.post("queue_clear", map[string]any{
		"execution_id": b64(executionID),
	}, &resp); err != nil {
		return 0, err
	}
	return resp.Cleared, nil
}

// ---------------------------------------------------------------------------
// Aggregate state and window partition (unsupported)
//
// The DO Worker doesn't expose these endpoints; aggregate-heavy workloads
// should run with the local SQLite backend. Returning a clear error keeps
// the failure mode obvious instead of silently corrupting state.
// ---------------------------------------------------------------------------

var errAggregateUnsupported = errors.New(
	"cfdo: aggregate functions are not supported with the Cloudflare DO storage backend; " +
		"use the SQLite backend (VGI_WORKER_SHARED_STORAGE=sqlite) for aggregate-using workers",
)

func (s *Storage) AggregateStateGet(executionID []byte, groupIDs []int64) ([]vgi.AggregateStateEntry, error) {
	return nil, errAggregateUnsupported
}

func (s *Storage) AggregateStatePut(executionID []byte, entries []vgi.AggregateStateEntry) error {
	return errAggregateUnsupported
}

func (s *Storage) AggregateStateClear(executionID []byte) error {
	return errAggregateUnsupported
}

func (s *Storage) AggregateConstArgsPut(executionID []byte, functionName string, args []byte) error {
	return errAggregateUnsupported
}

func (s *Storage) AggregateConstArgsGet(executionID []byte, functionName string) ([]byte, error) {
	return nil, errAggregateUnsupported
}

func (s *Storage) AggregateWindowPartitionPut(executionID []byte, partitionID int64, data []byte) error {
	return errAggregateUnsupported
}

func (s *Storage) AggregateWindowPartitionGet(executionID []byte, partitionID int64) ([]byte, error) {
	return nil, errAggregateUnsupported
}

func (s *Storage) AggregateWindowPartitionDelete(executionID []byte, partitionID int64) error {
	return errAggregateUnsupported
}

func (s *Storage) AggregateWindowPartitionClear(executionID []byte) error {
	return errAggregateUnsupported
}

// ---------------------------------------------------------------------------
// Transaction state
// ---------------------------------------------------------------------------

func (s *Storage) TransactionStateGet(transactionOpaqueData []byte, keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}
	enc := make([]string, len(keys))
	for i, k := range keys {
		enc[i] = b64(k)
	}
	var resp struct {
		Values []*string `json:"values"` // parallel to keys, null on miss
	}
	if err := s.post("transaction_state_get", map[string]any{
		"transaction_opaque_data": b64(transactionOpaqueData),
		"keys":           enc,
	}, &resp); err != nil {
		return nil, err
	}
	if len(resp.Values) != len(keys) {
		return nil, fmt.Errorf(
			"cfdo: transaction_state_get returned %d values for %d keys",
			len(resp.Values), len(keys),
		)
	}
	out := make([][]byte, len(keys))
	for i, v := range resp.Values {
		if v == nil {
			continue
		}
		decoded, err := unb64(*v)
		if err != nil {
			return nil, fmt.Errorf("cfdo: transaction_state_get decode value[%d]: %w", i, err)
		}
		out[i] = decoded
	}
	return out, nil
}

func (s *Storage) TransactionStatePut(transactionOpaqueData []byte, items []vgi.TransactionStateItem) error {
	if len(items) == 0 {
		return nil
	}
	enc := make([]map[string]string, len(items))
	for i, it := range items {
		enc[i] = map[string]string{
			"key":   b64(it.Key),
			"value": b64(it.Value),
		}
	}
	return s.post("transaction_state_put", map[string]any{
		"transaction_opaque_data": b64(transactionOpaqueData),
		"items":          enc,
	}, nil)
}

func (s *Storage) TransactionStateClear(transactionOpaqueData []byte) error {
	return s.post("transaction_state_clear", map[string]any{
		"transaction_opaque_data": b64(transactionOpaqueData),
	}, nil)
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

// CleanupOldEntries is a no-op for this backend. The DO Worker handles its
// own TTL sweep (storage is keyed by execution_id and reclaimed by Cloudflare
// when the DO is idle long enough), so client-driven cleanup isn't needed.
func (s *Storage) CleanupOldEntries(maxAge time.Duration) (int, error) {
	return 0, nil
}

// Compile-time interface check.
var _ vgi.FunctionStorage = (*Storage)(nil)
