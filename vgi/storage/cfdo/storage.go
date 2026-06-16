// Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
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

// Namespaces under the unified state_* table. scope_id is the execution_id
// (or transaction_opaque_data for txn state); ns separates the legacy
// FunctionStorage families that the DO server no longer has dedicated
// endpoints for. Internal to this client — one DO per attach, never shared
// across SDKs.
var (
	nsWorker     = []byte("worker")
	nsScanWorker = []byte("scan_worker")
	nsAgg        = []byte("agg")
	nsAggConst   = []byte("agg_const")
	nsWin        = []byte("win")
	nsTxn        = []byte("txn")
	nsLog        = []byte("log")
)

// newAttemptID returns a fresh 32-char lowercase-hex idempotency token, the
// shape the DO server requires on every destructive endpoint.
func newAttemptID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// int64Key encodes an int64 map/group/partition id as an 8-byte big-endian
// state key (opaque to the server).
func int64Key(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

func int64FromKey(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

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

// Storage implements vgi.FunctionStorage against a Cloudflare Worker DO. Every
// legacy FunctionStorage family (worker/scan-worker/aggregate/window/txn state,
// queue, append-log) maps onto the DO's unified state_* + queue_* endpoints,
// each request carrying the per-attach shard_key (set via ForShard) and, for
// destructive ops, a fresh attempt_id. Mirrors vgi-python's
// FunctionStorageCfDo wire-for-wire.
type Storage struct {
	baseURL      string
	token        string
	client       *http.Client
	postAttempts int
	// shardKey routes to the Durable Object for one logical ATTACH
	// (att-<hex uuid>); set per-execution by the framework via ForShard.
	shardKey string
}

// ForShard returns a view of this backend pinned to one shard key, so
// ExecutionStorage can route per logical ATTACH without the shard key being a
// parameter on every FunctionStorage method. Implements vgi.ShardedBackend.
// The returned view shares the HTTP client/config; only shardKey differs.
func (s *Storage) ForShard(shardKey string) vgi.FunctionStorage {
	cp := *s
	cp.shardKey = shardKey
	return &cp
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
func (s *Storage) post(endpoint string, body map[string]any, out any) error {
	if s.client == nil {
		return errors.New("cfdo: storage is closed")
	}
	// The Worker routes on shard_key (idFromName) and rejects requests without
	// one — always splice it in. attempt_id, when needed, is set once by the
	// caller before any retry so a replayed HTTP call carries the same id.
	body["shard_key"] = s.shardKey
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
// Unified state_* / queue_* helpers (composite key over (scope_id, ns, key))
//
// Every legacy FunctionStorage family maps onto these. attempt_id is minted
// per logical call (reused across HTTP retries inside post). shard_key is
// spliced in by post.
// ---------------------------------------------------------------------------

type kvPair struct{ key, value []byte }

func (s *Storage) statePutMany(scopeID, ns []byte, items []kvPair) error {
	enc := make([]map[string]string, len(items))
	for i, it := range items {
		enc[i] = map[string]string{"key": b64(it.key), "value": b64(it.value)}
	}
	return s.post("state_put_many", map[string]any{
		"scope_id":   b64(scopeID),
		"ns":         b64(ns),
		"items":      enc,
		"attempt_id": newAttemptID(),
	}, nil)
}

// stateGetMany returns values parallel to keys (nil entry == miss).
func (s *Storage) stateGetMany(scopeID, ns []byte, keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}
	enc := make([]string, len(keys))
	for i, k := range keys {
		enc[i] = b64(k)
	}
	var resp struct {
		Rows []*struct {
			Value string `json:"value"`
		} `json:"rows"`
	}
	if err := s.post("state_get_many", map[string]any{
		"scope_id": b64(scopeID),
		"ns":       b64(ns),
		"keys":     enc,
	}, &resp); err != nil {
		return nil, err
	}
	if len(resp.Rows) != len(keys) {
		return nil, fmt.Errorf("cfdo: state_get_many returned %d rows for %d keys", len(resp.Rows), len(keys))
	}
	out := make([][]byte, len(keys))
	for i, r := range resp.Rows {
		if r == nil {
			continue
		}
		v, err := unb64(r.Value)
		if err != nil {
			return nil, fmt.Errorf("cfdo: state_get_many decode value[%d]: %w", i, err)
		}
		out[i] = v
	}
	return out, nil
}

// statePaged drives state_scan / state_drain across pages, accumulating rows.
// attemptID is "" for state_scan (non-destructive) and a single reused id for
// state_drain (snapshot-then-page atomicity across pages).
func (s *Storage) statePaged(endpoint string, scopeID, ns []byte, attemptID string) ([]kvPair, error) {
	var out []kvPair
	afterKey := ""
	for {
		body := map[string]any{"scope_id": b64(scopeID), "ns": b64(ns)}
		if afterKey != "" {
			body["after_key"] = afterKey
		}
		if attemptID != "" {
			body["attempt_id"] = attemptID
		}
		var resp struct {
			Rows []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"rows"`
			NextAfter string `json:"next_after"`
		}
		if err := s.post(endpoint, body, &resp); err != nil {
			return nil, err
		}
		for _, r := range resp.Rows {
			k, err := unb64(r.Key)
			if err != nil {
				return nil, fmt.Errorf("cfdo: %s decode key: %w", endpoint, err)
			}
			v, err := unb64(r.Value)
			if err != nil {
				return nil, fmt.Errorf("cfdo: %s decode value: %w", endpoint, err)
			}
			out = append(out, kvPair{key: k, value: v})
		}
		if resp.NextAfter == "" {
			break
		}
		afterKey = resp.NextAfter
	}
	return out, nil
}

// stateDelete removes keys from a namespace, or the whole namespace when keys
// is nil. Returns the count deleted.
func (s *Storage) stateDelete(scopeID, ns []byte, keys [][]byte) (int, error) {
	body := map[string]any{
		"scope_id":   b64(scopeID),
		"ns":         b64(ns),
		"attempt_id": newAttemptID(),
	}
	if keys != nil {
		enc := make([]string, len(keys))
		for i, k := range keys {
			enc[i] = b64(k)
		}
		body["keys"] = enc
	}
	var resp struct {
		Deleted int `json:"deleted"`
	}
	if err := s.post("state_delete", body, &resp); err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}

// executionClear wipes ALL state + log rows for one scope across every ns.
func (s *Storage) executionClear(scopeID []byte) error {
	return s.post("execution_clear", map[string]any{
		"scope_id":   b64(scopeID),
		"attempt_id": newAttemptID(),
	}, nil)
}

// ---------------------------------------------------------------------------
// Worker state
// ---------------------------------------------------------------------------

// Worker state → ns=worker, key = int64(worker_id).

// WorkerPut stores a worker's state under the execution's worker namespace,
// keyed by worker ID.
func (s *Storage) WorkerPut(executionID []byte, workerID int64, state []byte) error {
	return s.statePutMany(executionID, nsWorker, []kvPair{{key: int64Key(workerID), value: state}})
}

// WorkerCollect drains and returns all worker states for the execution,
// removing them from the store.
func (s *Storage) WorkerCollect(executionID []byte) ([][]byte, error) {
	rows, err := s.statePaged("state_drain", executionID, nsWorker, newAttemptID())
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.value)
	}
	return out, nil
}

// WorkerScan returns all worker states for the execution without removing them,
// ordered by worker ID.
func (s *Storage) WorkerScan(executionID []byte) ([]vgi.WorkerStateEntry, error) {
	rows, err := s.statePaged("state_scan", executionID, nsWorker, "")
	if err != nil {
		return nil, err
	}
	out := make([]vgi.WorkerStateEntry, 0, len(rows))
	for _, r := range rows {
		out = append(out, vgi.WorkerStateEntry{WorkerID: int64FromKey(r.key), State: r.value})
	}
	return out, nil
}

// Scan-worker state → ns=scan_worker, key = stream_id.

// ScanWorkerPut stores a scan worker's state under the execution's scan-worker
// namespace, keyed by stream ID.
func (s *Storage) ScanWorkerPut(executionID, streamID, state []byte) error {
	return s.statePutMany(executionID, nsScanWorker, []kvPair{{key: streamID, value: state}})
}

// ScanWorkerScan returns all scan-worker states for the execution without
// removing them, ordered by stream ID.
func (s *Storage) ScanWorkerScan(executionID []byte) ([]vgi.ScanWorkerStateEntry, error) {
	rows, err := s.statePaged("state_scan", executionID, nsScanWorker, "")
	if err != nil {
		return nil, err
	}
	out := make([]vgi.ScanWorkerStateEntry, 0, len(rows))
	for _, r := range rows {
		out = append(out, vgi.ScanWorkerStateEntry{StreamID: r.key, State: r.value})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Work queue
// ---------------------------------------------------------------------------

// QueuePush appends the given items to the execution's work queue and returns
// the number pushed.
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
		"attempt_id":   newAttemptID(),
	}, &resp); err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// QueuePop atomically removes and returns the next item from the execution's
// work queue, or nil if the queue is empty.
func (s *Storage) QueuePop(executionID []byte) ([]byte, error) {
	var resp struct {
		Item *string `json:"item"` // nullable on the wire
	}
	if err := s.post("queue_pop", map[string]any{
		"execution_id": b64(executionID),
		"attempt_id":   newAttemptID(),
	}, &resp); err != nil {
		return nil, err
	}
	if resp.Item == nil {
		return nil, nil // registered but empty
	}
	return unb64(*resp.Item)
}

// QueueClear removes all items from the execution's work queue and returns the
// number removed.
func (s *Storage) QueueClear(executionID []byte) (int, error) {
	var resp struct {
		Cleared int `json:"cleared"`
	}
	if err := s.post("queue_clear", map[string]any{
		"execution_id": b64(executionID),
		"attempt_id":   newAttemptID(),
	}, &resp); err != nil {
		return 0, err
	}
	return resp.Cleared, nil
}

// ---------------------------------------------------------------------------
// Aggregate state → ns=agg, key = int64(group_id)
// ---------------------------------------------------------------------------

// AggregateStateGet returns the aggregate state for each requested group ID,
// parallel to groupIDs (nil state for groups with none stored).
func (s *Storage) AggregateStateGet(executionID []byte, groupIDs []int64) ([]vgi.AggregateStateEntry, error) {
	if len(groupIDs) == 0 {
		return []vgi.AggregateStateEntry{}, nil
	}
	keys := make([][]byte, len(groupIDs))
	for i, gid := range groupIDs {
		keys[i] = int64Key(gid)
	}
	values, err := s.stateGetMany(executionID, nsAgg, keys)
	if err != nil {
		return nil, err
	}
	out := make([]vgi.AggregateStateEntry, len(groupIDs))
	for i, gid := range groupIDs {
		out[i] = vgi.AggregateStateEntry{GroupID: gid, State: values[i]}
	}
	return out, nil
}

// AggregateStatePut stores the aggregate state for each entry, keyed by group
// ID.
func (s *Storage) AggregateStatePut(executionID []byte, entries []vgi.AggregateStateEntry) error {
	if len(entries) == 0 {
		return nil
	}
	items := make([]kvPair, len(entries))
	for i, e := range entries {
		items[i] = kvPair{key: int64Key(e.GroupID), value: e.State}
	}
	return s.statePutMany(executionID, nsAgg, items)
}

// AggregateStateClear removes all aggregate state for the execution.
func (s *Storage) AggregateStateClear(executionID []byte) error {
	_, err := s.stateDelete(executionID, nsAgg, nil)
	return err
}

// Aggregate const args → ns=agg_const, key = function_name.

// AggregateConstArgsPut stores the constant arguments for an aggregate
// function, keyed by function name.
func (s *Storage) AggregateConstArgsPut(executionID []byte, functionName string, args []byte) error {
	return s.statePutMany(executionID, nsAggConst, []kvPair{{key: []byte(functionName), value: args}})
}

// AggregateConstArgsGet returns the constant arguments stored for an aggregate
// function, or nil if none are stored.
func (s *Storage) AggregateConstArgsGet(executionID []byte, functionName string) ([]byte, error) {
	values, err := s.stateGetMany(executionID, nsAggConst, [][]byte{[]byte(functionName)})
	if err != nil {
		return nil, err
	}
	return values[0], nil
}

// Aggregate window partition → ns=win, key = int64(partition_id).

// AggregateWindowPartitionPut stores window-partition data, keyed by partition
// ID.
func (s *Storage) AggregateWindowPartitionPut(executionID []byte, partitionID int64, data []byte) error {
	return s.statePutMany(executionID, nsWin, []kvPair{{key: int64Key(partitionID), value: data}})
}

// AggregateWindowPartitionGet returns the window-partition data for the given
// partition ID, or nil if none is stored.
func (s *Storage) AggregateWindowPartitionGet(executionID []byte, partitionID int64) ([]byte, error) {
	values, err := s.stateGetMany(executionID, nsWin, [][]byte{int64Key(partitionID)})
	if err != nil {
		return nil, err
	}
	return values[0], nil
}

// AggregateWindowPartitionDelete removes the window-partition data for the
// given partition ID.
func (s *Storage) AggregateWindowPartitionDelete(executionID []byte, partitionID int64) error {
	_, err := s.stateDelete(executionID, nsWin, [][]byte{int64Key(partitionID)})
	return err
}

// AggregateWindowPartitionClear removes all window-partition data for the
// execution.
func (s *Storage) AggregateWindowPartitionClear(executionID []byte) error {
	_, err := s.stateDelete(executionID, nsWin, nil)
	return err
}

// ---------------------------------------------------------------------------
// Transaction state → scope = transaction_opaque_data, ns=txn
// ---------------------------------------------------------------------------

// TransactionStateGet returns the transaction-state value for each requested
// key, with nil entries for keys that have no stored value.
func (s *Storage) TransactionStateGet(transactionOpaqueData []byte, keys [][]byte) ([][]byte, error) {
	return s.stateGetMany(transactionOpaqueData, nsTxn, keys)
}

// TransactionStatePut stores each key/value item under the transaction scope.
func (s *Storage) TransactionStatePut(transactionOpaqueData []byte, items []vgi.TransactionStateItem) error {
	if len(items) == 0 {
		return nil
	}
	pairs := make([]kvPair, len(items))
	for i, it := range items {
		pairs[i] = kvPair{key: it.Key, value: it.Value}
	}
	return s.statePutMany(transactionOpaqueData, nsTxn, pairs)
}

// TransactionStateClear removes all transaction state for the given scope.
func (s *Storage) TransactionStateClear(transactionOpaqueData []byte) error {
	// Only ns=txn lives under a transaction scope, so a scope-wide clear is
	// equivalent and matches vgi-python's TransactionBoundStorage.clear().
	return s.executionClear(transactionOpaqueData)
}

// ---------------------------------------------------------------------------
// State log (StateLogStorage) → ns=log; buffering functions stash batches here
// ---------------------------------------------------------------------------

// StateAppend appends a value to the (executionID, key) log and returns the new
// monotonic ordinal.
func (s *Storage) StateAppend(executionID, key, value []byte) (int64, error) {
	var resp struct {
		Ordinal int64 `json:"ordinal"`
	}
	if err := s.post("state_append", map[string]any{
		"scope_id":   b64(executionID),
		"ns":         b64(nsLog),
		"key":        b64(key),
		"item":       b64(value),
		"attempt_id": newAttemptID(),
	}, &resp); err != nil {
		return 0, err
	}
	return resp.Ordinal, nil
}

// StateLogScan returns log entries for (executionID, key) with id > afterID,
// ordered by id. limit <= 0 means no limit.
func (s *Storage) StateLogScan(executionID, key []byte, afterID int64, limit int) ([]vgi.StateLogEntry, error) {
	body := map[string]any{
		"scope_id": b64(executionID),
		"ns":       b64(nsLog),
		"key":      b64(key),
		"after_id": afterID,
	}
	if limit > 0 {
		body["limit"] = limit
	}
	var resp struct {
		Rows []struct {
			ID    int64  `json:"id"`
			Value string `json:"value"`
		} `json:"rows"`
	}
	if err := s.post("state_log_scan", body, &resp); err != nil {
		return nil, err
	}
	out := make([]vgi.StateLogEntry, 0, len(resp.Rows))
	for _, r := range resp.Rows {
		v, err := unb64(r.Value)
		if err != nil {
			return nil, fmt.Errorf("cfdo: state_log_scan decode value: %w", err)
		}
		out = append(out, vgi.StateLogEntry{ID: r.ID, Value: v})
	}
	return out, nil
}

// StateLogClear removes all state-log rows for the execution.
func (s *Storage) StateLogClear(executionID []byte) error {
	// No log-only clear endpoint; execution_clear wipes state + log for the
	// scope. StateLogClear is a teardown call (buffering destructor), so the
	// broader sweep is fine.
	return s.executionClear(executionID)
}

// Compile-time interface checks.
var (
	_ vgi.FunctionStorage = (*Storage)(nil)
	_ vgi.StateLogStorage = (*Storage)(nil)
	_ vgi.ShardedBackend  = (*Storage)(nil)
)
