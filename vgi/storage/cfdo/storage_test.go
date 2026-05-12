// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package cfdo_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/cfdo"
	"github.com/Query-farm/vgi-go/vgi/storagetest"
)

// TestCfDoStorage_Conformance plugs the cfdo client into the shared
// FunctionStorage conformance suite, with a mock httptest.Server standing in
// for the Cloudflare Worker. The mock implements the same JSON+base64 wire
// format as the real DO Worker, so a passing run here is strong evidence
// that the client speaks the protocol correctly.
//
// Aggregate methods aren't supported on the CF DO backend (matches the
// Python implementation), so subtests that exercise them are skipped here
// by overriding the conformance factory to construct the storage and the
// runner to filter out aggregate subtests.
func TestCfDoStorage_Conformance(t *testing.T) {
	storagetest.RunConformanceFiltered(t, func(t *testing.T) vgi.FunctionStorage {
		srv := newMockServer(t)
		t.Cleanup(srv.Close)
		s, err := cfdo.NewStorage(cfdo.Options{
			URL:        srv.URL,
			HTTPClient: srv.Client(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}, storagetest.SkipAggregate, storagetest.SkipCleanup)
}

// ---------------------------------------------------------------------------
// Mock CF DO Worker
//
// In-memory implementation of the wire protocol the cfdo client speaks.
// Keeps state in a single struct guarded by a mutex; matches the semantics
// the real DO Worker provides.
// ---------------------------------------------------------------------------

type mockServer struct {
	mu sync.Mutex

	workerState     map[string]map[int64][]byte    // execID → workerID → state
	scanWorkerState map[string]map[string][]byte   // execID → streamID → state
	queues          map[string][][]byte            // execID → FIFO items
	registry        map[string]bool                // execID → registered
	txnState        map[string]map[string][]byte   // txnID → key → value
}

func newMockServer(t *testing.T) *httptest.Server {
	m := &mockServer{
		workerState:     map[string]map[int64][]byte{},
		scanWorkerState: map[string]map[string][]byte{},
		queues:          map[string][][]byte{},
		registry:        map[string]bool{},
		txnState:        map[string]map[string][]byte{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/worker_put", m.workerPut)
	mux.HandleFunc("/worker_collect", m.workerCollect)
	mux.HandleFunc("/worker_scan", m.workerScan)
	mux.HandleFunc("/scan_worker_put", m.scanWorkerPut)
	mux.HandleFunc("/scan_worker_scan", m.scanWorkerScan)
	mux.HandleFunc("/queue_push", m.queuePush)
	mux.HandleFunc("/queue_pop", m.queuePop)
	mux.HandleFunc("/queue_clear", m.queueClear)
	mux.HandleFunc("/transaction_state_get", m.txnGet)
	mux.HandleFunc("/transaction_state_put", m.txnPut)
	mux.HandleFunc("/transaction_state_clear", m.txnClear)
	return httptest.NewServer(mux)
}

func (m *mockServer) decode(r *http.Request, out any) error {
	return json.NewDecoder(r.Body).Decode(out)
}

func (m *mockServer) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func b64dec(s string) []byte {
	b, _ := base64.StdEncoding.DecodeString(s)
	return b
}

func b64enc(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// --- Worker state ---

func (m *mockServer) workerPut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"execution_id"`
		WorkerID    int64  `json:"worker_id"`
		State       string `json:"state"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket := m.workerState[req.ExecutionID]
	if bucket == nil {
		bucket = map[int64][]byte{}
		m.workerState[req.ExecutionID] = bucket
	}
	bucket[req.WorkerID] = b64dec(req.State)
	m.writeJSON(w, 200, map[string]any{})
}

func (m *mockServer) workerCollect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"execution_id"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	bucket := m.workerState[req.ExecutionID]
	delete(m.workerState, req.ExecutionID)
	m.mu.Unlock()
	var ids []int64
	for id := range bucket {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	states := make([]string, 0, len(ids))
	for _, id := range ids {
		states = append(states, b64enc(bucket[id]))
	}
	m.writeJSON(w, 200, map[string]any{"states": states})
}

func (m *mockServer) workerScan(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"execution_id"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	bucket := m.workerState[req.ExecutionID]
	m.mu.Unlock()
	rows := []map[string]any{}
	for id, state := range bucket {
		rows = append(rows, map[string]any{
			"worker_id": id,
			"state":     b64enc(state),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i]["worker_id"].(int64) < rows[j]["worker_id"].(int64)
	})
	m.writeJSON(w, 200, map[string]any{"rows": rows})
}

// --- Scan-worker state ---

func (m *mockServer) scanWorkerPut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"execution_id"`
		StreamID    string `json:"stream_id"`
		State       string `json:"state"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket := m.scanWorkerState[req.ExecutionID]
	if bucket == nil {
		bucket = map[string][]byte{}
		m.scanWorkerState[req.ExecutionID] = bucket
	}
	bucket[req.StreamID] = b64dec(req.State)
	m.writeJSON(w, 200, map[string]any{})
}

func (m *mockServer) scanWorkerScan(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"execution_id"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	bucket := m.scanWorkerState[req.ExecutionID]
	m.mu.Unlock()
	rows := []map[string]any{}
	for sid, state := range bucket {
		rows = append(rows, map[string]any{
			"stream_id": sid,
			"state":     b64enc(state),
		})
	}
	m.writeJSON(w, 200, map[string]any{"rows": rows})
}

// --- Work queue ---

func (m *mockServer) queuePush(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string   `json:"execution_id"`
		Items       []string `json:"items"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registry[req.ExecutionID] = true
	q := m.queues[req.ExecutionID]
	for _, item := range req.Items {
		q = append(q, b64dec(item))
	}
	m.queues[req.ExecutionID] = q
	m.writeJSON(w, 200, map[string]any{"count": len(req.Items)})
}

func (m *mockServer) queuePop(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"execution_id"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.registry[req.ExecutionID] {
		m.writeJSON(w, http.StatusNotFound, map[string]any{
			"error":   "unknown_invocation",
			"message": "Invocation is not registered.",
		})
		return
	}
	q := m.queues[req.ExecutionID]
	if len(q) == 0 {
		m.writeJSON(w, 200, map[string]any{"item": nil})
		return
	}
	item := q[0]
	m.queues[req.ExecutionID] = q[1:]
	m.writeJSON(w, 200, map[string]any{"item": b64enc(item)})
}

func (m *mockServer) queueClear(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"execution_id"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	defer m.mu.Unlock()
	cleared := len(m.queues[req.ExecutionID])
	delete(m.queues, req.ExecutionID)
	delete(m.registry, req.ExecutionID)
	m.writeJSON(w, 200, map[string]any{"cleared": cleared})
}

// --- Transaction state ---

func (m *mockServer) txnGet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TransactionID string   `json:"transaction_id"`
		Keys          []string `json:"keys"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	bucket := m.txnState[req.TransactionID]
	m.mu.Unlock()
	values := make([]*string, len(req.Keys))
	for i, k := range req.Keys {
		if v, ok := bucket[k]; ok {
			s := b64enc(v)
			values[i] = &s
		}
	}
	m.writeJSON(w, 200, map[string]any{"values": values})
}

func (m *mockServer) txnPut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TransactionID string `json:"transaction_id"`
		Items         []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"items"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket := m.txnState[req.TransactionID]
	if bucket == nil {
		bucket = map[string][]byte{}
		m.txnState[req.TransactionID] = bucket
	}
	for _, it := range req.Items {
		bucket[it.Key] = b64dec(it.Value)
	}
	m.writeJSON(w, 200, map[string]any{})
}

func (m *mockServer) txnClear(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TransactionID string `json:"transaction_id"`
	}
	_ = m.decode(r, &req)
	m.mu.Lock()
	delete(m.txnState, req.TransactionID)
	m.mu.Unlock()
	m.writeJSON(w, 200, map[string]any{})
}
