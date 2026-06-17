// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package cfdo_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"sync"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storage/cfdo"
	"github.com/Query-farm/vgi-go/vgi/storagetest"
)

// testShardKey is a representative att-<hex uuid> routing key; the conformance
// runs all ops under one shard (isolation is still exercised via distinct
// scope_ids / execution_ids within it).
const testShardKey = "att-0123456789abcdef0123456789abcdef"

// TestCfDoStorage_Conformance plugs the cfdo client into the shared
// FunctionStorage conformance suite, with a mock httptest.Server standing in
// for the Cloudflare Worker. The mock implements the DO's unified
// state_* / queue_* JSON+base64 protocol and REQUIRES a valid shard_key on
// every request plus a 32-hex attempt_id on every destructive op — so a
// passing run proves the client both speaks the protocol and shards correctly.
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
		// Pin a shard, as the worker does from the unwrapped attach UUID.
		return s.ForShard(testShardKey)
	})
}

// ---------------------------------------------------------------------------
// Mock CF DO Worker — unified state_* / queue_* protocol
//
// State is keyed by (shard_key, scope_id, ns, key); the append-log by
// (shard_key, scope_id, ns, key) with a monotonic ordinal. Every handler
// validates shard_key; destructive handlers additionally validate attempt_id.
// ---------------------------------------------------------------------------

var (
	shardKeyRe  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	attemptIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

type logRow struct {
	id    int64
	value []byte
}

type mockServer struct {
	mu sync.Mutex

	kv       map[string][]byte   // "shard\x1fscope\x1fns\x1fkey" → value
	log      map[string][]logRow // "shard\x1fscope\x1fns\x1fkey" → append-log
	logSeq   int64
	queues   map[string][][]byte // "shard\x1fexecID" → FIFO
	counters map[string]int64    // "shard\x1fscope\x1fns\x1fkey" → int64
}

func newMockServer(t *testing.T) *httptest.Server {
	m := &mockServer{
		kv:       map[string][]byte{},
		log:      map[string][]logRow{},
		queues:   map[string][][]byte{},
		counters: map[string]int64{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/state_put_many", m.statePutMany)
	mux.HandleFunc("/state_get_many", m.stateGetMany)
	mux.HandleFunc("/state_scan", m.stateScan)
	mux.HandleFunc("/state_drain", m.stateDrain)
	mux.HandleFunc("/state_delete", m.stateDelete)
	mux.HandleFunc("/state_append", m.stateAppend)
	mux.HandleFunc("/state_log_scan", m.stateLogScan)
	mux.HandleFunc("/state_counter_get", m.stateCounterGet)
	mux.HandleFunc("/state_counter_add", m.stateCounterAdd)
	mux.HandleFunc("/state_counter_set", m.stateCounterSet)
	mux.HandleFunc("/state_counter_delete", m.stateCounterDelete)
	mux.HandleFunc("/execution_clear", m.executionClear)
	mux.HandleFunc("/queue_push", m.queuePush)
	mux.HandleFunc("/queue_pop", m.queuePop)
	mux.HandleFunc("/queue_clear", m.queueClear)
	return httptest.NewServer(mux)
}

// bytesField decodes an optional base64 byte field; returns nil when absent.
func bytesField(body map[string]any, k string) []byte {
	s, _ := body[k].(string)
	if s == "" {
		return nil
	}
	return b64dec(s)
}

func b64dec(s string) []byte {
	b, _ := base64.StdEncoding.DecodeString(s)
	return b
}

func b64enc(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func (m *mockServer) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (m *mockServer) bad(w http.ResponseWriter, msg string) {
	m.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad_request", "message": msg})
}

// reqFields decodes the body and enforces the shard_key contract (and, when
// needsAttempt, the attempt_id contract). Returns false if a 400 was written.
func (m *mockServer) reqFields(w http.ResponseWriter, r *http.Request, out *map[string]any, needsAttempt bool) bool {
	body := map[string]any{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		m.bad(w, "invalid JSON")
		return false
	}
	sk, _ := body["shard_key"].(string)
	if !shardKeyRe.MatchString(sk) {
		m.bad(w, "shard_key is required (1-128 chars, [A-Za-z0-9._-])")
		return false
	}
	if needsAttempt {
		aid, _ := body["attempt_id"].(string)
		if !attemptIDRe.MatchString(aid) {
			m.bad(w, "attempt_id is required (32-char lowercase hex)")
			return false
		}
	}
	*out = body
	return true
}

func scopeKey(shard, scopeID, ns, key string) string {
	return shard + "\x1f" + scopeID + "\x1f" + ns + "\x1f" + key
}

func nsPrefix(shard, scopeID, ns string) string {
	return shard + "\x1f" + scopeID + "\x1f" + ns + "\x1f"
}

func strField(body map[string]any, k string) string {
	s, _ := body[k].(string)
	return s
}

// --- state_* ---

func (m *mockServer) statePutMany(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope, ns := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns")
	m.mu.Lock()
	defer m.mu.Unlock()
	items, _ := body["items"].([]any)
	for _, it := range items {
		im, _ := it.(map[string]any)
		key := strField(im, "key")
		m.kv[scopeKey(shard, scope, ns, key)] = b64dec(strField(im, "value"))
	}
	m.writeJSON(w, 200, map[string]any{"written": len(items)})
}

func (m *mockServer) stateGetMany(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, false) {
		return
	}
	shard, scope, ns := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns")
	keys, _ := body["keys"].([]any)
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := make([]any, len(keys))
	for i, k := range keys {
		ks, _ := k.(string)
		if v, ok := m.kv[scopeKey(shard, scope, ns, ks)]; ok {
			rows[i] = map[string]any{"value": b64enc(v)}
		} else {
			rows[i] = nil
		}
	}
	m.writeJSON(w, 200, map[string]any{"rows": rows})
}

func (m *mockServer) nsRows(shard, scope, ns string) []map[string]any {
	prefix := nsPrefix(shard, scope, ns)
	var keys []string // base64-encoded raw keys
	for ck := range m.kv {
		if len(ck) >= len(prefix) && ck[:len(prefix)] == prefix {
			keys = append(keys, ck[len(prefix):])
		}
	}
	// Order by the RAW key bytes (memcmp), mirroring the DO's BLOB ordering —
	// base64 lexical order does not preserve underlying byte order.
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(b64dec(keys[i]), b64dec(keys[j])) < 0 })
	rows := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, map[string]any{"key": k, "value": b64enc(m.kv[scopeKey(shard, scope, ns, k)])})
	}
	return rows
}

func (m *mockServer) stateScan(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, false) {
		return
	}
	shard, scope, ns := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns")
	start, end := bytesField(body, "start"), bytesField(body, "end")
	reverse, _ := body["reverse"].(bool)
	limit := 0
	if v, ok := body["limit"].(float64); ok {
		limit = int(v)
	}
	m.mu.Lock()
	rows := m.nsRows(shard, scope, ns) // ascending by raw key bytes
	m.mu.Unlock()
	// Apply the half-open [start, end) range on raw key bytes.
	filtered := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		k := b64dec(row["key"].(string))
		if start != nil && bytes.Compare(k, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(k, end) >= 0 {
			continue
		}
		filtered = append(filtered, row)
	}
	if reverse {
		for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
			filtered[i], filtered[j] = filtered[j], filtered[i]
		}
	}
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	m.writeJSON(w, 200, map[string]any{"rows": filtered}) // single page (no next_after)
}

func (m *mockServer) stateDrain(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope, ns := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns")
	m.mu.Lock()
	rows := m.nsRows(shard, scope, ns)
	prefix := nsPrefix(shard, scope, ns)
	for ck := range m.kv {
		if len(ck) >= len(prefix) && ck[:len(prefix)] == prefix {
			delete(m.kv, ck)
		}
	}
	m.mu.Unlock()
	m.writeJSON(w, 200, map[string]any{"rows": rows})
}

func (m *mockServer) stateDelete(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope, ns := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns")
	m.mu.Lock()
	defer m.mu.Unlock()
	deleted := 0
	if keys, ok := body["keys"].([]any); ok {
		for _, k := range keys {
			ks, _ := k.(string)
			ck := scopeKey(shard, scope, ns, ks)
			if _, ok := m.kv[ck]; ok {
				delete(m.kv, ck)
				deleted++
			}
		}
	} else {
		// Whole-namespace or half-open [start, end) range delete (raw key bytes).
		start, end := bytesField(body, "start"), bytesField(body, "end")
		prefix := nsPrefix(shard, scope, ns)
		for ck := range m.kv {
			if len(ck) >= len(prefix) && ck[:len(prefix)] == prefix {
				if start != nil || end != nil {
					k := b64dec(ck[len(prefix):])
					if start != nil && bytes.Compare(k, start) < 0 {
						continue
					}
					if end != nil && bytes.Compare(k, end) >= 0 {
						continue
					}
				}
				delete(m.kv, ck)
				deleted++
			}
		}
	}
	m.writeJSON(w, 200, map[string]any{"deleted": deleted})
}

func (m *mockServer) executionClear(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope := strField(body, "shard_key"), strField(body, "scope_id")
	prefix := shard + "\x1f" + scope + "\x1f"
	m.mu.Lock()
	defer m.mu.Unlock()
	deleted := 0
	for ck := range m.kv {
		if len(ck) >= len(prefix) && ck[:len(prefix)] == prefix {
			delete(m.kv, ck)
			deleted++
		}
	}
	for ck := range m.log {
		if len(ck) >= len(prefix) && ck[:len(prefix)] == prefix {
			delete(m.log, ck)
			deleted++
		}
	}
	for ck := range m.counters {
		if len(ck) >= len(prefix) && ck[:len(prefix)] == prefix {
			delete(m.counters, ck)
			deleted++
		}
	}
	m.writeJSON(w, 200, map[string]any{"deleted": deleted})
}

// --- state_counter_* ---

func (m *mockServer) stateCounterGet(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, false) {
		return
	}
	shard, scope, ns, key := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns"), strField(body, "key")
	m.mu.Lock()
	n := m.counters[scopeKey(shard, scope, ns, key)]
	m.mu.Unlock()
	m.writeJSON(w, 200, map[string]any{"n": n})
}

func (m *mockServer) stateCounterAdd(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope, ns, key := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns"), strField(body, "key")
	delta := int64(0)
	if v, ok := body["delta"].(float64); ok {
		delta = int64(v)
	}
	m.mu.Lock()
	ck := scopeKey(shard, scope, ns, key)
	m.counters[ck] += delta
	n := m.counters[ck]
	m.mu.Unlock()
	m.writeJSON(w, 200, map[string]any{"n": n})
}

func (m *mockServer) stateCounterSet(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope, ns, key := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns"), strField(body, "key")
	value := int64(0)
	if v, ok := body["value"].(float64); ok {
		value = int64(v)
	}
	m.mu.Lock()
	m.counters[scopeKey(shard, scope, ns, key)] = value
	m.mu.Unlock()
	m.writeJSON(w, 200, map[string]any{"n": value})
}

func (m *mockServer) stateCounterDelete(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope, ns, key := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns"), strField(body, "key")
	m.mu.Lock()
	delete(m.counters, scopeKey(shard, scope, ns, key))
	m.mu.Unlock()
	m.writeJSON(w, 200, map[string]any{})
}

func (m *mockServer) stateAppend(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	shard, scope, ns, key := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns"), strField(body, "key")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logSeq++
	ck := scopeKey(shard, scope, ns, key)
	m.log[ck] = append(m.log[ck], logRow{id: m.logSeq, value: b64dec(strField(body, "item"))})
	m.writeJSON(w, 200, map[string]any{"ordinal": m.logSeq})
}

func (m *mockServer) stateLogScan(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, false) {
		return
	}
	shard, scope, ns, key := strField(body, "shard_key"), strField(body, "scope_id"), strField(body, "ns"), strField(body, "key")
	afterID := int64(-1)
	if v, ok := body["after_id"].(float64); ok {
		afterID = int64(v)
	}
	limit := 0
	if v, ok := body["limit"].(float64); ok {
		limit = int(v)
	}
	m.mu.Lock()
	entries := m.log[scopeKey(shard, scope, ns, key)]
	m.mu.Unlock()
	rows := []map[string]any{}
	for _, e := range entries {
		if e.id <= afterID {
			continue
		}
		rows = append(rows, map[string]any{"id": e.id, "value": b64enc(e.value)})
		if limit > 0 && len(rows) >= limit {
			break
		}
	}
	m.writeJSON(w, 200, map[string]any{"rows": rows})
}

// --- queue_* ---

func (m *mockServer) queuePush(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	qk := strField(body, "shard_key") + "\x1f" + strField(body, "execution_id")
	items, _ := body["items"].([]any)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, it := range items {
		s, _ := it.(string)
		m.queues[qk] = append(m.queues[qk], b64dec(s))
	}
	m.writeJSON(w, 200, map[string]any{"count": len(items)})
}

func (m *mockServer) queuePop(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	qk := strField(body, "shard_key") + "\x1f" + strField(body, "execution_id")
	m.mu.Lock()
	defer m.mu.Unlock()
	// The real DO has no per-invocation registration: an empty/unregistered
	// queue pop returns {item:null}, never a 404 unknown_invocation.
	q := m.queues[qk]
	if len(q) == 0 {
		m.writeJSON(w, 200, map[string]any{"item": nil})
		return
	}
	item := q[0]
	m.queues[qk] = q[1:]
	m.writeJSON(w, 200, map[string]any{"item": b64enc(item)})
}

func (m *mockServer) queueClear(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !m.reqFields(w, r, &body, true) {
		return
	}
	qk := strField(body, "shard_key") + "\x1f" + strField(body, "execution_id")
	m.mu.Lock()
	defer m.mu.Unlock()
	cleared := len(m.queues[qk])
	delete(m.queues, qk)
	m.writeJSON(w, 200, map[string]any{"cleared": cleared})
}
