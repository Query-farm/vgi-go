// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var testSlot = storageSlot{ShardKey: "att-0123", ScopeID: bytes.Repeat([]byte{1}, 16), NS: []byte("_vgi/buffering_init"), Key: []byte("k")}

func TestStoragePayloadRoundTrip(t *testing.T) {
	ikm := bytes.Repeat([]byte("k"), 43)
	sealed, err := sealStoragePayloadWith([]byte("secret context"), ikm, testSlot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("secret context")) {
		t.Fatal("plaintext visible in the envelope")
	}
	if sealed[0] != 0x01 || len(sealed) != 1+12+len("secret context")+16 {
		t.Fatalf("envelope shape: version %#x, %d bytes", sealed[0], len(sealed))
	}
	got, err := openStoragePayloadWith(sealed, ikm, testSlot)
	if err != nil || string(got) != "secret context" {
		t.Fatalf("open = %q, %v", got, err)
	}
}

func TestStoragePayloadRefusals(t *testing.T) {
	ikm := bytes.Repeat([]byte("k"), 43)
	sealed, err := sealStoragePayloadWith([]byte("payload"), ikm, testSlot, nil)
	if err != nil {
		t.Fatal(err)
	}
	var spe *StoragePayloadError
	expect := func(name string, blob, key []byte, slot storageSlot, reason string) {
		t.Helper()
		_, err := openStoragePayloadWith(blob, key, slot)
		if !errors.As(err, &spe) {
			t.Fatalf("%s: err = %v, want *StoragePayloadError", name, err)
		}
		if !strings.Contains(err.Error(), reason) {
			t.Fatalf("%s: %v, want %q", name, err, reason)
		}
	}
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)-1] ^= 1
	expect("tampered", tampered, ikm, testSlot, "failed authentication")
	expect("wrong key", sealed, bytes.Repeat([]byte("z"), 43), testSlot, "failed authentication")
	for name, slot := range map[string]storageSlot{
		"other execution": {ShardKey: testSlot.ShardKey, ScopeID: bytes.Repeat([]byte{2}, 16), NS: testSlot.NS, Key: testSlot.Key},
		"other shard":     {ShardKey: "att-9999", ScopeID: testSlot.ScopeID, NS: testSlot.NS, Key: testSlot.Key},
		"other namespace": {ShardKey: testSlot.ShardKey, ScopeID: testSlot.ScopeID, NS: []byte("_vgi/aggregate_const"), Key: testSlot.Key},
		"other key":       {ShardKey: testSlot.ShardKey, ScopeID: testSlot.ScopeID, NS: testSlot.NS, Key: []byte("other")},
	} {
		expect(name, sealed, ikm, slot, "failed authentication")
	}
	expect("unknown version", append([]byte{0x02}, sealed[1:]...), ikm, testSlot, "unknown seal version")
	expect("truncated", sealed[:20], ikm, testSlot, "too short")
	expect("empty", nil, ikm, testSlot, "too short")
	// An unsealed value an older release wrote (a gob InitRecipe) is refused,
	// never decoded as plaintext.
	legacy, err := encodeInitRecipe(&InitRecipe{FunctionName: "f", ShardKey: "att-0123"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openStoragePayloadWith(legacy, ikm, testSlot); !errors.As(err, &spe) {
		t.Fatalf("legacy unsealed recipe: err = %v, want *StoragePayloadError", err)
	}
}

// Cross-SDK test vector for sealed framework payload v1 -- the same bytes
// vgi-python's tests/test_storage_seal.py publishes (Java/TS/Rust check them
// too). AES-256-GCM is deterministic for a fixed key and nonce.
func TestStoragePayloadCrossSDKVector(t *testing.T) {
	ikm := []byte("vgi-test-signing-key-0123456789abcdef")
	scope := make([]byte, 16)
	for i := range scope {
		scope[i] = byte(i)
	}
	nonce := make([]byte, 12)
	for i := range nonce {
		nonce[i] = byte(i)
	}
	slot := storageSlot{
		ShardKey: "att-00112233445566778899aabbccddeeff",
		ScopeID:  scope,
		NS:       []byte("_vgi/buffering_init"),
		Key:      int64Key(-1), // ff ff ff ff ff ff ff ff
	}
	const (
		wantKey = "f5f759ad2009e91cf4b963a20b524a800224d2ac4e01472396dc1a2dab78bea9"
		wantAAD = "7667692e73746f726167652e7061796c6f61642e7631000000246174742d30303131323233333434353536363737383839396161" +
			"6262636364646565666600000010000102030405060708090a0b0c0d0e0f000000135f7667692f627566666572696e675f696e6974" +
			"00000008ffffffffffffffff"
		wantWire = "01000102030405060708090a0bc79698647483a87eb533cf7332d1c527275cc7470dc6a3bdf1f72f624a97e246416d4de55281bd0eee" +
			"6d6491796a40"
		plaintext = "vgi sealed framework payload v1"
	)
	k, err := deriveStoragePayloadKey(ikm)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(k); got != wantKey {
		t.Fatalf("derived key %s, want %s", got, wantKey)
	}
	if got := hex.EncodeToString(storagePayloadAAD(slot)); got != wantAAD {
		t.Fatalf("aad %s, want %s", got, wantAAD)
	}
	wire, err := sealStoragePayloadWith([]byte(plaintext), ikm, slot, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(wire); got != wantWire {
		t.Fatalf("wire %s, want %s", got, wantWire)
	}
	want, _ := hex.DecodeString(wantWire)
	got, err := openStoragePayloadWith(want, ikm, slot)
	if err != nil || string(got) != plaintext {
		t.Fatalf("open vector = %q, %v", got, err)
	}
}

// The worker seals under its HTTP signing key when it seals opaque data, else
// under the host-local key; the two never cross.
func TestStoragePayloadKeySelection(t *testing.T) {
	t.Setenv("VGI_ATTACH_SECRET_KEY_FILE", filepath.Join(t.TempDir(), "key", "host.key"))
	local := &Worker{}
	http := &Worker{httpSigningKey: []byte("signing-key-for-http"), sealOpaqueData: true}
	sealed, err := local.sealStoragePayload([]byte("p"), testSlot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.openStoragePayload(sealed, testSlot); err != nil {
		t.Fatalf("host-local round trip: %v", err)
	}
	if _, err := http.openStoragePayload(sealed, testSlot); err == nil {
		t.Fatal("a host-local seal opened under the signing key")
	}
	sealed, err = http.sealStoragePayload([]byte("p"), testSlot)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := openStoragePayloadWith(sealed, []byte("signing-key-for-http"), testSlot); err != nil || string(got) != "p" {
		t.Fatalf("signing-key seal does not open under the raw signing key bytes: %v", err)
	}
}

// --- buffered function with a secret ----------------------------------------

const dummySecret = "dummy-secret-value-7f3a9c"

// secretBuffer records what each process call saw.
type secretBuffer struct {
	seen *[]ProcessParams
}

func (secretBuffer) Name() string               { return "secret_buffer" }
func (secretBuffer) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (secretBuffer) ArgumentSpecs() []ArgSpec   { return nil }
func (secretBuffer) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(emptyInputCounterSchema)
}
func (f secretBuffer) Process(ctx context.Context, p *ProcessParams, b arrow.RecordBatch) ([]byte, error) {
	*f.seen = append(*f.seen, *p)
	return []byte("s"), nil
}
func (secretBuffer) Combine(ctx context.Context, p *ProcessParams, ids [][]byte) ([][]byte, error) {
	return [][]byte{p.ExecutionID}, nil
}
func (secretBuffer) Finalize(ctx context.Context, p *ProcessParams, id []byte) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func secretsBatch(t *testing.T) []byte {
	t.Helper()
	st := arrow.StructOf(arrow.Field{Name: "token", Type: arrow.BinaryTypes.String})
	b := array.NewStructBuilder(memory.DefaultAllocator, st)
	defer b.Release()
	b.Append(true)
	b.FieldBuilder(0).(*array.StringBuilder).Append(dummySecret)
	col := b.NewArray()
	defer col.Release()
	batch := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "my_api", Type: st}}, nil), []arrow.Array{col}, 1)
	data, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type secretBufferRun struct {
	w      *Worker
	dbPath string
	seen   *[]ProcessParams
	exec   []byte
	attach *[]byte
}

func newSecretBufferRun(t *testing.T) *secretBufferRun {
	t.Helper()
	ctx := context.Background()
	t.Setenv("VGI_ATTACH_SECRET_KEY_FILE", filepath.Join(t.TempDir(), "key", "host.key"))
	dbPath := filepath.Join(t.TempDir(), "storage.db")
	back, err := NewSQLiteStorage(SQLiteStorageOptions{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = back.Close() })
	seen := &[]ProcessParams{}
	w := NewWorker()
	w.fsOnce.Do(func() { w.fs = back })
	w.RegisterTableBuffering(secretBuffer{seen: seen})
	attach := ccAttach(0xD4)
	inputSchema := shapeTestInputSchema(t)
	outSchema, _ := SerializeSchema(emptyInputCounterSchema)
	secrets := secretsBatch(t)
	phase := string(PhaseTableBuffering)
	sink, err := w.handleInit(ctx, shapeTestCallCtx(), InitRequestWire{
		BindCall: BindRequestWire{
			FunctionName: "secret_buffer", FunctionType: string(FunctionTypeTable),
			InputSchema: &inputSchema, AttachOpaqueData: attach, Secrets: &secrets,
		},
		OutputSchema: outSchema, Phase: &phase,
	})
	if err != nil {
		t.Fatalf("sink init: %v", err)
	}
	return &secretBufferRun{w: w, dbPath: dbPath, seen: seen, exec: sink.Header.(*GlobalInitResponseWire).ExecutionID, attach: attach}
}

func (r *secretBufferRun) process(t *testing.T, attach *[]byte) error {
	t.Helper()
	_, err := r.w.handleTableBufferingProcess(context.Background(), shapeTestCallCtx(), TableBufferingProcessRequestWire{
		FunctionName: "secret_buffer", ExecutionID: r.exec, AttachOpaqueData: attach,
		InputBatch: ccInt64Batch(t, []string{"x"}, []int64{1}),
	})
	return err
}

// The resolved secret never reaches the SQLite file, yet a cold process (no
// cache) still hands it to Process.
func TestBufferingInitSecretNotInStorageFile(t *testing.T) {
	r := newSecretBufferRun(t)
	r.w.bufferingParams = newBufferingCache() // cold: read the sealed recipe back
	if err := r.process(t, r.attach); err != nil {
		t.Fatalf("cold process: %v", err)
	}
	got := (*r.seen)[0].Secrets["my_api"]["token"]
	if got != dummySecret {
		t.Fatalf("Process saw secret %v, want the bind-time value", got)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		data, err := os.ReadFile(r.dbPath + suffix)
		if err != nil {
			continue
		}
		if bytes.Contains(data, []byte(dummySecret)) {
			t.Fatalf("resolved secret stored in plaintext in %s", filepath.Base(r.dbPath+suffix))
		}
	}
}

// A request presenting another attach (another shard) misses the cache and
// cannot open the execution's recipe.
func TestBufferingCacheIsolatedByShard(t *testing.T) {
	r := newSecretBufferRun(t)
	if err := r.process(t, r.attach); err != nil {
		t.Fatalf("own shard: %v", err)
	}
	err := r.process(t, ccAttach(0xEE))
	var spe *StoragePayloadError
	if !errors.As(err, &spe) {
		t.Fatalf("other shard: err = %v, want the recipe to fail authentication", err)
	}
	if len(*r.seen) != 1 {
		t.Fatalf("Process ran %d times, want only the own-shard call", len(*r.seen))
	}
	// And under another function name.
	_, err = r.w.handleTableBufferingProcess(context.Background(), shapeTestCallCtx(), TableBufferingProcessRequestWire{
		FunctionName: "cc_other", ExecutionID: r.exec, AttachOpaqueData: r.attach,
		InputBatch: ccInt64Batch(t, []string{"x"}, []int64{1}),
	})
	if err == nil || !strings.Contains(err.Error(), "belongs to") {
		t.Fatalf("other function name: err = %v", err)
	}
}

// Routing and attach scope come from the request even when the stored recipe
// (validly sealed) names others.
func TestBufferingStoredScopesIgnored(t *testing.T) {
	r := newSecretBufferRun(t)
	back, _ := r.w.functionStorage()
	shard := "att-" + hex.EncodeToString(bytes16(0xD4))
	entries, err := back.(StateLogStorage).StateLogScan(r.exec, bufferingRecipeKey, -1, 1)
	if err != nil || len(entries) != 1 {
		t.Fatalf("stored recipe: %v (%d)", err, len(entries))
	}
	plain, err := r.w.openStoragePayload(entries[0].Value, bufferingSlot(shard, r.exec))
	if err != nil {
		t.Fatal(err)
	}
	recipe, err := decodeInitRecipe(plain)
	if err != nil {
		t.Fatal(err)
	}
	if recipe.AttachScope != nil || recipe.AttachOptionsScope != nil {
		t.Fatal("the stored recipe carries the opened attach scope")
	}
	// Forge: another shard and attach scope in the recipe, stored under a new
	// execution and sealed for it (the strongest case: an authentic payload).
	exec2 := bytes.Repeat([]byte{0x42}, 16)
	recipe.ExecutionID = exec2
	recipe.ShardKey = "att-ffffffffffffffffffffffffffffffff"
	recipe.AttachScope = []byte("forged-scope")
	data, _ := encodeInitRecipe(recipe)
	sealed, err := r.w.sealStoragePayload(data, bufferingSlot(shard, exec2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := back.(StateLogStorage).StateAppend(exec2, bufferingRecipeKey, sealed); err != nil {
		t.Fatal(err)
	}
	r.exec = exec2
	if err := r.process(t, r.attach); err != nil {
		t.Fatalf("process: %v", err)
	}
	p := (*r.seen)[0]
	if string(p.AttachScope) != "cc-catalog" {
		t.Fatalf("Process saw attach scope %q, want the request's", p.AttachScope)
	}
	if p.Storage.shardKey != shard {
		t.Fatalf("storage routed to %q, want the request's shard %q", p.Storage.shardKey, shard)
	}
}

// The destructor evicts; the cache is bounded and expires.
func TestBufferingCacheEvictionAndBounds(t *testing.T) {
	r := newSecretBufferRun(t)
	if r.w.bufferingParams.Len() != 1 {
		t.Fatalf("sink init cached %d entries, want 1", r.w.bufferingParams.Len())
	}
	if _, err := r.w.handleTableBufferingDestructor(context.Background(), shapeTestCallCtx(), TableBufferingDestructorRequestWire{
		FunctionName: "secret_buffer", ExecutionID: r.exec, AttachOpaqueData: r.attach,
	}); err != nil {
		t.Fatal(err)
	}
	if r.w.bufferingParams.Len() != 0 {
		t.Fatal("destructor left the execution cached")
	}

	c := newBoundedCache[int, int](3, time.Minute)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	for i := range 5 {
		c.Put(i, i)
	}
	if c.Len() != 3 {
		t.Fatalf("cache holds %d, want its bound 3", c.Len())
	}
	if _, ok := c.Get(0); ok {
		t.Fatal("least recently used entry survived")
	}
	now = now.Add(2 * time.Minute)
	if _, ok := c.Get(4); ok {
		t.Fatal("expired entry served")
	}
	cost := newBoundedCache[int, []byte](10, time.Minute)
	cost.maxCost = 10
	cost.costOf = func(b []byte) int64 { return int64(len(b)) }
	cost.Put(1, make([]byte, 6))
	cost.Put(2, make([]byte, 6))
	if _, ok := cost.Get(1); ok || cost.Len() != 1 {
		t.Fatal("cost budget not enforced")
	}
}

// --- aggregates ---------------------------------------------------------------

// Constant arguments are sealed in storage and a cold process still reads them.
func TestAggregateConstArgsSealed(t *testing.T) {
	ctx := context.Background()
	w, counter := countingWorker(t)
	w.RegisterAggregate(ccScaledSum{})
	attach := ccAttach(0xA7)
	bind, err := w.handleAggregateBind(ctx, shapeTestCallCtx(), AggregateBindRequestWire{
		FunctionName: "cc_scaled_sum", Arguments: ccInt64Batch(t, []string{"scale"}, []int64{5}), AttachOpaqueData: attach,
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := counter.FunctionStorage.AggregateConstArgsGet(bind.ExecutionID, "cc_scaled_sum")
	if err != nil || len(stored) == 0 {
		t.Fatalf("no stored const args: %v", err)
	}
	if stored[0] != storagePayloadVersion {
		t.Fatal("const args stored unsealed")
	}
	w.aggConstArgs = newAggConstArgsCache() // cold process
	args, err := w.loadAggArgs(ccScaledSum{}, "cc_scaled_sum", bind.ExecutionID, "att-"+hex.EncodeToString(bytes16(0xA7)))
	if err != nil || args == nil || args.Positional[0].(*array.Int64).Value(0) != 5 {
		t.Fatalf("cold load: %v %v", args, err)
	}
	if _, err := w.loadAggArgs(ccScaledSum{}, "cc_scaled_sum", bind.ExecutionID, "att-other"); err == nil {
		t.Fatal("const args opened under another shard")
	}
}

type otherState struct{ X string }

// A stored state of the wrong type is a clean error, not a worker panic.
func TestAggregateWrongTypeStateIsAnError(t *testing.T) {
	ctx := context.Background()
	w, counter := countingWorker(t)
	w.RegisterAggregate(ccScaledSum{})
	gob.Register(&otherState{})
	attach := ccAttach(0xA8)
	bind, err := w.handleAggregateBind(ctx, shapeTestCallCtx(), AggregateBindRequestWire{
		FunctionName: "cc_scaled_sum", Arguments: ccInt64Batch(t, []string{"scale"}, []int64{1}), AttachOpaqueData: attach,
	})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := gobEncodeState(&otherState{X: "not a sum"})
	if err != nil {
		t.Fatal(err)
	}
	if err := counter.FunctionStorage.AggregateStatePut(bind.ExecutionID, []AggregateStateEntry{{GroupID: 0, State: bad}}); err != nil {
		t.Fatal(err)
	}
	_, err = w.handleAggregateUpdate(ctx, shapeTestCallCtx(), AggregateUpdateRequestWire{
		FunctionName: "cc_scaled_sum", ExecutionID: bind.ExecutionID, AttachOpaqueData: attach,
		InputBatch: ccInt64Batch(t, []string{GroupColumnName, "x"}, []int64{0}, []int64{1}),
	})
	var te *AggregateStateTypeError
	if !errors.As(err, &te) {
		t.Fatalf("update over a wrong-type state: err = %v, want *AggregateStateTypeError", err)
	}
	// Combine's direct assertion is recovered too.
	_, err = w.handleAggregateCombine(ctx, shapeTestCallCtx(), AggregateCombineRequestWire{
		FunctionName: "cc_scaled_sum", ExecutionID: bind.ExecutionID, AttachOpaqueData: attach,
		MergeBatch: ccInt64Batch(t, []string{"source_group_id", "target_group_id"}, []int64{0}, []int64{1}),
	})
	if err == nil {
		t.Fatal("combine over a wrong-type state succeeded")
	}
}

// The window destructor evicts the decoded partition.
func TestWindowPartitionCacheEvictedByDestructor(t *testing.T) {
	ctx := context.Background()
	w, _ := countingWorker(t)
	w.RegisterAggregate(ccScaledSum{})
	attach := ccAttach(0xA9)
	bind, err := w.handleAggregateBind(ctx, shapeTestCallCtx(), AggregateBindRequestWire{
		FunctionName: "cc_scaled_sum", Arguments: ccInt64Batch(t, []string{"scale"}, []int64{1}), AttachOpaqueData: attach,
	})
	if err != nil {
		t.Fatal(err)
	}
	outSchema, _ := SerializeSchema(ccOutSchema)
	if _, err := w.handleAggregateWindowInit(ctx, shapeTestCallCtx(), AggregateWindowInitRequestWire{
		FunctionName: "cc_scaled_sum", ExecutionID: bind.ExecutionID, PartitionID: 1, RowCount: 2, AttachOpaqueData: attach,
		PartitionBatch: ccInt64Batch(t, []string{"x"}, []int64{1, 2}), OutputSchema: outSchema,
	}); err != nil {
		t.Fatal(err)
	}
	if w.windowPartitions.Len() != 1 {
		t.Fatalf("window init cached %d partitions, want 1", w.windowPartitions.Len())
	}
	if _, err := w.handleAggregateWindowDestructor(ctx, shapeTestCallCtx(), AggregateWindowDestructorRequestWire{
		FunctionName: "cc_scaled_sum", ExecutionID: bind.ExecutionID, PartitionID: 1, AttachOpaqueData: attach,
	}); err != nil {
		t.Fatal(err)
	}
	if w.windowPartitions.Len() != 0 {
		t.Fatal("window destructor left the partition cached")
	}
}
