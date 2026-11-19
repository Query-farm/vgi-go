// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/storagetest"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// The externalizing wrapper is tested against vgi-rpc-python's fake blob
// vendor, the one the vgi extension's blob-vendor lane runs. It is started
// from VGI_RPC_FAKE_VENDOR_CMD (a command line, e.g. "uvx --from
// git+https://github.com/Query-farm/vgi-rpc-python@<ref> vgi-rpc-fake-vendor"),
// or VGI_RPC_FAKE_VENDOR_PYTHON (an interpreter with vgi-rpc installed), or
// reached at VGI_RPC_FAKE_VENDOR_URL when the caller runs one (token
// "test-token", --test-routes). Without any of them those tests skip.

const fakeVendorToken = "test-token"

var (
	fakeVendorOnce sync.Once
	fakeVendorURL  string
	fakeVendorErr  error
	fakeVendorCmd  *exec.Cmd
)

// TestMain stops the fake vendor this binary started, if any: it holds the
// test binary's stderr, and `go test` waits for that to close.
func TestMain(m *testing.M) {
	code := m.Run()
	if fakeVendorCmd != nil && fakeVendorCmd.Process != nil {
		stopGroup(fakeVendorCmd)
		_ = fakeVendorCmd.Wait()
	}
	os.Exit(code)
}

func startFakeVendor() (string, error) {
	if url := os.Getenv("VGI_RPC_FAKE_VENDOR_URL"); url != "" {
		return url, nil
	}
	var argv []string
	if cmd := strings.Fields(os.Getenv("VGI_RPC_FAKE_VENDOR_CMD")); len(cmd) > 0 {
		argv = cmd
	} else if py := os.Getenv("VGI_RPC_FAKE_VENDOR_PYTHON"); py != "" {
		argv = []string{py, "-m", "vgi_rpc.conformance.fake_vendor"}
	} else {
		return "", errors.New("set VGI_RPC_FAKE_VENDOR_CMD, VGI_RPC_FAKE_VENDOR_PYTHON or VGI_RPC_FAKE_VENDOR_URL")
	}
	argv = append(argv, "--test-routes", "--token", fakeVendorToken+"=tenant-a")
	cmd := exec.Command(argv[0], argv[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	// Not the test binary's stderr: under uvx the vendor is a grandchild that
	// outlives the kill in TestMain, and a held stderr makes go test wait on
	// it ("Test I/O incomplete").
	cmd.Stderr = nil
	startInGroup(cmd)
	if err := cmd.Start(); err != nil {
		return "", err
	}
	fakeVendorCmd = cmd
	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	deadline := time.After(3 * time.Minute)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return "", errors.New("fake vendor exited before printing its URL")
			}
			if rest, found := strings.CutPrefix(line, "URL:"); found {
				go func() {
					for range lines {
					}
				}()
				return rest, nil
			}
		case <-deadline:
			stopGroup(cmd)
			return "", errors.New("timed out waiting for the fake vendor")
		}
	}
}

func fakeVendor(t *testing.T) string {
	t.Helper()
	fakeVendorOnce.Do(func() { fakeVendorURL, fakeVendorErr = startFakeVendor() })
	if fakeVendorErr != nil {
		t.Skipf("fake blob vendor unavailable: %v", fakeVendorErr)
	}
	vendorGet(t, fakeVendorURL+"/__admin__/reset")
	return fakeVendorURL
}

func vendorGet(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

type vendorStats map[string]int64

func stats(t *testing.T, base string) vendorStats {
	t.Helper()
	out := vendorStats{}
	for k, v := range vendorGet(t, base+"/__stats__") {
		if f, ok := v.(float64); ok {
			out[k] = int64(f)
		}
	}
	return out
}

func (s vendorStats) delta(before vendorStats, key string) int64 { return s[key] - before[key] }

func newWrapped(t *testing.T, base string, threshold, maxValueBytes int) *vgi.ExternalizingStorage {
	t.Helper()
	back, err := vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	var blobs *vgirpc.VendorBlobClient
	if base != "" {
		blobs, err = vgirpc.NewVendorBlobClient(base, fakeVendorToken, vgirpc.BlobPurposeFS, vgirpc.VendorOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	s, err := vgi.NewExternalizingStorage(back, blobs, threshold, maxValueBytes)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestExternalizingStorage_ConformanceInline: with no vendor every value is
// an inline envelope, and the backend contract is unchanged.
func TestExternalizingStorage_ConformanceInline(t *testing.T) {
	storagetest.RunConformance(t, func(t *testing.T) vgi.FunctionStorage {
		return newWrapped(t, "", 4096, 0)
	})
}

// TestExternalizingStorage_ConformanceVendored: with threshold 1 every
// non-empty value is a vendor blob, and the backend contract is unchanged.
func TestExternalizingStorage_ConformanceVendored(t *testing.T) {
	base := fakeVendor(t)
	storagetest.RunConformance(t, func(t *testing.T) vgi.FunctionStorage {
		return newWrapped(t, base, 1, 0)
	})
}

func TestExternalizingStorage_ThresholdBoundary(t *testing.T) {
	base := fakeVendor(t)
	s := newWrapped(t, base, 4096, 0)
	defer s.Close()
	scope, ns := []byte("scope-1"), []byte("ns")
	values := map[string][]byte{
		"below": bytes.Repeat([]byte("a"), 4095),
		"at":    bytes.Repeat([]byte("b"), 4096),
		"above": bytes.Repeat([]byte("c"), 4097),
		// 2,048 two-byte runes: 4,096 UTF-8 bytes, so a blob.
		"utf8":  []byte(strings.Repeat("é", 2048)),
		"empty": {},
	}
	before := stats(t, base)
	for k, v := range values {
		if err := s.AttachStatePut(scope, ns, []byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
	mid := stats(t, base)
	if got := mid.delta(before, "puts"); got != 3 {
		t.Fatalf("puts delta = %d, want 3 (at, above, utf8)", got)
	}
	if got := mid.delta(before, "bytes_uploaded"); got != 4096+4097+4096 {
		t.Fatalf("bytes_uploaded delta = %d", got)
	}
	for k, v := range values {
		got, err := s.AttachStateGet(scope, ns, []byte(k))
		if err != nil || !bytes.Equal(got, v) || got == nil {
			t.Fatalf("%s: %v (len %d)", k, err, len(got))
		}
	}
	after := stats(t, base)
	if got := after.delta(mid, "gets"); got != 3 {
		t.Fatalf("gets delta = %d, want 3", got)
	}
	if got := after.delta(mid, "bytes_downloaded"); got != 4096+4097+4096 {
		t.Fatalf("bytes_downloaded delta = %d", got)
	}
	// The backend holds pointers (~73 bytes) for the blobs, envelopes inline.
	back := s.Base().(vgi.AttachStateStorage)
	raw, _ := back.AttachStateGet(scope, ns, []byte("at"))
	if raw[0] != 0x01 || len(raw) != 41+32 {
		t.Fatalf("stored pointer = %x", raw)
	}
	raw, _ = back.AttachStateGet(scope, ns, []byte("below"))
	if raw[0] != 0x00 || len(raw) != 4096 {
		t.Fatalf("stored inline = %d bytes", len(raw))
	}
}

func TestExternalizingStorage_ErrorContract(t *testing.T) {
	base := fakeVendor(t)
	s := newWrapped(t, base, 16, 0)
	defer s.Close()
	execID := []byte("exec-err")
	value := bytes.Repeat([]byte("z"), 100)
	for _, tc := range []struct {
		action string
		kind   vgi.StorageBlobErrorKind
		substr string
	}{
		{"corrupt", vgi.StorageBlobIntegrity, "failed its integrity check: sha256 mismatch"},
		{"missing", vgi.StorageBlobMissing, "is missing: not found"},
	} {
		key := []byte(tc.action)
		if _, err := s.StateAppend(execID, key, value); err != nil {
			t.Fatal(err)
		}
		vendorGet(t, fmt.Sprintf("%s/__admin__/fault?purpose=fs&action=%s&count=1", base, tc.action))
		_, err := s.StateLogScan(execID, key, -1, 0)
		var be *vgi.StorageBlobError
		if !errors.As(err, &be) || be.Kind != tc.kind || !strings.HasPrefix(err.Error(), "storage blob ") || !strings.Contains(err.Error(), tc.substr) {
			t.Fatalf("%s: %v", tc.action, err)
		}
		vendorGet(t, base+"/__admin__/reset")
	}
	// After a reset the corrupted blob reads back intact (the fault was in the
	// download, not the object).
	entries, err := s.StateLogScan(execID, []byte("corrupt"), -1, 0)
	if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].Value, value) {
		t.Fatalf("after reset: %v", err)
	}
}

func TestExternalizingStorage_CleanUp(t *testing.T) {
	base := fakeVendor(t)
	s := newWrapped(t, base, 16, 0)
	defer s.Close()
	execID := []byte("exec-clean")
	big := bytes.Repeat([]byte("q"), 64)
	if _, err := s.QueuePush(execID, [][]byte{big, big, []byte("small")}); err != nil {
		t.Fatal(err)
	}
	if err := s.WorkerPut(execID, []byte("w"), big); err != nil {
		t.Fatal(err)
	}
	objects := stats(t, base)["objects"]
	got, err := s.QueuePop(execID)
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("QueuePop: %v", err)
	}
	s.FlushBlobCleanup()
	if n := stats(t, base)["objects"]; n != objects-1 {
		t.Fatalf("objects after queue_pop = %d, want %d", n, objects-1)
	}
	if _, err := s.QueueClear(execID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecutionClear(execID); err != nil {
		t.Fatal(err)
	}
	s.FlushBlobCleanup()
	if n := stats(t, base)["objects"]; n != objects-3 {
		t.Fatalf("objects after clears = %d, want %d", n, objects-3)
	}
}

func TestExternalizingStorage_ShardScopes(t *testing.T) {
	base := fakeVendor(t)
	s := newWrapped(t, base, 16, 0)
	defer s.Close()
	view := s.ForShard("att-0011").(*vgi.ExternalizingStorage)
	execID := []byte("exec-shard")
	if err := view.WorkerPut(execID, []byte("w"), bytes.Repeat([]byte("s"), 64)); err != nil {
		t.Fatal(err)
	}
	objects := stats(t, base)["objects"]
	// Clearing the same execution under another shard leaves the blob.
	if _, err := s.ExecutionClear(execID); err != nil {
		t.Fatal(err)
	}
	s.FlushBlobCleanup()
	if n := stats(t, base)["objects"]; n != objects {
		t.Fatalf("an unsharded clear deleted the shard's blob (%d -> %d)", objects, n)
	}
	if _, err := view.ExecutionClear(execID); err != nil {
		t.Fatal(err)
	}
	view.FlushBlobCleanup()
	if n := stats(t, base)["objects"]; n != objects-1 {
		t.Fatalf("objects after the shard's clear = %d, want %d", n, objects-1)
	}
}

func TestExternalizingStorage_CapWithoutVendor(t *testing.T) {
	s := newWrapped(t, "", 1024, 1024)
	defer s.Close()
	if err := s.WorkerPut([]byte("e"), []byte("w"), make([]byte, 1023)); err != nil {
		t.Fatalf("value whose envelope fits: %v", err)
	}
	var tooLarge *vgi.StorageValueTooLargeError
	if err := s.WorkerPut([]byte("e"), []byte("w"), make([]byte, 1024)); !errors.As(err, &tooLarge) {
		t.Fatalf("over-cap value: %v", err)
	}
	if !strings.Contains(fmt.Sprint(tooLarge), vgirpc.EnvBlobVendorURL) {
		t.Fatalf("error does not name %s: %v", vgirpc.EnvBlobVendorURL, tooLarge)
	}
}

func TestWrapStorageFromEnv(t *testing.T) {
	back, err := vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer back.Close()
	t.Setenv(vgirpc.EnvBlobVendorURL, "")
	t.Setenv(vgi.StorageExternalizeEnv, "")
	if got, err := vgi.WrapStorageFromEnv(back, 0); err != nil || got != back {
		t.Fatalf("unconfigured: %T %v", got, err)
	}
	// A capped backend is always wrapped, vendor or not.
	got, err := vgi.WrapStorageFromEnv(back, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := got.(*vgi.ExternalizingStorage); !ok || w.Threshold() != vgi.DefaultStorageExternalizeBytes {
		t.Fatalf("capped backend: %T", got)
	}
	t.Setenv(vgi.StorageExternalizeEnv, "1")
	if _, err := vgi.WrapStorageFromEnv(back, 0); err == nil {
		t.Fatal("VGI_STORAGE_EXTERNALIZE=1 without a vendor accepted")
	}
	t.Setenv(vgirpc.EnvBlobVendorURL, "https://vendor.example")
	t.Setenv(vgirpc.EnvBlobVendorToken, "tok")
	t.Setenv(vgi.StorageExternalizeBytesEnv, "4096")
	got, err = vgi.WrapStorageFromEnv(back, 0)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := got.(*vgi.ExternalizingStorage); !ok || w.Threshold() != 4096 {
		t.Fatalf("opted-in backend: %T", got)
	}
	t.Setenv(vgi.StorageExternalizeBytesEnv, "-1")
	if _, err := vgi.WrapStorageFromEnv(back, 0); err == nil {
		t.Fatal("negative threshold accepted")
	}
}
