// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const testSecret = "s3cr3t-token-value"

func secretOptionsWorker(t *testing.T) *Worker {
	t.Helper()
	t.Setenv("VGI_ATTACH_SECRET_KEY_FILE", filepath.Join(t.TempDir(), "keys", "attach-secret-options.key"))
	return NewWorker(WithCatalogName("c"), WithAttachOptions(
		AttachOptionSpec{Name: "region", Type: arrow.BinaryTypes.String},
		AttachOptionSpec{Name: "token", Type: arrow.BinaryTypes.String, Secret: true},
	))
}

func optionsIPC(t *testing.T, values map[string]string) []byte {
	t.Helper()
	var fields []arrow.Field
	var cols []arrow.Array
	for _, name := range []string{"region", "token"} {
		v, ok := values[name]
		if !ok {
			continue
		}
		b := array.NewStringBuilder(memory.DefaultAllocator)
		b.Append(v)
		fields = append(fields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true})
		cols = append(cols, b.NewArray())
	}
	data, err := SerializeRecordBatch(array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func attachRaw(t *testing.T, w *Worker, opts []byte) []byte {
	t.Helper()
	res, err := workerService{Worker: w}.CatalogAttach(context.Background(), nil, CatalogAttachRequestWire{Name: "c", Options: &opts})
	if err != nil {
		t.Fatal(err)
	}
	return res.AttachOpaqueData
}

// Unsealed transport (stdio / unix): the attach value is plaintext on the
// wire, so the secret must be encrypted inside it.
func TestSecretOptionNeverPlaintextOnStdio(t *testing.T) {
	w := secretOptionsWorker(t)
	raw := attachRaw(t, w, optionsIPC(t, map[string]string{"region": "eu", "token": testSecret}))
	if bytes.Contains(raw, []byte(testSecret)) {
		t.Fatal("secret option in plaintext in attach_opaque_data")
	}
	catalog, err := w.openAttach(raw, nil)
	if err != nil || string(catalog) != "c" {
		t.Fatalf("catalog bytes %q %v: the options section leaked into them", catalog, err)
	}
	opts, err := w.attachOptionsOf(raw)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := opts.String("token"); !ok || v != testSecret {
		t.Fatalf("token %q %v", v, ok)
	}
	if v, ok := opts.String("region"); !ok || v != "eu" {
		t.Fatalf("region %q %v", v, ok)
	}
	if opts.Has("missing") || len(opts.Names()) != 2 {
		t.Fatalf("names %v", opts.Names())
	}
	var none *AttachOptions
	if _, ok := none.String("token"); ok || none.Names() != nil {
		t.Fatal("nil AttachOptions must have no options")
	}
}

// A secret blob moved into another attach value does not open.
func TestSecretBlobBoundToItsAttach(t *testing.T) {
	w := secretOptionsWorker(t)
	a := attachRaw(t, w, optionsIPC(t, map[string]string{"token": testSecret}))
	b := attachRaw(t, w, optionsIPC(t, map[string]string{"token": "other"}))
	moved := append(append([]byte(nil), b[:attachUUIDLen]...), a[attachUUIDLen:]...)
	if _, err := w.attachOptionsOf(moved); err == nil || !strings.Contains(err.Error(), "attach_opaque_data not recognized") {
		t.Fatalf("moved blob: %v", err)
	}
	tampered := append([]byte(nil), a...)
	tampered[len(tampered)-3] ^= 1 // inside the blob (catalog bytes are 1 byte)
	if _, err := w.attachOptionsOf(tampered); err == nil {
		t.Fatal("tampered blob opened")
	}
}

// The AEAD key is the HKDF subkey, never the root.
func TestSecretOptionsUseHKDFSubkey(t *testing.T) {
	root := bytes.Repeat([]byte{7}, 32)
	w := NewWorker(WithCatalogName("c"), WithAttachSecretOptionsKey(root),
		WithAttachOptions(AttachOptionSpec{Name: "token", Type: arrow.BinaryTypes.String, Secret: true}))
	k, err := w.attachSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(k, root) || len(k) != 32 {
		t.Fatal("the root key is used as the AEAD key")
	}
	raw := attachRaw(t, w, optionsIPC(t, map[string]string{"token": testSecret}))
	uuid, _, blob, _, _, _ := splitAttachPlaintext(raw)
	if _, err := openBytes(blob, k, attachSecretAAD(uuid), attachSecretVersion); err != nil {
		t.Fatalf("blob does not open with HKDF(root): %v", err)
	}
	if _, err := openBytes(blob, root, attachSecretAAD(uuid), attachSecretVersion); err == nil {
		t.Fatal("blob opens with the root key")
	}
}

// No key is needed (or read) when no secret option has a value; with a value
// and no usable key the ATTACH fails with a clear message.
func TestSecretOptionsKeyOnlyWhenNeeded(t *testing.T) {
	w := secretOptionsWorker(t)
	badDir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(badDir, 0o700); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(badDir, "bad.key")
	if err := os.WriteFile(bad, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VGI_ATTACH_SECRET_KEY_FILE", bad)
	raw := attachRaw(t, w, optionsIPC(t, map[string]string{"region": "eu"}))
	if opts, err := w.attachOptionsOf(raw); err != nil || opts.Has("token") {
		t.Fatalf("no secret: %v %v", opts, err)
	}
	opts := optionsIPC(t, map[string]string{"token": testSecret})
	_, err := workerService{Worker: w}.CatalogAttach(context.Background(), nil, CatalogAttachRequestWire{Name: "c", Options: &opts})
	if err == nil || !strings.Contains(err.Error(), bad) || !strings.Contains(err.Error(), "VGI_SIGNING_KEY") {
		t.Fatalf("unusable key: %v", err)
	}
}

func TestHostLocalKeyFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "vgi")
	path := filepath.Join(dir, "attach-secret-options.key")
	k1, err := loadHostLocalKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 || st.Size() != 32 {
		t.Fatalf("key file mode %v size %d", st.Mode().Perm(), st.Size())
	}
	if st, _ := os.Stat(dir); st.Mode().Perm() != 0o700 {
		t.Fatalf("key dir mode %v", st.Mode().Perm())
	}
	// Reused, not regenerated (also after the in-process cache is gone).
	hostKeyMu.Lock()
	delete(hostKeyCache, path)
	hostKeyMu.Unlock()
	k2, err := loadHostLocalKey(path)
	if err != nil || !bytes.Equal(k1, k2) {
		t.Fatalf("key not reused: %v", err)
	}

	// Concurrent first starts agree on one key.
	path2 := filepath.Join(t.TempDir(), "k", "attach-secret-options.key")
	if err := os.MkdirAll(filepath.Dir(path2), 0o700); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- createHostLocalKey(filepath.Dir(path2), path2)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(filepath.Dir(path2))
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %v", entries)
	}

	// Group/other-accessible files and directories are refused.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	hostKeyMu.Lock()
	delete(hostKeyCache, path)
	hostKeyMu.Unlock()
	if _, err := loadHostLocalKey(path); err == nil || !strings.Contains(err.Error(), "group/other") {
		t.Fatalf("0644 key accepted: %v", err)
	}
	open := filepath.Join(t.TempDir(), "open")
	if err := os.MkdirAll(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHostLocalKey(filepath.Join(open, "k.key")); err == nil {
		t.Fatal("0755 key directory accepted")
	}
}

func TestHostLocalKeyPath(t *testing.T) {
	t.Setenv("VGI_ATTACH_SECRET_KEY_FILE", "/x/y.key")
	if p, _ := HostLocalAttachSecretKeyPath(); p != "/x/y.key" {
		t.Fatal(p)
	}
	t.Setenv("VGI_ATTACH_SECRET_KEY_FILE", "")
	run := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", run)
	if p, _ := HostLocalAttachSecretKeyPath(); p != filepath.Join(run, "vgi", "attach-secret-options.key") {
		t.Fatal(p)
	}
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(run, "missing"))
	if p, _ := HostLocalAttachSecretKeyPath(); !strings.HasPrefix(p, filepath.Join(os.TempDir(), "vgi-")) ||
		filepath.Base(p) != "attach-secret-options.key" {
		t.Fatal(p)
	}
}

// Values minted before this release (uuid || catalog bytes) still open.
func TestAttachWithoutOptionsSectionUnchanged(t *testing.T) {
	w := NewWorker(WithCatalogName("c"))
	raw := attachRaw(t, w, nil)
	if len(raw) != attachUUIDLen+1 {
		t.Fatalf("attach without options grew: %d bytes", len(raw))
	}
	if opts, err := w.attachOptionsOf(raw); err != nil || opts != nil {
		t.Fatalf("%v %v", opts, err)
	}
}
