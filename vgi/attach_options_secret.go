// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"golang.org/x/crypto/hkdf"
)

// ---------------------------------------------------------------------------
// ATTACH options delivered by the framework, secret ones encrypted.
//
// catalog_attach records the ATTACH's options inside the attach value it
// mints, between the framework UUID and the catalog's own bytes:
//
//	uuid(16) || magic || u32 len || public IPC || u32 len || secret blob || catalog bytes
//
// Non-secret options travel as a one-row Arrow batch; options declared
// AttachOptionSpec.Secret travel only inside the secret blob, encrypted with a
// key the client never has (XChaCha20-Poly1305, AAD bound to the attach UUID,
// key derived with HKDF from the configured key, the HTTP signing key, or the
// host-local key file). The catalog's own bytes never contain them, so a
// catalog never has to put a credential into its attach bytes: functions and
// transaction hooks read every option through AttachOptions().
// ---------------------------------------------------------------------------

const (
	// attachOptionsMagic opens the framework's options section.
	attachOptionsMagic = "\x00vgi.attach_options.v1\x00"
	// attachSecretInfo is the HKDF info and the AAD prefix of the secret blob.
	attachSecretInfo = "vgi.attach_secret_options.v1"
	// attachSecretVersion identifies the blob's algorithm (XChaCha20-Poly1305).
	attachSecretVersion byte = 1
	// attachSecretKeyLen is the size of the host-local key file and of K.
	attachSecretKeyLen = 32
)

// AttachOptions are the options of the ATTACH a call runs under: for each
// option declared with WithAttachOptions / WithAttachOptionsForCatalog, the
// value the caller supplied, falling back to its default. Secret
// options (AttachOptionSpec.Secret) are included: the SDK carries them
// encrypted inside the attach value and decrypts them for the worker only.
//
// Read them with the typed getters, which report false when the option has no
// value. A nil *AttachOptions (a call with no attach) has no options.
//
// Never log the values of secret options, and never copy them into bind
// opaque data, function state or anything else a client sees.
type AttachOptions struct {
	values map[string]arrow.Array // one-element arrays, non-null
}

// Names lists the options that have a value, sorted.
func (o *AttachOptions) Names() []string {
	if o == nil {
		return nil
	}
	out := make([]string, 0, len(o.values))
	for n := range o.values {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Has reports whether the option has a (non-NULL) value.
func (o *AttachOptions) Has(name string) bool {
	_, ok := o.value(name)
	return ok
}

// Value returns the option's value as a one-element Arrow array (any type),
// or nil when it has none.
func (o *AttachOptions) Value(name string) arrow.Array {
	v, _ := o.value(name)
	return v
}

func (o *AttachOptions) value(name string) (arrow.Array, bool) {
	if o == nil {
		return nil, false
	}
	v, ok := o.values[name]
	return v, ok
}

// String returns a VARCHAR option.
func (o *AttachOptions) String(name string) (string, bool) {
	v, ok := o.value(name)
	if !ok {
		return "", false
	}
	switch a := v.(type) {
	case *array.String:
		return a.Value(0), true
	case *array.LargeString:
		return a.Value(0), true
	case *array.StringView:
		return a.Value(0), true
	}
	return "", false
}

// Bool returns a BOOLEAN option.
func (o *AttachOptions) Bool(name string) (bool, bool) {
	v, ok := o.value(name)
	if !ok {
		return false, false
	}
	if a, ok := v.(*array.Boolean); ok {
		return a.Value(0), true
	}
	return false, false
}

// Int64 returns an integer option of any width.
func (o *AttachOptions) Int64(name string) (int64, bool) {
	v, ok := o.value(name)
	if !ok {
		return 0, false
	}
	switch a := v.(type) {
	case *array.Int64:
		return a.Value(0), true
	case *array.Int32:
		return int64(a.Value(0)), true
	case *array.Int16:
		return int64(a.Value(0)), true
	case *array.Int8:
		return int64(a.Value(0)), true
	case *array.Uint32:
		return int64(a.Value(0)), true
	case *array.Uint16:
		return int64(a.Value(0)), true
	case *array.Uint8:
		return int64(a.Value(0)), true
	}
	return 0, false
}

// Float64 returns a floating-point (or integer) option.
func (o *AttachOptions) Float64(name string) (float64, bool) {
	v, ok := o.value(name)
	if !ok {
		return 0, false
	}
	switch a := v.(type) {
	case *array.Float64:
		return a.Value(0), true
	case *array.Float32:
		return float64(a.Value(0)), true
	}
	if n, ok := o.Int64(name); ok {
		return float64(n), true
	}
	return 0, false
}

// WithAttachSecretOptionsKey configures the root key secret ATTACH options are
// encrypted under (the subkey is derived from it with HKDF). Without it the
// SDK uses the HTTP signing key when the worker seals (HTTP), else the
// host-local key file shared by every VGI worker of the user on the host
// ($VGI_ATTACH_SECRET_KEY_FILE, $XDG_RUNTIME_DIR/vgi/attach-secret-options.key,
// or <temp dir>/vgi-<user>/attach-secret-options.key). Every worker process
// that serves the same ATTACH must use the same key.
func WithAttachSecretOptionsKey(key []byte) WorkerOption {
	return func(w *Worker) {
		w.attachSecretRoot = append([]byte(nil), key...)
	}
}

// ---------------------------------------------------------------- the section

// attachSecretAAD binds a secret blob to its attach UUID.
func attachSecretAAD(attachUUID []byte) []byte {
	out := make([]byte, 0, len(attachSecretInfo)+1+len(attachUUID))
	out = append(out, attachSecretInfo...)
	out = append(out, 0)
	return append(out, attachUUID...)
}

// splitAttachPlaintext splits an opened attach plaintext (uuid || [section] ||
// catalog bytes). section is nil for values minted without options. ok is
// false for a malformed section.
func splitAttachPlaintext(full []byte) (attachUUID, public, secret, catalog []byte, hasSection, ok bool) {
	if len(full) < attachUUIDLen {
		return nil, nil, nil, full, false, true
	}
	attachUUID, rest := full[:attachUUIDLen], full[attachUUIDLen:]
	if !bytes.HasPrefix(rest, []byte(attachOptionsMagic)) {
		return attachUUID, nil, nil, rest, false, true
	}
	rest = rest[len(attachOptionsMagic):]
	take := func() ([]byte, bool) {
		if len(rest) < 4 {
			return nil, false
		}
		n := binary.BigEndian.Uint32(rest)
		if uint64(len(rest)-4) < uint64(n) {
			return nil, false
		}
		part := rest[4 : 4+n]
		rest = rest[4+n:]
		return part, true
	}
	var ok1, ok2 bool
	if public, ok1 = take(); !ok1 {
		return nil, nil, nil, nil, true, false
	}
	if secret, ok2 = take(); !ok2 {
		return nil, nil, nil, nil, true, false
	}
	return attachUUID, public, secret, rest, true, true
}

// stripAttachPlaintext returns the catalog's own bytes of an opened attach
// plaintext (the UUID and any options section removed).
func stripAttachPlaintext(full []byte) []byte {
	_, _, _, catalog, _, ok := splitAttachPlaintext(full)
	if !ok {
		return nil
	}
	return catalog
}

// attachOptionsSection builds the options section for a fresh attach: each
// declared option's supplied value, else its default. Options that were not
// declared are not recorded (nothing says whether they are secret). Nil when
// there are no options.
func (w *Worker) attachOptionsSection(attachUUID []byte, optionsIPC []byte, specs []AttachOptionSpec) ([]byte, error) {
	supplied := map[string]arrow.Array{}
	if len(optionsIPC) > 0 {
		batch, err := DeserializeRecordBatch(optionsIPC)
		if err != nil {
			return nil, fmt.Errorf("decoding ATTACH options: %w", err)
		}
		defer batch.Release()
		if batch.NumRows() > 0 {
			for i, f := range batch.Schema().Fields() {
				col := batch.Column(i)
				if col.IsNull(0) {
					continue
				}
				supplied[f.Name] = array.NewSlice(col, 0, 1)
			}
		}
	}
	defer func() {
		for _, a := range supplied {
			a.Release()
		}
	}()
	var pubFields, secFields []arrow.Field
	var pubCols, secCols []arrow.Array
	add := func(name string, col arrow.Array, isSecret bool) {
		f := arrow.Field{Name: name, Type: col.DataType(), Nullable: true}
		if isSecret {
			secFields, secCols = append(secFields, f), append(secCols, col)
		} else {
			pubFields, pubCols = append(pubFields, f), append(pubCols, col)
		}
	}
	for _, spec := range specs {
		if col, ok := supplied[spec.Name]; ok {
			add(spec.Name, col, spec.Secret)
			continue
		}
		if spec.DefaultBatch != nil && spec.DefaultBatch.NumRows() > 0 && spec.DefaultBatch.NumCols() > 0 && !spec.DefaultBatch.Column(0).IsNull(0) {
			add(spec.Name, spec.DefaultBatch.Column(0), spec.Secret)
		}
	}
	if len(pubFields)+len(secFields) == 0 {
		return nil, nil
	}
	var public, blob []byte
	if len(pubFields) > 0 {
		b := array.NewRecordBatch(arrow.NewSchema(pubFields, nil), pubCols, 1)
		data, err := SerializeRecordBatch(b)
		b.Release()
		if err != nil {
			return nil, err
		}
		public = data
	}
	if len(secFields) > 0 {
		b := array.NewRecordBatch(arrow.NewSchema(secFields, nil), secCols, 1)
		plain, err := SerializeRecordBatch(b)
		b.Release()
		if err != nil {
			return nil, err
		}
		key, err := w.attachSecretKey()
		if err != nil {
			return nil, err
		}
		if blob, err = sealBytes(plain, key, attachSecretAAD(attachUUID), attachSecretVersion); err != nil {
			return nil, err
		}
	}
	out := append([]byte(nil), attachOptionsMagic...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(public)))
	out = append(out, public...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(blob)))
	return append(out, blob...), nil
}

// attachOptionsOf decodes the options of an opened attach plaintext. A blob
// that does not open (moved to another attach, tampered, other key) is the
// uniform attach_opaque_data rejection.
func (w *Worker) attachOptionsOf(full []byte) (*AttachOptions, error) {
	attachUUID, public, secret, _, hasSection, ok := splitAttachPlaintext(full)
	if !ok {
		return nil, attachRejected()
	}
	if !hasSection {
		return nil, nil
	}
	opts := &AttachOptions{values: map[string]arrow.Array{}}
	load := func(ipc []byte) error {
		b, err := DeserializeRecordBatch(ipc)
		if err != nil || b.NumRows() != 1 {
			return attachRejected()
		}
		for i, f := range b.Schema().Fields() {
			if !b.Column(i).IsNull(0) {
				col := b.Column(i)
				col.Retain()
				opts.values[f.Name] = col
			}
		}
		b.Release()
		return nil
	}
	if len(public) > 0 {
		if err := load(public); err != nil {
			return nil, err
		}
	}
	if len(secret) > 0 {
		key, err := w.attachSecretKey()
		if err != nil {
			return nil, attachRejected()
		}
		plain, err := openBytes(secret, key, attachSecretAAD(attachUUID), attachSecretVersion)
		if err != nil {
			return nil, attachRejected()
		}
		if err := load(plain); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

// openAttachOptions opens a wire attach value and decodes its options.
func (w *Worker) openAttachOptions(envelope []byte, cc *vgirpc.CallContext) (*AttachOptions, error) {
	if len(envelope) == 0 {
		return nil, nil
	}
	full, err := w.openAttachFull(envelope, cc)
	if err != nil {
		return nil, err
	}
	return w.attachOptionsOf(full)
}

// attachFrameworkPart is the uuid || options section of an opened attach
// plaintext (no catalog bytes), what a rehydrated turn needs to decode the
// options again; nil without a section.
func attachFrameworkPart(full []byte) []byte {
	_, _, _, catalog, hasSection, ok := splitAttachPlaintext(full)
	if !ok || !hasSection {
		return nil
	}
	return append([]byte(nil), full[:len(full)-len(catalog)]...)
}

// attachOptionsKey carries a catalog request's decoded options to its handler
// (the transaction hooks), next to the opened attach bytes.
type attachOptionsKey struct{}

func contextAttachOptions(cc *vgirpc.CallContext) *AttachOptions {
	if cc == nil || cc.Ctx == nil {
		return nil
	}
	o, _ := cc.Ctx.Value(attachOptionsKey{}).(*AttachOptions)
	return o
}

func withContextAttachOptions(cc *vgirpc.CallContext, o *AttachOptions) {
	if cc == nil || o == nil {
		return
	}
	base := cc.Ctx
	if base == nil {
		base = context.Background()
	}
	cc.Ctx = context.WithValue(base, attachOptionsKey{}, o)
}

// ---------------------------------------------------------------------- keys

// attachSecretKey is K = HKDF-SHA256(root, salt = empty,
// info = "vgi.attach_secret_options.v1", 32 bytes); the root is the
// configured key, else the HTTP signing key when the worker seals, else the
// host-local key file. The root is never used as the AEAD key itself.
func (w *Worker) attachSecretKey() ([]byte, error) {
	root := w.attachSecretRoot
	if len(root) == 0 && w.sealOpaqueData {
		root = w.httpSigningKey
	}
	if len(root) == 0 {
		path, err := HostLocalAttachSecretKeyPath()
		if err != nil {
			return nil, err
		}
		if root, err = loadHostLocalKey(path); err != nil {
			return nil, err
		}
	}
	return deriveAttachSecretKey(root)
}

func deriveAttachSecretKey(root []byte) ([]byte, error) {
	k := make([]byte, attachSecretKeyLen)
	if _, err := io.ReadFull(hkdf.New(sha256.New, root, nil, []byte(attachSecretInfo)), k); err != nil {
		return nil, err
	}
	return k, nil
}

// HostLocalAttachSecretKeyPath is where the host-local secret-options key
// lives (shared by the VGI workers of every SDK on the host):
// $VGI_ATTACH_SECRET_KEY_FILE, else $XDG_RUNTIME_DIR/vgi/attach-secret-options.key
// when XDG_RUNTIME_DIR is a directory, else
// <temp dir>/vgi-<user>/attach-secret-options.key.
func HostLocalAttachSecretKeyPath() (string, error) {
	if p := os.Getenv("VGI_ATTACH_SECRET_KEY_FILE"); p != "" {
		return p, nil
	}
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		if st, err := os.Stat(d); err == nil && st.IsDir() {
			return filepath.Join(d, "vgi", "attach-secret-options.key"), nil
		}
	}
	name := ""
	if u, err := user.Current(); err == nil {
		name = u.Username
	}
	if name == "" {
		name = os.Getenv("USER")
	}
	if name == "" {
		name = os.Getenv("USERNAME")
	}
	if name == "" {
		name = strconv.Itoa(os.Getuid())
	}
	return filepath.Join(os.TempDir(), "vgi-"+filepath.Base(name), "attach-secret-options.key"), nil
}

var (
	hostKeyMu    sync.Mutex
	hostKeyCache = map[string][]byte{}
)

// loadHostLocalKey reads the host-local key, creating it (32 random bytes,
// file 0600 in a 0700 directory, atomically: concurrent first starts agree)
// when it does not exist. An existing key is never overwritten.
func loadHostLocalKey(path string) ([]byte, error) {
	hostKeyMu.Lock()
	defer hostKeyMu.Unlock()
	if k, ok := hostKeyCache[path]; ok {
		return k, nil
	}
	hint := "set VGI_SIGNING_KEY or point VGI_ATTACH_SECRET_KEY_FILE at a 32-byte key file"
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("vgi: secret ATTACH options key directory %s: %w (%s)", dir, err, hint)
	}
	if err := checkPrivate(dir, true); err != nil {
		return nil, fmt.Errorf("vgi: secret ATTACH options key directory %s: %w (%s)", dir, err, hint)
	}
	key, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err = createHostLocalKey(dir, path); err != nil {
			return nil, fmt.Errorf("vgi: creating secret ATTACH options key %s: %w (%s)", path, err, hint)
		}
		key, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("vgi: reading secret ATTACH options key %s: %w (%s)", path, err, hint)
	}
	if err := checkPrivate(path, false); err != nil {
		return nil, fmt.Errorf("vgi: secret ATTACH options key %s: %w (%s)", path, err, hint)
	}
	if len(key) != attachSecretKeyLen {
		return nil, fmt.Errorf("vgi: secret ATTACH options key %s must hold exactly %d raw bytes, has %d (%s)", path, attachSecretKeyLen, len(key), hint)
	}
	hostKeyCache[path] = key
	return key, nil
}

func createHostLocalKey(dir, path string) error {
	key := make([]byte, attachSecretKeyLen)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".attach-secret-options.key.*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(key); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Link, never rename: an existing key (a concurrent first start won)
	// stays, and this one is discarded.
	if err := os.Link(tmpName, path); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return nil
}
