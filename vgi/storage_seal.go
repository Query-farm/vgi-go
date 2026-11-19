// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// Sealed framework payload v1 -- the same format in every VGI SDK.
//
// The framework persists some of its own context to the shared FunctionStorage
// so that any worker process can serve any RPC of an execution: the buffered
// table function's init recipe (resolved bind-time secrets, the opened
// transaction scope, settings, arguments) and aggregate constant arguments.
// The backend may be a SQLite file, or a Durable Object reached over the
// network, so none of that may sit there in plaintext, and nothing read back
// may be trusted unauthenticated.
//
//	K    = HKDF-SHA256(ikm, salt = "", info = "vgi.storage.payload.v1", L = 32)
//	AEAD = AES-256-GCM, random 12-byte nonce, 16-byte tag
//	AAD  = "vgi.storage.payload.v1" || u32be(len) || shard_key
//	       || u32be(len) || scope_id || u32be(len) || namespace || u32be(len) || key
//	wire = 0x01 || nonce(12) || ciphertext || tag(16)
//
// ikm is the worker's signing key exactly as it resolved it (the VGI_SIGNING_KEY
// / WithHttpSigningKey bytes) on HTTP, the transport that seals; on the
// subprocess / unix transports, which have no signing key, it is the host-local
// key file every VGI worker process of the user shares
// (HostLocalAttachSecretKeyPath): one query's RPCs are spread over several
// worker processes that share one SQLite file, so a per-process key would not
// do.
//
// The AAD binds a payload to its slot: copied to another execution, attach
// shard, namespace or key, it fails to open. An unknown version, a wrong length
// or a failed authentication is an error (*StoragePayloadError) -- never a
// plaintext fallback. Executions are short-lived and their ids never reused, so
// payloads written unsealed by an older release are simply refused.
//
// Data user code writes (process() appends, aggregate group states, window
// partitions, finalize output, work-queue items) is the function's own data and
// is not sealed.

const storagePayloadVersion byte = 0x01

const (
	storagePayloadKeyLen   = 32
	storagePayloadNonceLen = 12
	storagePayloadTagLen   = 16
)

// storagePayloadInfo is the HKDF info and the AAD prefix.
var storagePayloadInfo = []byte("vgi.storage.payload.v1")

// StoragePayloadError is returned when a framework payload read from storage
// cannot be authenticated: unknown version, truncated, tampered, moved to
// another slot, sealed under another key, or an unsealed value an older release
// wrote.
type StoragePayloadError struct {
	What   string
	Reason string
}

// Error describes which payload failed and why.
func (e *StoragePayloadError) Error() string {
	return fmt.Sprintf("vgi: framework storage payload %s %s", e.What, e.Reason)
}

// storageSlot names where a sealed payload lives; it is the AAD.
type storageSlot struct {
	ShardKey string
	ScopeID  []byte // execution id (or transaction scope)
	NS       []byte
	Key      []byte
}

func (s storageSlot) describe() string {
	return fmt.Sprintf("%q for execution %s", s.NS, hex.EncodeToString(s.ScopeID))
}

func deriveStoragePayloadKey(ikm []byte) ([]byte, error) {
	k := make([]byte, storagePayloadKeyLen)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, nil, storagePayloadInfo), k); err != nil {
		return nil, err
	}
	return k, nil
}

func appendLP(dst, b []byte) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	return append(append(dst, n[:]...), b...)
}

// storagePayloadAAD is the associated data binding a payload to its slot.
func storagePayloadAAD(s storageSlot) []byte {
	out := make([]byte, 0, len(storagePayloadInfo)+16+len(s.ShardKey)+len(s.ScopeID)+len(s.NS)+len(s.Key))
	out = append(out, storagePayloadInfo...)
	out = appendLP(out, []byte(s.ShardKey))
	out = appendLP(out, s.ScopeID)
	out = appendLP(out, s.NS)
	out = appendLP(out, s.Key)
	return out
}

func storagePayloadAEAD(ikm []byte) (cipher.AEAD, error) {
	k, err := deriveStoragePayloadKey(ikm)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithNonceSize(block, storagePayloadNonceLen)
}

// sealStoragePayloadWith seals payload to slot under ikm. nonce nil draws a
// random one (a fixed nonce is for test vectors only).
func sealStoragePayloadWith(payload, ikm []byte, slot storageSlot, nonce []byte) ([]byte, error) {
	if nonce == nil {
		nonce = make([]byte, storagePayloadNonceLen)
		if _, err := rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("vgi: storage payload nonce: %w", err)
		}
	}
	if len(nonce) != storagePayloadNonceLen {
		return nil, errors.New("vgi: storage payload nonce must be 12 bytes")
	}
	aead, err := storagePayloadAEAD(ikm)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+storagePayloadNonceLen+len(payload)+storagePayloadTagLen)
	out = append(out, storagePayloadVersion)
	out = append(out, nonce...)
	return aead.Seal(out, nonce, payload, storagePayloadAAD(slot)), nil
}

// openStoragePayloadWith opens an envelope from sealStoragePayloadWith for the
// same slot and ikm.
func openStoragePayloadWith(blob, ikm []byte, slot storageSlot) ([]byte, error) {
	what := slot.describe()
	if len(blob) < 1+storagePayloadNonceLen+storagePayloadTagLen {
		return nil, &StoragePayloadError{What: what, Reason: "is too short to be a sealed payload"}
	}
	if blob[0] != storagePayloadVersion {
		return nil, &StoragePayloadError{What: what, Reason: fmt.Sprintf("has unknown seal version %#02x", blob[0])}
	}
	aead, err := storagePayloadAEAD(ikm)
	if err != nil {
		return nil, err
	}
	nonce := blob[1 : 1+storagePayloadNonceLen]
	plain, err := aead.Open(nil, nonce, blob[1+storagePayloadNonceLen:], storagePayloadAAD(slot))
	if err != nil {
		return nil, &StoragePayloadError{What: what, Reason: "failed authentication (tampered, moved, or sealed under another key)"}
	}
	return plain, nil
}

// storagePayloadIKM is the key material framework payloads are sealed under:
// the resolved signing key on the transport that seals (HTTP), else the
// host-local key shared by the user's VGI workers on this host.
func (w *Worker) storagePayloadIKM() ([]byte, error) {
	if w.sealOpaqueData && len(w.httpSigningKey) > 0 {
		return w.httpSigningKey, nil
	}
	path, err := HostLocalAttachSecretKeyPath()
	if err != nil {
		return nil, err
	}
	return loadHostLocalKey(path)
}

func (w *Worker) sealStoragePayload(payload []byte, slot storageSlot) ([]byte, error) {
	ikm, err := w.storagePayloadIKM()
	if err != nil {
		return nil, fmt.Errorf("vgi: no key to seal framework storage payloads: %w", err)
	}
	return sealStoragePayloadWith(payload, ikm, slot, nil)
}

func (w *Worker) openStoragePayload(blob []byte, slot storageSlot) ([]byte, error) {
	ikm, err := w.storagePayloadIKM()
	if err != nil {
		return nil, &StoragePayloadError{What: slot.describe(), Reason: "cannot be opened: " + err.Error()}
	}
	return openStoragePayloadWith(blob, ikm, slot)
}

// Framework slots.
var (
	// storageNSBufferingInit / bufferingRecipeKey: the buffered table
	// function's init recipe, in the execution's state log.
	storageNSBufferingInit = []byte("_vgi/buffering_init")
	// storageNSAggregateConst: an aggregate's bind-time constant arguments;
	// the key is the function name.
	storageNSAggregateConst = []byte("_vgi/aggregate_const")
)
