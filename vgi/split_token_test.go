// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The split-token envelope is the one part of the splits change where five
// independent implementations can silently diverge AND where diverging is a
// vulnerability. Behavioural tests miss that: each SDK is self-consistent, so a
// disagreement on anchor_len endianness or fingerprint truncation only surfaces
// when a token crosses SDKs. These fixtures are byte-level and shared — every
// SDK parses them and reproduces the deterministic ones byte-for-byte.

type splitFixtureManifest struct {
	FormatVersion  int    `json:"format_version"`
	KeyHex         string `json:"key_hex"`
	FingerprintHex string `json:"fingerprint_hex"`
	AnchorHex      string `json:"anchor_hex"`
	Payload        string `json:"payload"`
	Cases          []struct {
		Name         string `json:"name"`
		Verdict      string `json:"verdict"`
		Note         string `json:"note"`
		Reproducible bool   `json:"reproducible"`
		WorkerKeyed  bool   `json:"worker_keyed"`
	} `json:"cases"`
}

func loadSplitFixtures(t *testing.T) (splitFixtureManifest, string) {
	t.Helper()
	dir := filepath.Join("testdata", "split_tokens")
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("reading split-token manifest: %v", err)
	}
	var m splitFixtureManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parsing split-token manifest: %v", err)
	}
	return m, dir
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// TestSplitTokenSharedFixtures runs every shared vector through OpenSplitToken
// and asserts the verdict the manifest declares.
func TestSplitTokenSharedFixtures(t *testing.T) {
	m, dir := loadSplitFixtures(t)
	if byte(m.FormatVersion) != SplitTokenFormatVersion {
		t.Fatalf("fixture format_version %d != SDK %d", m.FormatVersion, SplitTokenFormatVersion)
	}
	key := mustHex(t, m.KeyHex)
	fingerprint := mustHex(t, m.FingerprintHex)
	anchor := mustHex(t, m.AnchorHex)

	for _, c := range m.Cases {
		t.Run(c.Name, func(t *testing.T) {
			token, err := os.ReadFile(filepath.Join(dir, c.Name+".bin"))
			if err != nil {
				t.Fatalf("reading fixture: %v", err)
			}

			// The manifest states the key state rather than each SDK inferring
			// it: the alg:none vector is a structurally VALID unsealed token
			// whose whole point is that a KEYED worker refuses it, so guessing
			// from the token would test the opposite of the rule.
			var workerKey []byte
			if c.WorkerKeyed {
				workerKey = key
			}

			payload, err := OpenSplitToken(token, workerKey, nil, fingerprint, anchor)

			if c.Verdict == "ok" {
				if err != nil {
					t.Fatalf("expected accept, got %v (%s)", err, c.Note)
				}
				if string(payload) != m.Payload {
					t.Fatalf("payload = %q, want %q", payload, m.Payload)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected %s, token was ACCEPTED (%s)", c.Verdict, c.Note)
			}
			var ste *SplitTokenError
			if !errors.As(err, &ste) {
				t.Fatalf("expected a typed SplitTokenError, got %T: %v", err, err)
			}
			if ste.Kind != c.Verdict {
				t.Fatalf("error kind = %s, want %s (%s)", ste.Kind, c.Verdict, c.Note)
			}
		})
	}
}

// TestSplitTokenReproducesFixtureBytes proves the STAMPING side agrees too — a
// parser can be permissive enough to accept another SDK's bytes while emitting
// bytes that SDK would reject. Only the deterministic cases apply: a sealed
// token carries a random AEAD nonce.
func TestSplitTokenReproducesFixtureBytes(t *testing.T) {
	m, dir := loadSplitFixtures(t)
	fingerprint := mustHex(t, m.FingerprintHex)
	anchor := mustHex(t, m.AnchorHex)

	for _, c := range m.Cases {
		if !c.Reproducible || c.Name != "valid_unsealed" {
			continue
		}
		want, err := os.ReadFile(filepath.Join(dir, c.Name+".bin"))
		if err != nil {
			t.Fatalf("reading fixture: %v", err)
		}
		got, err := BuildSplitToken([]byte(m.Payload), fingerprint, anchor, nil, nil)
		if err != nil {
			t.Fatalf("BuildSplitToken: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s bytes differ:\n got %x\nwant %x", c.Name, got, want)
		}
	}
}

// TestSplitTokenAlgNoneRefusal is the alg:none rule stated directly rather than
// via a fixture name, because it is the rule most likely to be "simplified"
// away by a later reader: flags is attacker-controlled plaintext, so a parser
// that trusts bit 0 lets any caller forge a split against a fully-keyed worker.
func TestSplitTokenAlgNoneRefusal(t *testing.T) {
	key := bytes.Repeat([]byte{0x2a}, 32)
	fingerprint := bytes.Repeat([]byte{0x07}, 16)
	anchor := []byte{47, 0, 0, 0, 0, 0, 0, 0}

	forged, err := BuildSplitToken([]byte("file=evil"), fingerprint, anchor, nil, nil)
	if err != nil {
		t.Fatalf("BuildSplitToken: %v", err)
	}
	if _, err := OpenSplitToken(forged, key, nil, fingerprint, nil); err == nil {
		t.Fatal("a keyed worker accepted an UNSEALED token: alg:none downgrade")
	}

	// And the converse: a sealed token cannot be opened without the key.
	sealed, err := BuildSplitToken([]byte("file=ok"), fingerprint, anchor, key, nil)
	if err != nil {
		t.Fatalf("BuildSplitToken (sealed): %v", err)
	}
	if _, err := OpenSplitToken(sealed, nil, nil, fingerprint, nil); err == nil {
		t.Fatal("a keyless worker claimed to open a SEALED token")
	}

	// Round-trip with the right key, and reject a wrong one (the seal, not the
	// plaintext header, is what makes forgery hard where a key exists).
	got, err := OpenSplitToken(sealed, key, nil, fingerprint, nil)
	if err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if string(got) != "file=ok" {
		t.Fatalf("payload = %q", got)
	}
	wrong := bytes.Repeat([]byte{0x2b}, 32)
	if _, err := OpenSplitToken(sealed, wrong, nil, fingerprint, nil); err == nil {
		t.Fatal("a token opened under the WRONG key")
	}
}

// TestSplitTokenPrincipalBinding: a token minted for one principal must not be
// replayable by another. Dropping this while keeping it on attach would be a
// regression, and a split token names data (files, offsets, tenant partitions).
func TestSplitTokenPrincipalBinding(t *testing.T) {
	key := bytes.Repeat([]byte{0x11}, 32)
	fingerprint := bytes.Repeat([]byte{0x05}, 16)
	anchor := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	alice := authCtx("test", "alice")
	bob := authCtx("test", "bob")

	token, err := BuildSplitToken([]byte("tenant=alice"), fingerprint, anchor, key, alice)
	if err != nil {
		t.Fatalf("BuildSplitToken: %v", err)
	}
	if _, err := OpenSplitToken(token, key, alice, fingerprint, nil); err != nil {
		t.Fatalf("alice could not redeem her own split: %v", err)
	}
	if _, err := OpenSplitToken(token, key, bob, fingerprint, nil); err == nil {
		t.Fatal("bob redeemed a split minted for alice")
	}
}

// TestSplitTokenErrorKindsAreDistinct: expiry and invalidity are different
// situations for a connector — only one of them means "re-run the query" —
// and keeping the anchor in the PLAINTEXT header is what makes the distinction
// expressible at all. Inside the AAD both collapse into one tag-check failure.
func TestSplitTokenErrorKindsAreDistinct(t *testing.T) {
	fingerprint := bytes.Repeat([]byte{0x09}, 16)
	old := []byte{47, 0, 0, 0, 0, 0, 0, 0}
	current := []byte{48, 0, 0, 0, 0, 0, 0, 0}

	token, err := BuildSplitToken([]byte("file=1"), fingerprint, old, nil, nil)
	if err != nil {
		t.Fatalf("BuildSplitToken: %v", err)
	}

	_, err = OpenSplitToken(token, nil, nil, fingerprint, current)
	var ste *SplitTokenError
	if !errors.As(err, &ste) || ste.Kind != ErrKindSplitSnapshotExpired {
		t.Fatalf("stale anchor: got %v, want %s", err, ErrKindSplitSnapshotExpired)
	}

	other := bytes.Repeat([]byte{0x0a}, 16)
	_, err = OpenSplitToken(token, nil, nil, other, old)
	if !errors.As(err, &ste) || ste.Kind != ErrKindSplitTokenInvalid {
		t.Fatalf("wrong bind: got %v, want %s", err, ErrKindSplitTokenInvalid)
	}
}
