// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Split-token envelope: the framework's wrapper around a worker's split payload.
//
// A split token *names* a unit of scan work so a distributed engine can
// re-request exactly the work it was handed. The worker supplies only the
// payload; everything around it is stamped here, so an author cannot forget the
// consistency anchor or mis-bind the fingerprint, and never writes crypto.
//
// Layout (little-endian, fixed prefix) — byte-identical across every SDK:
//
//	offset  size  field
//	0       1     format_version      currently 1
//	1       1     flags               bit0 = payload_sealed; bits 1-7 reserved, MUST be 0
//	2       2     anchor_len          u16 LE
//	4       16    bind_fingerprint    truncated SHA-256 of the bind identity
//	20      var   consistency_anchor  anchor_len bytes
//	20+n    var   payload             the worker's own bytes
//
// The header is plaintext on every transport; only the payload is sealed. That
// is not a preference — the signing key is absent on subprocess and unix, which
// is DuckDB's primary path, so a header readable only through AEAD would be
// unreadable exactly where DuckDB runs.

const (
	// SplitTokenFormatVersion is checked unconditionally, before anything else.
	SplitTokenFormatVersion byte = 1

	// splitFlagPayloadSealed is bit0: the payload is AEAD-sealed, not plaintext.
	splitFlagPayloadSealed byte = 0x01

	// splitReservedFlagsMask covers bits 1-7, which MUST be zero.
	splitReservedFlagsMask byte = 0xFE

	splitFingerprintLen = 16
	splitHeaderLen      = 4 + splitFingerprintLen // version|flags|anchor_len + fingerprint

	// splitSealVersion matches the Python default (crypto.seal_bytes version=1),
	// so a token sealed by one SDK opens in another.
	splitSealVersion byte = 1
)

var splitAADPrefix = []byte("vgi.split_token.v1\x00")

// SplitTokenError classifies a split-token failure. The kind matters to a
// connector: only SPLIT_SNAPSHOT_EXPIRED means "re-run the query", and neither
// kind is retriable in place.
type SplitTokenError struct {
	Kind    string
	Message string
}

// Error prefixes the stable KIND, because the kind is the part a caller acts
// on: only SPLIT_SNAPSHOT_EXPIRED means "re-run the query", and a connector
// several layers up sees the message string rather than this type. Dropping it
// made the three failures indistinguishable to everyone downstream — and made
// the cross-SDK suite, which asserts on the code, pass only against the SDKs
// that kept it.
func (e *SplitTokenError) Error() string { return "[" + e.Kind + "] " + e.Message }

// Error kinds, stable across SDKs.
const (
	ErrKindSplitTokenInvalid     = "SPLIT_TOKEN_INVALID"
	ErrKindSplitSnapshotExpired  = "SPLIT_SNAPSHOT_EXPIRED"
	ErrKindSplitTransactionEnded = "SPLIT_TRANSACTION_ENDED"
)

func splitInvalid(format string, args ...any) *SplitTokenError {
	return &SplitTokenError{Kind: ErrKindSplitTokenInvalid, Message: fmt.Sprintf(format, args...)}
}

func splitExpired(format string, args ...any) *SplitTokenError {
	return &SplitTokenError{Kind: ErrKindSplitSnapshotExpired, Message: fmt.Sprintf(format, args...)}
}

// BindFingerprint derives the 16-byte binding check for a bind call.
//
// Minted AND verified by the same worker, so it needs self-consistency only; it
// does not have to agree with any client, which is why the cross-SDK byte
// fixtures do not cover it. 16 bytes is a binding check, not a MAC — forgery
// resistance comes from the seal where a key exists, and from the uid trust
// boundary where one does not.
func BindFingerprint(schemaName, functionName string, argsRepr, settingsRepr, projectionRepr []byte) []byte {
	h := sha256.New()
	h.Write(splitAADPrefix)
	feed := func(label string, value []byte) {
		h.Write([]byte(label))
		h.Write([]byte{0})
		h.Write(value)
		h.Write([]byte{0})
	}
	feed("schema_name", []byte(schemaName))
	feed("function_name", []byte(functionName))
	feed("arguments", argsRepr)
	feed("settings", settingsRepr)
	feed("projection_ids", projectionRepr)
	return h.Sum(nil)[:splitFingerprintLen]
}

// splitTokenAAD is the AAD for a sealed split payload: the plaintext header plus
// the caller identity. The identity half is load-bearing, not incidental — it is
// what stops a token minted for one principal being replayed by another, exactly
// as the attach envelope does.
func splitTokenAAD(header []byte, auth *vgirpc.AuthContext) []byte {
	out := make([]byte, 0, len(header)+32)
	out = append(out, header...)
	return append(out, identityTail(auth)...)
}

// BuildSplitToken stamps (and, when a key exists, seals) a worker payload.
func BuildSplitToken(payload, fingerprint, anchor, signingKey []byte, auth *vgirpc.AuthContext) ([]byte, error) {
	if len(fingerprint) != splitFingerprintLen {
		return nil, fmt.Errorf("bind_fingerprint must be %d bytes, got %d", splitFingerprintLen, len(fingerprint))
	}
	if len(anchor) > 0xFFFF {
		return nil, fmt.Errorf("consistency_anchor too long: %d bytes exceeds u16", len(anchor))
	}

	var flags byte
	if signingKey != nil {
		flags = splitFlagPayloadSealed
	}
	body := make([]byte, 0, splitHeaderLen+len(anchor)+len(payload))
	body = append(body, SplitTokenFormatVersion, flags)
	body = binary.LittleEndian.AppendUint16(body, uint16(len(anchor)))
	body = append(body, fingerprint...)
	body = append(body, anchor...)

	if signingKey == nil {
		return append(body, payload...), nil
	}
	sealed, err := sealBytes(payload, signingKey, splitTokenAAD(body, auth), splitSealVersion)
	if err != nil {
		return nil, err
	}
	return append(body, sealed...), nil
}

// OpenSplitToken verifies a token and returns the worker's payload.
//
// expectedFingerprint and currentAnchor are optional (nil skips that check).
func OpenSplitToken(token, signingKey []byte, auth *vgirpc.AuthContext, expectedFingerprint, currentAnchor []byte) ([]byte, error) {
	if len(token) < splitHeaderLen {
		return nil, splitInvalid("split token too short: %d bytes, need at least %d", len(token), splitHeaderLen)
	}
	version := token[0]
	flags := token[1]
	anchorLen := int(binary.LittleEndian.Uint16(token[2:4]))

	if version != SplitTokenFormatVersion {
		return nil, splitInvalid("unsupported split-token format_version %d; this worker speaks %d",
			version, SplitTokenFormatVersion)
	}
	if flags&splitReservedFlagsMask != 0 {
		return nil, splitInvalid("split token sets reserved flag bits (flags=0x%02x)", flags)
	}
	sealed := flags&splitFlagPayloadSealed != 0

	// ---- The alg:none refusal. Load-bearing; do not relax. ----
	// flags is attacker-controlled plaintext, so it may say "not sealed" on a
	// token an attacker wrote by hand. A keyed worker that honoured that would
	// redeem forged work without ever opening an envelope. The KEY STATE decides,
	// never the token.
	if signingKey != nil && !sealed {
		return nil, splitInvalid("split token is unsealed but this worker holds a signing key; refusing. " +
			"An unsealed token cannot be authenticated, so accepting one here would let any caller " +
			"forge a split (alg:none).")
	}
	if signingKey == nil && sealed {
		return nil, splitInvalid("split token is sealed but this worker holds no signing key; cannot open it")
	}

	endOfAnchor := splitHeaderLen + anchorLen
	if len(token) < endOfAnchor {
		return nil, splitInvalid("split token truncated: anchor_len=%d exceeds token length %d", anchorLen, len(token))
	}

	fingerprint := token[4:splitHeaderLen]
	anchor := token[splitHeaderLen:endOfAnchor]
	body := token[:endOfAnchor]
	rest := token[endOfAnchor:]

	if expectedFingerprint != nil && subtle.ConstantTimeCompare(fingerprint, expectedFingerprint) != 1 {
		return nil, splitInvalid("split token was minted for a different bind (fingerprint mismatch)")
	}
	// Anchor check AFTER the bind check, and as its own kind: "read version N" is
	// a different situation from "this token is not yours".
	if currentAnchor != nil && subtle.ConstantTimeCompare(anchor, currentAnchor) != 1 {
		return nil, splitExpired("split snapshot expired; re-run the query")
	}

	if !sealed {
		return rest, nil
	}
	payload, err := openBytes(rest, signingKey, splitTokenAAD(body, auth), splitSealVersion)
	if err != nil {
		if errors.Is(err, errOpaqueDataRejected) {
			return nil, splitInvalid("split token failed authentication")
		}
		return nil, splitInvalid("split token failed authentication: %v", err)
	}
	return payload, nil
}

// --- Framework stamping / verification ------------------------------------
//
// A worker sets only the payload. Everything around it is done here, once,
// rather than in every worker: an author cannot forget the consistency anchor
// (a silent staleness bug), cannot mis-bind the fingerprint, and never writes
// crypto — and the envelope stays a private implementation detail whose layout
// can change without touching worker code in five languages.

// bindFingerprintFor derives the fingerprint from a wire bind request. The
// inputs mirror vgi-python's canonicalization field-for-field, but the bytes
// need only be self-consistent within one worker: the same worker that mints a
// token verifies it, and no client ever computes this.
func bindFingerprintFor(req *BindRequestWire) []byte {
	schema := ""
	if req.SchemaName != nil {
		schema = *req.SchemaName
	}
	var settingsRepr []byte
	if req.Settings != nil {
		settingsRepr = *req.Settings
	}
	// projection_ids is not a BindRequest field (it rides InitRequest), so it
	// feeds in empty — matching the reference implementation, which reads it off
	// the bind call and likewise finds nothing there.
	return BindFingerprint(schema, req.FunctionName, req.Arguments, settingsRepr, nil)
}

// splitAnchor encodes the consistency anchor. catalog_version is the counter
// that MOVES within an attach — resolved_data_version is fixed at attach and
// would say nothing about staleness.
func splitAnchor(catalogVersion int64) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(catalogVersion))
}

// stampSplitTokens wraps each split's payload in the envelope.
func (w *Worker) stampSplitTokens(splits []ScanSplit, req *PlanRequestWire, catalogVersion int64, auth *vgirpc.AuthContext) error {
	fingerprint := bindFingerprintFor(&req.BindCall)
	anchor := splitAnchor(catalogVersion)
	for i := range splits {
		token, err := BuildSplitToken(splits[i].Payload, fingerprint, anchor, w.splitSigningKey(), auth)
		if err != nil {
			return err
		}
		splits[i].Token = token
		// Clear the payload. It is sealed INTO the token, and shipping the
		// plaintext in the field beside the ciphertext made the seal decorative.
		// No client reads it — the C++ side pulls Token alone — and redemption
		// recovers the payload from inside the envelope.
		splits[i].Payload = nil
	}
	return nil
}

// openSplitTokens verifies every token this init carries and returns the
// worker's own payloads. Called before any user code runs, so an unverified
// token can never be acted on.
func (w *Worker) openSplitTokens(tokens [][]byte, bindReq *BindRequestWire, callCtx *vgirpc.CallContext) ([][]byte, error) {
	expected := bindFingerprintFor(bindReq)
	var auth *vgirpc.AuthContext
	if callCtx != nil {
		auth = callCtx.Auth
	}
	payloads := make([][]byte, 0, len(tokens))
	for _, token := range tokens {
		// The anchor is CHECKED, not skipped. Passing nil here meant a plan that
		// outlived its snapshot was redeemed happily, so SPLIT_SNAPSHOT_EXPIRED —
		// the one error whose purpose is telling an operator the query is
		// re-runnable — could never be raised by this SDK.
		payload, err := OpenSplitToken(token, w.splitSigningKey(), auth, expected, w.splitLiveAnchor())
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, payload)
	}
	return payloads, nil
}

// splitLiveAnchor is the consistency anchor a split token must still name: the
// catalog version this worker currently reports.
//
// Read from ONE place by both the mint and the verify side on purpose. Minting
// from a different value than redemption compares against is not a subtle bug:
// it refuses every token, and the documented response to SPLIT_SNAPSHOT_EXPIRED
// is "re-run the query", which re-plans, mints the same mismatch and fails
// again — a livelock returning no rows, blaming the data for moving when it has
// not.
func (w *Worker) splitLiveAnchor() []byte {
	return splitAnchor(w.splitLiveCatalogVersion())
}

// splitLiveCatalogVersion is the catalog version this worker reports, matching
// what the catalog_version RPC answers.
func (w *Worker) splitLiveCatalogVersion() int64 {
	if w.catalog != nil {
		return w.catalog.version
	}
	return 1
}

// splitSigningKey returns the key to seal with, or nil when this worker holds
// none. Nil is the DuckDB-primary case (subprocess and unix), where the trust
// boundary is the uid rather than a key — see the transport docs.
func (w *Worker) splitSigningKey() []byte {
	if len(w.httpSigningKey) == 0 {
		return nil
	}
	return w.httpSigningKey
}
