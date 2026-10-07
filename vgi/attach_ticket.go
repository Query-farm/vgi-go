// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

// Attach tickets: a user's ATTACH, sealed so a runner can replay it later as
// that user.
//
// A ticket is the *what* half of an unattended session; a sealed grant
// (vgi_rpc.Identity.v1 issue_grant) is the *who*. While the user is attached
// and logged in, a client asks the worker (vgi.attach_tickets.v1 seal_attach)
// to seal the options it attached with -- secret ones included -- into a ticket
// only this worker can open. Later a runner holding the user's grant attaches
// with the single option vgi_attach_ticket, and catalog_attach restores the
// sealed attach before any catalog code runs. The runner never sees an option.
//
// A ticket carries no authority: it opens only under the caller's principal,
// so without a grant (or login) for the same principal it attaches nothing.
//
// Normative spec: vgi-python docs/protocol/vgi-attach-tickets.md. Byte-exact
// vectors: testdata/attach_ticket_vectors.json (copied from vgi-python's
// vgi/_test_fixtures/attach_ticket_vectors.json).
//
//	ticket   = "vgia1." base64url_nopad( envelope )
//	envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(VGI_SIGNING_KEY, payload, aad)
//	aad      = "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)
//
// The AAD binds the principal only, not the (domain, principal) pair the
// attach envelope binds: a ticket is sealed while the user is logged in
// (domain "jwt", say) and opened when a runner presents their grant (domain
// "grant").

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

const (
	// AttachTicketPrefix opens every ticket. The format version is in the
	// prefix, so an incompatible format is a different prefix -- never
	// half-parsed.
	AttachTicketPrefix = "vgia1."

	// AttachTicketOption is the reserved ATTACH option a runner presents a
	// ticket in. No catalog may declare an attach option with this name,
	// compared case-insensitively.
	AttachTicketOption = "vgi_attach_ticket"

	// AttachTicketsProtocolName is the wire name of the protocol hosting
	// seal_attach.
	AttachTicketsProtocolName = "vgi.attach_tickets.v1"

	// AttachTicketsProtocolVersion is vgi.attach_tickets.v1's declared
	// version.
	AttachTicketsProtocolVersion = "1.0.0"

	// SigningKeyEnv names the environment variable holding the worker's
	// signing key: the key that seals attach_opaque_data envelopes and attach
	// tickets. Read by the HTTP transport when WithHttpSigningKey supplies
	// none.
	SigningKeyEnv = "VGI_SIGNING_KEY"

	// attachTicketEnvelopeVersion is fixed by the ticket format, independent
	// of the attach envelope's own version.
	attachTicketEnvelopeVersion byte = 0x01

	// attachTicketMaxOptionsBytes is the largest options record a ticket may
	// carry.
	attachTicketMaxOptionsBytes = 16 * 1024

	// attachTicketMaxChars is the longest ticket text considered at all.
	attachTicketMaxChars = 32 * 1024

	// attachTicketClockSkew allows for clocks disagreeing between the sealing
	// and the redeeming worker.
	attachTicketClockSkew = 60

	attachTicketMaxText = 0xFFFF
)

// attachTicketAADPrefix differs from the attach envelope's, so an attach
// envelope never opens as a ticket, nor the reverse.
var attachTicketAADPrefix = []byte("vgi.attach_ticket.v1\x00")

var (
	attachTicketIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	attachTicketB64URL    = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// AttachTicketInvalidError is a vgi_attach_ticket this worker cannot accept.
//
// One type for every cause -- malformed, wrong prefix, non-canonical, wrong
// key, wrong principal, tampered, bad payload -- so a caller cannot tell a
// forged ticket from another user's. INVALID_ARGUMENT / attach_ticket_invalid.
// Detail never contains the ticket.
type AttachTicketInvalidError struct{ Detail string }

func (e *AttachTicketInvalidError) Error() string { return e.Detail }

// ErrorType is the exception class name a client sees.
func (e *AttachTicketInvalidError) ErrorType() string { return "AttachTicketInvalidError" }

// ErrorCode is INVALID_ARGUMENT.
func (e *AttachTicketInvalidError) ErrorCode() vgirpc.Code { return vgirpc.CodeInvalidArgument }

// ErrorKind is attach_ticket_invalid.
func (e *AttachTicketInvalidError) ErrorKind() string { return "attach_ticket_invalid" }

// ErrorDetails names the option as a BadRequest field violation.
func (e *AttachTicketInvalidError) ErrorDetails() []vgirpc.ErrorDetail {
	return []vgirpc.ErrorDetail{vgirpc.BadRequest{FieldViolations: []vgirpc.FieldViolation{
		{Field: AttachTicketOption, Description: e.Detail},
	}}}
}

// AttachTicketExpiredError is an authentic ticket outside its lifetime.
// FAILED_PRECONDITION / attach_ticket_expired: the remedy is a fresh export
// from a logged-in session, not a retry. Only returned once the ticket has
// opened under the caller's principal.
type AttachTicketExpiredError struct{ Detail string }

func (e *AttachTicketExpiredError) Error() string { return e.Detail }

// ErrorType is the exception class name a client sees.
func (e *AttachTicketExpiredError) ErrorType() string { return "AttachTicketExpiredError" }

// ErrorCode is FAILED_PRECONDITION.
func (e *AttachTicketExpiredError) ErrorCode() vgirpc.Code { return vgirpc.CodeFailedPrecondition }

// ErrorKind is attach_ticket_expired.
func (e *AttachTicketExpiredError) ErrorKind() string { return "attach_ticket_expired" }

// ErrorDetails is a PreconditionFailure of type ATTACH_TICKET.
func (e *AttachTicketExpiredError) ErrorDetails() []vgirpc.ErrorDetail {
	return []vgirpc.ErrorDetail{vgirpc.PreconditionFailure{Violations: []vgirpc.PreconditionViolation{
		{Type: "ATTACH_TICKET", Subject: AttachTicketOption, Description: e.Detail},
	}}}
}

func ticketInvalid(detail string) error { return &AttachTicketInvalidError{Detail: detail} }

// ticketInvalidRequest is invalid_request / INVALID_ARGUMENT with one BadRequest
// violation per (field, description) pair.
func ticketInvalidRequest(message string, violations [][2]string) error {
	fv := make([]vgirpc.FieldViolation, 0, len(violations))
	for _, v := range violations {
		fv = append(fv, vgirpc.FieldViolation{Field: v[0], Description: v[1]})
	}
	return &vgirpc.StatusError{
		Code:    vgirpc.CodeInvalidArgument,
		Kind:    "invalid_request",
		Message: message,
		Details: []vgirpc.ErrorDetail{vgirpc.BadRequest{FieldViolations: fv}},
	}
}

// ticketActionDenied is action_denied / PERMISSION_DENIED naming seal_attach.
func ticketActionDenied(message string) error {
	return &vgirpc.StatusError{
		Code:    vgirpc.CodePermissionDenied,
		Kind:    "action_denied",
		Message: message,
		Details: []vgirpc.ErrorDetail{vgirpc.ErrorInfo{Metadata: map[string]string{"action": "seal_attach"}}},
	}
}

// ---------------------------------------------------------------------------
// Token format
// ---------------------------------------------------------------------------

// AttachTicketClaims is what a ticket carries.
type AttachTicketClaims struct {
	// IssuedAt and ExpiresAt are Unix seconds; ExpiresAt 0 means no expiry.
	IssuedAt  int64
	ExpiresAt int64
	// TicketID is 32 lowercase hex: a correlation handle, not a secret.
	TicketID string
	// CatalogName is the catalog the user attached.
	CatalogName string
	// DataVersionSpec and ImplementationVersion are "" when the user gave none.
	DataVersionSpec       string
	ImplementationVersion string
	// OptionsIPC is the Arrow IPC stream of the one-row options record,
	// exactly as CatalogAttachRequest.options carries it; empty for none.
	OptionsIPC []byte
}

// attachTicketAAD returns "vgi.attach_ticket.v1" 0x00 || UTF-8(principal).
func attachTicketAAD(principal string) []byte {
	return append(append([]byte{}, attachTicketAADPrefix...), principal...)
}

func encodeTicketPayload(c *AttachTicketClaims) ([]byte, error) {
	if len(c.OptionsIPC) > attachTicketMaxOptionsBytes {
		return nil, fmt.Errorf("options are %d bytes; a ticket carries at most %d",
			len(c.OptionsIPC), attachTicketMaxOptionsBytes)
	}
	var buf bytes.Buffer
	var word [8]byte
	binary.LittleEndian.PutUint64(word[:], uint64(c.IssuedAt))
	buf.Write(word[:])
	binary.LittleEndian.PutUint64(word[:], uint64(c.ExpiresAt))
	buf.Write(word[:])
	for _, f := range []struct{ name, value string }{
		{"ticket_id", c.TicketID},
		{"catalog_name", c.CatalogName},
		{"data_version_spec", c.DataVersionSpec},
		{"implementation_version", c.ImplementationVersion},
	} {
		if len(f.value) > attachTicketMaxText {
			return nil, fmt.Errorf("%s is longer than 65535 bytes", f.name)
		}
		var half [2]byte
		binary.LittleEndian.PutUint16(half[:], uint16(len(f.value)))
		buf.Write(half[:])
		buf.WriteString(f.value)
	}
	var quad [4]byte
	binary.LittleEndian.PutUint32(quad[:], uint32(len(c.OptionsIPC)))
	buf.Write(quad[:])
	buf.Write(c.OptionsIPC)
	return buf.Bytes(), nil
}

// decodeTicketPayload parses strictly: exact lengths, valid UTF-8, the field
// rules, no trailing bytes.
func decodeTicketPayload(payload []byte) (*AttachTicketClaims, error) {
	pos := 0
	take := func(n int) ([]byte, error) {
		if n < 0 || pos+n > len(payload) {
			return nil, ticketInvalid("attach ticket payload is truncated")
		}
		chunk := payload[pos : pos+n]
		pos += n
		return chunk, nil
	}
	text := func() (string, error) {
		raw, err := take(2)
		if err != nil {
			return "", err
		}
		body, err := take(int(binary.LittleEndian.Uint16(raw)))
		if err != nil {
			return "", err
		}
		if !utf8.Valid(body) {
			return "", ticketInvalid("attach ticket payload is not UTF-8")
		}
		return string(body), nil
	}
	head, err := take(16)
	if err != nil {
		return nil, err
	}
	c := &AttachTicketClaims{
		IssuedAt:  int64(binary.LittleEndian.Uint64(head[:8])),
		ExpiresAt: int64(binary.LittleEndian.Uint64(head[8:])),
	}
	for _, dst := range []*string{&c.TicketID, &c.CatalogName, &c.DataVersionSpec, &c.ImplementationVersion} {
		if *dst, err = text(); err != nil {
			return nil, err
		}
	}
	raw, err := take(4)
	if err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(raw)
	if n > attachTicketMaxOptionsBytes {
		return nil, ticketInvalid("attach ticket options exceed 16 KiB")
	}
	opts, err := take(int(n))
	if err != nil {
		return nil, err
	}
	c.OptionsIPC = bytes.Clone(opts)
	if pos != len(payload) {
		return nil, ticketInvalid("attach ticket payload has trailing bytes")
	}
	if !attachTicketIDPattern.MatchString(c.TicketID) {
		return nil, ticketInvalid("attach ticket id is not 32 lowercase hex")
	}
	if c.CatalogName == "" {
		return nil, ticketInvalid("attach ticket names no catalog")
	}
	if c.ExpiresAt != 0 && c.ExpiresAt <= c.IssuedAt {
		return nil, ticketInvalid("attach ticket lifetime is empty")
	}
	return c, nil
}

// decodeTicketBase64URL decodes unpadded base64url, rejecting every spelling
// but the canonical one.
func decodeTicketBase64URL(text string) ([]byte, error) {
	if !attachTicketB64URL.MatchString(text) || len(text)%4 == 1 {
		return nil, ticketInvalid("attach ticket is not unpadded base64url")
	}
	raw, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != text {
		return nil, ticketInvalid("attach ticket is not canonical base64url")
	}
	return raw, nil
}

// MintAttachTicket seals claims into a ticket for principal under signingKey
// (VGI_SIGNING_KEY bytes, normalized exactly as the attach envelope's key is).
// An empty TicketID draws a random one. Returns the ticket text and the claims
// it carries.
func MintAttachTicket(signingKey []byte, principal string, claims AttachTicketClaims) (string, AttachTicketClaims, error) {
	nonce := make([]byte, cryptoNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return "", claims, fmt.Errorf("attach ticket nonce: %w", err)
	}
	return mintAttachTicket(signingKey, principal, claims, nonce)
}

// mintAttachTicket is MintAttachTicket with a fixed nonce, for the vectors.
func mintAttachTicket(signingKey []byte, principal string, claims AttachTicketClaims, nonce []byte) (string, AttachTicketClaims, error) {
	if principal == "" {
		return "", claims, fmt.Errorf("a ticket needs a principal")
	}
	if claims.TicketID == "" {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return "", claims, fmt.Errorf("attach ticket id: %w", err)
		}
		claims.TicketID = hex.EncodeToString(id)
	}
	if claims.CatalogName == "" {
		return "", claims, fmt.Errorf("a ticket needs a catalog name")
	}
	if !attachTicketIDPattern.MatchString(claims.TicketID) {
		return "", claims, fmt.Errorf("ticket_id must be 32 lowercase hex")
	}
	if claims.ExpiresAt != 0 && claims.ExpiresAt <= claims.IssuedAt {
		return "", claims, fmt.Errorf("expires_at must be 0 or after issued_at")
	}
	payload, err := encodeTicketPayload(&claims)
	if err != nil {
		return "", claims, err
	}
	envelope, err := sealBytesWithNonce(payload, signingKey, attachTicketAAD(principal), attachTicketEnvelopeVersion, nonce)
	if err != nil {
		return "", claims, err
	}
	token := AttachTicketPrefix + base64.RawURLEncoding.EncodeToString(envelope)
	if len(token) > attachTicketMaxChars {
		return "", claims, fmt.Errorf("the ticket would be %d characters; at most %d are accepted",
			len(token), attachTicketMaxChars)
	}
	return token, claims, nil
}

// OpenAttachTicket verifies token for the calling principal and returns what
// it carries.
//
// Order, normative: prefix, length, canonical base64url, a non-anonymous
// caller, AEAD open under the caller's principal, strict payload parse, then
// lifetime with a 60 s skew. The lifetime is inside the ciphertext, so it is
// trusted only after the tag verified. Every failure but the lifetime is an
// *AttachTicketInvalidError; the lifetime is an *AttachTicketExpiredError.
func OpenAttachTicket(signingKey []byte, token, principal string, now time.Time) (*AttachTicketClaims, error) {
	if !strings.HasPrefix(token, AttachTicketPrefix) {
		return nil, ticketInvalid("not an attach ticket")
	}
	if len(token) > attachTicketMaxChars {
		return nil, ticketInvalid("attach ticket is too long")
	}
	envelope, err := decodeTicketBase64URL(token[len(AttachTicketPrefix):])
	if err != nil {
		return nil, err
	}
	if principal == "" {
		return nil, ticketInvalid("an anonymous caller cannot redeem an attach ticket")
	}
	payload, err := openBytes(envelope, signingKey, attachTicketAAD(principal), attachTicketEnvelopeVersion)
	if err != nil {
		return nil, ticketInvalid("attach ticket failed verification")
	}
	claims, err := decodeTicketPayload(payload)
	if err != nil {
		return nil, err
	}
	current := now.Unix()
	if claims.IssuedAt > current+attachTicketClockSkew {
		return nil, &AttachTicketExpiredError{Detail: "attach ticket is not yet valid"}
	}
	if claims.ExpiresAt != 0 && current >= claims.ExpiresAt+attachTicketClockSkew {
		return nil, &AttachTicketExpiredError{Detail: "attach ticket has expired"}
	}
	return claims, nil
}

// ---------------------------------------------------------------------------
// Redemption: what catalog_attach does with vgi_attach_ticket
// ---------------------------------------------------------------------------

// callerPrincipal is the authenticated principal, or "" for anonymous.
func callerPrincipal(auth *vgirpc.AuthContext) string {
	if auth == nil || !auth.Authenticated {
		return ""
	}
	return auth.Principal
}

// attachOptionsRow reads the options record's column names, in order, and
// its first row. A record with no rows has no options.
func attachOptionsRow(optionsIPC []byte) (names []string, row arrow.RecordBatch, err error) {
	if len(optionsIPC) == 0 {
		return nil, nil, nil
	}
	r, err := ipc.NewReader(bytes.NewReader(optionsIPC))
	if err != nil {
		return nil, nil, fmt.Errorf("reading attach options: %w", err)
	}
	defer r.Release()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return nil, nil, fmt.Errorf("reading attach options: %w", err)
		}
		return nil, nil, nil
	}
	batch := r.RecordBatch()
	if batch.NumRows() == 0 {
		return nil, nil, nil
	}
	batch.Retain()
	for _, f := range batch.Schema().Fields() {
		names = append(names, f.Name)
	}
	return names, batch, nil
}

// stringOption returns column i's first-row value when it is a non-null
// string.
func stringOption(batch arrow.RecordBatch, i int) (string, bool) {
	col := batch.Column(i)
	if col.IsNull(0) {
		return "", false
	}
	switch c := col.(type) {
	case *array.String:
		return c.Value(0), true
	case *array.LargeString:
		return c.Value(0), true
	case *array.StringView:
		return c.Value(0), true
	}
	return "", false
}

// redeemAttachTicket replaces a ticket-carrying catalog_attach request with
// the attach it seals. It returns (nil, nil) when the options carry no
// vgi_attach_ticket (the request is untouched); otherwise the request the user
// originally made -- the sealed catalog name, options and version specs, with
// this request's client_capabilities.
//
// Another option beside the ticket is invalid_request (the sealed options are
// authoritative, so there is nothing to merge), checked before the ticket is
// opened. signingKey nil (subprocess / unix transports) never redeems.
func redeemAttachTicket(req *CatalogAttachRequestWire, signingKey []byte, auth *vgirpc.AuthContext, now time.Time) (*CatalogAttachRequestWire, error) {
	if req.Options == nil {
		return nil, nil
	}
	names, batch, err := attachOptionsRow(*req.Options)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, nil
	}
	defer batch.Release()
	ticketCol := -1
	for i, name := range names {
		if strings.EqualFold(name, AttachTicketOption) {
			ticketCol = i
			break
		}
	}
	if ticketCol < 0 {
		return nil, nil
	}
	var violations [][2]string
	for i, name := range names {
		if i != ticketCol {
			violations = append(violations, [2]string{"options." + name, "not allowed alongside " + AttachTicketOption})
		}
	}
	if len(violations) > 0 {
		return nil, ticketInvalidRequest(AttachTicketOption+" must be the only attach option", violations)
	}
	token, ok := stringOption(batch, ticketCol)
	if !ok {
		return nil, ticketInvalid(AttachTicketOption + " must be a string")
	}
	if len(signingKey) == 0 {
		return nil, ticketInvalid("this worker does not redeem attach tickets")
	}
	claims, err := OpenAttachTicket(signingKey, token, callerPrincipal(auth), now)
	if err != nil {
		return nil, err
	}
	restored := &CatalogAttachRequestWire{
		Name:               claims.CatalogName,
		ClientCapabilities: req.ClientCapabilities,
	}
	if len(claims.OptionsIPC) > 0 {
		if _, err := DeserializeRecordBatch(claims.OptionsIPC); err != nil {
			return nil, ticketInvalid("attach ticket options are not an Arrow IPC record")
		}
		opts := claims.OptionsIPC
		restored.Options = &opts
	}
	if claims.DataVersionSpec != "" {
		v := claims.DataVersionSpec
		restored.DataVersionSpec = &v
	}
	if claims.ImplementationVersion != "" {
		v := claims.ImplementationVersion
		restored.ImplementationVersion = &v
	}
	return restored, nil
}

// redeemAttachTicketInPlace is catalog_attach's first step: req becomes the
// attach a vgi_attach_ticket seals, or is left alone. It runs before routing,
// so the sealed catalog name -- not the request's -- picks the catalog.
// signingKeyForTickets is nil off HTTP and on a worker whose key was not
// configured explicitly: neither ever redeems.
func (w *Worker) redeemAttachTicketInPlace(req *CatalogAttachRequestWire, cc *vgirpc.CallContext) error {
	var auth *vgirpc.AuthContext
	if cc != nil {
		auth = cc.Auth
	}
	restored, err := redeemAttachTicket(req, w.ticketSigningKey(), auth, time.Now())
	if err != nil || restored == nil {
		return err
	}
	*req = *restored
	return nil
}

// ticketSigningKey is the key tickets are sealed and opened with: the worker's
// signing key when it was configured explicitly, else nil.
func (w *Worker) ticketSigningKey() []byte {
	if !w.signingKeyConfigured || len(w.httpSigningKey) == 0 {
		return nil
	}
	return w.httpSigningKey
}

// checkReservedAttachOptions refuses an attach option named vgi_attach_ticket
// (any case) on this worker and every sub-catalog: the framework reads that
// option as a ticket before any catalog code runs.
func (w *Worker) checkReservedAttachOptions() error {
	check := func(catalog string, specs []AttachOptionSpec) error {
		for _, spec := range specs {
			if strings.EqualFold(spec.Name, AttachTicketOption) {
				return fmt.Errorf("catalog %q declares attach option %q, which uses the reserved name %q: "+
					"the framework reads it as an attach ticket before any catalog code runs. Rename the option",
					catalog, spec.Name, AttachTicketOption)
			}
		}
		return nil
	}
	if err := check(w.catalogName, w.attachOptions); err != nil {
		return err
	}
	for name, specs := range w.catalogAttachOptions {
		if err := check(name, specs); err != nil {
			return err
		}
	}
	for _, b := range w.routes {
		if sc, ok := b.(*subCatalog); ok {
			if err := sc.child.checkReservedAttachOptions(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// vgi.attach_tickets.v1
// ---------------------------------------------------------------------------

// SealAttachRequestWire is seal_attach's request.
type SealAttachRequestWire struct {
	// CatalogName is the catalog the caller attached.
	CatalogName string `vgirpc:"catalog_name"`
	// Options is the Arrow IPC stream of the one-row options record the caller
	// attached with, secret ones included, exactly as
	// CatalogAttachRequest.options; nil for none.
	Options *[]byte `vgirpc:"options"`
	// DataVersionSpec and ImplementationVersion are as given at ATTACH; ""
	// for none.
	DataVersionSpec       string `vgirpc:"data_version_spec"`
	ImplementationVersion string `vgirpc:"implementation_version"`
	// TTLSeconds is the requested lifetime; 0 asks for as long as the worker
	// allows. The worker caps it at its grant maximum.
	TTLSeconds int64 `vgirpc:"ttl_seconds"`
}

var sealAttachParamsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "request", Type: arrow.BinaryTypes.Binary},
}, nil)

// VgiRpcParamsSchema advertises the wrapped protocol shape: one binary
// "request" column holding the IPC-encoded request, as vgi-python sends it.
func (SealAttachRequestWire) VgiRpcParamsSchema() *arrow.Schema { return sealAttachParamsSchema }

// AttachTicketWire is seal_attach's result.
type AttachTicketWire struct {
	// Ticket is the vgia1. text. Not a credential, but never logged.
	Ticket string `vgirpc:"ticket"`
	// ExpiresAt is Unix seconds after which the worker refuses the ticket;
	// +Inf when the worker sets no maximum lifetime.
	ExpiresAt float64 `vgirpc:"expires_at"`
}

// attachTickets implements vgi.attach_tickets.v1 over a worker.
type attachTickets struct {
	worker *Worker
	// maxTTL is the lifetime ceiling in seconds; 0 means no maximum.
	maxTTL int64
	now    func() time.Time
}

// resolveTicketMaxTTL is the ticket lifetime ceiling: the grant keys'
// max_ttl_seconds when grant keys are configured, else
// VGI_RPC_GRANT_MAX_TTL_SECONDS when set, else no maximum (0).
func resolveTicketMaxTTL(grantKeys *vgirpc.GrantKeys) (int64, error) {
	if grantKeys != nil {
		return grantKeys.MaxTTLSeconds(), nil
	}
	raw := strings.TrimSpace(os.Getenv(vgirpc.GrantMaxTTLEnv))
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s=%q must be a positive integer", vgirpc.GrantMaxTTLEnv, raw)
	}
	return v, nil
}

// hostAttachTickets hosts vgi.attach_tickets.v1 on s when both halves of an
// unattended session can be issued: the signing key was configured explicitly
// (a per-process key would make every ticket die on restart) and the worker
// can issue grants (grant keys, or its own MintGrant) -- a ticket is useless
// without a grant. Otherwise it is absent, not hosted-and-refusing, so a client
// learns the answer from reflection. HTTP only: called from buildServer for
// that transport alone.
func (w *Worker) hostAttachTickets(s *vgirpc.Server, grantKeys *vgirpc.GrantKeys) error {
	if !w.signingKeyConfigured || len(w.httpSigningKey) == 0 {
		return nil
	}
	if w.identity != nil && w.identity.GrantKeys != nil {
		grantKeys = w.identity.GrantKeys
	}
	canGrant := grantKeys != nil || (w.identity != nil && w.identity.MintGrant != nil)
	if !canGrant {
		return nil
	}
	maxTTL, err := resolveTicketMaxTTL(grantKeys)
	if err != nil {
		return fmt.Errorf("attach tickets: %w", err)
	}
	impl := &attachTickets{worker: w, maxTTL: maxTTL, now: time.Now}
	p := vgirpc.NewProtocol(AttachTicketsProtocolName)
	p.SetProtocolVersion(AttachTicketsProtocolVersion)
	vgirpc.Unary(p, "seal_attach", impl.sealAttach)
	if err := s.AddProtocol(p); err != nil {
		return fmt.Errorf("attach tickets: %w", err)
	}
	return nil
}

// declaredAttachOptions returns the attach options catalogName declares, as
// catalog_catalogs advertises them, or false when the worker lists no such
// catalog.
func (w *Worker) declaredAttachOptions(catalogName string) ([]AttachOptionSpec, bool, error) {
	fromSerialized := func(raw [][]byte) ([]AttachOptionSpec, error) {
		out := make([]AttachOptionSpec, 0, len(raw))
		for _, b := range raw {
			spec, err := DeserializeAttachOptionSpec(b)
			if err != nil {
				return nil, err
			}
			out = append(out, spec)
		}
		return out, nil
	}
	primary := w.catalogName
	if w.catalogInfoOverride != nil && w.catalogInfoOverride.Name != "" {
		primary = w.catalogInfoOverride.Name
	}
	if catalogName == primary {
		if w.catalogInfoOverride != nil && w.catalogInfoOverride.AttachOptionSpecs != nil {
			specs, err := fromSerialized(w.catalogInfoOverride.AttachOptionSpecs)
			return specs, true, err
		}
		return w.attachOptions, true, nil
	}
	if info, ok := w.catalogAliasInfos[catalogName]; ok {
		if info.AttachOptionSpecs != nil {
			specs, err := fromSerialized(info.AttachOptionSpecs)
			return specs, true, err
		}
		return w.catalogAttachOptions[catalogName], true, nil
	}
	if b, ok := w.routes[catalogName]; ok {
		if sc, ok := b.(*subCatalog); ok {
			return sc.child.declaredAttachOptions(catalogName)
		}
		return nil, true, nil
	}
	return nil, false, nil
}

// sealAttach validates and seals the caller's attach into a ticket.
func (a *attachTickets) sealAttach(_ context.Context, cc *vgirpc.CallContext, req SealAttachRequestWire) (AttachTicketWire, error) {
	var auth *vgirpc.AuthContext
	if cc != nil {
		auth = cc.Auth
	}
	principal := callerPrincipal(auth)
	if principal == "" {
		return AttachTicketWire{}, ticketActionDenied("an anonymous caller cannot seal an attach ticket")
	}
	key := a.worker.ticketSigningKey()
	if key == nil {
		return AttachTicketWire{}, ticketActionDenied("this worker does not seal attach tickets")
	}

	var violations [][2]string
	if req.TTLSeconds < 0 {
		violations = append(violations, [2]string{"ttl_seconds", "must be 0 (as long as allowed) or positive"})
	}

	var optionsIPC []byte
	var names []string
	if req.Options != nil && len(*req.Options) > 0 {
		batch, err := DeserializeRecordBatch(*req.Options)
		if err != nil {
			violations = append(violations, [2]string{"options", "not an Arrow IPC record"})
		} else {
			switch {
			case batch.NumRows() > 1:
				violations = append(violations, [2]string{"options", "must be a one-row record"})
			case batch.NumRows() == 1:
				for _, f := range batch.Schema().Fields() {
					names = append(names, f.Name)
				}
				if len(names) > 0 {
					optionsIPC = *req.Options
				}
			}
			batch.Release()
		}
	}

	specs, known, err := a.worker.declaredAttachOptions(req.CatalogName)
	if err != nil {
		return AttachTicketWire{}, err
	}
	if !known {
		violations = append(violations, [2]string{"catalog_name", fmt.Sprintf("no catalog named %q", req.CatalogName)})
	} else {
		declared := make(map[string]struct{}, len(specs))
		for _, spec := range specs {
			declared[strings.ToLower(spec.Name)] = struct{}{}
		}
		supplied := make(map[string]struct{}, len(names))
		for _, name := range names {
			lower := strings.ToLower(name)
			supplied[lower] = struct{}{}
			if lower == AttachTicketOption {
				violations = append(violations, [2]string{"options." + name, "a ticket cannot seal another ticket"})
			} else if _, ok := declared[lower]; !ok {
				violations = append(violations, [2]string{"options." + name, "not an attach option this catalog declares"})
			}
		}
		for _, spec := range specs {
			if _, ok := supplied[strings.ToLower(spec.Name)]; spec.Required && !ok {
				violations = append(violations, [2]string{"options." + spec.Name, "required"})
			}
		}
	}
	if len(optionsIPC) > attachTicketMaxOptionsBytes {
		violations = append(violations, [2]string{"options",
			fmt.Sprintf("%d bytes; a ticket carries at most %d", len(optionsIPC), attachTicketMaxOptionsBytes)})
	}
	if len(violations) > 0 {
		return AttachTicketWire{}, ticketInvalidRequest("seal_attach request is invalid", violations)
	}

	issuedAt := a.now().Unix()
	// 0 asks for the ceiling; otherwise the request, capped at the ceiling.
	lifetime := a.maxTTL
	if req.TTLSeconds > 0 && (a.maxTTL == 0 || req.TTLSeconds < a.maxTTL) {
		lifetime = req.TTLSeconds
	}
	var expiresAt int64
	if lifetime > 0 {
		expiresAt = issuedAt + lifetime
	}
	token, _, err := MintAttachTicket(key, principal, AttachTicketClaims{
		IssuedAt:              issuedAt,
		ExpiresAt:             expiresAt,
		CatalogName:           req.CatalogName,
		DataVersionSpec:       req.DataVersionSpec,
		ImplementationVersion: req.ImplementationVersion,
		OptionsIPC:            optionsIPC,
	})
	if err != nil {
		return AttachTicketWire{}, ticketInvalidRequest("seal_attach request is invalid", [][2]string{{"request", err.Error()}})
	}
	out := AttachTicketWire{Ticket: token, ExpiresAt: math.Inf(1)}
	if expiresAt != 0 {
		out.ExpiresAt = float64(expiresAt)
	}
	return out, nil
}
