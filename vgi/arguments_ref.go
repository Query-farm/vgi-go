// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

// Argument references (protocol 3.4; vgi docs/large-values-3.4.md §2, §5, §5a).
//
// A client binding a function with a large argument over HTTP uploads the
// arguments once, through this worker's __upload_url__ grant, and sends a
// BindRequest whose `arguments` is null and whose `arguments_location` carries
// the grant's sealed ref plus the upload's size and SHA-256. Every request that
// embeds that bind call (cardinality, statistics, plan, projection,
// dynamic-to-string, init) carries the small location, never the value.
//
// The framework reads arguments only through resolveBindArguments, which
// resolves a location at the turn's entry -- where a CallContext exists --
// through vgi-rpc (open the sealed ref, check size and SHA-256, the process's
// object cache, single-flight fetch, verify the bytes). Everything the
// framework re-serializes (the init StateRef, table-buffering storage) holds
// the bind call bytes exactly as received, so the reference, never the value,
// goes back on the wire and into state.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

const (
	// ArgumentsRefBytesHeader is the capability header that tells the client
	// this worker accepts argument references, and above how many bytes of
	// arguments IPC to use one.
	ArgumentsRefBytesHeader = "VGI-Arguments-Ref-Bytes"
	// ArgumentsRefBytesEnv overrides the advertised value.
	ArgumentsRefBytesEnv = "VGI_ARGUMENTS_REF_BYTES"
	// DefaultArgumentsRefBytes is the advertised value without an override:
	// 4 MiB.
	DefaultArgumentsRefBytes int64 = 4 * 1024 * 1024
	// DisableArgumentsRefEnv is a test switch: the worker stops sending the
	// header and so behaves like a worker without argument references (the
	// extension's arguments_ref_fallback.test).
	DisableArgumentsRefEnv = "VGI_TEST_DISABLE_ARGUMENTS_REF"

	// ErrorKindInvalidBindRequest is the refusal kind for a BindRequest that
	// does not set exactly one of arguments and arguments_location.
	ErrorKindInvalidBindRequest = "invalid_bind_request"
	// InvalidBindRequestMessage is that refusal's exact message.
	InvalidBindRequestMessage = "BindRequest must set exactly one of 'arguments' and 'arguments_location'"
	// ArgumentsRefMissingMessage is the exact message for an argument object
	// storage reports gone (large-values spec §5a, "Prepared statements"):
	// argument objects live about a day, so a prepared statement's large
	// argument can expire, and there is no transparent rebind.
	ArgumentsRefMissingMessage = "this prepared statement's large argument has expired; prepare it again"

	maxArgumentsRefHeaderDigits = 19
)

// ArgumentsRefBytesFromEnv returns the VGI-Arguments-Ref-Bytes value to
// advertise: VGI_ARGUMENTS_REF_BYTES when set, else 4 MiB. A value that is not
// a positive decimal integer of at most 19 digits (no sign, no leading zero,
// the header's grammar) is an error, so a typo stops startup instead of
// silently advertising nothing. getenv defaults to os.Getenv.
func ArgumentsRefBytesFromEnv(getenv func(string) string) (int64, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	raw := strings.TrimSpace(getenv(ArgumentsRefBytesEnv))
	if raw == "" {
		return DefaultArgumentsRefBytes, nil
	}
	valid := len(raw) <= maxArgumentsRefHeaderDigits && raw[0] != '0'
	for _, c := range raw {
		if c < '0' || c > '9' {
			valid = false
		}
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if !valid || err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer number of bytes (no sign, no leading zero, at most %d digits), got %q",
			ArgumentsRefBytesEnv, maxArgumentsRefHeaderDigits, raw)
	}
	return v, nil
}

// argumentsRefAdvertised is the VGI-Arguments-Ref-Bytes value this server
// sends, or 0 for none. The header is the mandatory capability signal: sent
// whenever the server vends upload URLs (VGI-Upload-URL-Support) from a
// storage that can fetch the objects back by id (vgirpc.ObjectStore), so it
// can resolve what its grant mints, and never otherwise.
func argumentsRefAdvertised(config *vgirpc.ExternalLocationConfig, getenv func(string) string) (int64, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if config == nil || config.Storage == nil {
		return 0, nil
	}
	if _, ok := config.Storage.(vgirpc.UploadURLProvider); !ok {
		return 0, nil
	}
	if _, ok := config.Storage.(vgirpc.ObjectStore); !ok {
		LogRPC.Warn("argument references disabled: the configured storage cannot fetch objects by id (no ObjectStore)")
		return 0, nil
	}
	switch strings.ToLower(strings.TrimSpace(getenv(DisableArgumentsRefEnv))) {
	case "1", "true", "yes":
		return 0, nil
	}
	return ArgumentsRefBytesFromEnv(getenv)
}

// withArgumentsRefHeader wraps the HTTP handler so every response (capability
// discovery included) carries VGI-Arguments-Ref-Bytes alongside vgi-rpc's own
// capability headers. value <= 0 returns h unchanged.
func withArgumentsRefHeader(h http.Handler, value int64) http.Handler {
	if value <= 0 {
		return h
	}
	v := strconv.FormatInt(value, 10)
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set(ArgumentsRefBytesHeader, v)
		h.ServeHTTP(rw, r)
	})
}

// invalidBindRequest is the refusal for a bind call that sets both or neither
// of arguments and arguments_location.
func invalidBindRequest() error {
	return &vgirpc.RpcError{
		Type:    "InvalidBindRequestError",
		Message: InvalidBindRequestMessage,
		Kind:    ErrorKindInvalidBindRequest,
		Code:    string(vgirpc.CodeInvalidArgument),
	}
}

func invalidArgumentsRef(message string) error {
	return &vgirpc.RpcError{
		Type:    "InvalidArgumentsRefError",
		Message: vgirpc.ErrorKindInvalidArgumentsRef + ": " + message,
		Kind:    vgirpc.ErrorKindInvalidArgumentsRef,
		Code:    string(vgirpc.CodeInvalidArgument),
	}
}

// validateBindArguments requires exactly one of arguments and
// arguments_location (large-values spec §2). A nil []byte and an Arrow null
// are the same thing here, so this is asserted explicitly.
func validateBindArguments(req *BindRequestWire) error {
	if (req.Arguments == nil) == (req.ArgumentsLocation == nil) {
		return invalidBindRequest()
	}
	return nil
}

// resolveBindArguments returns the bind call's arguments IPC stream: the
// inline bytes, or the object arguments_location names, resolved through cc
// (vgi-rpc opens the sealed ref under this request's principal, checks its
// sealed size and SHA-256 against the location's, looks the object up in the
// process's object cache, fetches it once on a miss and verifies it).
//
// Refusals: invalid_bind_request (both or neither set); invalid_arguments_ref
// (a ref that does not open or match, no storage, no context, a transport
// that cannot resolve); arguments_ref_integrity (fetched bytes differ);
// arguments_ref_missing with ArgumentsRefMissingMessage (the object is gone);
// vgi-rpc's storage_unavailable passes through. invalid_arguments_ref and
// arguments_ref_integrity messages start with their kind, because a client
// that shows only "<type>: <message>" (the DuckDB extension) would otherwise
// never show the reason. The returned slice may be shared with the cache:
// read-only.
func resolveBindArguments(req *BindRequestWire, cc *vgirpc.CallContext) ([]byte, error) {
	if err := validateBindArguments(req); err != nil {
		return nil, err
	}
	if req.Arguments != nil {
		return *req.Arguments, nil
	}
	loc := req.ArgumentsLocation
	if cc == nil {
		return nil, invalidArgumentsRef("argument references need the request's context to resolve")
	}
	data, err := cc.ResolveArgumentRef(loc.Ref, loc.Size, loc.Sha256)
	if err == nil {
		return data, nil
	}
	switch vgirpc.ErrorKindOf(err) {
	case vgirpc.ErrorKindArgumentsRefMissing:
		return nil, &vgirpc.RpcError{
			Type:    "ArgumentsRefMissingError",
			Message: ArgumentsRefMissingMessage,
			Kind:    vgirpc.ErrorKindArgumentsRefMissing,
			Code:    string(vgirpc.CodeNotFound),
		}
	case vgirpc.ErrorKindInvalidArgumentsRef, vgirpc.ErrorKindArgumentsRefIntegrity:
		if re, ok := err.(*vgirpc.RpcError); ok && !strings.HasPrefix(re.Message, re.Kind+":") {
			out := *re
			out.Message = re.Kind + ": " + re.Message
			return nil, &out
		}
	}
	return nil, err
}

// parseBindArguments resolves and decodes the bind call's arguments. Bytes
// that came through a reference are exactly what the client uploaded and
// claimed (size and SHA-256 checked), so a stream that does not decode is the
// client's: invalid_arguments_ref.
func parseBindArguments(req *BindRequestWire, cc *vgirpc.CallContext) (*Arguments, error) {
	data, err := resolveBindArguments(req, cc)
	if err != nil {
		return nil, err
	}
	args, err := ParseArguments(data)
	if err != nil {
		if req.Arguments == nil {
			return nil, invalidArgumentsRef(fmt.Sprintf("the uploaded arguments are not a valid arguments IPC stream: %v", err))
		}
		return nil, fmt.Errorf("parsing arguments: %w", err)
	}
	return args, nil
}

// argumentsFingerprintInput is the arguments' part of a split-token
// fingerprint (large-values spec §5a, "Split-token fingerprints"). An argument
// reference contributes 0x01 || u64le(size) || sha256[32], never the sealed
// ref (it holds a random nonce); inline arguments contribute 0x00 ||
// u64le(len) || their IPC bytes (vgi-python's layout). Plan and init carry the
// same bind-call bytes, so both sides take the same branch.
func argumentsFingerprintInput(req *BindRequestWire) []byte {
	var out bytes.Buffer
	if loc := req.ArgumentsLocation; loc != nil {
		out.WriteByte(0x01)
		out.Write(binary.LittleEndian.AppendUint64(nil, uint64(loc.Size)))
		digest, err := hex.DecodeString(loc.Sha256)
		if err != nil || len(digest) != 32 {
			// Malformed digests never resolve (resolveBindArguments refuses
			// them first); hash the text so the fingerprint stays defined.
			out.WriteString(loc.Sha256)
		} else {
			out.Write(digest)
		}
		return out.Bytes()
	}
	var raw []byte
	if req.Arguments != nil {
		raw = *req.Arguments
	}
	out.WriteByte(0x00)
	out.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(raw))))
	out.Write(raw)
	return out.Bytes()
}

// bindCallValue decodes an embedding request's bind_call (wrapper rule 2/3):
// a decode failure names the field path and surfaces here, at access, as an
// INVALID_ARGUMENT refusal.
func bindCallValue(get func() (generated.BindRequest, error)) (BindRequestWire, error) {
	v, err := get()
	if err != nil {
		return BindRequestWire{}, &vgirpc.RpcError{
			Type:    "ProtocolDecodeError",
			Message: err.Error(),
			Code:    string(vgirpc.CodeInvalidArgument),
		}
	}
	return BindRequestWire(v), nil
}

// bindCallFromBytes decodes bind_call bytes kept from an earlier request (the
// init recipe's).
func bindCallFromBytes(raw []byte) (BindRequestWire, error) {
	n := generated.NestedFromBytes[generated.BindRequest](raw)
	return bindCallValue(func() (generated.BindRequest, error) {
		v, err := n.Get()
		if err != nil {
			return v, fmt.Errorf("InitRecipe.bind_call: %w", err)
		}
		return v, nil
	})
}

// argumentNamesOf converts the generated argument_names list to the
// BindParams form, where an unnamed vararg is nil. The generated record holds
// a []string, which cannot carry a null list item: vgi-rpc-go decodes one as
// "". No argument has an empty name (SQL identifiers are never empty), so ""
// is read back as nil.
func argumentNamesOf(names []string) []*string {
	out := make([]*string, len(names))
	for i, name := range names {
		if name != "" {
			v := name
			out[i] = &v
		}
	}
	return out
}
