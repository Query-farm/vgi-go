// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

// Orchard hosted mode (VGI_ORCHARD_HOSTED=1).
//
// A hosted worker (a Field Worker) serves VGI over HTTP to callers who hold a
// Pass: a short-lived RFC 9068 access token (typ at+jwt, ES256) issued by the
// Orchard Front Desk for exactly one canonical Desk Address. The worker
// verifies Passes locally against the Front Desk's JWKS and accepts nothing
// else. Everything here fails closed:
//
//   - Startup is refused unless VGI_JWT_ISSUER, VGI_JWT_AUDIENCE and
//     VGI_JWT_JWKS_URI are each set to a single non-empty value, and
//     VGI_SIGNING_KEY is configured with at least 32 bytes.
//   - Startup is refused when any other way in is configured: static bearer
//     tokens, sealed-grant keys, a ResolveToken / MintGrant hook, OAuth
//     resource metadata or PKCE, a proxy-proof gate, Iroh or other peer
//     identity, a custom authenticator, or a transport other than HTTP.
//   - Every request except HEAD /health and CORS preflights must present a
//     valid Pass, or gets a 401 with WWW-Authenticate: Bearer
//     error="invalid_token".
//   - The JWKS is cached for at most 10 minutes; once that passes and the
//     re-fetch fails, requests get a 503 rather than stale keys.
//
// The design is in the extension repository's docs/location-delegation.md
// ("The Pass" and "The Field Worker: SDK hosted mode"); vgi-python's
// vgi/hosted.py is the reference implementation.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/Query-farm/vgi-rpc-go/vgirpc/jwtauth"
)

const (
	// HostedEnv turns Orchard hosted mode on. The hosted base image sets it.
	HostedEnv = "VGI_ORCHARD_HOSTED"
	// HostedChallenge is the WWW-Authenticate value on every hosted-mode
	// 401. It carries no resource_metadata: a Field Worker never sends a
	// client off to discover an authorization server.
	HostedChallenge = `Bearer error="invalid_token"`
	// HostedDomain is AuthContext.Domain for a caller authenticated by Pass.
	HostedDomain = "orchard"
	// HostedRequiredTyp is the JOSE typ a Pass must carry (RFC 9068).
	HostedRequiredTyp = "at+jwt"
	// HostedLeeway is the clock skew allowed on exp and nbf.
	HostedLeeway = 60 * time.Second
	// HostedJWKSMaxAge is the hard upper bound on how long JWKS keys are
	// trusted.
	HostedJWKSMaxAge = 10 * time.Minute
	// HostedJWKSRefreshMinInterval is the minimum time between JWKS
	// re-fetches triggered by an unknown kid.
	HostedJWKSRefreshMinInterval = 60 * time.Second
	// HostedJWKSFetchTimeout bounds each JWKS request.
	HostedJWKSFetchTimeout = 10 * time.Second
	// HostedMinSigningKeyBytes is the shortest VGI_SIGNING_KEY a hosted
	// worker accepts.
	HostedMinSigningKeyBytes = 32
	// HostedDefaultMaxRequestBytes is the request-body cap, advertised and
	// enforced, when VGI_RPC_MAX_REQUEST_BYTES is unset in hosted mode. Go's
	// server reads no body before the Pass check; the cap bounds what an
	// authenticated caller can send inline (larger requests go by upload URL
	// when a blob vendor is configured).
	HostedDefaultMaxRequestBytes = 32 << 20
)

// hostedDefaultAlgorithms is the Pass algorithm allow-list unless
// VGI_JWT_ALGORITHMS overrides it.
var hostedDefaultAlgorithms = []string{"ES256"}

// HostedRefusal is a hosted-mode configuration the worker refuses to start
// with. The Run methods print it to stderr and exit with status 1.
type HostedRefusal struct {
	// Reason says what is wrong and how to fix it.
	Reason string
}

// Error returns the refusal as printed to stderr.
func (e *HostedRefusal) Error() string {
	return fmt.Sprintf("%s=1 (Orchard hosted mode): %s", HostedEnv, e.Reason)
}

func refuseHosted(format string, args ...any) error {
	return &HostedRefusal{Reason: fmt.Sprintf(format, args...)}
}

// HostedModeEnabled reports whether VGI_ORCHARD_HOSTED turns hosted mode on:
// true for 1/true/yes/on, false when unset or 0/false/no/off (any case,
// surrounding space ignored). Any other value is an error, not "off", so a
// typo cannot quietly start a worker without authentication.
func HostedModeEnabled() (bool, error) {
	raw, ok := os.LookupEnv(HostedEnv)
	if !ok {
		return false, nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "", "0", "false", "no", "off":
		return false, nil
	}
	return false, &HostedRefusal{Reason: fmt.Sprintf("%s must be 1 or 0 (or true/false), got %q", HostedEnv, raw)}
}

// HostedConfig is the verified hosted-mode configuration.
type HostedConfig struct {
	// Issuer is the Front Desk issuer (iss), matched exactly.
	Issuer string
	// Audience is the one canonical Desk Address (aud), matched exactly.
	Audience string
	// JWKSURI is where the Front Desk publishes its signing keys.
	JWKSURI string
	// Algorithms are the accepted JWS algorithms (ES256 unless overridden).
	Algorithms []string
}

// singleHostedValue reads one required, single-valued variable. Unset counts
// as empty.
func singleHostedValue(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", refuseHosted("%s must be set (unset and empty are both errors).", name)
	}
	if strings.Contains(value, ",") {
		return "", refuseHosted("%s must be exactly one value; got a comma-separated list. "+
			"A Pass is issued for one canonical Desk Address and one issuer.", name)
	}
	return value, nil
}

// ResolveHostedConfig reads and validates the hosted-mode environment:
// VGI_JWT_ISSUER, VGI_JWT_AUDIENCE and VGI_JWT_JWKS_URI (one non-empty value
// each) and the optional VGI_JWT_ALGORITHMS, which may never name "none" or
// an HS* algorithm.
func ResolveHostedConfig() (HostedConfig, error) {
	var cfg HostedConfig
	var err error
	if cfg.Issuer, err = singleHostedValue("VGI_JWT_ISSUER"); err != nil {
		return cfg, err
	}
	if cfg.Audience, err = singleHostedValue("VGI_JWT_AUDIENCE"); err != nil {
		return cfg, err
	}
	if cfg.JWKSURI, err = singleHostedValue("VGI_JWT_JWKS_URI"); err != nil {
		return cfg, err
	}
	cfg.Algorithms = hostedDefaultAlgorithms
	if raw := os.Getenv("VGI_JWT_ALGORITHMS"); strings.TrimSpace(raw) != "" {
		algorithms, err := ParseJWTAlgorithms(raw)
		if err != nil {
			return cfg, refuseHosted("VGI_JWT_ALGORITHMS=%q is not allowed; Passes are asymmetric "+
				"(default ES256), never 'none' or HS*.", raw)
		}
		cfg.Algorithms = algorithms
	}
	return cfg, nil
}

// ParseJWTAlgorithms parses a comma-separated VGI_JWT_ALGORITHMS value. It
// refuses an empty list, "none" and HS* algorithms: an HMAC algorithm checked
// against a JWKS lets anyone holding the public key mint tokens.
func ParseJWTAlgorithms(raw string) ([]string, error) {
	var algorithms []string
	for _, a := range strings.Split(raw, ",") {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		lowered := strings.ToLower(a)
		if lowered == "none" || strings.HasPrefix(lowered, "hs") {
			return nil, fmt.Errorf("JWT algorithm %q is not allowed", a)
		}
		algorithms = append(algorithms, a)
	}
	if len(algorithms) == 0 {
		return nil, errors.New("no JWT algorithms given")
	}
	return algorithms, nil
}

// checkHostedEnvironment refuses an environment that configures another
// credential.
func checkHostedEnvironment() error {
	for _, v := range []struct{ name, what string }{
		{"VGI_BEARER_TOKENS", "static bearer tokens"},
		{vgirpc.GrantKeysEnv, "sealed-grant keys"},
		{"VGI_OAUTH_RESOURCE", "OAuth resource metadata (a Field Worker never advertises resource_metadata)"},
	} {
		if strings.TrimSpace(os.Getenv(v.name)) != "" {
			return refuseHosted("%s is set (%s); the only credential a hosted worker accepts is a Pass.", v.name, v.what)
		}
	}
	if mode := strings.ToLower(strings.TrimSpace(os.Getenv("VGI_PROXY_PROOF_MODE"))); mode != "" && mode != "off" {
		return refuseHosted("VGI_PROXY_PROOF_MODE must be off; hosted workers authenticate by Pass only.")
	}
	return nil
}

// checkHostedWorker refuses a worker configured to admit a non-Pass caller.
func (w *Worker) checkHostedWorker() error {
	if w.authenticateFunc != nil {
		return refuseHosted("a custom authenticate callback is configured (SetAuthenticate); hosted workers verify Passes only.")
	}
	if w.oauthMetadata != nil {
		return refuseHosted("OAuth resource metadata is configured; a Field Worker never advertises it.")
	}
	if w.oauthPkce != nil {
		return refuseHosted("OAuth PKCE is configured; hosted workers verify Passes only.")
	}
	if w.grantKeysSet && w.grantKeys != nil {
		return refuseHosted("sealed-grant keys (--grant-key / WithGrantKeys / %s) are configured.", vgirpc.GrantKeysEnv)
	}
	if w.identity != nil {
		switch {
		case w.identity.ResolveToken != nil:
			return refuseHosted("the worker supplies a resolve_token hook (WithIdentity ResolveToken).")
		case w.identity.MintGrant != nil:
			return refuseHosted("the worker supplies a mint_grant hook (WithIdentity MintGrant, issue_grant).")
		case w.identity.GrantKeys != nil:
			return refuseHosted("sealed-grant keys (WithIdentity GrantKeys) are configured.")
		}
	}
	if w.irohBridge != nil {
		return refuseHosted("peer identity (Iroh) is configured; hosted workers accept Passes only.")
	}
	return nil
}

// checkHostedSigningKey refuses a key that is not stable across restarts.
// The key seals catalog opaque data and HTTP state tokens; a Field Worker's
// machines stop and start, and a deployment can run on several of them, so a
// key minted per process would make every restart (and every other machine)
// reject the sessions DuckDB already holds.
func (w *Worker) checkHostedSigningKey() error {
	if !w.signingKeyConfigured || len(w.httpSigningKey) == 0 {
		return refuseHosted("%s is not set; hosted workers need a stable key shared by every machine of the "+
			"deployment, or every restart breaks attached sessions.", SigningKeyEnv)
	}
	if len(w.httpSigningKey) < HostedMinSigningKeyBytes {
		return refuseHosted("%s must be at least %d bytes.", SigningKeyEnv, HostedMinSigningKeyBytes)
	}
	return nil
}

// hostedPassAuthenticate checks the whole hosted configuration and builds
// the Pass verifier. It returns (nil, nil, nil) when hosted mode is off.
func (w *Worker) hostedPassAuthenticate() (vgirpc.AuthenticateFunc, func(), error) {
	hosted, err := HostedModeEnabled()
	if err != nil || !hosted {
		return nil, nil, err
	}
	if err := checkHostedEnvironment(); err != nil {
		return nil, nil, err
	}
	if err := w.checkHostedWorker(); err != nil {
		return nil, nil, err
	}
	if err := w.checkHostedSigningKey(); err != nil {
		return nil, nil, err
	}
	cfg, err := ResolveHostedConfig()
	if err != nil {
		return nil, nil, err
	}
	authFn, cleanup, err := jwtauth.NewAuthenticateFunc(jwtauth.JWTAuthConfig{
		Issuer:                 cfg.Issuer,
		Audience:               []string{cfg.Audience},
		JWKSURI:                cfg.JWKSURI,
		PrincipalClaim:         "sub",
		Domain:                 HostedDomain,
		Algorithms:             cfg.Algorithms,
		RequiredTyp:            HostedRequiredTyp,
		RequireExp:             true,
		SingleAudience:         true,
		Leeway:                 HostedLeeway,
		JWKSMaxAge:             HostedJWKSMaxAge,
		JWKSRefreshMinInterval: HostedJWKSRefreshMinInterval,
		JWKSFetchTimeout:       HostedJWKSFetchTimeout,
	})
	if err != nil {
		return nil, nil, refuseHosted("%v", err)
	}
	return authFn, cleanup, nil
}

// refuseNonHTTPTransportIfHosted refuses every transport but HTTP in hosted
// mode: stdio, AF_UNIX, raw TCP and the raw Iroh upstream carry no Pass.
func refuseNonHTTPTransportIfHosted(transport string) error {
	hosted, err := HostedModeEnabled()
	if err != nil {
		return err
	}
	if hosted {
		return refuseHosted("only --http is allowed; %s carries no Pass.", transport)
	}
	return nil
}

// exitIfHostedRefusal prints a hosted-mode refusal to stderr and exits with
// status 1, so a misconfigured hosted worker never starts however its main
// handles the error. Other errors are returned unchanged.
func exitIfHostedRefusal(err error) error {
	var refusal *HostedRefusal
	if errors.As(err, &refusal) {
		fmt.Fprintf(os.Stderr, "Error: %s\n", refusal.Error())
		os.Exit(1)
	}
	return err
}
