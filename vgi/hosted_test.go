// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

// Orchard hosted mode (VGI_ORCHARD_HOSTED=1), ported from vgi-python's
// tests/test_orchard_hosted.py:
//
//   - a valid Pass reaches function code, with claims in their JSON types;
//   - no Pass, a tampered claim, the wrong aud, an expired Pass, the wrong
//     typ and RS256 / HS256 tokens all get 401 with
//     WWW-Authenticate: Bearer error="invalid_token", the capability headers
//     and no resource_metadata; HEAD /health alone is anonymous;
//   - the Authorization header never reaches the logs, even at debug level;
//   - every misconfiguration refuses to start, with a non-zero exit.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/golang-jwt/jwt/v5"
)

const (
	orchardIssuer     = "https://auth.orchard.test"
	orchardAudience   = "https://desk.orchard.test/d/dep_123"
	orchardSigningKey = "orchard-hosted-test-signing-key-0001"
	orchardKID        = "desk-es256-1"
	orchardRSAKID     = "desk-rs256-1"
	passProbeProtocol = "orchard.PassProbe.v1"
)

var (
	orchardKeysOnce sync.Once
	orchardEC       *ecdsa.PrivateKey
	orchardRSA      *rsa.PrivateKey
)

func orchardKeys(t *testing.T) (*ecdsa.PrivateKey, *rsa.PrivateKey) {
	t.Helper()
	orchardKeysOnce.Do(func() {
		orchardEC, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		orchardRSA, _ = rsa.GenerateKey(rand.Reader, 2048)
	})
	return orchardEC, orchardRSA
}

// orchardHostedEnvNames are cleared for every test, so the developer's or
// CI's environment never leaks into the hosted configuration.
var orchardHostedEnvNames = []string{
	"VGI_BEARER_TOKENS", "VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI", "VGI_JWT_ALGORITHMS",
	"VGI_RPC_GRANT_KEYS", "VGI_OAUTH_RESOURCE", "VGI_OAUTH_AUTH_SERVERS", "VGI_PROXY_PROOF_MODE",
	"VGI_INTROSPECT_PRINCIPALS", "VGI_ORCHARD_HOSTED", "VGI_SIGNING_KEY", "VGI_HTTP_CORS_ORIGINS",
}

// orchardJWKS serves the EC and RSA public keys; fail answers 500.
type orchardJWKS struct {
	srv  *httptest.Server
	mu   sync.Mutex
	fail bool
}

func newOrchardJWKS(t *testing.T) *orchardJWKS {
	t.Helper()
	ec, rk := orchardKeys(t)
	body, _ := json.Marshal(map[string]any{"keys": []map[string]any{
		{
			"kty": "EC", "crv": "P-256", "kid": orchardKID, "use": "sig", "alg": "ES256",
			"x": base64.RawURLEncoding.EncodeToString(ecPoint(&ec.PublicKey)[1:33]),
			"y": base64.RawURLEncoding.EncodeToString(ecPoint(&ec.PublicKey)[33:65]),
		},
		{
			"kty": "RSA", "kid": orchardRSAKID, "use": "sig", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(rk.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(rk.E)).Bytes()),
		},
	}})
	j := &orchardJWKS{}
	j.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		j.mu.Lock()
		fail := j.fail
		j.mu.Unlock()
		if fail {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(j.srv.Close)
	return j
}

// setHostedEnv clears the auth environment and sets a valid hosted one.
// overrides set (non-empty) or unset ("<unset>") individual variables.
func setHostedEnv(t *testing.T, jwksURI string, overrides map[string]string) {
	t.Helper()
	for _, name := range orchardHostedEnvNames {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}
	env := map[string]string{
		"VGI_ORCHARD_HOSTED": "1",
		"VGI_JWT_ISSUER":     orchardIssuer,
		"VGI_JWT_AUDIENCE":   orchardAudience,
		"VGI_JWT_JWKS_URI":   jwksURI,
		"VGI_SIGNING_KEY":    orchardSigningKey,
	}
	for k, v := range overrides {
		env[k] = v
	}
	for k, v := range env {
		if v == "<unset>" {
			_ = os.Unsetenv(k)
			continue
		}
		t.Setenv(k, v)
	}
}

func orchardPass(t *testing.T, method jwt.SigningMethod, key any, kid, typ string, overrides map[string]any) string {
	t.Helper()
	now := time.Now().Unix()
	claims := jwt.MapClaims{
		"iss": orchardIssuer, "aud": orchardAudience, "sub": "user_01M7KNJYC18T", "kind": "user",
		"email": "ada@example.com", "email_verified": true, "name": "Ada Lovelace",
		"org_id": "org_01M7", "org_name": "Acme Analytics", "client_id": "vgi-duckdb",
		"iat": now, "nbf": now, "exp": now + 900, "jti": fmt.Sprintf("jti-%d", time.Now().UnixNano()),
	}
	for k, v := range overrides {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}
	tok := jwt.NewWithClaims(method, claims)
	tok.Header["kid"] = kid
	tok.Header["typ"] = typ
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func goodPass(t *testing.T, overrides map[string]any) string {
	ec, _ := orchardKeys(t)
	return orchardPass(t, jwt.SigningMethodES256, ec, orchardKID, "at+jwt", overrides)
}

func tamperPass(token string, field string, value any) string {
	parts := strings.Split(token, ".")
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	_ = json.Unmarshal(raw, &claims)
	claims[field] = value
	forged, _ := json.Marshal(claims)
	return parts[0] + "." + base64.RawURLEncoding.EncodeToString(forged) + "." + parts[2]
}

// probeParams is the PassProbe.claims request.
type probeParams struct {
	Value string `vgirpc:"value"`
}

// passProbe is a protocol hosted beside vgi.v3 whose one method returns the
// caller's AuthContext as JSON: the same context vgi.v3 hands a function as
// ProcessParams.Auth.
func passProbe() *vgirpc.Server {
	p := vgirpc.NewProtocol(passProbeProtocol)
	vgirpc.Unary(p, "claims", func(_ context.Context, cc *vgirpc.CallContext, _ probeParams) (string, error) {
		encoded, err := json.Marshal(map[string]any{
			"principal": cc.Auth.Principal, "domain": cc.Auth.Domain, "claims": cc.Auth.Claims,
		})
		return string(encoded), err
	})
	return p
}

// hostedWorker is a hosted-mode worker serving pass_claims and the probe.
func hostedWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	opts = append([]vgi.WorkerOption{
		vgi.WithCatalogName("orchard"),
		vgi.WithHostedProtocols(func() ([]*vgirpc.Server, error) { return []*vgirpc.Server{passProbe()}, nil }),
	}, opts...)
	w := vgi.NewWorker(opts...)
	w.RegisterScalar(&scalar.PassClaimsFunction{})
	return w
}

type hostedServer struct {
	url  string
	jwks *orchardJWKS
	logs *lockedBuffer
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startHostedServer(t *testing.T) *hostedServer {
	t.Helper()
	jwks := newOrchardJWKS(t)
	setHostedEnv(t, jwks.srv.URL, nil)
	logs := &lockedBuffer{}
	w := hostedWorker(vgi.WithLogHandler(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	return &hostedServer{url: ts.URL, jwks: jwks, logs: logs}
}

func (s *hostedServer) request(t *testing.T, method, path, token string, headers map[string]string) *http.Response {
	t.Helper()
	var body io.Reader
	if method == http.MethodPost {
		body = strings.NewReader("")
	}
	req, err := http.NewRequest(method, s.url+path, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

func (s *hostedServer) claims(t *testing.T, token string) (map[string]any, error) {
	t.Helper()
	c, err := vgirpc.NewHttpClient(s.url, vgirpc.WithClientProtocol(passProbeProtocol),
		vgirpc.WithClientHeader("Authorization", "Bearer "+token))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).Append("x")
	params := b.NewRecordBatch()
	defer params.Release()
	res, err := c.CallUnary(context.Background(), "claims", params, nil)
	if err != nil {
		return nil, err
	}
	defer res.Release()
	col, ok := res.Batch.Column(0).(*array.String)
	if !ok || col.Len() != 1 {
		t.Fatalf("unexpected probe result %v", res.Batch)
	}
	out := map[string]any{}
	dec := json.NewDecoder(strings.NewReader(col.Value(0)))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out, nil
}

func assertHosted401(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); got != vgi.HostedChallenge {
		t.Fatalf("WWW-Authenticate = %q", got)
	}
	if resp.Header.Get("VGI-Supported-Encodings") == "" {
		t.Fatal("401 lacks the capability headers")
	}
	switch resp.Header.Get("VGI-Auth-Reason") {
	case "missing_credential", "invalid_credential", "expired_credential":
	default:
		t.Fatalf("VGI-Auth-Reason = %q", resp.Header.Get("VGI-Auth-Reason"))
	}
}

func TestHostedValidPassReachesFunctionWithJSONTypes(t *testing.T) {
	s := startHostedServer(t)
	result, err := s.claims(t, goodPass(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if result["principal"] != "user_01M7KNJYC18T" || result["domain"] != vgi.HostedDomain {
		t.Fatalf("principal/domain = %v / %v", result["principal"], result["domain"])
	}
	claims := result["claims"].(map[string]any)
	if claims["email_verified"] != true {
		t.Fatalf("email_verified = %#v, want JSON true", claims["email_verified"])
	}
	if exp, ok := claims["exp"].(json.Number); !ok || strings.ContainsAny(exp.String(), ".eE") {
		t.Fatalf("exp = %#v, want a JSON integer", claims["exp"])
	}
	for name, want := range map[string]string{
		"kind": "user", "email": "ada@example.com", "name": "Ada Lovelace",
		"org_id": "org_01M7", "org_name": "Acme Analytics", "client_id": "vgi-duckdb",
	} {
		if claims[name] != want {
			t.Fatalf("claim %s = %#v, want %q", name, claims[name], want)
		}
	}
}

func TestHostedHeadHealthIsAnonymous(t *testing.T) {
	s := startHostedServer(t)
	resp := s.request(t, http.MethodHead, "/health", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /health: %d", resp.StatusCode)
	}
	if resp.Header.Get("VGI-Supported-Encodings") == "" {
		t.Fatal("HEAD /health lacks the capability headers")
	}
}

func TestHostedNoPass401(t *testing.T) {
	s := startHostedServer(t)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/health"}, {http.MethodOptions, "/health"}, {http.MethodGet, "/"},
		{http.MethodGet, "/describe"}, {http.MethodGet, "/vgi-client.js"}, {http.MethodGet, "/nope"},
		{http.MethodPost, "/vgi.v3/catalog_attach"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			resp := s.request(t, tc.method, tc.path, "", map[string]string{"Origin": "https://shell.example.com"})
			assertHosted401(t, resp)
			if resp.Header.Get("VGI-Auth-Reason") != "missing_credential" {
				t.Fatalf("VGI-Auth-Reason = %q", resp.Header.Get("VGI-Auth-Reason"))
			}
		})
	}
}

func TestHostedBadPass401(t *testing.T) {
	s := startHostedServer(t)
	_, rk := orchardKeys(t)
	now := time.Now().Unix()
	cases := map[string]func() string{
		"garbage":   func() string { return "not-a-jwt" },
		"tampered":  func() string { return tamperPass(goodPass(t, nil), "org_id", "org_EVIL") },
		"wrong_aud": func() string { return goodPass(t, map[string]any{"aud": "https://desk.orchard.test/d/dep_other"}) },
		"multi_aud": func() string {
			return goodPass(t, map[string]any{"aud": []string{orchardAudience, "https://desk.orchard.test/d/dep_other"}})
		},
		"expired": func() string {
			return goodPass(t, map[string]any{"iat": now - 1200, "nbf": now - 1200, "exp": now - 120})
		},
		"wrong_typ": func() string {
			ec, _ := orchardKeys(t)
			return orchardPass(t, jwt.SigningMethodES256, ec, orchardKID, "JWT", nil)
		},
		"rs256": func() string { return orchardPass(t, jwt.SigningMethodRS256, rk, orchardRSAKID, "at+jwt", nil) },
		"hs256": func() string {
			return orchardPass(t, jwt.SigningMethodHS256, []byte(strings.Repeat("k", 32)), orchardKID, "at+jwt", nil)
		},
		"wrong_iss": func() string { return goodPass(t, map[string]any{"iss": "https://evil.test"}) },
		"no_exp":    func() string { return goodPass(t, map[string]any{"exp": nil}) },
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			resp := s.request(t, http.MethodPost, "/vgi.v3/catalog_attach", mk(),
				map[string]string{"Origin": "https://shell.example.com"})
			assertHosted401(t, resp)
			want := "invalid_credential"
			if name == "expired" {
				want = "expired_credential"
			}
			if got := resp.Header.Get("VGI-Auth-Reason"); got != want {
				t.Fatalf("VGI-Auth-Reason = %q, want %q", got, want)
			}
		})
	}
}

func TestHostedPreflightOpenCORS(t *testing.T) {
	s := startHostedServer(t)
	resp := s.request(t, http.MethodOptions, "/vgi.v3/catalog_attach", "", map[string]string{
		"Origin":                         "https://shell.example.com",
		"Access-Control-Request-Method":  "POST",
		"Access-Control-Request-Headers": "authorization",
	})
	if resp.StatusCode/100 != 2 {
		t.Fatalf("preflight: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
}

func TestHostedJWKSUnavailableIs503(t *testing.T) {
	s := startHostedServer(t)
	s.jwks.mu.Lock()
	s.jwks.fail = true
	s.jwks.mu.Unlock()
	resp := s.request(t, http.MethodPost, "/vgi.v3/catalog_attach", goodPass(t, nil), nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("JWKS down: expected 503, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("503 lacks Retry-After")
	}
}

func TestHostedAuthorizationNeverLogged(t *testing.T) {
	s := startHostedServer(t)
	var used []string
	good := goodPass(t, nil)
	used = append(used, good)
	if _, err := s.claims(t, good); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		tamperPass(goodPass(t, nil), "sub", "mallory"),
		goodPass(t, map[string]any{"aud": "https://other.test"}),
		func() string {
			ec, _ := orchardKeys(t)
			return orchardPass(t, jwt.SigningMethodES256, ec, orchardKID, "JWT", nil)
		}(),
	} {
		used = append(used, bad)
		if resp := s.request(t, http.MethodPost, "/vgi.v3/catalog_attach", bad, nil); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("bad pass: %d", resp.StatusCode)
		}
	}
	text := s.logs.String()
	if text == "" {
		t.Fatal("expected debug log output to be captured")
	}
	for _, token := range used {
		if strings.Contains(text, token) || strings.Contains(text, strings.Split(token, ".")[2]) {
			t.Fatalf("a Pass reached the logs:\n%s", text)
		}
	}
	if strings.Contains(text, "Bearer ") {
		t.Fatalf("an Authorization value reached the logs:\n%s", text)
	}
}

// ---------------------------------------------------------------------------
// Startup refusals (in process)
// ---------------------------------------------------------------------------

const unusedJWKS = "http://127.0.0.1:9/jwks" // never contacted: startup refuses first

func refused(t *testing.T, w *vgi.Worker) string {
	t.Helper()
	_, err := w.NewHttpServerForTest()
	var refusal *vgi.HostedRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("expected a HostedRefusal, got %v", err)
	}
	return refusal.Error()
}

func TestHostedStartupRefusals(t *testing.T) {
	grantKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	envCases := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{"issuer unset", map[string]string{"VGI_JWT_ISSUER": "<unset>"}, []string{"VGI_JWT_ISSUER"}},
		{"audience unset", map[string]string{"VGI_JWT_AUDIENCE": "<unset>"}, []string{"VGI_JWT_AUDIENCE"}},
		{"jwks unset", map[string]string{"VGI_JWT_JWKS_URI": "<unset>"}, []string{"VGI_JWT_JWKS_URI"}},
		{"issuer empty", map[string]string{"VGI_JWT_ISSUER": "  "}, []string{"VGI_JWT_ISSUER"}},
		{"audience empty", map[string]string{"VGI_JWT_AUDIENCE": "  "}, []string{"VGI_JWT_AUDIENCE"}},
		{"jwks empty", map[string]string{"VGI_JWT_JWKS_URI": "  "}, []string{"VGI_JWT_JWKS_URI"}},
		{"comma audience", map[string]string{"VGI_JWT_AUDIENCE": orchardAudience + ",https://desk.orchard.test/d/other"},
			[]string{"VGI_JWT_AUDIENCE", "exactly one"}},
		{"comma issuer", map[string]string{"VGI_JWT_ISSUER": orchardIssuer + ",https://x.test"}, []string{"VGI_JWT_ISSUER", "exactly one"}},
		{"comma jwks", map[string]string{"VGI_JWT_JWKS_URI": unusedJWKS + ",http://x.test"}, []string{"VGI_JWT_JWKS_URI", "exactly one"}},
		{"signing key unset", map[string]string{"VGI_SIGNING_KEY": "<unset>"}, []string{"VGI_SIGNING_KEY"}},
		{"signing key short", map[string]string{"VGI_SIGNING_KEY": "short-key"}, []string{"VGI_SIGNING_KEY", "32 bytes"}},
		{"bearer tokens", map[string]string{"VGI_BEARER_TOKENS": "tok=alice"}, []string{"VGI_BEARER_TOKENS"}},
		{"grant keys env", map[string]string{"VGI_RPC_GRANT_KEYS": grantKey}, []string{"VGI_RPC_GRANT_KEYS"}},
		{"oauth resource", map[string]string{"VGI_OAUTH_RESOURCE": "https://x.test", "VGI_OAUTH_AUTH_SERVERS": orchardIssuer},
			[]string{"VGI_OAUTH_RESOURCE"}},
		{"proxy proof", map[string]string{"VGI_PROXY_PROOF_MODE": "require"}, []string{"VGI_PROXY_PROOF_MODE"}},
		{"hs algorithm", map[string]string{"VGI_JWT_ALGORITHMS": "ES256,HS256"}, []string{"VGI_JWT_ALGORITHMS"}},
		{"none algorithm", map[string]string{"VGI_JWT_ALGORITHMS": "none"}, []string{"VGI_JWT_ALGORITHMS"}},
		{"unrecognised flag", map[string]string{"VGI_ORCHARD_HOSTED": "yes please"}, []string{"VGI_ORCHARD_HOSTED"}},
	}
	for _, tc := range envCases {
		t.Run(tc.name, func(t *testing.T) {
			setHostedEnv(t, unusedJWKS, tc.env)
			msg := refused(t, hostedWorker())
			for _, want := range tc.want {
				if !strings.Contains(msg, want) {
					t.Fatalf("refusal %q does not mention %q", msg, want)
				}
			}
		})
	}

	keys, err := vgirpc.NewGrantKeys([][]byte{bytes.Repeat([]byte{7}, 32)}, "orchard", 3600)
	if err != nil {
		t.Fatal(err)
	}
	workerCases := []struct {
		name string
		mk   func() *vgi.Worker
		want string
	}{
		{"custom authenticate", func() *vgi.Worker {
			w := hostedWorker()
			w.SetAuthenticate(func(*http.Request) (*vgirpc.AuthContext, error) { return vgirpc.Anonymous(), nil })
			return w
		}, "authenticate"},
		{"grant keys option", func() *vgi.Worker { return hostedWorker(vgi.WithGrantKeys(keys)) }, "grant"},
		{"resolve_token", func() *vgi.Worker {
			return hostedWorker(vgi.WithIdentity(vgirpc.IdentityConfig{
				ResolveToken:         func(string) (vgirpc.TokenIdentity, bool, error) { return vgirpc.TokenIdentity{}, false, nil },
				IntrospectPrincipals: []string{"ops"},
			}))
		}, "resolve_token"},
		{"oauth metadata", func() *vgi.Worker {
			w := hostedWorker()
			w.SetOAuthResourceMetadata(&vgirpc.OAuthResourceMetadata{
				Resource: "https://x.test", AuthorizationServers: []string{orchardIssuer},
			})
			return w
		}, "OAuth"},
		{"iroh bridge", func() *vgi.Worker {
			w := hostedWorker()
			w.SetIrohBridge(vgi.IrohBridgeOptions{Issuer: "iroh"})
			return w
		}, "peer identity"},
		{"signing key option too short", func() *vgi.Worker {
			return hostedWorker(vgi.WithHttpSigningKey([]byte("short")))
		}, "32 bytes"},
	}
	for _, tc := range workerCases {
		t.Run(tc.name, func(t *testing.T) {
			setHostedEnv(t, unusedJWKS, nil)
			if msg := refused(t, tc.mk()); !strings.Contains(msg, tc.want) {
				t.Fatalf("refusal %q does not mention %q", msg, tc.want)
			}
		})
	}
}

func TestHostedModeOffValues(t *testing.T) {
	for _, v := range []string{"0", "false", "", "off", "NO", " Off "} {
		t.Setenv("VGI_ORCHARD_HOSTED", v)
		if on, err := vgi.HostedModeEnabled(); on || err != nil {
			t.Fatalf("%q: on=%v err=%v", v, on, err)
		}
	}
	for _, v := range []string{"1", "true", "YES", "on"} {
		t.Setenv("VGI_ORCHARD_HOSTED", v)
		if on, err := vgi.HostedModeEnabled(); !on || err != nil {
			t.Fatalf("%q: on=%v err=%v", v, on, err)
		}
	}
	_ = os.Unsetenv("VGI_ORCHARD_HOSTED")
	if on, err := vgi.HostedModeEnabled(); on || err != nil {
		t.Fatalf("unset: on=%v err=%v", on, err)
	}
}

// Outside hosted mode nothing changes: no Pass needed, GET /health is open.
func TestNotHostedUnchanged(t *testing.T) {
	setHostedEnv(t, unusedJWKS, map[string]string{"VGI_ORCHARD_HOSTED": "0"})
	hs, err := hostedWorker().NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health outside hosted mode: %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Real processes: refusals exit non-zero; RunHttp serves.
// ---------------------------------------------------------------------------

// TestHostedHelperProcess is not a test: the tests below re-run the test
// binary with VGI_HOSTED_HELPER set, and it then serves a hosted worker on
// the named transport.
func TestHostedHelperProcess(t *testing.T) {
	mode := os.Getenv("VGI_HOSTED_HELPER")
	if mode == "" {
		t.Skip("helper process for the hosted-mode tests")
	}
	w := hostedWorker()
	var err error
	switch mode {
	case "http":
		err = w.RunHttp("127.0.0.1:0")
	case "unix":
		err = w.RunUnix(filepath.Join(os.TempDir(), fmt.Sprintf("vgi-hosted-%d.sock", os.Getpid())), time.Second)
	case "tcp":
		err = w.RunTcp("127.0.0.1", 0, time.Second)
	case "iroh":
		err = w.RunIrohTcpUpstream("127.0.0.1", 0, time.Second, vgi.IrohBridgeOptions{Issuer: "iroh"})
	case "stdio":
		w.RunStdio()
	}
	fmt.Fprintf(os.Stderr, "helper returned: %v\n", err)
	os.Exit(3)
}

func helperCmd(t *testing.T, mode string, env map[string]string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHostedHelperProcess$")
	cmd.Env = append(os.Environ(), "VGI_HOSTED_HELPER="+mode)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd
}

func TestHostedProcessRefusesNonHTTPTransports(t *testing.T) {
	setHostedEnv(t, unusedJWKS, nil)
	for _, mode := range []string{"stdio", "unix", "tcp", "iroh"} {
		t.Run(mode, func(t *testing.T) {
			cmd := helperCmd(t, mode, nil)
			cmd.Stdin = strings.NewReader("")
			var stderr, stdout bytes.Buffer
			cmd.Stderr, cmd.Stdout = &stderr, &stdout
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("expected exit 1, got %v\nstderr: %s", err, stderr.String())
			}
			if !strings.Contains(stderr.String(), "only --http") {
				t.Fatalf("stderr does not explain the refusal: %s", stderr.String())
			}
			if strings.Contains(stdout.String(), "UNIX:") || strings.Contains(stdout.String(), "TCP:") {
				t.Fatalf("worker announced a listener: %s", stdout.String())
			}
		})
	}
}

func TestHostedProcessRefusesMisconfiguredHTTP(t *testing.T) {
	setHostedEnv(t, unusedJWKS, map[string]string{"VGI_JWT_AUDIENCE": orchardAudience + ",https://x.test"})
	cmd := helperCmd(t, "http", nil)
	var stderr, stdout bytes.Buffer
	cmd.Stderr, cmd.Stdout = &stderr, &stdout
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("expected exit 1, got %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), "VGI_JWT_AUDIENCE") || strings.Contains(stdout.String(), "PORT:") {
		t.Fatalf("unexpected output\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
	}
}

func TestHostedProcessServesHTTP(t *testing.T) {
	jwks := newOrchardJWKS(t)
	setHostedEnv(t, jwks.srv.URL, nil)
	cmd := helperCmd(t, "http", nil)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	var line string
	buf := make([]byte, 256)
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(line, "\n") && time.Now().Before(deadline) {
		n, err := stdout.Read(buf)
		line += string(buf[:n])
		if err != nil {
			break
		}
	}
	var port int
	if _, err := fmt.Sscanf(strings.TrimSpace(line), "PORT:%d", &port); err != nil {
		t.Fatalf("no PORT line: %q", line)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	head, err := http.Head(url + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = head.Body.Close()
	if head.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /health: %d", head.StatusCode)
	}
	get, err := http.Get(url + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = get.Body.Close()
	if get.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /health without a Pass: %d", get.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, url+"/health", nil)
	req.Header.Set("Authorization", "Bearer "+goodPass(t, nil))
	ok, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = ok.Body.Close()
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("GET /health with a Pass: %d", ok.StatusCode)
	}
}

// ecPoint is the uncompressed P-256 point 0x04 || X || Y.
func ecPoint(key *ecdsa.PublicKey) []byte {
	b, err := key.Bytes()
	if err != nil || len(b) != 65 {
		panic("unexpected P-256 public key encoding")
	}
	return b
}
