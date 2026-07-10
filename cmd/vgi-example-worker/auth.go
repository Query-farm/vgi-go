// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package main

// Auth + OAuth environment-variable wiring for the example worker.
// Mirrors vgi-python serve.py.

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/Query-farm/vgi-rpc-go/vgirpc/jwtauth"
)

// resolveAuthenticate builds an AuthenticateFunc from environment variables.
// Returns nil if no auth env vars are set. When both bearer and JWT are
// configured, they are chained (JWT first, bearer fallback).
func resolveAuthenticate() (vgirpc.AuthenticateFunc, func()) {
	bearerAuth := resolveBearerAuthenticate()
	jwtAuth, jwtCleanup := resolveJWTAuthenticate()

	if bearerAuth != nil && jwtAuth != nil {
		return vgirpc.ChainAuthenticate(jwtAuth, bearerAuth), jwtCleanup
	}
	if jwtAuth != nil {
		return jwtAuth, jwtCleanup
	}
	if bearerAuth != nil {
		return bearerAuth, nil
	}
	return optionalTestBearerAuthenticate(), nil
}

// optionalTestBearerAuthenticate is a test-only OPTIONAL bearer authenticator.
// It lets the cache identity-isolation test attach the same worker under
// different principals (alice/bob) while every other test on this shared server
// stays anonymous: no / blank / unknown bearer resolves to anonymous (the
// existing behaviour), a known token to its principal. It never returns 401, so
// no anonymous test breaks. Mirrors vgi-python's _test_fixtures/http_server.py.
func optionalTestBearerAuthenticate() vgirpc.AuthenticateFunc {
	validate := vgirpc.BearerAuthenticateStatic(map[string]*vgirpc.AuthContext{
		"vgi-test-alice": {Principal: "alice", Authenticated: true, Domain: "bearer"},
		"vgi-test-bob":   {Principal: "bob", Authenticated: true, Domain: "bearer"},
	})
	return func(r *http.Request) (*vgirpc.AuthContext, error) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			return vgirpc.Anonymous(), nil
		}
		ctx, err := validate(r)
		if err != nil {
			return vgirpc.Anonymous(), nil
		}
		return ctx, nil
	}
}

// resolveBearerAuthenticate parses VGI_BEARER_TOKENS into a static bearer
// token authenticator. Format: "token=principal,token2=principal2".
func resolveBearerAuthenticate() vgirpc.AuthenticateFunc {
	raw := os.Getenv("VGI_BEARER_TOKENS")
	if raw == "" {
		return nil
	}

	tokens := make(map[string]*vgirpc.AuthContext)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if !strings.Contains(entry, "=") {
			log.Fatalf("Error: malformed VGI_BEARER_TOKENS entry: %q\nExpected format: token=principal (e.g. 'mytoken=alice')", entry)
		}
		// Split on first = only — principals may contain =
		token, principal, _ := strings.Cut(entry, "=")
		tokens[token] = &vgirpc.AuthContext{
			Principal:     principal,
			Authenticated: true,
			Domain:        "bearer",
		}
	}

	if len(tokens) == 0 {
		return nil
	}
	return vgirpc.BearerAuthenticateStatic(tokens)
}

// resolveJWTAuthenticate parses VGI_JWT_ISSUER, VGI_JWT_AUDIENCE, and
// optional VGI_JWT_JWKS_URI into a JWT authenticator.
func resolveJWTAuthenticate() (vgirpc.AuthenticateFunc, func()) {
	issuer := os.Getenv("VGI_JWT_ISSUER")
	if issuer == "" {
		return nil, nil
	}

	audienceRaw := os.Getenv("VGI_JWT_AUDIENCE")
	if audienceRaw == "" {
		log.Fatal("Error: VGI_JWT_ISSUER is set but VGI_JWT_AUDIENCE is missing")
	}

	var audiences []string
	for _, s := range strings.Split(audienceRaw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			audiences = append(audiences, s)
		}
	}
	if len(audiences) == 0 {
		log.Fatal("Error: VGI_JWT_AUDIENCE is set but contains no valid values")
	}

	jwksURI := os.Getenv("VGI_JWT_JWKS_URI")
	if jwksURI == "" {
		// Derive from issuer (matching Python behavior where jwks_uri is optional)
		jwksURI = strings.TrimSuffix(issuer, "/") + "/.well-known/jwks.json"
	}

	authFn, cleanup, err := jwtauth.NewAuthenticateFunc(jwtauth.JWTAuthConfig{
		Issuer:   issuer,
		Audience: audiences,
		JWKSURI:  jwksURI,
	})
	if err != nil {
		log.Fatalf("Error: failed to initialize JWT auth: %v", err)
	}
	return authFn, cleanup
}

// resolveOAuthResourceMetadata parses VGI_OAUTH_* env vars into an
// OAuthResourceMetadata for RFC 9728 discovery.
func resolveOAuthResourceMetadata() *vgirpc.OAuthResourceMetadata {
	resource := os.Getenv("VGI_OAUTH_RESOURCE")
	if resource == "" {
		return nil
	}

	authServersRaw := os.Getenv("VGI_OAUTH_AUTH_SERVERS")
	if authServersRaw == "" {
		log.Fatal("Error: VGI_OAUTH_RESOURCE is set but VGI_OAUTH_AUTH_SERVERS is missing")
	}

	var authServers []string
	for _, s := range strings.Split(authServersRaw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			authServers = append(authServers, s)
		}
	}

	var scopes []string
	if scopesRaw := os.Getenv("VGI_OAUTH_SCOPES"); scopesRaw != "" {
		for _, s := range strings.Split(scopesRaw, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				scopes = append(scopes, s)
			}
		}
	}

	useIDToken := false
	if v := strings.ToLower(os.Getenv("VGI_OAUTH_USE_ID_TOKEN")); v == "1" || v == "true" || v == "yes" {
		useIDToken = true
	}

	m := &vgirpc.OAuthResourceMetadata{
		Resource:               resource,
		AuthorizationServers:   authServers,
		ScopesSupported:        scopes,
		ResourceName:           os.Getenv("VGI_OAUTH_RESOURCE_NAME"),
		ClientID:               os.Getenv("VGI_OAUTH_CLIENT_ID"),
		ClientSecret:           os.Getenv("VGI_OAUTH_CLIENT_SECRET"),
		DeviceCodeClientID:     os.Getenv("VGI_OAUTH_DEVICE_CODE_CLIENT_ID"),
		DeviceCodeClientSecret: os.Getenv("VGI_OAUTH_DEVICE_CODE_CLIENT_SECRET"),
		UseIDTokenAsBearer:     useIDToken,
	}

	if err := m.Validate(); err != nil {
		log.Fatalf("Error: invalid OAuth config: %v", err)
	}
	return m
}
