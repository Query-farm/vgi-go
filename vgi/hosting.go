// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"os"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// What a worker's server hosts, in reflection order:
//
//  1. vgi.v2 -- the worker's own protocol.
//  2. The worker's hosted protocols (WithHostedProtocols), in the order the
//     hook returns them -- on EVERY transport.
//  3. vgi_rpc.Reflection.v1 -- on every transport.
//  4. vgi_rpc.Identity.v1 -- HTTP only, and only when WithIdentity supplies a
//     resolve and/or mint hook, or grant keys are configured.
//  5. vgi.attach_tickets.v1 -- HTTP only, and only when the signing key is
//     configured explicitly and the worker can issue grants (attach_ticket.go).
//
// Extra protocols cannot change vgi.v2's behaviour: vgi-rpc routes every
// request on its vgi_rpc.protocol key with no fallback to the primary, so a
// client that only ever names vgi.v2 (the DuckDB extension) dispatches exactly
// as it would against a single-protocol server.

// HostedProtocolsFunc returns the additional application protocols a worker
// hosts beside vgi.v2. Each is a protocol definition built with
// [vgirpc.NewProtocol] and its methods registered with [vgirpc.Unary] and
// friends -- the handlers are the implementation, so one value is the
// (protocol, implementation) pair.
//
// It is called once, when the worker's server is built, and may consult
// configuration or the environment; its result is hosted on every transport
// the worker serves and is fixed for the life of the process.
type HostedProtocolsFunc func() ([]*vgirpc.Server, error)

// WithHostedProtocols sets the hook returning the additional protocols this
// worker hosts. See [HostedProtocolsFunc].
//
// A returned protocol may not claim the reserved "vgi_rpc." prefix
// (reflection is hosted automatically; identity is enabled with
// [WithIdentity]), may not reuse vgi.v2 or another returned protocol's name,
// and must be a fresh value: building the server fails, naming this hook,
// otherwise.
func WithHostedProtocols(fn HostedProtocolsFunc) WorkerOption {
	return func(w *Worker) {
		w.hostedProtocols = fn
	}
}

// WithIdentity opts the worker into hosting vgi_rpc.Identity.v1 over HTTP.
//
// cfg.ResolveToken backs introspect_token and cfg.MintGrant backs issue_grant;
// a hook left nil leaves its method absent rather than hosted-and-refusing,
// and with neither the protocol is not hosted at all. Identity is hosted on
// HTTP only: its introspector allowlist is a list of principals, which only a
// transport that authenticates callers can check. Configure authentication
// with [Worker.SetAuthenticate].
//
// Introspection requires an allowlist of principals permitted to ask:
// cfg.IntrospectPrincipals, or else the comma-separated
// VGI_INTROSPECT_PRINCIPALS environment variable. There is no permissive
// default -- "any authenticated caller" lets any user resolve any other user's
// credential to its owner -- so a worker with a ResolveToken hook and no
// allowlist refuses to start.
//
// For "the answer is not knowable right now" (a store or sidecar is down),
// return [*vgirpc.AuthUnavailableError] with a RetryAfter -- the same error an
// authenticator returns for an outage -- or [*vgirpc.IdentityUnavailableError].
// The framework emits either as identity_unavailable (UNAVAILABLE) carrying
// the retry hint as RetryInfo, which tells a caller not to negative-cache the
// failure. Never return a plain *RpcError of type "ValueError" for an outage:
// ChainAuthenticate reads that as "not my credential, try the next".
func WithIdentity(cfg vgirpc.IdentityConfig) WorkerOption {
	return func(w *Worker) {
		c := cfg
		c.IntrospectPrincipals = append([]string(nil), cfg.IntrospectPrincipals...)
		w.identity = &c
	}
}

// WithGrantKeys turns on sealed grants with an explicit configuration
// ([vgirpc.NewGrantKeys], [vgirpc.ParseGrantKeys]). Without it the worker
// reads VGI_RPC_GRANT_KEYS, VGI_RPC_GRANT_AUDIENCE and
// VGI_RPC_GRANT_MAX_TTL_SECONDS; the example workers also accept repeated
// --grant-key flags. Nil turns grants off regardless of the environment.
//
// With keys configured, over HTTP the worker hosts vgi_rpc.Identity.v1's
// issue_grant -- minting sealed grants unless WithIdentity supplies MintGrant
// -- and accepts its own grants back as bearer credentials, after the
// authenticator set with SetAuthenticate. A grant authenticates as domain
// "grant" with no auth_time, so it can never mint another grant. Grants are
// not individually revocable: keep the max TTL short and remove a key to
// revoke everything it minted.
func WithGrantKeys(keys *vgirpc.GrantKeys) WorkerOption {
	return func(w *Worker) { w.SetGrantKeys(keys) }
}

// SetGrantKeys is [WithGrantKeys] for an already-built worker.
func (w *Worker) SetGrantKeys(keys *vgirpc.GrantKeys) {
	w.grantKeys = keys
	w.grantKeysSet = true
}

// resolveGrantKeys returns the explicit configuration, else the environment's.
// A malformed key is an error: the worker must not start with a key it
// misread.
func (w *Worker) resolveGrantKeys() (*vgirpc.GrantKeys, error) {
	if w.grantKeysSet {
		return w.grantKeys, nil
	}
	keys, err := vgirpc.GrantKeysFromEnv()
	if err != nil {
		return nil, fmt.Errorf("sealed grant keys: %w", err)
	}
	return keys, nil
}

// IntrospectPrincipalsEnv names the environment variable read for the
// introspector allowlist when WithIdentity supplies none.
const IntrospectPrincipalsEnv = "VGI_INTROSPECT_PRINCIPALS"

// hostProtocols calls the worker's hosted-protocols hook once and hosts the
// result on s, after validating it so a failure names the hook rather than a
// protocol the framework knows nothing about.
func (w *Worker) hostProtocols(s *vgirpc.Server) error {
	if w.hostedProtocols == nil {
		return nil
	}
	const owner = "vgi.WithHostedProtocols hook"
	protocols, err := w.hostedProtocols()
	if err != nil {
		return fmt.Errorf("%s: %w", owner, err)
	}
	seen := make(map[string]int, len(protocols))
	for i, p := range protocols {
		if p == nil {
			return fmt.Errorf("%s: entry %d is nil; build each protocol with vgirpc.NewProtocol", owner, i)
		}
		name := p.ServiceName()
		if name == "" {
			return fmt.Errorf("%s: entry %d has no name; build it with vgirpc.NewProtocol(name)", owner, i)
		}
		if strings.HasPrefix(name, "vgi_rpc.") {
			return fmt.Errorf(
				"%s: entry %d is named %q, which claims the reserved \"vgi_rpc.\" prefix. Framework protocols "+
					"are not supplied through this hook: reflection is hosted automatically, and "+
					"vgi_rpc.Identity.v1 is enabled with vgi.WithIdentity", owner, i, name)
		}
		if name == ProtocolName {
			return fmt.Errorf("%s: entry %d is named %q, the worker's own protocol; give it a distinct name",
				owner, i, name)
		}
		if prev, dup := seen[name]; dup {
			return fmt.Errorf("%s: entries %d and %d are both named %q; the name is the routing key, so each "+
				"hosted protocol needs a distinct one", owner, prev, i, name)
		}
		seen[name] = i
		if err := s.AddProtocol(p); err != nil {
			return fmt.Errorf("%s: entry %d (%q): %w", owner, i, name, err)
		}
	}
	return nil
}

// hostIdentity hosts vgi_rpc.Identity.v1 on s when the worker opted in --
// with WithIdentity hooks, or with sealed-grant keys alone, which host
// issue_grant minting sealed grants.
func (w *Worker) hostIdentity(s *vgirpc.Server, grantKeys *vgirpc.GrantKeys) error {
	var cfg vgirpc.IdentityConfig
	if w.identity != nil {
		cfg = *w.identity
	}
	if cfg.GrantKeys == nil {
		cfg.GrantKeys = grantKeys
	}
	if cfg.ResolveToken == nil && cfg.MintGrant == nil && cfg.GrantKeys == nil {
		return nil
	}
	if cfg.ResolveToken != nil && len(cfg.IntrospectPrincipals) == 0 {
		for _, p := range strings.Split(os.Getenv(IntrospectPrincipalsEnv), ",") {
			if p = strings.TrimSpace(p); p != "" {
				cfg.IntrospectPrincipals = append(cfg.IntrospectPrincipals, p)
			}
		}
		if len(cfg.IntrospectPrincipals) == 0 {
			return fmt.Errorf(
				"vgi.WithIdentity supplies ResolveToken but no introspector allowlist: set "+
					"IdentityConfig.IntrospectPrincipals or %s. There is no permissive default -- "+
					"authenticating and introspecting are different capabilities, and allowing any "+
					"authenticated caller lets any user resolve any other user's credential to its owner",
				IntrospectPrincipalsEnv)
		}
	}
	impl, err := vgirpc.NewIdentity(cfg)
	if err != nil {
		return fmt.Errorf("vgi.WithIdentity: %w", err)
	}
	if err := vgirpc.RegisterIdentity(s, impl); err != nil {
		return fmt.Errorf("vgi.WithIdentity: %w", err)
	}
	return nil
}
