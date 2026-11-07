package vgi

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Secrets holds the resolved secrets passed to a worker, keyed by each secret's
// unique DuckDB secret name (not by type), so several secrets of the same type
// (e.g. one per S3 bucket) coexist. Each secret is a map of its fields, including
// the connector-serialized "type" (the DuckDB secret type) and "scope"
// (newline-joined scope prefixes), plus type-specific fields like "key_id".
//
// Secrets is a plain map, so direct access (s[name][field]) still works; the
// methods below add type- and scope-aware selection.
type Secrets map[string]map[string]interface{}

// RenderSecretValue renders a secret field value to a string.
func RenderSecretValue(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Field returns the first secret (of any name) carrying field, rendered to a
// string; ok is false when no secret carries it.
func (s Secrets) Field(field string) (string, bool) {
	for _, fields := range s {
		if v, ok := fields[field]; ok {
			return RenderSecretValue(v), true
		}
	}
	return "", false
}

// NamedField returns a field of the named secret, rendered to a string.
func (s Secrets) NamedField(name, field string) (string, bool) {
	if fields, ok := s[name]; ok {
		if v, ok := fields[field]; ok {
			return RenderSecretValue(v), true
		}
	}
	return "", false
}

// SecretType returns the DuckDB secret type of the named secret (its serialized
// "type" field).
func (s Secrets) SecretType(name string) (string, bool) {
	return s.NamedField(name, "type")
}

// OfType returns every resolved secret whose serialized "type" field matches
// secretType (since secrets are keyed by name, not type).
func (s Secrets) OfType(secretType string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, fields := range s {
		if RenderSecretValue(fields["type"]) == secretType {
			out = append(out, fields)
		}
	}
	return out
}

// ForScope returns the fields of the resolved secret whose "scope" is the
// longest prefix of path. Use this when the worker requested secrets for several
// scopes (e.g. one per cloud path / bucket) and must pick the right one per path.
// The connector serializes each secret's scope as a newline-joined list of
// prefixes; a secret with no (or empty) scope matches as a last-resort fallback.
// ok is false only when there are no candidate secrets.
func (s Secrets) ForScope(path string) (map[string]interface{}, bool) {
	return s.selectForScope(path, "")
}

// ForScopeOfType is like ForScope but only over secrets of secretType — the
// precise selector for cloud paths (e.g. the s3 secret matching a given s3://…
// URL when several buckets are in play).
func (s Secrets) ForScopeOfType(path, secretType string) (map[string]interface{}, bool) {
	return s.selectForScope(path, secretType)
}

// FieldForScope returns a field of the best scope-matching secret for path.
func (s Secrets) FieldForScope(path, field string) (string, bool) {
	if fields, ok := s.ForScope(path); ok {
		if v, ok := fields[field]; ok {
			return RenderSecretValue(v), true
		}
	}
	return "", false
}

func (s Secrets) selectForScope(path, secretType string) (map[string]interface{}, bool) {
	var best map[string]interface{}
	bestLen := -1
	var fallback map[string]interface{}
	for _, fields := range s {
		if secretType != "" && RenderSecretValue(fields["type"]) != secretType {
			continue
		}
		scope := RenderSecretValue(fields["scope"])
		if scope == "" {
			if fallback == nil {
				fallback = fields
			}
			continue
		}
		for _, prefix := range strings.Split(scope, "\n") {
			if prefix == "" {
				continue
			}
			if strings.HasPrefix(path, prefix) && len(prefix) > bestLen {
				bestLen = len(prefix)
				best = fields
			}
		}
	}
	if best != nil {
		return best, true
	}
	if fallback != nil {
		return fallback, true
	}
	return nil, false
}

// MissingSecretError is returned by Secrets.Require when no secret of the
// requested type was resolved for the call -- typically because the user has
// not run CREATE SECRET. Listing a type in FunctionMetadata.RequiredSecrets
// only asks DuckDB to resolve it if present; it does not fail the call.
type MissingSecretError struct {
	// SecretType is the DuckDB secret type that was required (e.g. "s3").
	SecretType string
}

// Error implements the error interface.
func (e *MissingSecretError) Error() string {
	return fmt.Sprintf("no %s secret found: create one with CREATE SECRET (TYPE %s, ...)", e.SecretType, e.SecretType)
}

// ErrorCode is FAILED_PRECONDITION: the call is well-formed but the session
// lacks the secret it needs.
func (e *MissingSecretError) ErrorCode() vgirpc.Code { return vgirpc.CodeFailedPrecondition }

// Require returns the fields (rendered to strings) of a resolved secret of
// secretType, or a *MissingSecretError when none was resolved. With several
// secrets of the type, the one whose name sorts first is returned; use
// ForScopeOfType to choose by path instead.
//
//	creds, err := params.Secrets.Require("s3")
//	if err != nil {
//	    return nil, err // "no s3 secret found: ..."
//	}
//	keyID := creds["key_id"]
func (s Secrets) Require(secretType string) (map[string]string, error) {
	names := make([]string, 0, len(s))
	for name, fields := range s {
		if RenderSecretValue(fields["type"]) == secretType {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, &MissingSecretError{SecretType: secretType}
	}
	sort.Strings(names)
	fields := s[names[0]]
	out := make(map[string]string, len(fields))
	for k, v := range fields {
		out[k] = RenderSecretValue(v)
	}
	return out, nil
}
