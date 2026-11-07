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

// SecretType returns the DuckDB secret type of the named secret: its serialized
// "type" field, or -- for a secret serialized without one -- the type implied by
// its column name (see secretTypeOf). ok is false when no such secret exists.
func (s Secrets) SecretType(name string) (string, bool) {
	fields, ok := s[name]
	if !ok {
		return "", false
	}
	return secretTypeOf(name, fields), true
}

// defaultSecretPrefix is the prefix DuckDB gives an unnamed secret: an unnamed
// CREATE SECRET (TYPE brave, ...) is stored as "__default_brave".
const defaultSecretPrefix = "__default_"

// secretTypeOf returns the DuckDB secret type of a resolved secret. The extension
// keys each secret by its unique DuckDB NAME, not its type, and serializes the
// type in the "type" field, so that field is authoritative. Only when it is
// absent (an older extension) does the column name stand in: "__default_<type>"
// for an unnamed secret, otherwise the name itself.
func secretTypeOf(name string, fields map[string]interface{}) string {
	if v, ok := fields["type"]; ok && v != nil {
		return RenderSecretValue(v)
	}
	return strings.TrimPrefix(name, defaultSecretPrefix)
}

// secretScopes returns a secret's scope prefixes (the connector serializes them
// newline-joined); nil means the secret is unscoped.
func secretScopes(fields map[string]interface{}) []string {
	var out []string
	for _, prefix := range strings.Split(RenderSecretValue(fields["scope"]), "\n") {
		if prefix != "" {
			out = append(out, prefix)
		}
	}
	return out
}

// namesOfType returns the names of the secrets of secretType, sorted, so every
// selector is deterministic (Go map iteration is not).
func (s Secrets) namesOfType(secretType string) []string {
	var names []string
	for name, fields := range s {
		if secretType == "" || secretTypeOf(name, fields) == secretType {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// OfType returns every resolved secret of secretType, ordered by secret name.
// The type is matched against each secret's serialized "type" field -- never the
// column name, which is the secret's DuckDB name: an unnamed secret arrives as
// "__default_<type>", and a named one under whatever name the user chose.
func (s Secrets) OfType(secretType string) []map[string]interface{} {
	names := s.namesOfType(secretType)
	if len(names) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(names))
	for _, name := range names {
		out = append(out, s[name])
	}
	return out
}

// Lookup selects the secret of secretType the way the client's own by-type
// lookup does: a secret of that type with an empty scope, failing that the first
// secret of that type (by name). ok is false when none of that type was resolved.
// This is the selector for a requirement declared with a type and no scope.
//
//	creds, ok := params.Secrets.Lookup("brave")
//	if ok { apiKey := vgi.RenderSecretValue(creds["api_key"]) }
func (s Secrets) Lookup(secretType string) (map[string]interface{}, bool) {
	names := s.namesOfType(secretType)
	for _, name := range names {
		if len(secretScopes(s[name])) == 0 {
			return s[name], true
		}
	}
	if len(names) > 0 {
		return s[names[0]], true
	}
	return nil, false
}

// LookupNamed selects the secret called name, but only if it is of secretType
// (an empty secretType accepts any type). ok is false when no secret of that
// name was resolved or it has another type.
func (s Secrets) LookupNamed(secretType, name string) (map[string]interface{}, bool) {
	fields, ok := s[name]
	if !ok {
		return nil, false
	}
	if secretType != "" && secretTypeOf(name, fields) != secretType {
		return nil, false
	}
	return fields, true
}

// ForScope returns the fields of the resolved secret (of any type) whose scope
// has the longest prefix of path; failing that, an unscoped secret. Prefer
// ForScopeOfType, which restricts the candidates to one secret type.
// ok is false when no secret's scope matches and none is unscoped.
func (s Secrets) ForScope(path string) (map[string]interface{}, bool) {
	return s.selectForScope(path, "")
}

// ForScopeOfType selects, among the secrets of secretType, the one whose scope
// (newline-joined prefixes) has the longest prefix of path; failing that, one
// with an empty scope; failing that, none (ok false). Use it when a function
// requested several secrets of one type for different scopes (e.g. one per
// cloud bucket) and must pick the right one per path.
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
	var best, fallback map[string]interface{}
	bestLen := -1
	for _, name := range s.namesOfType(secretType) {
		fields := s[name]
		scopes := secretScopes(fields)
		if len(scopes) == 0 {
			if fallback == nil {
				fallback = fields
			}
			continue
		}
		for _, prefix := range scopes {
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

// Require returns the fields (rendered to strings) of the resolved secret of
// secretType that Lookup selects (an unscoped one, else the first by name), or a
// *MissingSecretError when none was resolved. Use ForScopeOfType to choose by
// path instead.
//
//	creds, err := params.Secrets.Require("s3")
//	if err != nil {
//	    return nil, err // "no s3 secret found: ..."
//	}
//	keyID := creds["key_id"]
func (s Secrets) Require(secretType string) (map[string]string, error) {
	fields, ok := s.Lookup(secretType)
	if !ok {
		return nil, &MissingSecretError{SecretType: secretType}
	}
	out := make(map[string]string, len(fields))
	for k, v := range fields {
		out[k] = RenderSecretValue(v)
	}
	return out, nil
}

// validateSecretRequirements rejects a requirement the client could not resolve.
// A SecretRequirement without a SecretType used to be serialized as-is, so the
// extension never asked DuckDB for anything and the function silently ran with no
// secret; it is now an error naming the function and the offending entry.
func validateSecretRequirements(function string, reqs []SecretRequirement) error {
	for i, rs := range reqs {
		if strings.TrimSpace(rs.SecretType) == "" {
			return fmt.Errorf("function %q: RequiredSecrets[%d] has no SecretType (name=%q, scope=%q); "+
				"every requirement must name the DuckDB secret type to resolve, e.g. vgi.SecretRequirement{SecretType: \"s3\"}",
				function, i, rs.SecretName, rs.Scope)
		}
	}
	return nil
}

// validateSecretLookups is validateSecretRequirements for a two-phase bind's
// SecretScopeRequest.
func validateSecretLookups(function string, lookups []SecretLookup) error {
	for i, sl := range lookups {
		if strings.TrimSpace(sl.SecretType) == "" {
			return fmt.Errorf("function %q: SecretScopeRequest[%d] has no SecretType (name=%q, scope=%q)",
				function, i, sl.SecretName, sl.Scope)
		}
	}
	return nil
}
