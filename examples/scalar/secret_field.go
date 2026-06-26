// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// SecretFieldFunction exercises the named/positional secret-field accessors over
// the resolved vgi_example secret: a named field on a specific secret (port) and
// the first secret of any name carrying a field (secret_string).
type SecretFieldFunction struct{}

func (f *SecretFieldFunction) Name() string { return "secret_field" }

func (f *SecretFieldFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Look up secret fields by name",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: "vgi_example"},
		},
	}
}

func (f *SecretFieldFunction) ArgumentSpecs() []vgi.ArgSpec {
	return nil
}

func (f *SecretFieldFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *SecretFieldFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	port := namedSecretField(params.Secrets, "vgi_example", "port")
	name := anySecretField(params.Secrets, "secret_string")
	s := fmt.Sprintf("port=%s;name=%s", port, name)

	return vgi.GenerateColumn(params, batch, array.NewStringBuilder,
		func(i int) string { return s })
}

// namedSecretField looks up a field on the first secret of secretType (secrets
// are keyed by name, so select by type), rendering its value to a string.
// Returns "" when the secret or field is absent.
func namedSecretField(secrets vgi.Secrets, secretType, field string) string {
	for _, fields := range secrets.OfType(secretType) {
		if v, ok := fields[field]; ok {
			return renderSecretValue(v)
		}
	}
	return ""
}

// anySecretField returns the first secret (of any type) carrying the named
// field, rendered to a string. Returns "" when no secret carries it.
func anySecretField(secrets map[string]map[string]interface{}, field string) string {
	for _, fields := range secrets {
		if v, ok := fields[field]; ok {
			return renderSecretValue(v)
		}
	}
	return ""
}

// renderSecretValue renders a secret field value (which may be numeric, bool, or
// string) to its string form.
func renderSecretValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}
