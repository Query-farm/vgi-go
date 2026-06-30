// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package copy_to

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// secretShardNS is the state-log key the secret writer counts shards under.
var secretShardNS = []byte("copy_to_secret_shard")

// secretLinesOutArgs are the COPY options for the secret_lines_out format. The
// destination file_path is supplied by the COPY statement, never as an option.
type secretLinesOutArgs struct {
	SecretType string `vgi:"default=vgi_example,doc=Secret type to fetch, scoped by the destination path"`
}

// SecretLinesCopyToFunction is a COPY ... TO writer that forwards a CREATE SECRET
// credential. It exercises the COPY-TO secret-bind hook (CopyToSecretProvider):
// it requests the secret_type secret scoped to the destination path during bind,
// and Close writes the resolved secret's api_key (or NONE) plus the row count —
// so a test can assert the caller's secret reached the writer for a secret-backed
// cloud write. Mirrors vgi-python's SecretLinesCopyToFunction.
type SecretLinesCopyToFunction struct{}

var (
	_ vgi.CopyToFunction       = (*SecretLinesCopyToFunction)(nil)
	_ vgi.CopyToCommenter      = (*SecretLinesCopyToFunction)(nil)
	_ vgi.CopyToSecretProvider = (*SecretLinesCopyToFunction)(nil)
)

func (f *SecretLinesCopyToFunction) Name() string { return "secret_lines_writer" }

func (f *SecretLinesCopyToFunction) CopyToFormat() string { return "secret_lines_out" }

func (f *SecretLinesCopyToFunction) CopyToComment() string {
	return "Writer that forwards a CREATE SECRET credential (test fixture)"
}

func (f *SecretLinesCopyToFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Write the resolved secret's api_key + row count to the destination",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"copy", "test", "secret"},
		Tags:        map[string]string{"category": "copy_to", "stability": "test"},
	}
}

func (f *SecretLinesCopyToFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(secretLinesOutArgs{})
}

// secretType resolves the secret_type option (default vgi_example).
func secretLinesOutType(args *vgi.Arguments) string {
	var opts secretLinesOutArgs
	_ = vgi.BindArgs(args, &opts)
	if opts.SecretType == "" {
		return "vgi_example"
	}
	return opts.SecretType
}

// SecretLookups requests the destination-scoped secret; the framework's two-phase
// secret bind resolves it and surfaces it on params.Secrets at Close time.
func (f *SecretLinesCopyToFunction) SecretLookups(params *vgi.BindParams) []vgi.SecretLookup {
	if params.CopyTo == nil {
		return nil
	}
	return []vgi.SecretLookup{{
		SecretType: secretLinesOutType(params.Args),
		Scope:      params.CopyTo.FilePath,
	}}
}

// Write records this shard's row count (cross-process-safe append).
func (f *SecretLinesCopyToFunction) Write(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) error {
	_, err := params.Storage.StateAppend(secretShardNS, []byte(strconv.FormatInt(batch.NumRows(), 10)))
	return err
}

// Close writes the forwarded secret's api_key + total row count, once.
func (f *SecretLinesCopyToFunction) Close(ctx context.Context, params *vgi.ProcessParams) error {
	if params.CopyTo == nil {
		return fmt.Errorf("secret_lines_out: missing COPY TO context at close")
	}
	path := params.CopyTo.FilePath

	apiKey := "NONE"
	if fields, ok := params.Secrets.ForScopeOfType(path, secretLinesOutType(params.Args)); ok {
		if v, ok := fields["api_key"]; ok {
			apiKey = vgi.RenderSecretValue(v)
		}
	}

	entries, err := params.Storage.StateLogScan(secretShardNS, -1, 0)
	if err != nil {
		return err
	}
	var total int64
	for _, e := range entries {
		n, _ := strconv.ParseInt(string(e.Value), 10, 64)
		total += n
	}

	fh, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("secret_lines_out: creating %q: %w", path, err)
	}
	_, werr := fmt.Fprintf(fh, "api_key=%s\nrows=%d\n", apiKey, total)
	if cerr := fh.Close(); cerr != nil && werr == nil {
		werr = cerr
	}
	return werr
}
