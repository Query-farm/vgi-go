// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package versioned_tables implements the shared version-spec resolver and
// scan functions used by the vgi-example-versioned-tables-worker binary.
package versioned_tables

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

// Resolve resolves an npm-style version spec against a supported set, matching
// vgi-python's _resolve_against. Accepts exact X.Y.Z, bare X, bare X.Y,
// ^X.Y.Z, ~X.Y.Z. Empty/nil spec returns the default. The label is
// interpolated into error messages ("data_version_spec" / "implementation_version")
// so tests can distinguish failure kinds.
func Resolve(spec string, supported []string, dflt string, label string) (string, error) {
	if spec == "" {
		return dflt, nil
	}
	sorted := make([]parsedVersion, 0, len(supported))
	for _, v := range supported {
		p, err := parseVersion(v)
		if err != nil {
			return "", err
		}
		sorted = append(sorted, parsedVersion{p, v})
	}
	sort.Slice(sorted, func(i, j int) bool { return compareTuples(sorted[i].t, sorted[j].t) < 0 })

	// Exact X.Y.Z
	if exactRe.MatchString(spec) {
		for _, s := range supported {
			if s == spec {
				return spec, nil
			}
		}
		return "", fmt.Errorf("Unsupported %s %q; this worker serves %v", label, spec, supported)
	}
	// Bare major
	if m := majorRe.FindStringSubmatch(spec); m != nil {
		major, _ := strconv.Atoi(m[1])
		var candidates []string
		for _, sv := range sorted {
			if sv.t[0] == major {
				candidates = append(candidates, sv.raw)
			}
		}
		if len(candidates) == 0 {
			return "", fmt.Errorf("Unsupported %s %q; no major %d version available", label, spec, major)
		}
		return candidates[len(candidates)-1], nil
	}
	// Bare major.minor → pinned to X.Y.0
	if m := majorMinorRe.FindStringSubmatch(spec); m != nil {
		pinned := fmt.Sprintf("%s.%s.0", m[1], m[2])
		for _, s := range supported {
			if s == pinned {
				return pinned, nil
			}
		}
		return "", fmt.Errorf("Unsupported %s %q; %q not in %v", label, spec, pinned, supported)
	}
	// Caret ^X.Y.Z
	if m := caretRe.FindStringSubmatch(spec); m != nil {
		base := tupleFromRegex(m)
		var candidates []string
		for _, sv := range sorted {
			if sv.t[0] == base[0] && compareTuples(sv.t, base) >= 0 {
				candidates = append(candidates, sv.raw)
			}
		}
		if len(candidates) == 0 {
			return "", fmt.Errorf("Unsupported %s %q; no match in major %d", label, spec, base[0])
		}
		return candidates[len(candidates)-1], nil
	}
	// Tilde ~X.Y.Z
	if m := tildeRe.FindStringSubmatch(spec); m != nil {
		base := tupleFromRegex(m)
		var candidates []string
		for _, sv := range sorted {
			if sv.t[0] == base[0] && sv.t[1] == base[1] && compareTuples(sv.t, base) >= 0 {
				candidates = append(candidates, sv.raw)
			}
		}
		if len(candidates) == 0 {
			return "", fmt.Errorf("Unsupported %s %q; no match in %d.%d.x", label, spec, base[0], base[1])
		}
		return candidates[len(candidates)-1], nil
	}
	return "", fmt.Errorf("Unsupported %s %q; accepted forms: X.Y.Z, X, X.Y, ^X.Y.Z, ~X.Y.Z", label, spec)
}

var (
	exactRe      = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)$`)
	majorRe      = regexp.MustCompile(`^(\d+)$`)
	majorMinorRe = regexp.MustCompile(`^(\d+)\.(\d+)$`)
	caretRe      = regexp.MustCompile(`^\^(\d+)\.(\d+)\.(\d+)$`)
	tildeRe      = regexp.MustCompile(`^~(\d+)\.(\d+)\.(\d+)$`)
)

type parsedVersion struct {
	t   [3]int
	raw string
}

func parseVersion(v string) ([3]int, error) {
	m := exactRe.FindStringSubmatch(v)
	if m == nil {
		return [3]int{}, fmt.Errorf("not a valid version: %q", v)
	}
	a, _ := strconv.Atoi(m[1])
	b, _ := strconv.Atoi(m[2])
	c, _ := strconv.Atoi(m[3])
	return [3]int{a, b, c}, nil
}

func compareTuples(a, b [3]int) int {
	for i := 0; i < 3; i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

func tupleFromRegex(m []string) [3]int {
	a, _ := strconv.Atoi(m[1])
	b, _ := strconv.Atoi(m[2])
	c, _ := strconv.Atoi(m[3])
	return [3]int{a, b, c}
}
