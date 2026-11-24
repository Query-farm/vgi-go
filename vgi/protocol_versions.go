// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

// Protocol version negotiation (vgi-rpc-python
// tools/cross-port/specs/PROTOCOL_VERSIONS_V1.md). vgi-rpc does the gating and
// advertising; what the SDK owns is which protocols are hosted (each declares
// its own version, so each is advertised) and the fixture switch below.

import (
	"fmt"
	"os"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// TestProtocolRangeEnv is the fixture switch (PROTOCOL_VERSIONS_V1 §9): make a
// worker advertise and accept a different version window, so the extension's
// tests can stand up a "fake older worker" from any fixture. A bare range
// ("3.3-3.3") applies to vgi.v3; "name=MIN-MAX[, ...]" names protocols. A
// malformed value refuses to start: a switch that silently did nothing would
// make the test it serves pass for the wrong reason.
const TestProtocolRangeEnv = "VGI_TEST_PROTOCOL_RANGE"

// testProtocolRanges reads [TestProtocolRangeEnv]; empty when unset.
func testProtocolRanges(getenv func(string) string) (map[string]string, error) {
	raw := strings.TrimSpace(getenv(TestProtocolRangeEnv))
	if raw == "" {
		return nil, nil
	}
	if strings.Contains(raw, "=") {
		parsed := vgirpc.ParseProtocolVersions(raw)
		entries := 0
		for _, e := range strings.Split(raw, ",") {
			if strings.TrimSpace(e) != "" {
				entries++
			}
		}
		if len(parsed) != entries {
			return nil, fmt.Errorf("%s=%q: expected name=MAJ.MIN-MAJ.MIN[, ...]", TestProtocolRangeEnv, raw)
		}
		out := make(map[string]string, len(parsed))
		for name, rng := range parsed {
			out[name] = rng.String()
		}
		return out, nil
	}
	rng, err := vgirpc.ParseVersionRange(raw)
	if err != nil {
		return nil, fmt.Errorf("%s=%q: expected MAJ.MIN-MAJ.MIN (%v)", TestProtocolRangeEnv, raw, err)
	}
	return map[string]string{ProtocolName: rng.String()}, nil
}

// applyTestProtocolRanges applies [TestProtocolRangeEnv] to s, after every
// protocol is hosted.
func applyTestProtocolRanges(s *vgirpc.Server) error {
	ranges, err := testProtocolRanges(os.Getenv)
	if err != nil || len(ranges) == 0 {
		return err
	}
	if err := s.SetProtocolVersionRanges(ranges); err != nil {
		return fmt.Errorf("%s: %w", TestProtocolRangeEnv, err)
	}
	LogRPC.Warn(TestProtocolRangeEnv+" is set: advertising and accepting these windows instead of the "+
		"declared protocol versions (test fixture switch)", "ranges", ranges)
	return nil
}
