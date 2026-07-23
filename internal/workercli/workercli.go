// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package workercli is the shared command-line front end for the example
// worker binaries: the standard transport flags (--http / --unix / --tcp /
// --idle-timeout), the launcher-tolerant argv parse, and the serve dispatch.
//
// Every fixture worker needs --unix, not just the main one: the integration
// suite's launcher lane fronts EVERY VGI_*_WORKER with `launch:`, and the C++
// launcher spawns the binary with `--unix <path> --idle-timeout <n>` and waits
// for it to print `UNIX:<path>`. A worker that ignores --unix (as the versioned
// / versioned-tables / attach-options / simple-writable workers once did) exits
// on stdin EOF and the ATTACH fails with "worker exited before emitting
// UNIX:<path>".
package workercli

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/internal/covflush"
	"github.com/Query-farm/vgi-go/vgi"
)

// Flags holds the transport flags registered on flag.CommandLine.
type Flags struct {
	// HTTP reports whether --http was given. Exported because a worker may
	// need to install HTTP-only hooks (auth, OAuth metadata) before serving.
	HTTP *bool

	unixPath *string
	tcpAddr  *string
	idle     *float64
	logFlags *vgi.LoggingFlags
}

// Register defines the standard worker transport + logging flags on
// flag.CommandLine. Call before Parse.
func Register() *Flags {
	f := &Flags{}
	f.HTTP = flag.Bool("http", false, "Run as HTTP server instead of stdio")
	f.unixPath = flag.String("unix", "", "Bind to this AF_UNIX socket path (launcher transport); mutually exclusive with --http")
	f.tcpAddr = flag.String("tcp", "", "Bind a raw TCP socket ([HOST:]PORT, host defaults to 127.0.0.1, port 0 auto-selects); mutually exclusive with --http/--unix")
	f.idle = flag.Float64("idle-timeout", 300, "Self-shutdown after N seconds idle when serving --unix/--tcp (0 = never)")
	// --describe / --no-describe: accepted for launcher compatibility (the VGI
	// extension passes it through). Description pages aren't served over the
	// socket/stdio transports, so they are a no-op here.
	flag.Bool("describe", true, "Enable description pages (accepted for launcher compatibility)")
	flag.Bool("no-describe", false, "Disable description pages (accepted for launcher compatibility)")
	f.logFlags = vgi.RegisterLoggingFlags(flag.CommandLine)
	return f
}

// Parse parses args (normally os.Args[1:]) tolerantly, applies the logging
// flags, and starts coverage flushing. Returns an error for a bad flag value
// or mutually exclusive transports.
func (f *Flags) Parse(args []string) error {
	// The launcher varies a worker's argv (e.g. --threaded, --quiet) to produce
	// distinct cache keys for the same binary; tolerate unknown flags rather
	// than failing to start. Flags named here consume a value token.
	if err := flag.CommandLine.Parse(FilterKnownFlags(args, map[string]bool{
		"unix":         true,
		"tcp":          true,
		"idle-timeout": true,
		"log-level":    true,
		"log-format":   true,
		"log-logger":   true,
	})); err != nil {
		return err
	}
	if err := f.logFlags.Apply(); err != nil {
		return fmt.Errorf("logging flags: %w", err)
	}
	n := 0
	for _, on := range []bool{*f.unixPath != "", *f.tcpAddr != "", *f.HTTP} {
		if on {
			n++
		}
	}
	if n > 1 {
		return fmt.Errorf("--unix, --tcp, and --http are mutually exclusive")
	}
	// Flush coverage on SIGTERM (+ periodic) during integration coverage runs
	// (no-op otherwise); the harness kills pooled/long-lived workers with SIGTERM.
	covflush.Start()
	return nil
}

// Serve runs w on the transport the flags selected, blocking until it stops.
// The default is stdio.
func (f *Flags) Serve(w *vgi.Worker) error {
	switch {
	case *f.unixPath != "":
		return w.RunUnix(*f.unixPath, f.idleDuration())
	case *f.tcpAddr != "":
		host, port, err := ParseTCPAddr(*f.tcpAddr)
		if err != nil {
			return err
		}
		return w.RunTcp(host, port, f.idleDuration())
	case *f.HTTP:
		return w.RunHttp("127.0.0.1:0")
	default:
		w.RunStdio()
		return nil
	}
}

func (f *Flags) idleDuration() time.Duration {
	return time.Duration(*f.idle * float64(time.Second))
}

// ParseTCPAddr splits a [HOST:]PORT spec, defaulting the host to 127.0.0.1.
func ParseTCPAddr(spec string) (string, int, error) {
	host := "127.0.0.1"
	portStr := spec
	if i := strings.LastIndex(spec, ":"); i >= 0 {
		if spec[:i] != "" {
			host = spec[:i]
		}
		portStr = spec[i+1:]
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("--tcp expects [HOST:]PORT, got %q", spec)
	}
	return host, port, nil
}

// FilterKnownFlags drops command-line tokens for flags this binary doesn't
// define, so launcher-injected argv-differentiation flags (e.g. --threaded,
// --quiet) don't abort flag parsing. Flags named in valueFlags consume the
// following token as their value (when not given in --flag=value form); all
// other recognized flags are treated as valueless. Unknown flags and stray
// positionals are dropped.
func FilterKnownFlags(args []string, valueFlags map[string]bool) []string {
	defined := map[string]bool{}
	flag.CommandLine.VisitAll(func(f *flag.Flag) { defined[f.Name] = true })
	var out []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			continue // stray positional
		}
		name := strings.TrimLeft(a, "-")
		hasInlineValue := strings.ContainsRune(name, '=')
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			name = name[:eq]
		}
		if !defined[name] {
			continue // unknown flag — ignore
		}
		out = append(out, a)
		if valueFlags[name] && !hasInlineValue && i+1 < len(args) {
			i++
			out = append(out, args[i])
		}
	}
	return out
}
