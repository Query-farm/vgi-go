// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// LogFormat selects the stderr log format.
type LogFormat string

const (
	// LogFormatText selects the human-readable text log format.
	LogFormatText LogFormat = "text"
	// LogFormatJSON selects the structured JSON log format.
	LogFormatJSON LogFormat = "json"
)

// Known logger names. Mirrors vgi-python's vgi.logging_config registry so the
// --log-logger flag accepts the same identifiers across the two SDKs.
const (
	LoggerName               = "vgi"
	LoggerNameWorker         = "vgi.worker"
	LoggerNameCatalog        = "vgi.catalog"
	LoggerNameRPC            = "vgi.rpc"
	LoggerNameClient         = "vgi.client"
	LoggerNameFilterPushdown = "vgi.filter_pushdown"
)

// KnownLoggers lists the named loggers the SDK emits records on, in the order
// they should appear in --help output.
var KnownLoggers = []struct {
	Name        string
	Description string
}{
	{LoggerName, "VGI root logger"},
	{LoggerNameWorker, "Worker lifecycle (startup, shutdown, storage)"},
	{LoggerNameCatalog, "Catalog operations (list, attach, table_get)"},
	{LoggerNameRPC, "RPC dispatch (bind, init, protocol)"},
	{LoggerNameClient, "Client operations"},
	{LoggerNameFilterPushdown, "Filter pushdown debug"},
}

// Named loggers. ConfigureLogging() rebinds these to the configured root
// handler. Until then, every logger writes through slog.Default(). Use these
// instead of slog.Debug/Info/... directly so per-subsystem filtering works.
var (
	Log                = slog.Default()
	LogWorker          = Log.With("logger", LoggerNameWorker)
	LogCatalog         = Log.With("logger", LoggerNameCatalog)
	LogRPC             = Log.With("logger", LoggerNameRPC)
	LogClient          = Log.With("logger", LoggerNameClient)
	LogFilterPushdown  = Log.With("logger", LoggerNameFilterPushdown)
	loggerRegistryLock sync.Mutex
)

// LoggingConfig describes the desired logging setup for a worker.
type LoggingConfig struct {
	// Level is the minimum level for enabled loggers. Default Info.
	Level slog.Level
	// Format selects the stderr formatter. Default text.
	Format LogFormat
	// Output is where records are written. Default os.Stderr.
	Output io.Writer
	// Loggers restricts which named loggers emit records. Empty means
	// "all known loggers". Unknown names are passed through with a warning
	// on stderr (matching vgi-python's behaviour).
	Loggers []string
	// Debug forces Level to Debug regardless of the explicit Level value.
	// Mirrors vgi-python's --debug shortcut.
	Debug bool
}

// effectiveLevel returns the level after applying the Debug shortcut.
func (c LoggingConfig) effectiveLevel() slog.Level {
	if c.Debug {
		return slog.LevelDebug
	}
	return c.Level
}

// loggerFilterHandler wraps a base handler and drops records emitted by a
// named logger (logger=...) that is not in the enabled set. The name is
// captured when a logger is constructed via slog.New(...).With("logger", N),
// which routes through WithAttrs — we extract and remember the value, and
// also keep checking the per-record attributes so direct calls like
// slog.Info("...", "logger", "vgi.foo") still filter correctly.
//
// Records without any "logger" attribute always pass — they originate from
// code paths not migrated to the named loggers and we prefer not to silently
// swallow them.
type loggerFilterHandler struct {
	base       slog.Handler
	enabled    map[string]struct{}
	loggerName string // captured via WithAttrs (slog.With("logger", X))
}

// Enabled reports whether the underlying handler is enabled for the level;
// per-logger filtering happens later, in Handle.
func (h *loggerFilterHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.base.Enabled(ctx, lvl)
}

func (h *loggerFilterHandler) allow(name string) bool {
	for n := name; n != ""; n = parentLogger(n) {
		if _, ok := h.enabled[n]; ok {
			return true
		}
	}
	return false
}

// Handle drops the record when its logger name is not in the enabled set,
// otherwise forwards it to the base handler.
func (h *loggerFilterHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.enabled == nil {
		return h.base.Handle(ctx, r)
	}
	name := h.loggerName
	if name == "" {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "logger" {
				name = a.Value.String()
				return false
			}
			return true
		})
	}
	if name == "" {
		return h.base.Handle(ctx, r)
	}
	if !h.allow(name) {
		return nil
	}
	return h.base.Handle(ctx, r)
}

// WithAttrs returns a copy capturing any "logger" attribute so the name is
// known when filtering records.
func (h *loggerFilterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	name := h.loggerName
	for _, a := range attrs {
		if a.Key == "logger" {
			name = a.Value.String()
		}
	}
	return &loggerFilterHandler{base: h.base.WithAttrs(attrs), enabled: h.enabled, loggerName: name}
}

// WithGroup returns a copy with the group applied to the base handler,
// preserving the captured logger name and enabled set.
func (h *loggerFilterHandler) WithGroup(name string) slog.Handler {
	return &loggerFilterHandler{base: h.base.WithGroup(name), enabled: h.enabled, loggerName: h.loggerName}
}

// parentLogger returns the dotted parent of name, or "" at the root.
// "vgi.catalog" → "vgi"; "vgi" → "".
func parentLogger(name string) string {
	i := strings.LastIndex(name, ".")
	if i < 0 {
		return ""
	}
	return name[:i]
}

// ConfigureLogging installs a fresh root handler reflecting cfg and rebinds the
// package-level named loggers. Subsequent calls replace the configuration.
// Safe to call from main() before vgi.NewWorker(...).
func ConfigureLogging(cfg LoggingConfig) {
	loggerRegistryLock.Lock()
	defer loggerRegistryLock.Unlock()

	if cfg.Output == nil {
		cfg.Output = os.Stderr
	}
	level := cfg.effectiveLevel()
	opts := &slog.HandlerOptions{Level: level}

	var base slog.Handler
	switch cfg.Format {
	case LogFormatJSON:
		base = slog.NewJSONHandler(cfg.Output, opts)
	default:
		base = slog.NewTextHandler(cfg.Output, opts)
	}

	if len(cfg.Loggers) > 0 {
		enabled := make(map[string]struct{}, len(cfg.Loggers))
		known := map[string]struct{}{}
		for _, k := range KnownLoggers {
			known[k.Name] = struct{}{}
		}
		for _, name := range cfg.Loggers {
			if _, ok := known[name]; !ok {
				fmt.Fprintf(os.Stderr, "warning: unknown logger %q\n", name)
			}
			enabled[name] = struct{}{}
		}
		base = &loggerFilterHandler{base: base, enabled: enabled}
	}

	root := slog.New(base)
	slog.SetDefault(root)

	Log = root
	LogWorker = root.With("logger", LoggerNameWorker)
	LogCatalog = root.With("logger", LoggerNameCatalog)
	LogRPC = root.With("logger", LoggerNameRPC)
	LogClient = root.With("logger", LoggerNameClient)
	LogFilterPushdown = root.With("logger", LoggerNameFilterPushdown)
}

// ParseLogLevel parses a level string ("debug", "info", "warn", "error", any
// case). Returns slog.LevelInfo for empty input. Returns an error on unknown
// values.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("unknown log level %q (want debug|info|warn|error)", s)
}

// LoggingFlags holds the values parsed from the standard logging CLI flags.
// Resolve it into a LoggingConfig with .Config().
type LoggingFlags struct {
	debug     *bool
	level     *string
	format    *string
	loggers   *string
	loggerSet *stringSliceFlag
}

// RegisterLoggingFlags registers --debug, --log-level, --log-format, and
// --log-logger on the given FlagSet. The returned LoggingFlags resolves to a
// LoggingConfig after Parse() runs. Env-var defaults: VGI_LOG_LEVEL,
// VGI_LOG_FORMAT, VGI_LOG_LOGGER. VGI_WORKER_DEBUG=1 enables --debug.
func RegisterLoggingFlags(fs *flag.FlagSet) *LoggingFlags {
	envDebug := envBool("VGI_WORKER_DEBUG")
	envLevel := os.Getenv("VGI_LOG_LEVEL")
	envFormat := os.Getenv("VGI_LOG_FORMAT")
	envLogger := os.Getenv("VGI_LOG_LOGGER")
	if envLevel == "" {
		envLevel = "info"
	}
	if envFormat == "" {
		envFormat = "text"
	}

	lf := &LoggingFlags{}
	lf.debug = fs.Bool("debug", envDebug, "Enable DEBUG on all vgi loggers (overrides --log-level)")
	lf.level = fs.String("log-level", envLevel, "Log level: debug, info, warn, error")
	lf.format = fs.String("log-format", envFormat, "Log format: text or json")
	lf.loggerSet = &stringSliceFlag{}
	if envLogger != "" {
		for _, n := range strings.Split(envLogger, ",") {
			if n = strings.TrimSpace(n); n != "" {
				_ = lf.loggerSet.Set(n)
			}
		}
	}
	fs.Var(lf.loggerSet, "log-logger", "Target specific logger(s) (repeatable; comma-separated also accepted)")
	return lf
}

// Config resolves the parsed flags into a LoggingConfig. Call after fs.Parse().
// Returns an error if a flag value is malformed.
func (lf *LoggingFlags) Config() (LoggingConfig, error) {
	level, err := ParseLogLevel(*lf.level)
	if err != nil {
		return LoggingConfig{}, err
	}
	format := LogFormat(strings.ToLower(*lf.format))
	if format != LogFormatText && format != LogFormatJSON {
		return LoggingConfig{}, fmt.Errorf("unknown log format %q (want text|json)", *lf.format)
	}
	return LoggingConfig{
		Level:   level,
		Format:  format,
		Loggers: lf.loggerSet.values,
		Debug:   *lf.debug,
	}, nil
}

// Apply parses, validates, and installs the logging configuration in one step.
// Convenience wrapper for the common case where a worker main() just wants
// "honor the flags".
func (lf *LoggingFlags) Apply() error {
	cfg, err := lf.Config()
	if err != nil {
		return err
	}
	ConfigureLogging(cfg)
	return nil
}

// stringSliceFlag is a flag.Value that accumulates repeatable --log-logger
// values. It also splits a single value on commas, so users can pass either
// "--log-logger=vgi.catalog --log-logger=vgi.rpc" or "--log-logger=vgi.catalog,vgi.rpc".
type stringSliceFlag struct {
	values []string
}

func (s *stringSliceFlag) String() string {
	return strings.Join(s.values, ",")
}

// Set appends the comma-separated values in v, ignoring blank entries, to
// implement flag.Value for repeatable string flags.
func (s *stringSliceFlag) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			s.values = append(s.values, part)
		}
	}
	return nil
}

func envBool(name string) bool {
	switch strings.ToLower(os.Getenv(name)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
