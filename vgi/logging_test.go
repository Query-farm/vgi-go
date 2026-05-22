// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"flag"
	"log/slog"
	"strings"
	"testing"
)

func TestConfigureLogging_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	ConfigureLogging(LoggingConfig{Level: slog.LevelInfo, Format: LogFormatText, Output: &buf})
	t.Cleanup(func() { ConfigureLogging(LoggingConfig{}) })

	LogCatalog.Info("hello", "k", "v")
	out := buf.String()
	if !strings.Contains(out, "logger=vgi.catalog") {
		t.Fatalf("expected logger attribute, got: %q", out)
	}
	if !strings.Contains(out, "msg=hello") || !strings.Contains(out, "k=v") {
		t.Fatalf("expected message + attrs, got: %q", out)
	}
}

func TestConfigureLogging_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	ConfigureLogging(LoggingConfig{Level: slog.LevelInfo, Format: LogFormatJSON, Output: &buf})
	t.Cleanup(func() { ConfigureLogging(LoggingConfig{}) })

	LogWorker.Info("hi")
	out := buf.String()
	if !strings.Contains(out, `"logger":"vgi.worker"`) {
		t.Fatalf("expected logger key, got: %q", out)
	}
	if !strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Fatalf("expected JSON object, got: %q", out)
	}
}

func TestConfigureLogging_Debug(t *testing.T) {
	var buf bytes.Buffer
	ConfigureLogging(LoggingConfig{Level: slog.LevelInfo, Format: LogFormatText, Output: &buf, Debug: true})
	t.Cleanup(func() { ConfigureLogging(LoggingConfig{}) })

	LogCatalog.Debug("trace")
	if !strings.Contains(buf.String(), "msg=trace") {
		t.Fatalf("expected debug record when Debug=true: %q", buf.String())
	}
}

func TestConfigureLogging_LoggersFilter(t *testing.T) {
	var buf bytes.Buffer
	ConfigureLogging(LoggingConfig{
		Level:   slog.LevelInfo,
		Format:  LogFormatText,
		Output:  &buf,
		Loggers: []string{LoggerNameCatalog},
	})
	t.Cleanup(func() { ConfigureLogging(LoggingConfig{}) })

	LogCatalog.Info("kept")
	LogWorker.Info("dropped")

	out := buf.String()
	if !strings.Contains(out, "kept") {
		t.Fatalf("expected vgi.catalog record kept, got: %q", out)
	}
	if strings.Contains(out, "dropped") {
		t.Fatalf("expected vgi.worker record dropped, got: %q", out)
	}
}

func TestConfigureLogging_ParentAllowsChild(t *testing.T) {
	var buf bytes.Buffer
	ConfigureLogging(LoggingConfig{
		Level:   slog.LevelInfo,
		Format:  LogFormatText,
		Output:  &buf,
		Loggers: []string{LoggerName}, // "vgi"
	})
	t.Cleanup(func() { ConfigureLogging(LoggingConfig{}) })

	LogCatalog.Info("child-kept")
	if !strings.Contains(buf.String(), "child-kept") {
		t.Fatalf("expected vgi.catalog passed by parent allow, got: %q", buf.String())
	}
}

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":      slog.LevelInfo,
		"info":  slog.LevelInfo,
		"DEBUG": slog.LevelDebug,
		"Warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		got, err := ParseLogLevel(in)
		if err != nil {
			t.Errorf("ParseLogLevel(%q) err: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := ParseLogLevel("trace"); err == nil {
		t.Errorf("expected error for unknown level")
	}
}

func TestRegisterLoggingFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	lf := RegisterLoggingFlags(fs)
	if err := fs.Parse([]string{
		"--debug",
		"--log-format=json",
		"--log-logger=vgi.catalog,vgi.rpc",
		"--log-logger=vgi.worker",
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := lf.Config()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Debug {
		t.Errorf("expected Debug=true")
	}
	if cfg.Format != LogFormatJSON {
		t.Errorf("expected JSON format, got %q", cfg.Format)
	}
	got := strings.Join(cfg.Loggers, ",")
	want := "vgi.catalog,vgi.rpc,vgi.worker"
	if got != want {
		t.Errorf("loggers: got %q, want %q", got, want)
	}
}

func TestRegisterLoggingFlags_BadLevelErrs(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	lf := RegisterLoggingFlags(fs)
	if err := fs.Parse([]string{"--log-level=ludicrous"}); err != nil {
		t.Fatal(err)
	}
	if _, err := lf.Config(); err == nil {
		t.Errorf("expected error for bad log level")
	}
}
