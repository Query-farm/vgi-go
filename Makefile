# VGI-Go Makefile
#
# Builds the example VGI worker binary and runs integration tests against
# the DuckDB VGI extension (expected at ../vgi).
#
# Usage:
#   make build                  Build the worker binary
#   make test                   Run all integration tests (release build)
#   make test BUILD_TYPE=debug  Run all integration tests (debug build)
#   make test-single TEST=...   Run a single test file
#   make test-http              Run all tests over HTTP transport
#   make test-all               Run both stdio and HTTP tests
#   make fmt                    Format all Go source files
#   make vet                    Run go vet static analysis
#   make lint                   Run golangci-lint (requires golangci-lint in PATH)
#   make test-unit              Run pure-Go unit tests with coverage
#   make clean                  Remove the built binary

# Output binary name.
BINARY       := vgi-example-worker-go

# Go package containing the worker entrypoint.
CMD          := ./cmd/vgi-example-worker

# Versioned-example worker binaries + packages (mirror vgi-python's
# vgi-example-versioned-worker / vgi-example-versioned-tables-worker so the
# require-env VGI_VERSIONED_WORKER / VGI_VERSIONED_TABLES_WORKER test files
# run against the Go SDK too).
VERSIONED_BINARY        := vgi-example-versioned-worker-go
VERSIONED_CMD           := ./cmd/vgi-example-versioned-worker
VERSIONED_TABLES_BINARY := vgi-example-versioned-tables-worker-go
VERSIONED_TABLES_CMD    := ./cmd/vgi-example-versioned-tables-worker
ATTACH_OPTIONS_BINARY   := vgi-example-attach-options-worker-go
ATTACH_OPTIONS_CMD      := ./cmd/vgi-example-attach-options-worker
SIMPLE_WRITABLE_BINARY  := vgi-example-simple-writable-worker-go
SIMPLE_WRITABLE_CMD     := ./cmd/vgi-example-simple-writable-worker

# Path to the sibling DuckDB VGI extension repo (contains tests).
VGI_EXT_DIR  := ../vgi

# Toggle between "release" and "debug" DuckDB builds.
# Override on the command line or via environment:
#   make test BUILD_TYPE=debug
BUILD_TYPE   ?= release

# Timeout per individual test (seconds).
TEST_TIMEOUT ?= 60

# Shared-memory side-channel segment size (bytes) for `make test-shm`.
# Setting VGI_RPC_SHM_SIZE_BYTES makes the DuckDB extension (the RPC client)
# create a POSIX shm segment and advertise it per request; the Go worker then
# attaches and uses zero-copy batch transfer. Default: 256 MiB.
SHM_SIZE_BYTES ?= 268435456

# Path to the DuckDB unittest runner for the selected build type.
UNITTEST     := $(VGI_EXT_DIR)/build/$(BUILD_TYPE)/test/unittest
DEBUG_BIN    := $(VGI_EXT_DIR)/build/debug/test/unittest

# Absolute path to the worker binaries, passed to the test runner.
WORKER_PATH                  := $(CURDIR)/$(BINARY)
VERSIONED_WORKER_PATH        := $(CURDIR)/$(VERSIONED_BINARY)
VERSIONED_TABLES_WORKER_PATH := $(CURDIR)/$(VERSIONED_TABLES_BINARY)
ATTACH_OPTIONS_WORKER_PATH   := $(CURDIR)/$(ATTACH_OPTIONS_BINARY)
SIMPLE_WRITABLE_WORKER_PATH  := $(CURDIR)/$(SIMPLE_WRITABLE_BINARY)

# Test directory inside the extension repo.
TEST_DIR     := $(VGI_EXT_DIR)/test/sql

# Discover all .test files and derive target names: test/sql/foo/bar.test → test/foo/bar
TEST_FILES       := $(shell find $(TEST_DIR) -name '*.test' 2>/dev/null)
TEST_TARGETS     := $(patsubst $(TEST_DIR)/%.test,test/%,$(TEST_FILES))
HTTP_TEST_TARGETS := $(patsubst $(TEST_DIR)/%.test,test-http/%,$(TEST_FILES))

# Tests expected to fail over HTTP (currently none)
HTTP_XFAIL_TESTS :=

.PHONY: build clean fmt vet lint test test-unit test-single test-shm test-http test-all new-worker

# COVER=1 builds coverage-instrumented worker binaries (`go build -cover`).
# The integration suite runs the workers as separate processes, so `go test`
# can't measure them — instead the instrumented binaries write coverage pods to
# $GOCOVERDIR on clean exit (which the workers do: graceful SIGTERM shutdown /
# stdin EOF / idle timeout). -coverpkg covers the whole module, so the report
# reflects how much of the vgi/ SDK real protocol traffic exercises, not just
# the example functions. See ci/run-integration.sh (COVERAGE=1).
GO_BUILD_FLAGS :=
ifeq ($(COVER),1)
# -covermode=atomic (not the -cover default of `set`) so the worker can snapshot
# live counters mid-run via runtime/coverage.WriteCountersDir — the harness kills
# the long-lived HTTP worker before a clean exit (see cmd/.../coverage.go).
GO_BUILD_FLAGS := -cover -covermode=atomic -coverpkg=./...
endif

# Compile the example worker binaries.
build:
	go build $(GO_BUILD_FLAGS) -o $(BINARY) $(CMD)
	go build $(GO_BUILD_FLAGS) -o $(VERSIONED_BINARY) $(VERSIONED_CMD)
	go build $(GO_BUILD_FLAGS) -o $(VERSIONED_TABLES_BINARY) $(VERSIONED_TABLES_CMD)
	go build $(GO_BUILD_FLAGS) -o $(ATTACH_OPTIONS_BINARY) $(ATTACH_OPTIONS_CMD)
	go build $(GO_BUILD_FLAGS) -o $(SIMPLE_WRITABLE_BINARY) $(SIMPLE_WRITABLE_CMD)

# Remove built binaries.
clean:
	rm -f $(BINARY) $(VERSIONED_BINARY) $(VERSIONED_TABLES_BINARY) $(ATTACH_OPTIONS_BINARY)

# Format all Go source files.
fmt:
	go fmt ./...

# Run go vet across all packages.
vet:
	go vet ./...

# Run golangci-lint (config: .golangci.yml).
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install: https://golangci-lint.run/usage/install/"; \
		exit 1; \
	}
	golangci-lint run ./...

# Pure-Go unit tests with coverage. Writes coverage.out and prints a summary.
test-unit:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out | tail -1

# Scaffold a new VGI worker module from templates/worker. Usage:
#   make new-worker NAME=myproj            # creates ./myproj/
#   make new-worker NAME=myproj DIR=../    # creates ../myproj/
DIR ?= .
new-worker:
	@test -n "$(NAME)" || { echo "usage: make new-worker NAME=<module-name> [DIR=<parent>]"; exit 2; }
	@dest="$(DIR)/$(NAME)"; \
	if [ -e "$$dest" ]; then echo "$$dest already exists"; exit 1; fi; \
	mkdir -p "$$dest"; \
	for f in templates/worker/*.tmpl; do \
		out="$$dest/$$(basename $${f%.tmpl})"; \
		sed "s/__NAME__/$(NAME)/g" "$$f" > "$$out"; \
	done; \
	echo "scaffolded $$dest"; \
	echo "next: cd $$dest && go mod tidy && go build ./..."

# Run the full integration test suite.
# Rebuilds the workers first to ensure tests use the latest code.
# Versioned-worker env vars make require-env-gated tests in
# test/sql/integration/attach/versioning*.test and versioned_tables*.test
# run against the Go workers too. The ~writable glob excludes the
# writable-catalog tests (opt-in via VGI_WORKER_ENABLE_WRITABLE).
#
# VGI_SYNC_INIT_GLOBAL=1 forces the C++ extension's init_global RPC to run
# synchronously rather than on a background future. Without this, the
# extension reports max_processes=1 to DuckDB at pipeline-schedule time
# (the actual max_workers comes back from the worker after EnsureInitApplied,
# but by then the scheduler has already committed to a single-thread plan),
# so multi-conn parallel-init tests (partitioned_sequence,
# filter_echo_partitioned, order_preservation_modes, vgi_integration) only
# observe a single connection. Default async mode is fine for production
# (async hides RPC latency); the tests assert what max_workers _should_
# yield, so we run the suite with sync init to exercise that path.
test: build
	cd $(VGI_EXT_DIR) && \
	    VGI_SYNC_INIT_GLOBAL=1 \
	    VGI_TEST_WORKER=$(WORKER_PATH) \
	    VGI_VERSIONED_WORKER=$(VERSIONED_WORKER_PATH) \
	    VGI_VERSIONED_TABLES_WORKER=$(VERSIONED_TABLES_WORKER_PATH) \
	    VGI_ATTACH_OPTIONS_WORKER=$(ATTACH_OPTIONS_WORKER_PATH) \
	    VGI_SIMPLE_WRITABLE_WORKER=$(SIMPLE_WRITABLE_WORKER_PATH) \
	    $(UNITTEST) "test/*" "~test/sql/integration/writable/*"

# Run a single integration test file.
# Example:
#   make test-single TEST=test/sql/integration/scalar/add_values.test
test-single: build
	cd $(VGI_EXT_DIR) && \
	    VGI_SYNC_INIT_GLOBAL=1 \
	    VGI_TEST_WORKER=$(WORKER_PATH) \
	    VGI_VERSIONED_WORKER=$(VERSIONED_WORKER_PATH) \
	    VGI_VERSIONED_TABLES_WORKER=$(VERSIONED_TABLES_WORKER_PATH) \
	    VGI_ATTACH_OPTIONS_WORKER=$(ATTACH_OPTIONS_WORKER_PATH) \
	    VGI_SIMPLE_WRITABLE_WORKER=$(SIMPLE_WRITABLE_WORKER_PATH) \
	    $(UNITTEST) "$(TEST)"

# Run the full integration test suite with the shared-memory side-channel
# enabled. Identical to `make test` plus VGI_RPC_SHM_SIZE_BYTES, which makes
# the extension (RPC client) create a POSIX shm segment and advertise it; the
# Go worker attaches and uses zero-copy batch transfer over stdio. SHM is
# transparent to test outcomes, so the same .test files exercise the SHM path.
# Set VGI_RPC_SHM_DEBUG=1 in your environment to trace resolved/fallback batches.
#   make test-shm                         # 256 MiB segment (default)
#   make test-shm SHM_SIZE_BYTES=67108864 # 64 MiB segment
test-shm: build
	cd $(VGI_EXT_DIR) && \
	    VGI_SYNC_INIT_GLOBAL=1 \
	    VGI_RPC_SHM_SIZE_BYTES=$(SHM_SIZE_BYTES) \
	    VGI_TEST_WORKER=$(WORKER_PATH) \
	    VGI_VERSIONED_WORKER=$(VERSIONED_WORKER_PATH) \
	    VGI_VERSIONED_TABLES_WORKER=$(VERSIONED_TABLES_WORKER_PATH) \
	    VGI_ATTACH_OPTIONS_WORKER=$(ATTACH_OPTIONS_WORKER_PATH) \
	    VGI_SIMPLE_WRITABLE_WORKER=$(SIMPLE_WRITABLE_WORKER_PATH) \
	    $(UNITTEST) "test/*" "~test/sql/integration/writable/*"

# Run the full integration test suite over HTTP transport.
# Each test starts a fresh HTTP worker, discovers the port, runs the test,
# and cleans up. Tests in HTTP_XFAIL_TESTS are expected to fail.
test-http: build $(HTTP_TEST_TARGETS)

# Run the launcher (AF_UNIX 'launch:' transport) integration tests. The worker
# is wrapped in a launch: LOCATION so the extension spawns it directly with
# --unix/--idle-timeout appended; VGI_REQUIRE_LAUNCHER_TRANSPORT un-skips the
# launcher-only test group.
test-launcher: build
	cd $(VGI_EXT_DIR) && \
	    VGI_SYNC_INIT_GLOBAL=1 \
	    VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	    VGI_TEST_WORKER="launch:$(WORKER_PATH)" \
	    $(UNITTEST) "test/sql/integration/launcher/*"

# Run stdio, stdio+shm, HTTP, and launcher tests.
test-all: test test-shm test-http test-launcher

# Pattern rule: HTTP transport — starts server per test, discovers port, cleans up
test-http/%: build
	@test_file="$(TEST_DIR)/$*.test"; \
	if [ ! -f "$$test_file" ]; then \
		echo "ERROR: test file not found: $$test_file"; \
		exit 1; \
	fi; \
	port_fifo=$$(mktemp -u); \
	mkfifo "$$port_fifo"; \
	./$(BINARY) --http > "$$port_fifo" 2>/dev/null & \
	http_pid=$$!; \
	cleanup() { kill $$http_pid 2>/dev/null; wait $$http_pid 2>/dev/null; rm -f "$$port_fifo"; }; \
	trap cleanup EXIT; \
	port_line=""; \
	read -t 10 port_line < "$$port_fifo" || { \
		echo "ERROR: HTTP worker did not print PORT line within 10s"; \
		kill $$http_pid 2>/dev/null; rm -f "$$port_fifo"; \
		exit 1; \
	}; \
	rm -f "$$port_fifo"; \
	port=$${port_line#PORT:}; \
	export VGI_TEST_WORKER="http://localhost:$$port/vgi"; \
	is_xfail=false; \
	for xf in $(HTTP_XFAIL_TESTS); do \
		if [ "$$xf" = "$*" ]; then is_xfail=true; break; fi; \
	done; \
	if timeout $(TEST_TIMEOUT) $(UNITTEST) --test-dir $(TEST_DIR) "$$test_file" > /dev/null 2>&1; then \
		if $$is_xfail; then \
			echo "XPASS $* [http] (expected failure now passes — remove from HTTP_XFAIL_TESTS)"; \
		else \
			echo "PASS  $* [http]"; \
		fi; \
	else \
		rc=$$?; \
		if $$is_xfail; then \
			echo "XFAIL $* [http] (expected failure)"; \
		else \
			echo "FAIL  $* [http] (release, rc=$$rc) — rerunning with debug binary..."; \
			timeout $(TEST_TIMEOUT) $(DEBUG_BIN) --test-dir $(TEST_DIR) -s "$$test_file" 2>&1 || true; \
			kill $$http_pid 2>/dev/null; wait $$http_pid 2>/dev/null; \
			exit 1; \
		fi; \
	fi; \
	kill $$http_pid 2>/dev/null; wait $$http_pid 2>/dev/null; true
