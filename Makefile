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
#   make clean                  Remove the built binary

# Output binary name.
BINARY       := vgi-example-worker-go

# Go package containing the worker entrypoint.
CMD          := ./cmd/vgi-example-worker

# Path to the sibling DuckDB VGI extension repo (contains tests).
VGI_EXT_DIR  := ../vgi

# Toggle between "release" and "debug" DuckDB builds.
# Override on the command line or via environment:
#   make test BUILD_TYPE=debug
BUILD_TYPE   ?= release

# Timeout per individual test (seconds).
TEST_TIMEOUT ?= 60

# Path to the DuckDB unittest runner for the selected build type.
UNITTEST     := $(VGI_EXT_DIR)/build/$(BUILD_TYPE)/test/unittest
DEBUG_BIN    := $(VGI_EXT_DIR)/build/debug/test/unittest

# Absolute path to the worker binary, passed to the test runner.
WORKER_PATH  := $(CURDIR)/$(BINARY)

# Test directory inside the extension repo.
TEST_DIR     := $(VGI_EXT_DIR)/test/sql

# Discover all .test files and derive target names: test/sql/foo/bar.test → test/foo/bar
TEST_FILES       := $(shell find $(TEST_DIR) -name '*.test' 2>/dev/null)
TEST_TARGETS     := $(patsubst $(TEST_DIR)/%.test,test/%,$(TEST_FILES))
HTTP_TEST_TARGETS := $(patsubst $(TEST_DIR)/%.test,test-http/%,$(TEST_FILES))

# Tests expected to fail over HTTP (currently none)
HTTP_XFAIL_TESTS :=

.PHONY: build clean fmt vet test test-single test-http test-all

# Compile the example worker binary.
build:
	go build -o $(BINARY) $(CMD)

# Remove the built binary.
clean:
	rm -f $(BINARY)

# Format all Go source files.
fmt:
	go fmt ./...

# Run go vet across all packages.
vet:
	go vet ./...

# Run the full integration test suite.
# Rebuilds the worker first to ensure tests use the latest code.
test: build
	cd $(VGI_EXT_DIR) && VGI_TEST_WORKER=$(WORKER_PATH) $(UNITTEST) "test/*"

# Run a single integration test file.
# Example:
#   make test-single TEST=test/sql/integration/scalar/add_values.test
test-single: build
	cd $(VGI_EXT_DIR) && VGI_TEST_WORKER=$(WORKER_PATH) $(UNITTEST) "$(TEST)"

# Run the full integration test suite over HTTP transport.
# Each test starts a fresh HTTP worker, discovers the port, runs the test,
# and cleans up. Tests in HTTP_XFAIL_TESTS are expected to fail.
test-http: build $(HTTP_TEST_TARGETS)

# Run both stdio and HTTP tests.
test-all: test test-http

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
