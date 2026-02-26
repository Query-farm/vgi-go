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

# Path to the DuckDB unittest runner for the selected build type.
UNITTEST     := $(VGI_EXT_DIR)/build/$(BUILD_TYPE)/test/unittest

# Absolute path to the worker binary, passed to the test runner.
WORKER_PATH  := $(CURDIR)/$(BINARY)

.PHONY: build clean fmt vet test test-single

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
