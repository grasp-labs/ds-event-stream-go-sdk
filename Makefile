# Go SDK Makefile
# Provides common development tasks for the Golang SDK

.PHONY: help test test-verbose test-coverage build clean lint fmt vet mod-tidy mod-download generate-types check install-tools integration-tests

# Default target
help:
	@echo "Available targets:"
	@echo "  test          - Run all tests"
	@echo "  test-verbose  - Run tests with verbose output"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  build         - Build all packages"
	@echo "  clean         - Clean build artifacts"
	@echo "  lint          - Run golangci-lint (requires golangci-lint installed)"
	@echo "  fmt           - Format code using gofmt"
	@echo "  vet           - Run go vet"
	@echo "  mod-tidy      - Tidy go modules"
	@echo "  mod-download  - Download dependencies"
	@echo "  generate-types - Generate Go structs from JSON schemas"

# Test targets
test:
	@echo "Running tests..."
	go test ./dskafka ./models

test-verbose:
	@echo "Running tests with verbose output..."
	go test -v ./dskafka ./models

test-coverage:
	@echo "Running tests with coverage..."
	go test -v -cover ./dskafka ./models
	@echo "Generating coverage report..."
	go test -coverprofile=coverage.out ./dskafka ./models
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

integration-test:
	@echo "Running integration tests..."
	@if [ -z "$$DS_CONSUMPTION_INGRESS_V1_PASSWORD" ]; then \
		echo "Error: DS_CONSUMPTION_INGRESS_V1_PASSWORD environment variable is not set."; \
		echo "Please set it with: export DS_CONSUMPTION_INGRESS_V1_PASSWORD=your_password"; \
		exit 1; \
	fi
	go test -tags=integration ./dskafka ./models

# Build targets
build:
	@echo "Building packages..."
	go build ./...

clean:
	@echo "Cleaning build artifacts..."
	go clean ./...
	rm -f coverage.out coverage.html
	rm -f kafka/system_topics.go models/event.go

# Code quality targets
lint:
	@echo "Running golangci-lint..."
	golangci-lint run

fmt:
	@echo "Formatting code..."
	gofmt -w .

vet:
	@echo "Running go vet..."
	go vet ./...

# Module management
mod-tidy:
	@echo "Tidying go modules..."
	go mod tidy

mod-download:
	@echo "Downloading dependencies..."
	go mod download

# Comprehensive check (useful for CI)
check: mod-tidy fmt vet test-coverage
	@echo "All checks passed!"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "Installing go-jsonschema for JSON schema to Go conversion..."
	go install github.com/atombender/go-jsonschema@latest

# Code generation targets
generate-types:
	@echo "Generating Go types from JSON schemas..."
	@echo "Converting system-topics.json to Go types..."
	@go-jsonschema --package kafka --output kafka/system_topics.go schemas/system-topics.json
	@echo "Converting event.json to Go types..."
	@go-jsonschema --package models --output models/event.go schemas/event.json
	@echo "Go types generated successfully:"
	@echo "  - kafka/system_topics.go (from schemas/system-topics.json)"
	@echo "  - models/event.go (from schemas/event.json)"
	@echo ""
	@echo "NOTE: After generation, manually update models/event.go to use uuid.UUID types:"
	@echo "  1. Add import: github.com/google/uuid"
	@echo "  2. Change Id, SessionId, RequestId, TenantId from string to uuid.UUID"
