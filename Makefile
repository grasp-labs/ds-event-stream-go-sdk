# Go SDK Makefile
# Provides common development tasks for the Golang SDK

.PHONY: help test test-verbose test-coverage build clean lint fmt vet mod-tidy mod-download

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

# Test targets
test:
	@echo "Running tests..."
	go test ./...

test-verbose:
	@echo "Running tests with verbose output..."
	go test -v ./...

test-coverage:
	@echo "Running tests with coverage..."
	go test -v -cover ./...
	@echo "Generating coverage report..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Build targets
build:
	@echo "Building packages..."
	go build ./...

clean:
	@echo "Cleaning build artifacts..."
	go clean ./...
	rm -f coverage.out coverage.html

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
