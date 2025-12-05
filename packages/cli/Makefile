.PHONY: build test clean install run build-all build-darwin build-linux build-windows

# Build the CLI binary for current platform
build:
	go build -o bin/onemcp ./cmd/onemcp

# Build for all platforms
build-all: build-darwin build-linux build-windows

# Build for macOS (Intel and Apple Silicon)
build-darwin:
	GOOS=darwin GOARCH=amd64 go build -o bin/onemcp-darwin-amd64 ./cmd/onemcp
	GOOS=darwin GOARCH=arm64 go build -o bin/onemcp-darwin-arm64 ./cmd/onemcp

# Build for Linux (64-bit and 386)
build-linux:
	GOOS=linux GOARCH=amd64 go build -o bin/onemcp-linux-amd64 ./cmd/onemcp
	GOOS=linux GOARCH=386 go build -o bin/onemcp-linux-386 ./cmd/onemcp

# Build for Windows (64-bit and 386)
build-windows:
	GOOS=windows GOARCH=amd64 go build -o bin/onemcp-windows-amd64.exe ./cmd/onemcp
	GOOS=windows GOARCH=386 go build -o bin/onemcp-windows-386.exe ./cmd/onemcp

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -cover ./...

# Clean build artifacts
clean:
	rm -rf bin/

# Install dependencies
deps:
	go mod download
	go mod tidy

# Install the CLI locally
install:
	go install ./cmd/onemcp

# Run the CLI
run:
	go run ./cmd/onemcp

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	golangci-lint run

# Run property-based tests with verbose output
test-pbt:
	go test -v -run TestProperty ./...
