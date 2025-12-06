.PHONY: build test clean install run build-all build-darwin build-linux build-windows test-coverage deps fmt lint test-pbt build-release

# Default version if not specified
VERSION ?= cli-v0.0.1
BUILD_DIR = build

# Extract semantic version (strip cli-v prefix)
SEMVER = $(VERSION:cli-v%=%)

# Get git commit and date for build info
GIT_COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Build flags to inject ALL version metadata into internal/cli package
LDFLAGS = -X github.com/onemcp/cli/internal/cli.Version=$(SEMVER) \
          -X github.com/onemcp/cli/internal/cli.Commit=$(GIT_COMMIT) \
          -X github.com/onemcp/cli/internal/cli.Date=$(BUILD_DATE)

# Build the CLI binary for current platform
build:
	go build -ldflags "$(LDFLAGS)" -o bin/onemcp ./cmd/onemcp

# Build for all platforms
build-all: build-darwin build-linux build-windows

# Build for macOS (Intel and Apple Silicon)
build-darwin:
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/onemcp-darwin-amd64 ./cmd/onemcp
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o bin/onemcp-darwin-arm64 ./cmd/onemcp

# Build for Linux (64-bit and 386)
build-linux:
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/onemcp-linux-amd64 ./cmd/onemcp
	GOOS=linux GOARCH=386 go build -ldflags "$(LDFLAGS)" -o bin/onemcp-linux-386 ./cmd/onemcp

# Build for Windows (64-bit and 386)
build-windows:
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/onemcp-windows-amd64.exe ./cmd/onemcp
	GOOS=windows GOARCH=386 go build -ldflags "$(LDFLAGS)" -o bin/onemcp-windows-386.exe ./cmd/onemcp

# Build release artifacts for all platforms with versioning and packaging
build-release:
	@echo "Building OneMCP CLI $(VERSION)"
	@echo "Semantic version: $(SEMVER)"
	@echo "Git commit: $(GIT_COMMIT)"
	@echo "Build date: $(BUILD_DATE)"
	@echo ""
	
	# Clean previous builds
	rm -rf $(BUILD_DIR)
	mkdir -p $(BUILD_DIR)
	
	# Build for Mac Apple Silicon (M1/M2/M3)
	@echo "Building for macOS ARM64..."
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/onemcp ./cmd/onemcp
	tar -czf $(BUILD_DIR)/onemcp-darwin-arm64.tar.gz -C $(BUILD_DIR) onemcp
	rm $(BUILD_DIR)/onemcp
	
	# Build for Mac Intel
	@echo "Building for macOS AMD64..."
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/onemcp ./cmd/onemcp
	tar -czf $(BUILD_DIR)/onemcp-darwin-amd64.tar.gz -C $(BUILD_DIR) onemcp
	rm $(BUILD_DIR)/onemcp
	
	# Build for Linux (64 bit)
	@echo "Building for Linux AMD64..."
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/onemcp ./cmd/onemcp
	tar -czf $(BUILD_DIR)/onemcp-linux-amd64.tar.gz -C $(BUILD_DIR) onemcp
	rm $(BUILD_DIR)/onemcp
	
	# Build for Linux (32 bit)
	@echo "Building for Linux AMD32..."
	GOOS=linux GOARCH=386 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/onemcp ./cmd/onemcp
	tar -czf $(BUILD_DIR)/onemcp-linux-386.tar.gz -C $(BUILD_DIR) onemcp
	rm $(BUILD_DIR)/onemcp
	
	# Build for Windows
	@echo "Building for Windows AMD64..."
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/onemcp.exe ./cmd/onemcp
	cd $(BUILD_DIR) && zip -q onemcp-windows-amd64.zip onemcp.exe && cd ..
	rm $(BUILD_DIR)/onemcp.exe
	
	@echo "Building for Windows AMD32..."
	GOOS=windows GOARCH=386 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/onemcp.exe ./cmd/onemcp
	cd $(BUILD_DIR) && zip -q onemcp-windows-386.zip onemcp.exe && cd ..
	rm $(BUILD_DIR)/onemcp.exe
	
	@echo ""
	@echo "✅ Build complete!"
	@echo ""
	@echo "Archives created:"
	@ls -lh $(BUILD_DIR)/*.tar.gz $(BUILD_DIR)/*.zip 2>/dev/null || ls -lh $(BUILD_DIR)/*
	@echo ""
	
	# Calculate checksums
	@echo "SHA256 Checksums:"
	shasum -a 256 $(BUILD_DIR)/*.tar.gz $(BUILD_DIR)/*.zip | tee $(BUILD_DIR)/checksums.txt
	@echo ""
	@echo "Checksums saved to $(BUILD_DIR)/checksums.txt"
	@echo ""
	
	@echo "Next steps:"
	@echo "1. Go to: https://github.com/Gentoro-OneMCP/onemcp/releases/new?tag=$(VERSION)"
	@echo "2. Upload files from $(BUILD_DIR)/"
	@echo "3. Update Homebrew formula with checksums from $(BUILD_DIR)/checksums.txt"
	@echo "4. Update Scoop manifest with Windows checksums"

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -cover ./...

# Clean build artifacts
clean:
	rm -rf bin/ $(BUILD_DIR)/

# Install dependencies
deps:
	go mod download
	go mod tidy

# Install the CLI locally
install:
	go install -ldflags "$(LDFLAGS)" ./cmd/onemcp

# Run the CLI
run:
	go run -ldflags "$(LDFLAGS)" ./cmd/onemcp

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	golangci-lint run

# Run property-based tests with verbose output
test-pbt:
	go test -v -run TestProperty ./...