APP_NAME := db-etl
BUILD_DIR := dist
SRC := main.go
GO ?= go
CGO_ENABLED ?= 0
TARGET_OS ?= linux
TARGET_ARCH ?= amd64

EXE_SUFFIX :=
ifeq ($(TARGET_OS),windows)
EXE_SUFFIX := .exe
endif

VERSION    := $(shell cat VERSION 2>/dev/null || echo dev)
COMMIT     := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS    := -X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)

BIN_NAME := $(APP_NAME)$(EXE_SUFFIX)

.PHONY: all build build-linux build-exe build-windows build-macos clean

all: build-linux build-exe build-macos

build:
	@echo "Cross-compiling $(BIN_NAME) for $(TARGET_OS)/$(TARGET_ARCH)..."
	@mkdir -p $(BUILD_DIR)
	@CGO_ENABLED=$(CGO_ENABLED) GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) $(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BIN_NAME) $(SRC)
	@echo "Build complete: $(BUILD_DIR)/$(BIN_NAME)"

build-linux:
	@$(MAKE) build TARGET_OS=linux TARGET_ARCH=amd64

build-exe build-windows:
	@$(MAKE) build TARGET_OS=windows TARGET_ARCH=amd64

build-macos:
	@$(MAKE) build TARGET_OS=darwin TARGET_ARCH=amd64

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete."

