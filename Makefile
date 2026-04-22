APP_NAME := db-etl
BUILD_DIR := bin
SRC := main.go
GO ?= go
CGO_ENABLED ?= 0
TARGET_OS ?= linux
TARGET_ARCH ?= amd64

EXE_SUFFIX :=
ifeq ($(TARGET_OS),windows)
EXE_SUFFIX := .exe
endif

BIN_NAME := $(APP_NAME)-$(TARGET_OS)-$(TARGET_ARCH)$(EXE_SUFFIX)

.PHONY: all build build-linux build-exe build-windows build-macos clean

all: build-linux

build:
	@echo "Cross-compiling $(BIN_NAME) for $(TARGET_OS)/$(TARGET_ARCH)..."
	@mkdir -p $(BUILD_DIR)
	@CGO_ENABLED=$(CGO_ENABLED) GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) $(GO) build -o $(BUILD_DIR)/$(BIN_NAME) $(SRC)
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

