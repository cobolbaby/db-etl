# Makefile for Go HTTP Proxy with Docker build/push

APP_NAME := db-etl
BUILD_DIR := bin
SRC := main.go

.PHONY: all build run clean docker-build docker-push

all: build

build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(APP_NAME) $(SRC)
	@echo "Build complete: $(BUILD_DIR)/$(APP_NAME)"

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete."

