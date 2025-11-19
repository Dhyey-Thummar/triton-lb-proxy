#!/bin/bash
set -e

echo "=== Installing Protobuf Compiler ==="

sudo apt-get update
sudo apt-get install -y protobuf-compiler

echo "=== Installing Go ==="

# Detect OS and install Go if not present
if ! command -v go &> /dev/null; then
    GO_VERSION="1.25.4"
    OS=$(uname | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    if [[ "$ARCH" == "x86_64" ]]; then
        ARCH="amd64"
    elif [[ "$ARCH" == "aarch64" ]]; then
        ARCH="arm64"
    fi

    wget https://go.dev/dl/go${GO_VERSION}.${OS}-${ARCH}.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf go${GO_VERSION}.${OS}-${ARCH}.tar.gz
    rm go${GO_VERSION}.${OS}-${ARCH}.tar.gz
    echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.profile
    echo "export PATH=$PATH:$HOME/go/bin" >> ~/.profile
    source ~/.profile
else
    echo "Go is already installed"
fi

echo "=== Setting up Go modules ==="
go mod edit -module=triton-lb-proxy
go mod tidy

sudo apt-get update
sudo apt-get install -y protobuf-compiler

echo "=== Installing protoc Go plugins ==="
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

echo "=== Generating protobuf files ==="
cd proto
protoc --go_out=. --go-grpc_out=. grpc_service.proto model_config.proto
protoc --go_out=. --go-grpc_out=. coordpb/coordinator.proto
cd ..

echo "=== Building binaries ==="
mkdir -p bin
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/proxy ./cmd/proxy
go build -o bin/loadgen ./cmd/loadgen

echo "=== Setup complete ==="
