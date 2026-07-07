#!/usr/bin/env bash
set -e

export PATH=$PATH:/usr/local/go/bin
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin

echo "=== Go version ==="
go version

echo ""
echo "=== Gerando stubs Go ==="
cd /home/breba/sd-raft-pyro-trab-5/sistema-distribuido-raft-python-remote-objects/client_go

protoc \
    --go_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_out=. \
    --go-grpc_opt=paths=source_relative \
    raft/raft.proto

echo "STUBS_GERADOS"
ls -la raft/

echo ""
echo "=== go mod tidy ==="
go mod tidy

echo ""
echo "=== Verificando build ==="
go build ./...

echo "BUILD_OK"
