.PHONY: build test proto lint clean fmt

GO := go
BINARY := bin/membraned
MODULE := github.com/GustyCube/membrane
PROTO_DIR := api/proto/membrane/v1

build:
	$(GO) build -o $(BINARY) ./cmd/membraned

test:
	$(GO) test ./...

proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

lint:
	$(GO) vet ./...
	staticcheck ./...

clean:
	rm -rf bin/

fmt:
	$(GO) fmt ./...
