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
	mkdir -p api/grpc/gen/membranev1
	protoc \
		--go_out=api/grpc/gen/membranev1 --go_opt=paths=source_relative \
		--go-grpc_out=api/grpc/gen/membranev1 --go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/*.proto

lint:
	$(GO) vet ./...
	staticcheck ./...

clean:
	rm -rf bin/

fmt:
	$(GO) fmt ./...
