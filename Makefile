.PHONY: build test proto lint clean fmt eval eval-all eval-typed eval-revision eval-decay eval-trust eval-competence eval-plan eval-vector eval-consolidation eval-metrics eval-invariants eval-grpc

GO := go
BINARY := bin/membraned
MODULE := github.com/GustyCube/membrane
PROTO_DIR := api/proto/membrane/v1

build:
	$(GO) build -o $(BINARY) ./cmd/membraned

test:
	$(GO) test ./...

eval:
	./tools/eval/run.sh

eval-vector:
	./tools/eval/run.sh

eval-typed:
	$(GO) test ./tests -run TestEvalTypedMemory

eval-revision:
	$(GO) test ./tests -run TestEvalRevisionLifecycle

eval-decay:
	$(GO) test ./tests -run TestEvalDecayAndReinforce

eval-trust:
	$(GO) test ./tests -run TestEvalTrustGating

eval-competence:
	$(GO) test ./tests -run TestEvalCompetenceSelection

eval-plan:
	$(GO) test ./tests -run TestEvalPlanGraphSelection

eval-consolidation:
	$(GO) test ./tests -run TestEvalConsolidation

eval-metrics:
	$(GO) test ./tests -run TestEvalMetrics

eval-invariants:
	$(GO) test ./tests -run "TestEval(Ingestion|Retrieval|Revision|Trust)"

eval-grpc:
	$(GO) test ./tests -run TestEvalGRPC

eval-all:
	$(GO) test ./tests -run TestEval
	./tools/eval/run.sh

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
