.PHONY: build test verify proto check-go-proto-sync check-python-proto-sync check-python-package check-postgres-only check-postgres-only-selftest lint clean fmt eval eval-script-test eval-all eval-typed eval-revision eval-decay eval-trust eval-competence eval-plan eval-vector eval-lifecycle eval-consolidation eval-metrics eval-invariants eval-grpc py-test ts-install ts-typecheck ts-test ts-build openclaw-install openclaw-test openclaw-build agent-harness-install agent-harness-typecheck agent-harness-test agent-harness-deterministic

GO := go
NPM := npm
PYTHON ?= $(shell if [ -x ./.venv/bin/python ]; then echo ./.venv/bin/python; else echo python3; fi)
BINARY := bin/membraned
MODULE := github.com/BennettSchwartz/membrane
PROTO_DIR := api/proto/membrane/v1
TS_DIR := clients/typescript
OPENCLAW_DIR := clients/openclaw
AGENT_HARNESS_DIR := examples/agent-harness

build:
	$(GO) build -o $(BINARY) ./cmd/membraned

test:
	$(GO) test ./...

verify: check-postgres-only-selftest check-postgres-only check-go-proto-sync-selftest check-go-proto-sync check-python-proto-sync check-python-package eval-script-test
	$(GO) test ./...
	$(PYTHON) -m pytest clients/python/tests -q
	cd $(TS_DIR) && $(NPM) run typecheck
	cd $(TS_DIR) && $(NPM) test -- --run
	cd $(OPENCLAW_DIR) && $(NPM) test
	cd $(OPENCLAW_DIR) && $(NPM) run build
	cd $(AGENT_HARNESS_DIR) && $(NPM) run typecheck
	cd $(AGENT_HARNESS_DIR) && $(NPM) test
	$(NPM) run test:docs-image-size
	$(NPM) run docs:build

eval:
	./tools/eval/run.sh

eval-script-test:
	./tools/eval/run_test.sh

eval-vector:
	./tools/eval/run.sh

eval-lifecycle:
	go run ./cmd/membrane-eval-lifecycle \
		-postgres-dsn "$$MEMBRANE_POSTGRES_DSN" \
		-embedding-endpoint "$${MEMBRANE_EMBEDDING_ENDPOINT:-https://openrouter.ai/api/v1/embeddings}" \
		-embedding-model "$${MEMBRANE_EMBEDDING_MODEL:-openai/text-embedding-3-small}" \
		-embedding-api-key "$$MEMBRANE_EMBEDDING_API_KEY" \
		-embedding-dimensions "$${MEMBRANE_EMBEDDING_DIMENSIONS:-1536}"

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
	$(MAKE) eval-lifecycle

py-test:
	$(PYTHON) -m pytest clients/python/tests -q

ts-install:
	cd $(TS_DIR) && $(NPM) ci

ts-typecheck:
	cd $(TS_DIR) && $(NPM) run typecheck

ts-test:
	cd $(TS_DIR) && $(NPM) test -- --hookTimeout=120000

ts-build:
	cd $(TS_DIR) && $(NPM) run build

openclaw-install:
	cd $(OPENCLAW_DIR) && $(NPM) install --package-lock=false

openclaw-test:
	cd $(OPENCLAW_DIR) && $(NPM) test

openclaw-build:
	cd $(OPENCLAW_DIR) && $(NPM) run build

agent-harness-install:
	cd $(AGENT_HARNESS_DIR) && $(NPM) ci

agent-harness-typecheck:
	cd $(AGENT_HARNESS_DIR) && $(NPM) run typecheck

agent-harness-test:
	cd $(AGENT_HARNESS_DIR) && $(NPM) test

agent-harness-deterministic:
	cd $(AGENT_HARNESS_DIR) && $(NPM) run test:deterministic

proto:
	mkdir -p api/grpc/gen/membranev1
	./scripts/protoc-go.sh \
		--go_out=api/grpc/gen/membranev1 --go_opt=paths=source_relative \
		--go-grpc_out=api/grpc/gen/membranev1 --go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/*.proto

check-go-proto-sync:
	./scripts/check-go-proto-sync.sh

.PHONY: check-go-proto-sync-selftest
check-go-proto-sync-selftest:
	./scripts/check-go-proto-sync_test.sh

check-python-proto-sync:
	$(PYTHON) clients/python/scripts/check_proto_sync.py

check-python-package:
	$(PYTHON) clients/python/scripts/check_package.py

check-postgres-only:
	./scripts/check-postgres-only.sh

check-postgres-only-selftest:
	./scripts/check-postgres-only_test.sh

lint:
	$(GO) vet ./...
	GOTOOLCHAIN="$$($(GO) env GOVERSION)" $(GO) run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...

clean:
	rm -rf bin/

fmt:
	$(GO) fmt ./...
