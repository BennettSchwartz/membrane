# Contributing to Membrane

Thank you for your interest in contributing to Membrane!

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Create a branch for your changes

## Development Setup

### Prerequisites

- Go 1.22 or later
- Make
- Protocol Buffers compiler (`protoc` >= 3.20) for gRPC work
- Python 3.10+ (for Python client development)

### Building

```bash
make build    # Build the daemon binary
make test     # Run all tests
make lint     # Run linters (go vet + staticcheck)
make fmt      # Format code
```

### Python Client

```bash
pip install -e clients/python[dev]
pytest clients/python/tests/
```

## Code Style

- Follow standard Go conventions and idioms
- Run `make fmt` before committing
- Run `make lint` to catch common issues
- Write table-driven tests where appropriate

## Testing

- All new code must include tests
- Run `make test` to verify all tests pass
- Aim for meaningful coverage of business logic

## Pull Request Process

1. Ensure your code passes `make lint` and `make test`
2. Update documentation if your changes affect public APIs
3. Write a clear PR description explaining what changed and why
4. Keep PRs focused - one feature or fix per PR

### SDK Changes

When modifying the protobuf definition (`api/proto/membrane/v1/membrane.proto`):

1. Regenerate Go stubs: `make proto`
2. Update gRPC handlers in `api/grpc/handlers.go`
3. Update the Python client in `clients/python/membrane/client.py`
4. Update Python types in `clients/python/membrane/types.py` if new enums/types are added
5. Update `clients/python/membrane/__init__.py` exports
6. Update the Python client README

## Commit Messages

Use conventional commit messages with optional scope:

- `feat: add new retrieval filter option`
- `feat(python): add working state ingestion method`
- `fix: correct decay calculation for linear curves`
- `fix(grpc): validate merge ID count`
- `docs: update retrieval API documentation`
- `test: add atomicity tests for revision operations`
- `ci: add staticcheck to CI pipeline`

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
