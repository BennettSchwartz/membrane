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
- Protocol Buffers compiler (`protoc`) for gRPC work

### Building

```bash
make build    # Build the daemon binary
make test     # Run all tests
make lint     # Run linters
make fmt      # Format code
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

## Commit Messages

Use conventional commit messages:

- `feat: add new retrieval filter option`
- `fix: correct decay calculation for linear curves`
- `docs: update retrieval API documentation`
- `test: add atomicity tests for revision operations`

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
