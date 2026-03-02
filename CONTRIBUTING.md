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
- Node.js 20+ (for TypeScript client development)
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
python -m pip install -e "clients/python[dev]"
python -m pytest clients/python/tests/
```

### TypeScript Client

```bash
make ts-install
cd clients/typescript && npm run check:proto-sync
make ts-typecheck
make ts-test
make ts-build
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
3. Sync the TypeScript proto copy: `cd clients/typescript && npm run sync:proto`
4. Update the TypeScript client in `clients/typescript/src/client.ts`
5. Update TypeScript types in `clients/typescript/src/types.ts` if new enums/types are added
6. Update TypeScript exports in `clients/typescript/src/index.ts`
7. Update the TypeScript client README
8. Update the Python client in `clients/python/membrane/client.py`
9. Update Python types in `clients/python/membrane/types.py` if new enums/types are added
10. Update `clients/python/membrane/__init__.py` exports
11. Update the Python client README

## TypeScript SDK Release

To release a new version of the TypeScript SDK to npm:

### Prerequisites

- You must have `NPM_TOKEN` set as a repository secret in GitHub
- The token must have publish permissions for the `@membrane` npm scope

### Version and Tag Requirements

- The git tag `vX.Y.Z` **must match** the version in `clients/typescript/package.json`
- For example, to release version `1.2.3`:
  1. Update `clients/typescript/package.json` version to `1.2.3`
  2. Commit and push
  3. Create tag `v1.2.3` (note the `v` prefix)

### Release Checklist

1. ✅ Ensure all tests pass (`npm test`)
2. ✅ Update `CHANGELOG.md` if it exists
3. ✅ Bump version in `clients/typescript/package.json`
4. ✅ Commit version changes
5. ✅ Create git tag: `git tag v1.2.3`
6. ✅ Push tag: `git push origin v1.2.3`
7. ✅ GitHub Actions will automatically publish to npm

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
