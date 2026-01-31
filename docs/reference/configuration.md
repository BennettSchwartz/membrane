# Configuration

Membrane is configured via a YAML file or command-line flags. Fields not present in the config file retain their default values.

## Config File

Pass a config file with the `-config` flag:

```bash
./bin/membraned -config membrane.yaml
```

### Full Example

```yaml
# SQLite database path
db_path: /var/lib/membrane/data.db

# gRPC listen address
listen_addr: ":9090"

# How often the decay scheduler runs
decay_interval: 1h

# How often the consolidation scheduler runs
consolidation_interval: 6h

# Default sensitivity for ingested records
default_sensitivity: low

# Minimum confidence for the retrieval selector
selection_confidence_threshold: 0.7

# Security (keys should come from environment variables)
# encryption_key: ""       # or set MEMBRANE_ENCRYPTION_KEY
# api_key: ""              # or set MEMBRANE_API_KEY
# tls_cert_file: ""
# tls_key_file: ""
rate_limit_per_second: 100
```

## Options Reference

### `db_path`

- **Type:** string
- **Default:** `"membrane.db"`
- **Description:** Path to the SQLite database file. Created automatically if it does not exist.

### `listen_addr`

- **Type:** string
- **Default:** `":9090"`
- **Description:** Network address for the gRPC server to listen on. Use `":0"` to let the OS pick a free port.

### `decay_interval`

- **Type:** duration
- **Default:** `1h`
- **Description:** How often the background decay scheduler runs. The scheduler applies the configured decay curve to all non-pinned records, reducing their salience over time. Shorter intervals mean more frequent decay updates; longer intervals reduce CPU usage.

### `consolidation_interval`

- **Type:** duration
- **Default:** `6h`
- **Description:** How often the background consolidation scheduler runs. Consolidation promotes episodic experience into semantic facts, competence records, and plan graphs. Shorter intervals mean faster learning; longer intervals reduce overhead.

### `default_sensitivity`

- **Type:** string
- **Default:** `"low"`
- **Values:** `public`, `low`, `medium`, `high`, `hyper`
- **Description:** The sensitivity level assigned to ingested records when the ingest request does not specify one. See [Security](/guide/security) for details on sensitivity levels.

### `selection_confidence_threshold`

- **Type:** float
- **Default:** `0.7`
- **Range:** [0, 1]
- **Description:** Minimum confidence for the multi-solution selector. When the selector ranks competence or plan graph candidates, if confidence falls below this threshold the `needs_more` flag is set, signaling that additional information or user disambiguation may be needed. See [Retrieval](/guide/retrieval) for details.

### `encryption_key`

- **Type:** string
- **Default:** `""` (no encryption)
- **Env var:** `MEMBRANE_ENCRYPTION_KEY`
- **Description:** SQLCipher encryption key for the database. When set, all data is encrypted at rest. If both the config field and the environment variable are set, the environment variable takes precedence. See [Security](/guide/security) for details.

### `api_key`

- **Type:** string
- **Default:** `""` (authentication disabled)
- **Env var:** `MEMBRANE_API_KEY`
- **Description:** Shared secret for authenticating gRPC clients. Clients must send this as a `Bearer` token in the `authorization` metadata header. If both the config field and the environment variable are set, the environment variable takes precedence. See [Security](/guide/security) for details.

### `tls_cert_file`

- **Type:** string
- **Default:** `""` (TLS disabled)
- **Description:** Path to the TLS certificate PEM file. Both `tls_cert_file` and `tls_key_file` must be set for TLS to activate.

### `tls_key_file`

- **Type:** string
- **Default:** `""` (TLS disabled)
- **Description:** Path to the TLS private key PEM file. Both `tls_cert_file` and `tls_key_file` must be set for TLS to activate.

### `rate_limit_per_second`

- **Type:** integer
- **Default:** `100`
- **Description:** Maximum gRPC requests per second. Uses a token bucket algorithm. Set to `0` to disable rate limiting. Requests exceeding the limit receive a `ResourceExhausted` gRPC error.

## Command-Line Overrides

Command-line flags override values from the config file:

| Flag | Overrides | Description |
|------|-----------|-------------|
| `-config` | -- | Path to YAML config file |
| `-db` | `db_path` | SQLite database path |
| `-addr` | `listen_addr` | gRPC listen address |

### Examples

```bash
# Use defaults
./bin/membraned

# Use a config file
./bin/membraned -config /etc/membrane/config.yaml

# Override specific values
./bin/membraned -config config.yaml -db /tmp/test.db -addr :8080

# No config file, just flags
./bin/membraned -db membrane.db -addr :9090
```

## Duration Format

Duration values use Go's duration syntax:

| Suffix | Meaning |
|--------|---------|
| `s` | seconds |
| `m` | minutes |
| `h` | hours |

Examples: `30s`, `5m`, `1h`, `2h30m`
