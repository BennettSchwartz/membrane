---
outline: deep
---

# Deployment

This guide covers everything you need to deploy Membrane in development, staging, and production environments -- from building the binary to hardening a production installation.

## Building from Source

### Prerequisites

- **Go 1.24+** (the module requires `go 1.24.0`; see `go.mod`)
- **Make** (GNU Make or compatible)
- **protoc** and the Go gRPC plugins (only if you need to regenerate protobuf code)
- **SQLCipher** development headers (only if you enable encryption at rest)

### Compile the Binary

```bash
# Clone the repository
git clone https://github.com/GustyCube/membrane.git
cd membrane

# Build the daemon
make build
```

This produces `bin/membraned`, a statically-linked binary you can copy to any machine with a compatible OS and architecture.

### Build Targets

| Target | Description |
|--------|-------------|
| `make build` | Compile `bin/membraned` |
| `make test` | Run the full test suite |
| `make lint` | Run `go vet` and `staticcheck` |
| `make fmt` | Format all Go source files |
| `make proto` | Regenerate gRPC stubs from `.proto` files |
| `make clean` | Remove the `bin/` directory |

### Cross-Compilation

Go supports cross-compilation natively. Set `GOOS` and `GOARCH` before building:

```bash
# Linux amd64
GOOS=linux GOARCH=amd64 go build -o bin/membraned-linux-amd64 ./cmd/membraned

# Linux arm64
GOOS=linux GOARCH=arm64 go build -o bin/membraned-linux-arm64 ./cmd/membraned
```

### Build Flags

You can embed version information at build time using `-ldflags`:

```bash
go build -ldflags "-s -w" -o bin/membraned ./cmd/membraned
```

| Flag | Effect |
|------|--------|
| `-s` | Omit the symbol table (smaller binary) |
| `-w` | Omit DWARF debug information (smaller binary) |

## Binary Distribution

The `membraned` binary is a self-contained gRPC daemon. It:

- Opens (or creates) a SQLite database at the configured path
- Starts background schedulers for memory decay and consolidation
- Listens for gRPC connections on the configured address
- Handles graceful shutdown on `SIGINT` or `SIGTERM`

No external runtime dependencies are required beyond the database file.

## Configuration

Membrane is configured through three layers, applied in order of increasing priority:

1. **Defaults** -- sensible values built into the binary
2. **YAML config file** -- loaded with the `-config` flag
3. **CLI flags and environment variables** -- override everything else

### CLI Flags

| Flag | Overrides | Description |
|------|-----------|-------------|
| `-config` | -- | Path to YAML config file |
| `-db` | `db_path` | SQLite database path |
| `-addr` | `listen_addr` | gRPC listen address |

### Environment Variables

| Variable | Config field | Description |
|----------|-------------|-------------|
| `MEMBRANE_API_KEY` | `api_key` | Shared secret for gRPC authentication |
| `MEMBRANE_ENCRYPTION_KEY` | `encryption_key` | SQLCipher database encryption key |

::: tip
Always use environment variables for secrets rather than putting them in config files. This keeps sensitive material out of version control and config management systems.
:::

### Config File

Pass a YAML config file with `-config`:

```bash
./bin/membraned -config /etc/membrane/config.yaml
```

Full example:

```yaml
db_path: /var/lib/membrane/data.db
listen_addr: ":9090"
decay_interval: 1h
consolidation_interval: 6h
default_sensitivity: low
selection_confidence_threshold: 0.7
rate_limit_per_second: 100

# TLS (optional)
tls_cert_file: /etc/membrane/tls/server.crt
tls_key_file: /etc/membrane/tls/server.key
```

See the full [Configuration Reference](/reference/configuration) for details on every option.

## Running the Daemon

### Basic Startup

```bash
# With all defaults (listens on :9090, database at ./membrane.db)
./bin/membraned

# With a config file
./bin/membraned -config config.yaml

# Override the database path and listen address
./bin/membraned -db /var/lib/membrane/data.db -addr 127.0.0.1:9090
```

### Choosing a Database Path

The database file is created automatically if it does not exist. Pick a path on durable storage with enough free space for your expected data volume.

```bash
# Create the directory first
sudo mkdir -p /var/lib/membrane
sudo chown membrane:membrane /var/lib/membrane

./bin/membraned -db /var/lib/membrane/data.db
```

::: warning
SQLite requires write access to the directory containing the database file, because it creates WAL and SHM journal files alongside the main database.
:::

### Address Binding

By default, Membrane listens on `:9090` (all interfaces, port 9090). For production, bind to a specific interface or use a reverse proxy.

```bash
# Listen on localhost only
./bin/membraned -addr 127.0.0.1:9090

# Listen on all interfaces, custom port
./bin/membraned -addr :8443

# Let the OS pick a free port
./bin/membraned -addr :0
```

## Systemd Service

For production Linux hosts, run `membraned` as a systemd service. Create the file `/etc/systemd/system/membrane.service`:

```ini
[Unit]
Description=Membrane Memory Substrate Daemon
After=network.target
Documentation=https://github.com/GustyCube/membrane

[Service]
Type=simple
User=membrane
Group=membrane
ExecStart=/usr/local/bin/membraned -config /etc/membrane/config.yaml
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/membrane
PrivateTmp=true

# Environment variables for secrets
EnvironmentFile=-/etc/membrane/env

[Install]
WantedBy=multi-user.target
```

Create the environment file at `/etc/membrane/env`:

```bash
MEMBRANE_API_KEY=your-api-key-here
MEMBRANE_ENCRYPTION_KEY=your-encryption-key-here
```

::: danger
Set permissions on the environment file so only root and the membrane user can read it:
```bash
sudo chmod 640 /etc/membrane/env
sudo chown root:membrane /etc/membrane/env
```
:::

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable membrane
sudo systemctl start membrane

# Check status
sudo systemctl status membrane

# View logs
sudo journalctl -u membrane -f
```

## Docker

### Dockerfile

```dockerfile
# -- Build stage --
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache make gcc musl-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build

# -- Runtime stage --
FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
    && addgroup -S membrane && adduser -S membrane -G membrane
COPY --from=builder /src/bin/membraned /usr/local/bin/membraned
USER membrane
EXPOSE 9090
ENTRYPOINT ["membraned"]
CMD ["-config", "/etc/membrane/config.yaml"]
```

Build and run:

```bash
docker build -t membrane:latest .
docker run -d \
  --name membrane \
  -p 9090:9090 \
  -v membrane-data:/var/lib/membrane \
  -v ./config.yaml:/etc/membrane/config.yaml:ro \
  -e MEMBRANE_API_KEY=your-api-key \
  -e MEMBRANE_ENCRYPTION_KEY=your-encryption-key \
  membrane:latest
```

### Docker Compose

```yaml
version: "3.9"

services:
  membrane:
    build: .
    image: membrane:latest
    container_name: membrane
    restart: unless-stopped
    ports:
      - "9090:9090"
    volumes:
      - membrane-data:/var/lib/membrane
      - ./config.yaml:/etc/membrane/config.yaml:ro
      - ./tls:/etc/membrane/tls:ro
    environment:
      MEMBRANE_API_KEY: "${MEMBRANE_API_KEY}"
      MEMBRANE_ENCRYPTION_KEY: "${MEMBRANE_ENCRYPTION_KEY}"
    healthcheck:
      test: ["CMD", "grpc_health_probe", "-addr=:9090"]
      interval: 10s
      timeout: 3s
      retries: 3

volumes:
  membrane-data:
```

Start the stack:

```bash
docker compose up -d
docker compose logs -f membrane
```

## TLS Configuration

TLS encrypts all traffic between gRPC clients and the Membrane daemon. Both `tls_cert_file` and `tls_key_file` must be set for TLS to activate; if either is empty, the server starts without TLS.

### Generating Self-Signed Certificates (Development)

```bash
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout server.key -out server.crt \
  -days 365 -subj "/CN=membrane.local"
```

### Production Certificates

For production, use certificates from a trusted CA or an internal PKI. Place the files in a secure directory:

```bash
sudo mkdir -p /etc/membrane/tls
sudo cp server.crt /etc/membrane/tls/
sudo cp server.key /etc/membrane/tls/
sudo chmod 600 /etc/membrane/tls/server.key
sudo chmod 644 /etc/membrane/tls/server.crt
sudo chown -R membrane:membrane /etc/membrane/tls
```

Add the paths to your config file:

```yaml
tls_cert_file: /etc/membrane/tls/server.crt
tls_key_file: /etc/membrane/tls/server.key
```

::: tip
Use a tool like [cert-manager](https://cert-manager.io/) in Kubernetes environments to automate certificate rotation.
:::

## Authentication

When an API key is configured, every gRPC request must include it as a Bearer token in the `authorization` metadata header. Requests with a missing or invalid key receive an `Unauthenticated` gRPC error.

### Enabling Authentication

Set the key via environment variable (preferred):

```bash
export MEMBRANE_API_KEY="$(openssl rand -hex 32)"
./bin/membraned -config config.yaml
```

Or in the config file (less secure):

```yaml
api_key: "your-api-key"
```

### Client Usage

Clients must attach the token to every RPC call:

```bash
# Using grpcurl
grpcurl -H "authorization: Bearer your-api-key" \
  localhost:9090 membrane.v1.MembraneService/Retrieve
```

::: warning
If no API key is configured, authentication is disabled entirely. Always set an API key in production.
:::

## Database Setup

### SQLite Path and Creation

Membrane uses SQLite as its storage engine. The database file is created automatically on first startup. No schema migration commands are required -- the daemon handles table creation internally.

```yaml
db_path: /var/lib/membrane/data.db
```

### WAL Mode

SQLite is configured to use Write-Ahead Logging (WAL) mode, which provides better concurrency for simultaneous reads and writes. WAL mode creates two additional files alongside the database:

- `data.db-wal` -- the write-ahead log
- `data.db-shm` -- shared memory index

::: warning
Never delete the `-wal` or `-shm` files while the daemon is running. They are integral to the database and removing them can cause data loss.
:::

### Encryption at Rest

Membrane supports [SQLCipher](https://www.zetetic.net/sqlcipher/) for transparent encryption at rest. When an encryption key is set, all data -- records, payloads, and audit logs -- is encrypted.

```bash
# Set via environment variable (recommended)
export MEMBRANE_ENCRYPTION_KEY="$(openssl rand -hex 32)"
./bin/membraned -config config.yaml
```

::: danger
Store the encryption key securely. If you lose the key, the database contents are irrecoverable. Use a secrets manager (HashiCorp Vault, AWS Secrets Manager, etc.) in production.
:::

::: warning
Encryption must be enabled when the database is first created. You cannot retroactively encrypt an existing unencrypted database. To migrate, start a new encrypted database and re-ingest your records.
:::

## Backup and Restore

### Backing Up the Database

Because Membrane uses SQLite, backups are straightforward. The safest approach is to use the SQLite `.backup` command, which creates a consistent snapshot even while the daemon is running:

```bash
sqlite3 /var/lib/membrane/data.db ".backup /backups/membrane-$(date +%Y%m%d).db"
```

Alternatively, stop the daemon and copy the database file directly:

```bash
sudo systemctl stop membrane
cp /var/lib/membrane/data.db /backups/membrane-$(date +%Y%m%d).db
sudo systemctl start membrane
```

::: tip
For encrypted databases, use the `sqlite3` binary from the SQLCipher distribution so the backup is also encrypted. Alternatively, back up the raw file -- it is encrypted on disk.
:::

### Automated Backups

Create a cron job or systemd timer for regular backups:

```bash
# /etc/cron.d/membrane-backup
0 2 * * * membrane sqlite3 /var/lib/membrane/data.db ".backup /backups/membrane-$(date +\%Y\%m\%d).db"
```

### Restoring from Backup

1. Stop the daemon:
   ```bash
   sudo systemctl stop membrane
   ```
2. Replace the database file:
   ```bash
   cp /backups/membrane-20260130.db /var/lib/membrane/data.db
   chown membrane:membrane /var/lib/membrane/data.db
   ```
3. Remove stale WAL files (if present from the old database):
   ```bash
   rm -f /var/lib/membrane/data.db-wal /var/lib/membrane/data.db-shm
   ```
4. Start the daemon:
   ```bash
   sudo systemctl start membrane
   ```

## Monitoring

### Health Checks

Use [`grpc_health_probe`](https://github.com/grpc-ecosystem/grpc-health-probe) to check whether the daemon is serving:

```bash
grpc_health_probe -addr=localhost:9090
```

In Docker or Kubernetes, configure this as a liveness and readiness probe.

### Prometheus Metrics

Membrane exposes behavioral metrics through its observability collector. To integrate with Prometheus, configure a scrape target in your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: membrane
    scrape_interval: 15s
    static_configs:
      - targets: ["localhost:9090"]
```

Key metrics to watch:

| Metric | Description |
|--------|-------------|
| Ingest throughput | Records ingested per second |
| Retrieval latency | Time to serve retrieval requests |
| Decay cycle duration | Time taken by each decay scheduler run |
| Consolidation cycle duration | Time taken by each consolidation run |
| Active record count | Total non-decayed records in the database |
| Rate limit rejections | Requests rejected by the rate limiter |

### Log Output

`membraned` writes structured logs to standard output. In a systemd deployment, logs are captured by the journal:

```bash
# Follow live logs
sudo journalctl -u membrane -f

# View logs from the last hour
sudo journalctl -u membrane --since "1 hour ago"
```

In Docker, use `docker logs`:

```bash
docker logs -f membrane
```

## Production Checklist

Before going live, verify every item on this checklist.

### Security

- [ ] **TLS enabled** -- `tls_cert_file` and `tls_key_file` are set with valid certificates
- [ ] **Authentication enabled** -- `MEMBRANE_API_KEY` is set to a strong random value
- [ ] **Encryption at rest** -- `MEMBRANE_ENCRYPTION_KEY` is set and the database was created with encryption
- [ ] **Rate limiting active** -- `rate_limit_per_second` is set (default: 100)
- [ ] **Secrets in environment** -- API key and encryption key are loaded from environment variables or a secrets manager, not the config file
- [ ] **File permissions** -- config files are `640`, TLS key is `600`, database directory is owned by the `membrane` user

### Performance

- [ ] **Database on fast storage** -- place the SQLite database on SSD or NVMe storage for best performance
- [ ] **Decay interval tuned** -- adjust `decay_interval` based on your data volume (shorter for small datasets, longer for large)
- [ ] **Consolidation interval tuned** -- adjust `consolidation_interval` based on how quickly you need episodic-to-semantic promotion
- [ ] **Confidence threshold set** -- tune `selection_confidence_threshold` for your retrieval accuracy requirements
- [ ] **File descriptor limit raised** -- set `LimitNOFILE=65536` or higher in the systemd unit

### Reliability

- [ ] **Automated backups** -- a cron job or systemd timer creates daily database backups
- [ ] **Backup retention** -- old backups are rotated and cleaned up
- [ ] **Graceful shutdown** -- the daemon is managed by systemd with `Restart=on-failure`
- [ ] **Health checks** -- a monitoring system polls the gRPC health endpoint
- [ ] **Log aggregation** -- logs are shipped to a centralized logging system (Loki, Elasticsearch, etc.)
- [ ] **Alerting** -- alerts are configured for daemon restarts, high error rates, and disk space

### Network

- [ ] **Bind address restricted** -- the daemon listens on a specific interface or behind a reverse proxy, not `0.0.0.0` in a public network
- [ ] **Firewall rules** -- only authorized clients can reach port 9090 (or your configured port)
- [ ] **DNS configured** -- clients connect via a stable hostname, not a raw IP address
