# Troubleshooting

This page covers common issues you may encounter when running Membrane, along with their causes and solutions. If your issue is not listed here, check the [FAQ](#faq) at the bottom or open an issue on GitHub.

## Connection Issues

### Cannot connect to gRPC server

**Symptom:** Client receives `connection refused` or `deadline exceeded` when trying to reach `membraned`.

**Possible causes:**

1. **Server not running.** Verify `membraned` is up:
   ```bash
   ps aux | grep membraned
   ```

2. **Wrong address or port.** The default listen address is `:9090`. If you changed it via `--addr` or `listen_addr` in YAML, make sure the client targets the same host and port.
   ```yaml
   # config.yaml
   listen_addr: ":9090"
   ```

3. **Firewall or network policy.** Ensure that the port is reachable from the client host. In containerized environments, check that the port is exposed and service discovery is configured correctly.

4. **Binding to localhost only.** If you set `listen_addr: "127.0.0.1:9090"`, the server will only accept connections from the same machine. Use `0.0.0.0:9090` or `:9090` to listen on all interfaces.

### TLS handshake failures

**Symptom:** `transport: authentication handshake failed` on the client side.

**Possible causes:**

1. **Mismatched TLS configuration.** If the server has `tls_cert_file` and `tls_key_file` set, clients must connect with TLS enabled. Conversely, if TLS is not configured on the server, clients must not use TLS credentials.

2. **Expired or invalid certificate.** Verify the certificate is valid:
   ```bash
   openssl x509 -in /path/to/cert.pem -noout -dates
   ```

3. **Wrong CA or self-signed certs.** If using a self-signed certificate, the client must be configured to trust it. Add the CA certificate to the client's trusted roots or pass it explicitly.

4. **Certificate does not match hostname.** The Subject Alternative Name (SAN) in the certificate must match the hostname or IP the client uses to connect.

### Authentication failures

**Symptom:** `Unauthenticated: invalid or missing API key` or `Unauthenticated: missing metadata`.

**Possible causes:**

1. **API key not set on client.** When `api_key` is configured (or `MEMBRANE_API_KEY` is set), every gRPC request must include metadata with key `authorization` and value `Bearer <your-key>`.
   ```go
   md := metadata.Pairs("authorization", "Bearer "+apiKey)
   ctx := metadata.NewOutgoingContext(ctx, md)
   ```

2. **Key mismatch.** The key the client sends must exactly match the server's configured key. Check for trailing whitespace or newline characters, especially when loading from environment variables.

3. **Missing metadata entirely.** Some gRPC client libraries strip metadata by default. Ensure your client framework is configured to send metadata on every call.

## Ingestion Errors

### Validation failures

**Symptom:** `InvalidArgument: validation error for <field>: <message>`.

Every memory record is validated before storage. The following fields are checked:

| Field | Requirement |
|-------|-------------|
| `id` | Must be non-empty. Use a UUID. |
| `type` | Must be one of: `episodic`, `working`, `semantic`, `competence`, `plan_graph`. |
| `sensitivity` | Must be one of: `public`, `low`, `medium`, `high`, `hyper`. |
| `confidence` | Must be in the range `[0, 1]`. |
| `salience` | Must be `>= 0`. |
| `payload` | Must be non-nil and match the declared memory type. |

If you see `validation error for confidence: confidence must be in range [0, 1]`, check that you are not passing a value greater than 1.0 or a negative number.

### Payload format errors

**Symptom:** `InvalidArgument: invalid args JSON: ...` or `invalid result JSON: ...`.

Payload fields like `args`, `result`, `object`, and `active_constraints` must be valid JSON. Common mistakes:

- Sending a bare string instead of a JSON object: use `{"key": "value"}` not `key=value`.
- Sending `null` bytes or truncated JSON due to encoding issues.
- Exceeding the maximum payload size of **10 MB** (`10 * 1024 * 1024` bytes). If you hit this limit, consider summarizing the payload before ingestion.

### Missing required fields

**Symptom:** `InvalidArgument: <field> exceeds maximum length` or empty response.

Each ingestion endpoint requires specific fields:

- **IngestEvent:** `source`, `event_kind`, `ref`, `summary` are validated for length (max 100 KB each).
- **IngestToolOutput:** `source`, `tool_name` are required; `args` and `result` must be valid JSON if provided.
- **IngestObservation:** `source`, `subject`, `predicate` are required; `object` must be valid JSON if provided.
- **IngestOutcome:** `source`, `target_record_id`, `outcome_status` are required.
- **IngestWorkingState:** `source`, `thread_id`, `context_summary` are required.

Tags are limited to **100 tags** with each tag at most **256 characters**.

### Duplicate record IDs

**Symptom:** `UNIQUE constraint failed` or `ErrAlreadyExists`.

Memory record IDs must be globally unique. If you are generating IDs client-side, ensure you use a proper UUID generator. If you see this error, the record with that ID already exists in the database.

## Retrieval Problems

### Empty results

**Symptom:** `Retrieve` returns zero records when you expect matches.

Check the following:

1. **Trust context is too restrictive.** Every `Retrieve` call requires a `TrustContext`. If `max_sensitivity` is set to `public`, records with sensitivity `low` or higher will be filtered out.

2. **Scope mismatch.** If the records were ingested with a `scope` (e.g., `"project-alpha"`) but the trust context's `scopes` list does not include that value, those records will not appear.

3. **Salience too low.** Records with salience below `min_salience` in the request are excluded. If decay has reduced salience significantly, records may fall below the threshold. Try setting `min_salience: 0` to see all records.

4. **Wrong memory type filter.** If you specify `memory_types` in the request, only those types are returned. Omit the filter or include all types you expect.

5. **Records have been retracted.** Retracted records may not appear in normal retrieval depending on their revision status.

### Trust context filtering too strict

**Symptom:** You know records exist but they do not appear in results.

The sensitivity hierarchy from least to most restrictive is:

```
public < low < medium < high < hyper
```

A trust context with `max_sensitivity: "medium"` will return records with sensitivity `public`, `low`, or `medium`, but not `high` or `hyper`.

To debug, try retrieving with `max_sensitivity: "hyper"` and `authenticated: true` to see all records, then narrow down.

### Selection confidence threshold

**Symptom:** Competence and plan graph records are not being selected.

The `selection_confidence_threshold` config (default: `0.7`) filters out competence and plan graph candidates with confidence below this value. If your records have low confidence, either:

- Lower the threshold in your config.
- Increase the confidence on relevant records by reinforcing them.

## Decay Issues

### Memories disappearing too fast

**Symptom:** Records you recently ingested have very low salience or are gone.

1. **Half-life too short.** The default half-life is 86400 seconds (1 day) for exponential decay. For episodic records that should last longer, increase `half_life_seconds` in the decay profile.

2. **Decay interval too frequent.** The decay scheduler runs every `decay_interval` (default: 1 hour). Each run applies the decay formula. This is normally fine, but if your half-life is very short, records decay rapidly.

3. **Deletion policy is `auto_prune`.** Records with `deletion_policy: "auto_prune"` (the default) will be deleted when salience drops below the minimum threshold. Switch to `manual_only` or `never` if you want records to persist even at low salience.

4. **`max_age_seconds` is set.** If a record has `max_age_seconds` configured, it becomes eligible for deletion after that age regardless of salience.

### Salience not updating

**Symptom:** You called `Reinforce` but the salience did not change.

1. **Record not found.** Reinforce returns an error if the record ID does not exist. Check the error response.

2. **Reinforcement gain is zero.** The `reinforcement_gain` field in the decay profile controls how much salience increases on reinforcement. If it is `0`, reinforcement has no effect. Set a positive value (e.g., `0.3`).

3. **Record is pinned.** Pinned records do not decay, but reinforcement still updates `last_reinforced_at`. Check if the salience was already at its maximum.

## Consolidation

### Consolidation not running

**Symptom:** Episodic records are accumulating but no semantic or competence records are being created automatically.

1. **Scheduler not started.** The consolidation scheduler starts when `membrane.Start(ctx)` is called. If you are using the library directly (not `membraned`), ensure you call `Start`.

2. **Interval too long.** The default `consolidation_interval` is 6 hours. For testing, reduce it:
   ```yaml
   consolidation_interval: 5m
   ```

3. **Not enough episodic data.** Consolidation extracts patterns from episodic records. If there are very few records, the consolidation engine may not find patterns worth promoting.

### Records not being consolidated

**Symptom:** Consolidation runs but does not produce new records.

1. **Observations not repeated enough.** Semantic extraction requires repeated observations of the same pattern. A single observation will not be promoted.

2. **Tool-use patterns not established.** Competence extraction looks for successful tool-use patterns. If all outcomes are `failure`, no competence records will be created.

3. **Episodic records already compressed.** Once episodic records have been compressed (salience reduced), they may not be candidates for further consolidation.

## Storage

### Database locked errors

**Symptom:** `database is locked` or similar SQLite locking errors.

SQLite has limited concurrency support. Membrane configures WAL mode and a busy timeout of 5000ms to mitigate this, but issues can still arise:

1. **Multiple processes accessing the same file.** Only one `membraned` process should write to a given database file at a time. Do not run multiple instances pointing to the same `db_path`.

2. **NFS or network filesystems.** SQLite does not work reliably on network filesystems. Use a local disk.

3. **Long-running transactions.** If you are using the Go library directly and holding transactions open for too long, other operations will block. Keep transactions short.

### Encryption key issues

**Symptom:** `encryption key verification failed` on startup.

1. **Wrong key.** If the database was created with an encryption key, you must provide the same key to open it. The key is read from `encryption_key` in config or the `MEMBRANE_ENCRYPTION_KEY` environment variable.

2. **Opening an encrypted database without a key.** If `encryption_key` is empty but the database file is encrypted, the schema query will fail. Set the correct key.

3. **Opening an unencrypted database with a key.** This will also fail verification. Remove the encryption key setting if the database was created without encryption.

4. **Key changed.** SQLCipher does not support changing the encryption key after creation through Membrane. You would need to export and re-import the data.

### Disk space

**Symptom:** `disk I/O error` or write failures.

1. **Disk full.** SQLite needs space for the database file, WAL file, and temporary files. Ensure adequate free space.

2. **WAL file growing.** The WAL file can grow if checkpointing is delayed. Restarting `membraned` will trigger a checkpoint. You can also run `PRAGMA wal_checkpoint(TRUNCATE)` manually.

## Performance

### Slow queries

1. **Large result sets.** Use `limit` in your retrieval requests. The maximum is 10,000 but fetching thousands of records with full payloads is slow. Start with a smaller limit.

2. **Too many tags in filter.** Tag filtering uses subqueries for each tag. Filtering by more than 5-10 tags simultaneously can be slow. Consider using broader tags.

3. **Missing indexes.** The schema creates indexes on common query patterns. If you added custom queries against the database, ensure appropriate indexes exist.

### High memory usage

1. **Large payloads.** If you ingest records with very large JSON payloads (approaching the 10 MB limit), memory usage can spike during serialization. Keep payloads concise.

2. **Many concurrent requests.** The rate limiter (default: 100 req/s) helps prevent overload. Lower it if memory is constrained:
   ```yaml
   rate_limit_per_second: 50
   ```

3. **Consolidation on large datasets.** Consolidation scans all episodic records. With tens of thousands of records, this can use significant memory. Consider more aggressive decay to keep the active record count manageable.

### Tuning tips

- **Adjust decay interval.** More frequent decay keeps the active record set smaller, improving query performance. Try `decay_interval: 15m` for high-throughput workloads.
- **Use scopes.** Scoping records by project, user, or workspace enables more targeted queries and avoids scanning irrelevant data.
- **Set appropriate half-lives.** Episodic records that are only relevant for a few hours should have a short half-life (e.g., 3600s). Semantic records can use much longer half-lives (e.g., 604800s / 1 week).
- **Pin critical records.** Pinning prevents decay computation for those records, saving CPU during decay runs.
- **Limit tag cardinality.** Avoid creating unique tags per record. Use a controlled vocabulary of tags for efficient filtering.

## Common Error Messages

| Error Message | Cause | Solution |
|---|---|---|
| `missing metadata` | gRPC call has no metadata; auth is enabled | Add `authorization: Bearer <key>` to call metadata |
| `invalid or missing API key` | API key in metadata does not match server config | Check the key value; look for whitespace issues |
| `rate limit exceeded` | Too many requests per second | Reduce request rate or increase `rate_limit_per_second` |
| `trust context is required` | `Retrieve` or `RetrieveByID` called without `trust` | Always provide a `TrustContext` in retrieval requests |
| `min_salience must be non-negative and finite` | `min_salience` is negative, NaN, or Inf | Pass a valid non-negative float |
| `limit must be between 0 and 10000` | Retrieval `limit` is negative or exceeds 10,000 | Use a value in the valid range |
| `<field> exceeds maximum length of 100000` | String field exceeds 100 KB | Shorten the field value or summarize the content |
| `too many tags: N (max 100)` | More than 100 tags on a single record | Reduce the number of tags |
| `tag exceeds maximum length of 256` | A single tag is longer than 256 characters | Shorten the tag |
| `<field> exceeds maximum payload size of 10485760 bytes` | JSON payload exceeds 10 MB | Reduce payload size; summarize large data |
| `invalid args JSON` / `invalid result JSON` | Malformed JSON in tool output fields | Validate JSON before sending |
| `invalid object JSON` | Malformed JSON in observation object | Validate JSON before sending |
| `invalid active_constraints JSON` | Malformed JSON in working state constraints | Validate JSON before sending |
| `invalid timestamp` | Timestamp not in RFC 3339 format | Use format: `2024-01-15T09:30:00Z` |
| `validation error for id: id is required` | Record missing `id` field | Provide a UUID for the record |
| `validation error for type: type is required` | Record missing `type` field | Set a valid `MemoryType` |
| `validation error for sensitivity: sensitivity is required` | Record missing `sensitivity` | Set a valid `Sensitivity` level |
| `validation error for confidence: confidence must be in range [0, 1]` | Confidence outside valid range | Use a value between 0 and 1 |
| `validation error for salience: salience must be >= 0` | Negative salience value | Use a non-negative value |
| `validation error for payload: payload is required` | Record has no payload | Provide a type-appropriate payload |
| `UNIQUE constraint failed` | Record with this ID already exists | Use a unique ID for each new record |
| `amount must be non-negative and finite` | Penalize called with negative or invalid amount | Pass a valid non-negative float |
| `encryption key verification failed` | Wrong SQLCipher key or key/encryption mismatch | Verify the encryption key matches the database |
| `database is locked` | SQLite concurrency conflict | Ensure single-writer access; check for NFS issues |
| `failed to load config` | Config file not found or invalid YAML | Check file path and YAML syntax |
| `grpc: listen <addr>` | Cannot bind to the specified address | Check port availability; ensure no other process is using it |
| `grpc: load TLS credentials` | Certificate or key file cannot be loaded | Verify file paths and permissions |

## FAQ

### What database does Membrane use?

Membrane uses SQLite with optional SQLCipher encryption. The database is a single file on disk (default: `membrane.db`). SQLite was chosen for its simplicity, zero-configuration setup, and suitability for single-node deployments. WAL mode and foreign keys are enabled automatically.

### Can I run Membrane in memory only?

Yes. Set `db_path` to `":memory:"` in your config. Note that all data will be lost when the process exits. This is useful for testing.

### How do I back up the database?

Stop `membraned` (or ensure no writes are in progress) and copy the database file. If WAL mode is active, also copy the `-wal` and `-shm` files. Alternatively, use SQLite's `.backup` command.

### Can I run multiple Membrane instances?

Not against the same database file. SQLite does not support multi-process concurrent writes reliably. For multi-node setups, each instance should have its own database, with application-level synchronization if needed.

### How do I change the encryption key?

Membrane does not support re-keying an existing database through its API. To change the key, you would need to: (1) export all data using the old key, (2) create a new database with the new key, and (3) re-import the data.

### What happens when salience reaches zero?

It depends on the deletion policy. With `auto_prune` (the default), the record is eligible for automatic deletion during the next decay pass. With `manual_only`, the record persists at zero salience until explicitly deleted. With `never`, the record is never deleted.

### How do I prevent a record from decaying?

Set `pinned: true` in the record's lifecycle. Pinned records retain their salience indefinitely. You can also set `min_salience` to a non-zero floor value to allow partial decay but prevent the record from falling below a threshold.

### What is the difference between Reinforce and Penalize?

**Reinforce** boosts a record's salience by its `reinforcement_gain` value and resets the decay clock (`last_reinforced_at`). **Penalize** reduces salience by a specified amount. Both create audit log entries.

### How do I monitor Membrane?

Call the `GetMetrics` RPC to get a point-in-time snapshot of substrate metrics, including record counts by type, salience distributions, and operational statistics. The response is a JSON-encoded metrics snapshot.

### Why are my ingested records not showing up in retrieval?

The most common cause is a trust context mismatch. Ensure your retrieval request has a `TrustContext` with `max_sensitivity` at least as high as the record's sensitivity, and that the `scopes` list includes the record's scope. See [Retrieval Problems](#retrieval-problems) above.

### Can I use Membrane as a Go library without gRPC?

Yes. Import `github.com/GustyCube/membrane/pkg/membrane` and create a `Membrane` instance directly with `membrane.New(cfg)`. Call methods like `IngestEvent`, `Retrieve`, `Supersede`, etc. on the instance. The gRPC layer is optional.

### What is the maximum request rate?

The default rate limit is 100 requests per second, configured via `rate_limit_per_second`. Set it to `0` to disable rate limiting. The rate limiter uses a token bucket algorithm, so short bursts above the limit are allowed as long as the average stays within bounds.

### How do I reset the database?

Stop `membraned`, delete the database file (and any `-wal` / `-shm` files), then restart. A fresh schema will be created automatically on the next startup.
