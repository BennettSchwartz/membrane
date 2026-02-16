# Security

Membrane implements trust-aware retrieval to ensure that sensitive memories are only accessible to authorized requesters.

## Trust Context

Every retrieval request must include a `TrustContext` with four fields:

| Field | Type | Description |
|-------|------|-------------|
| `max_sensitivity` | string | Maximum sensitivity level the requester can access |
| `authenticated` | bool | Whether the requester has been authenticated |
| `actor_id` | string | Identifier for the requesting actor |
| `scopes` | string[] | Scopes the requester is allowed to query |

::: warning
Retrieval requests without a trust context are rejected with an error. There is no anonymous access mode.
:::

## Sensitivity Levels

Every memory record has a `sensitivity` field set during ingestion. The five levels form a strict hierarchy:

| Level | Numeric | Description |
|-------|---------|-------------|
| `public` | 0 | Freely shareable content |
| `low` | 1 | Minimal sensitivity (default for ingested records) |
| `medium` | 2 | Moderately sensitive content |
| `high` | 3 | Highly sensitive, requires elevated trust |
| `hyper` | 4 | Maximum protection, strictest access |

During retrieval, a record is accessible only if its sensitivity level is **less than or equal to** the trust context's `max_sensitivity`.

### Example

A trust context with `max_sensitivity: "medium"` can access:
- `public` records (level 0)
- `low` records (level 1)
- `medium` records (level 2)

But **cannot** access:
- `high` records (level 3)
- `hyper` records (level 4)

## Scope-Based Access

Scopes provide a second dimension of access control. Common scope values include:

- `user` -- personal to a single user
- `device` -- specific to a device
- `project` -- shared within a project
- `workspace` -- shared within a workspace
- `global` -- available everywhere

### Scope Rules

1. If the trust context specifies **one or more scopes**, only records whose `scope` matches one of the allowed scopes are returned
2. **Unscoped records** (empty `scope` field) are always accessible, regardless of the trust context's scope list
3. If the trust context has an **empty scopes list**, all scopes are allowed

### Example

```json
{
  "trust": {
    "max_sensitivity": "high",
    "authenticated": true,
    "actor_id": "agent-1",
    "scopes": ["project-alpha", "global"]
  }
}
```

This trust context can access:
- Records with `scope: "project-alpha"`
- Records with `scope: "global"`
- Records with no scope (unscoped)

But **cannot** access:
- Records with `scope: "project-beta"`
- Records with `scope: "user-private"`

## Sensitivity at Ingestion

The sensitivity level is set when a memory is ingested. If not specified, the default sensitivity from the server configuration is used (default: `"low"`).

```json
{
  "source": "my-agent",
  "event_kind": "user_input",
  "ref": "session-001/msg-1",
  "sensitivity": "high"
}
```

::: tip
Set sensitivity at ingestion time based on the content. User credentials and secrets should be `hyper`. Personal preferences might be `medium`. Public documentation references can be `public`.
:::

## Encryption at Rest

Membrane uses [SQLCipher](https://www.zetetic.net/sqlcipher/) to encrypt the database at rest. When an encryption key is configured, all data (records, payloads, audit logs) is encrypted transparently.

### Enabling Encryption

Set the key via environment variable (recommended) or config file:

```bash
# Environment variable (recommended -- keeps the key out of config files)
export MEMBRANE_ENCRYPTION_KEY="your-secret-key"
./bin/membraned
```

```yaml
# Or in config file (less secure -- key is stored on disk)
encryption_key: "your-secret-key"
```

::: warning
If you enable encryption on an existing unencrypted database, you must migrate the data. Start a new encrypted database and re-ingest records.
:::

::: tip
If no encryption key is set, the database is stored unencrypted. This is suitable for development but not recommended for production.
:::

## TLS Transport

The gRPC server supports TLS for encrypted connections between clients and the daemon.

### Enabling TLS

Provide paths to a certificate and private key in PEM format:

```yaml
tls_cert_file: /etc/membrane/tls/server.crt
tls_key_file: /etc/membrane/tls/server.key
```

Both fields must be set for TLS to activate. If either is empty, the server runs without TLS.

## Authentication

Membrane supports Bearer token authentication for gRPC clients. When an API key is configured, every request must include it in the `authorization` metadata header.

### Enabling Authentication

```bash
# Environment variable (recommended)
export MEMBRANE_API_KEY="your-api-key"
./bin/membraned
```

```yaml
# Or in config file
api_key: "your-api-key"
```

Clients must send the key as a Bearer token:

```
authorization: Bearer your-api-key
```

Requests with a missing or invalid key receive an `Unauthenticated` gRPC error. If no API key is configured, authentication is disabled.

## Rate Limiting

Membrane includes a token bucket rate limiter for gRPC endpoints.

```yaml
rate_limit_per_second: 100
```

The current limiter is applied at the server instance level (shared across incoming traffic). When the limit is exceeded, requests receive a `ResourceExhausted` gRPC error. Set `rate_limit_per_second` to `0` to disable rate limiting.

## Audit Trail

All access and modifications to memory records are tracked in the audit log. Each audit entry records:

- **Action** -- what happened (`create`, `revise`, `fork`, `merge`, `delete`, `reinforce`, `decay`)
- **Actor** -- who or what performed the action
- **Timestamp** -- when it happened
- **Rationale** -- why it was done

This provides a full trace of who accessed or modified a memory, supporting compliance and debugging.

## Security Checklist

For production deployments:

1. Set `MEMBRANE_ENCRYPTION_KEY` to encrypt the database
2. Set `MEMBRANE_API_KEY` to require authentication
3. Configure `tls_cert_file` and `tls_key_file` for encrypted transport
4. Set `rate_limit_per_second` to a sensible value (default: 100)
5. Use `sensitivity: "high"` or `"hyper"` for records containing secrets or credentials
6. Restrict `scopes` in trust contexts to limit access by project or team
