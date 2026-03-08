# pg_turret

`pg_turret` is a PostgreSQL extension that captures database logs and streams them to external destinations in real-time. It transforms PostgreSQL from a log file producer into a structured event source for modern observability platforms.

## Core Concept

Instead of parsing log files or running sidecar agents, `pg_turret` hooks directly into PostgreSQL's logging pipeline (`emit_log_hook`) to capture events as they happen, normalize them, and export them via background workers.

```
Postgres logs → pg_turret → structured events → Observability systems
```

## Features

- **Real-time Capture**: Hooks directly into PostgreSQL's error reporting.
- **Background Exporting**: Network I/O is handled by background workers to avoid impacting database performance.
- **Structured JSON**: Logs are converted to machine-readable JSON objects.
- **Multiple Adapters**: Extensible architecture supporting various destinations.
- **Batched Delivery**: Configurable batch sizes and polling intervals.

---

## Installation

### Prerequisites

- Rust and Cargo
- PostgreSQL (13-17)
- `pgrx` (v0.17.0)

### Building & Running

```bash
# Install pgrx if you haven't already
cargo install --locked cargo-pgrx
cargo pgrx init --pg16 /path/to/pg_config

# Run the extension
cargo pgrx run pg16
```

Inside the psql shell:
```sql
CREATE EXTENSION pg_turret;
```

---

## Configuration

Add `pg_turret` to `shared_preload_libraries` in your `postgresql.conf` (requires restart):

```ini
shared_preload_libraries = 'pg_turret'
```

### Global Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pg_turret.poll_interval_s` | `int` | `10` | How often (in seconds) background workers check for new logs. |

---

## Output Adapters

`pg_turret` supports multiple output destinations. Each adapter can be enabled and configured independently.

### HTTP Adapter

Exports logs to any HTTP(S) endpoint.

**Configuration:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pg_turret.http.enabled` | `bool` | `false` | Enable/disable HTTP export. |
| `pg_turret.http.endpoint` | `string` | `''` | The destination URL (e.g., `https://logs.example.com/v1/ingest`). |
| `pg_turret.http.api_key` | `string` | `''` | Optional Bearer token for the `Authorization` header. |
| `pg_turret.http.batch_size` | `int` | `100` | Max logs per request. |
| `pg_turret.http.timeout_ms` | `int` | `5000` | Request timeout. |

### Other Adapters (Coming Soon)
- **Sentry**: Native integration for error tracking.
- **Axiom**: High-performance log storage.
- **Datadog**: Direct ingestion to Datadog logs.
- **S3**: Archive logs to S3-compatible buckets.
- **WebSockets**: Real-time streaming to frontend dashboards.

---

## Log Format

Events are sent as a JSON array. Example object:

```json
{
  "timestamp": "2026-03-09T12:00:00Z",
  "level": "ERROR",
  "message": "duplicate key value violates unique constraint",
  "detail": "Key (id)=(1) already exists.",
  "sqlerrcode": 23505,
  "filename": "nbtinsert.c",
  "lineno": 671,
  "funcname": "_bt_check_unique"
}
```

---

## Development & Testing

### Regression Tests
`pg_turret` uses `pgrx` regression tests to verify behavior inside a live Postgres instance.

```bash
cargo pgrx test pg16
```

## License

MIT
