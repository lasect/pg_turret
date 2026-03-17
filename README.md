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
- **Log Filtering**: Filter by log level and regex patterns (include/exclude).
- **Retry Queue**: Automatic retry for failed submissions with configurable attempts.
- **Multiple Workers**: Scale export throughput with multiple background workers.
- **Compression**: Optional gzip compression for HTTP payloads.

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
| `pg_turret.ring_buffer_size` | `int` | `1024` | Maximum log entries in shared memory ring buffer (128-65536). |
| `pg_turret.num_workers` | `int` | `1` | Number of background workers (1-8). |

### Filter Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pg_turret.filter.level_min` | `int` | `10` | Minimum log level to capture (10=DEBUG, 17=INFO, 19=WARNING, 20=ERROR, 21=FATAL). |
| `pg_turret.filter.pattern` | `string` | `''` | Regex pattern - only logs matching this pattern will be captured. |
| `pg_turret.filter.pattern_exclude` | `string` | `''` | Regex pattern - logs matching this pattern will be excluded. |

### Retry Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pg_turret.retry.enabled` | `bool` | `true` | Enable retry queue for failed submissions. |
| `pg_turret.retry.max_attempts` | `int` | `3` | Maximum retry attempts (1-10). |
| `pg_turret.retry.queue_size` | `int` | `512` | Maximum failed entries to keep for retry (64-4096). |

---

## Output Adapters

`pg_turret` supports multiple output destinations. Each adapter can be enabled and configured independently.

### Kafka Adapter

Exports logs to a Kafka topic as JSON batches.

**Configuration:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pg_turret.kafka.enabled` | `bool` | `false` | Enable/disable Kafka export. |
| `pg_turret.kafka.brokers` | `string` | `''` | Comma-separated list of Kafka brokers (e.g., `kafka-1:9092,kafka-2:9092`). |
| `pg_turret.kafka.topic` | `string` | `''` | Kafka topic to publish logs to. |
| `pg_turret.kafka.api_key` | `string` | `''` | API key for Kafka authentication (if required by your cluster). |
| `pg_turret.kafka.api_secret` | `string` | `''` | API secret for Kafka authentication (if required by your cluster). |
| `pg_turret.kafka.timeout_ms` | `int` | `5000` | Producer request timeout in milliseconds (100-60000). |
| `pg_turret.kafka.batch_size` | `int` | `100` | Maximum number of log events per Kafka message (1-1000). |

Logs are serialized as an array of JSON objects (same structure as the [Log Format](#log-format) section) and sent to the configured topic in batches. The adapter includes retry logic with exponential backoff for transient errors such as `UnknownTopicOrPartition` to tolerate topic auto-creation and temporary broker issues without blocking PostgreSQL.

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
| `pg_turret.http.compression` | `bool` | `false` | Enable gzip compression for request bodies. |

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
  "hint": "...",
  "context": "...",
  "sqlerrcode": 23505,
  "filename": "nbtinsert.c",
  "lineno": 671,
  "funcname": "_bt_check_unique",
  "database": "mydb",
  "user": "postgres",
  "query": "INSERT INTO ..."
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
