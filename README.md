# pg_turret

`pg_turret` is a PostgreSQL extension that captures database logs and streams them to external destinations in real-time.

## Features

- **Real-time Log Capture**: Hooks into PostgreSQL's error reporting to capture logs as they happen.
- **HTTP Output Stream**: Send logs to any HTTP endpoint in JSON format.
- **Batched Delivery**: Configure batch sizes to optimize network usage.
- **Customizable Scheduling**: Control how often logs are polled and sent.
- **Extensible Architecture**: Built to support multiple adapters (Axiom, Sentry, Datadog, etc. - *coming soon*).

## Installation

### Prerequisites

- Rust and Cargo
- PostgreSQL (13-17)
- `pgrx`

### Building

```bash
cargo pgrx run
```

## Configuration

Add the following to your `postgresql.conf` or set via `ALTER SYSTEM`:

```sql
# Shared preload libraries (requires restart)
shared_preload_libraries = 'pg_turret'

# HTTP Export Configuration
pg_turret.http.enabled = true
pg_turret.http.endpoint = 'https://your-log-sink.com/api/logs'
pg_turret.http.api_key = 'your-secret-api-key'
pg_turret.http.batch_size = 100
pg_turret.http.timeout_ms = 5000

# General Configuration
pg_turret.poll_interval_s = 10
```

### Configuration Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pg_turret.poll_interval_s` | `int` | `10` | Frequency in seconds to check for new logs to send. |
| `pg_turret.http.enabled` | `bool` | `false` | Enable/disable the HTTP output stream. |
| `pg_turret.http.endpoint` | `string` | `''` | The HTTP URL where logs will be POSTed. |
| `pg_turret.http.api_key` | `string` | `''` | Optional API key sent as a Bearer token in the Authorization header. |
| `pg_turret.http.batch_size` | `int` | `100` | Number of logs to include in each HTTP request. |
| `pg_turret.http.timeout_ms` | `int` | `5000` | Timeout in milliseconds for each HTTP request. |

## Log Format

Logs are sent as a JSON array. Each log object contains:

```json
{
  "timestamp": "2026-03-09T12:00:00Z",
  "level": "INFO",
  "message": "connection received",
  "detail": null,
  "hint": null,
  "context": null,
  "sqlerrcode": 0,
  "filename": "postmaster.c",
  "lineno": 1234,
  "funcname": "ProcessStartupPacket"
}
```

## License

MIT
