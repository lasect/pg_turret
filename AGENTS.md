# pg_turret

Postgres log export extension.

Rust PostgreSQL extension (pgrx).  
Captures PostgreSQL log events internally and forwards them to external systems.

PostgreSQL exposes a logging hook (`emit_log_hook`) that allows extensions to intercept log messages before they reach the server log. :contentReference[oaicite:0]{index=0}

pg_turret uses this mechanism to capture logs inside the database process and export them to observability pipelines.

---

# Core Concept

Turn PostgreSQL logs into a structured event stream.

Instead of writing logs only to files, pg_turret converts them into structured events and sends them to external systems.

```
Postgres logs
      ↓
pg_turret
      ↓
structured events
      ↓
observability systems
```

No file parsing.  
No sidecar log agents.  
No platform-specific integrations.

Runs directly inside PostgreSQL.

---

# Purpose

Provide a universal log export layer for PostgreSQL.

Typical use cases:

- centralized logging
- observability pipelines
- production debugging
- security auditing
- real-time monitoring
- incident investigation

The extension allows PostgreSQL logs to integrate with modern observability stacks.

---

# Supported Destinations

pg_turret forwards events to external ingestion endpoints.

Examples:

- Kafka
- HTTP ingestion APIs
- object storage (S3 compatible)
- OpenTelemetry collectors
- Sentry
- Datadog
- Axiom
- WebSocket / WSS streams

Multiple exporters may run simultaneously.

---

# Event Sources

pg_turret captures log messages emitted by PostgreSQL.

Typical log sources:

- SQL query logs
- errors
- warnings
- connection events
- background workers
- autovacuum logs
- extension logs

PostgreSQL normally writes these logs to files via the logging collector. :contentReference[oaicite:1]{index=1}  
pg_turret intercepts them earlier in the logging pipeline.

---

# Event Model

Logs are converted into structured events.

Typical fields:

- timestamp
- log level
- database
- user
- process id
- message
- SQL query (if available)
- duration (if present)

Events are emitted as structured objects suitable for downstream systems.

Goal: machine-readable logs.

---

# Architecture

High-level architecture:

```
PostgreSQL
     │
     │ log events
     ▼
pg_turret
     │
log capture layer
     │
event normalization
     │
internal queue
     │
background exporters
     │
 ┌──────────┬──────────┬──────────┬──────────┐
 │ Kafka    │ HTTP     │ S3       │ OTEL     │
 │ Sentry   │ Datadog  │ Axiom    │ WSS      │
 └──────────┴──────────┴──────────┴──────────┘
```

Key components:

1. log capture hook  
2. event normalization  
3. internal event buffer  
4. exporter workers

---

# Export Pipeline

The export pipeline separates log capture from network IO.

Flow:

```
log hook
  ↓
event struct
  ↓
internal queue
  ↓
background workers
  ↓
export destinations
```

Network communication never runs in the logging hook.

---

# Background Workers

Exporters run inside PostgreSQL background workers.

Responsibilities:

- consume queued events
- batch events
- send to external systems
- handle retry logic
- handle exporter failures

Workers must not block or impact database performance.

---

# Performance Constraints

PostgreSQL can generate very high log volume.

pg_turret must:

- avoid blocking the database
- avoid synchronous network operations
- avoid unbounded memory usage
- tolerate exporter failures

Database stability takes priority over log delivery.

---

# Failure Handling

Exporter failures are expected.

Examples:

- network outages
- slow ingestion endpoints
- destination rate limits

pg_turret must handle these safely.

Possible strategies:

- bounded queues
- dropping oldest events
- retry with backoff
- exporter isolation

Database operations must never stall due to exporters.

---

# Configuration

pg_turret is configured through PostgreSQL settings.

Typical configuration includes:

- enabled exporters
- exporter endpoints
- authentication tokens
- batching parameters
- queue sizes
- retry policies

Configuration should allow enabling or disabling exporters without modifying code.

---

# Non Goals

pg_turret does not attempt to provide:

- SQL performance analysis
- query optimization tooling
- metrics dashboards
- historical analytics
- log visualization

Other tools handle those areas.

pg_turret focuses only on log export.

---

# Related Work

Some PostgreSQL extensions interact with logs but solve different problems.

Examples include extensions that expose log files via SQL tables or store logs inside database tables.

For example, `pg_sqlog` allows querying PostgreSQL log files through SQL using foreign tables. :contentReference[oaicite:2]{index=2}

These tools analyze logs after they are written.

pg_turret focuses on real-time log streaming.

---

# Design Principles

Safety  
Database operations must never block due to pg_turret.

Low overhead  
Minimal CPU and memory usage.

Isolation  
Exporter failures must not impact PostgreSQL.

Structured events  
Logs converted to machine-readable format.

Extensibility  
New exporters should be easy to add.

---

# Intended Use

pg_turret is intended for environments where PostgreSQL logs need to integrate with centralized observability systems.

Examples:

- production database monitoring
- infrastructure observability
- distributed systems debugging
- audit logging pipelines
- platform engineering tooling

---

# Summary

pg_turret converts PostgreSQL logs into structured events and exports them to external observability systems.

It acts as a log export layer embedded inside PostgreSQL.

The extension transforms PostgreSQL from a log file producer into a structured event source for observability platforms.
