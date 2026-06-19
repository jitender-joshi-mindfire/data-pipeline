# Data Processing Pipeline

A concurrent, multi-stage data processing pipeline built in Go. It ingests data from CSV files, JSON files, and HTTP APIs; validates and transforms each record; computes aggregations; and exports results to SQLite, PostgreSQL, CSV, or JSON — all managed through a REST API with real-time progress tracking, error inspection, and Prometheus metrics.

---

## Table of Contents

1. [Prerequisites & Dependencies](#1-prerequisites--dependencies)
2. [Build & Run](#2-build--run)
3. [System Architecture](#3-system-architecture)
4. [How the Pipeline Works](#4-how-the-pipeline-works)
5. [REST API Reference](#5-rest-api-reference)
6. [Running on Different Dataset Sizes](#6-running-on-different-dataset-sizes)
7. [Prometheus Metrics](#7-prometheus-metrics)
8. [PostgreSQL Export Target](#8-postgresql-export-target)
9. [Project Structure](#9-project-structure)
10. [Testing](#10-testing)

---

## 1. Prerequisites & Dependencies

**Runtime requirements:**

| Requirement | Version | Notes |
|-------------|---------|-------|
| Go | 1.23+ | Uses `net/http` pattern matching from 1.22 |
| CGO | enabled | Required for SQLite via `go-sqlite3` |
| C compiler | any | macOS: `xcode-select --install` · Linux: `gcc` / `build-essential` |

**Go module dependencies:**

| Package | Purpose |
|---------|---------|
| `github.com/mattn/go-sqlite3` | SQLite export target (CGO) |
| `github.com/prometheus/client_golang` | `/metrics` endpoint |
| `github.com/brianvoe/gofakeit/v7` | Synthetic data generation |
| `github.com/stretchr/testify` | Test assertions |
| `pgregory.net/rapid` | Property-based testing |

---

## 2. Build & Run

### Install dependencies

```bash
go mod download
```

### Build

```bash
# Standard build
go build -o server ./cmd/server

# Explicit CGO (needed on some CI environments)
CGO_ENABLED=1 go build -o server ./cmd/server
```

### Run the server

```bash
# Default port :8080
go run ./cmd/server

# Custom port
PORT=9090 go run ./cmd/server

# Run the pre-built binary
./server
```

The server logs each request with method, path, status, and latency:

```
2026/06/19 10:15:00 Starting server on :8080
2026/06/19 10:15:03 POST /api/v1/pipelines 201 1.2ms
2026/06/19 10:15:04 GET  /api/v1/pipelines/abc123/progress 200 0.3ms
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP listen port |

---

## 3. System Architecture

### High-level overview

```
                         ┌──────────────────────────────────────────────────────┐
                         │                  REST API  (:8080)                   │
                         │                                                      │
                         │  POST   /api/v1/pipelines          — create job      │
                         │  GET    /api/v1/pipelines          — list jobs       │
                         │  GET    /api/v1/pipelines/:id      — job details     │
                         │  GET    /api/v1/pipelines/:id/progress               │
                         │  GET    /api/v1/pipelines/:id/results                │
                         │  GET    /api/v1/pipelines/:id/errors                 │
                         │  PATCH  /api/v1/pipelines/:id/cancel                 │
                         │  DELETE /api/v1/pipelines/:id                        │
                         │  GET    /metrics                   — Prometheus      │
                         └───────────────────┬──────────────────────────────────┘
                                             │ creates & monitors
                         ┌───────────────────▼──────────────────────────────────┐
                         │                Pipeline Engine                        │
                         │                                                      │
   ┌──────────────┐      │  ┌──────────┐  ch1  ┌──────────┐  ch2  ┌──────────┐ │
   │ Data Sources │      │  │          ├───────►│          ├───────►│          │ │
   │              │      │  │ Ingester │       │ Validator│       │Transform │ │
   │  CSV files   ├─────►│  │ (fan-out)│       │ (fan-out)│       │ (fan-out)│ │
   │  JSON files  │      │  └──────────┘       └──────────┘       └────┬─────┘ │
   │  HTTP APIs   │      │                                         ch3  │       │
   └──────────────┘      │            ┌──────────────────────────────────▼──┐   │
                         │            │  Aggregator (fan-in) ──ch4──► Exporter│ │
                         │            └────────────────────────────────────┬─┘  │
                         └────────────────────────────────────────────────┼────┘
                                                                          │
                         ┌────────────────────────────────────────────────▼──────┐
                         │                  Export Targets                        │
                         │                                                        │
                         │   SQLite database (.db)                                │
                         │   CSV file (.csv)                                      │
                         │   JSON file (.json)                                    │
                         └────────────────────────────────────────────────────────┘
```

### Side channels (always running alongside the main pipeline)

```
Every stage ──► errorCh ──► Error Collector ──► ErrorStore  (queryable via API)
Every stage ──► progressCh ──► Progress Tracker ──► ProgressStore (queryable via API + /metrics)
context.Context ──────────────────────────────────► all goroutines (cancellation / timeout)
```

### Component responsibilities

| Component | Package | Role |
|-----------|---------|------|
| `Handler` | `internal/api` | HTTP request handling for all endpoints |
| `Router` | `internal/api` | Route registration, middleware chain, Prometheus registry |
| `Pipeline` | `internal/pipeline` | Orchestrates stage goroutines, channels, worker pools |
| `JobStore` | `internal/store` | In-memory CRUD for job metadata |
| `ErrorStore` | `internal/store` | Append-only in-memory error log per job |
| `ProgressTracker` | `internal/store` | Atomic counters and latency tracking per job |
| `ResultStore` | `internal/api` | In-memory storage of aggregated output |
| Export targets | `internal/export` | SQLite / CSV / JSON writers |

---

## 4. How the Pipeline Works

### Step-by-step data flow

```
Job config (JSON)
      │
      ▼
[1] POST /api/v1/pipelines
      │  ── validates config
      │  ── creates Job{status: queued}
      │  ── spawns goroutine → returns 201 immediately
      │
      ▼
[2] Ingester stage
      │  ── one goroutine per source (fan-out)
      │  ── CSV source: reads row-by-row, emits Record per row
      │  ── JSON source: reads file/response body, emits Record per array element
      │  ── HTTP source: GETs URL, parses JSON array, emits Records
      │  ── all sources write to ch1 (buffered, size 100)
      │  ── closes ch1 when all sources are drained
      │
      ▼  ch1 (chan *Record, buf 100)
[3] Validator stage
      │  ── N worker goroutines (fan-out) pull from ch1
      │  ── each record is checked against the schema: required fields,
      │     type constraints, min/max, regex patterns
      │  ── valid records → ch2
      │  ── invalid records → errorCh (stored in ErrorStore, skipped)
      │
      ▼  ch2 (chan *Record, buf 100)
[4] Transformer stage
      │  ── N worker goroutines pull from ch2
      │  ── applies transformations in order: trim, lowercase, uppercase,
      │     type_convert (string→number/date/boolean), enrich
      │  ── failed conversions → errorCh
      │  ── transformed records → ch3
      │
      ▼  ch3 (chan *Record, buf 100)
[5] Aggregator stage
      │  ── single goroutine (fan-in) accumulates all records
      │  ── groups by configured fields, computes count / sum / average
      │  ── when ch3 closes: emits aggregated rows to ch4
      │
      ▼  ch4 (chan map[string]interface{}, buf 100)
[6] Exporter stage
      │  ── one goroutine per export target (parallel)
      │  ── writes each aggregated row to SQLite / CSV / JSON
      │  ── stores results in ResultStore for API retrieval
      │
      ▼
Job{status: completed}   ──  results available at GET /pipelines/:id/results
```

### Goroutine lifecycle

Every stage goroutine:
1. **Reads** from its input channel until it is closed (signals end-of-stream).
2. **Writes** results to its output channel.
3. **Checks `ctx.Done()`** between records — exits immediately on cancellation or timeout.
4. **Decrements a `sync.WaitGroup`** when done. The output channel is closed only after all workers in that stage have exited, guaranteeing no data is lost and the next stage terminates cleanly.

### Back-pressure

Buffered channels (size 100) provide natural back-pressure. If the Exporter is slower than the Aggregator, ch4 fills up and the Aggregator blocks — it does not race ahead and exhaust memory. This self-regulates the entire pipeline without explicit throttling logic.

### Cancellation and timeouts

A `context.Context` is created per job with an optional deadline (`timeout_seconds` in the config). The context is passed to every goroutine. Calling `PATCH /api/v1/pipelines/:id/cancel` triggers the stored `context.CancelFunc`, which propagates immediately to all stage goroutines. They finish the record currently in flight and then exit.

---

## 5. REST API Reference

### Create a pipeline job

```bash
POST /api/v1/pipelines
Content-Type: application/json
```

**Minimal valid body:**

```json
{
  "sources": [
    {"type": "csv", "path": "./testdata/sample.csv"}
  ],
  "validation": {"fields": []},
  "transformations": [],
  "aggregation": {
    "functions": [{"name": "count", "field": "*", "alias": "total"}]
  },
  "exports": [
    {"type": "json", "path": "./output/results.json"}
  ]
}
```

**Full body with all options:**

```json
{
  "sources": [
    {"type": "csv",  "path": "./testdata/sample.csv"},
    {"type": "json", "path": "./testdata/sample.json"},
    {"type": "http", "path": "https://jsonplaceholder.typicode.com/posts", "timeout_seconds": 30}
  ],
  "validation": {
    "fields": [
      {"name": "amount",   "type": "number", "required": true, "min": 0, "max": 1000000},
      {"name": "email",    "type": "string", "required": true, "pattern": "^[\\w.]+@[\\w.]+$"},
      {"name": "name",     "type": "string", "required": true},
      {"name": "date",     "type": "date",   "required": false},
      {"name": "active",   "type": "boolean","required": false}
    ]
  },
  "transformations": [
    {"field": "amount",   "operation": "type_convert", "target_type": "number"},
    {"field": "email",    "operation": "lowercase"},
    {"field": "name",     "operation": "trim"}
  ],
  "aggregation": {
    "group_by": ["category"],
    "functions": [
      {"name": "count",   "field": "*",      "alias": "total_count"},
      {"name": "sum",     "field": "amount", "alias": "total_amount"},
      {"name": "average", "field": "amount", "alias": "avg_amount"}
    ]
  },
  "exports": [
    {"type": "sqlite", "path": "./output/results.db", "table_name": "aggregated_results"},
    {"type": "csv",    "path": "./output/results.csv"},
    {"type": "json",   "path": "./output/results.json"}
  ],
  "worker_pools": {
    "ingester":    2,
    "validator":   4,
    "transformer": 4,
    "aggregator":  1,
    "exporter":    2
  },
  "timeout_seconds": 3600
}
```

**Response `201 Created`:**

```json
{"id": "a1b2c3d4-...", "status": "queued", "created_at": "2026-06-19T10:15:00Z"}
```

**Validation error `400 Bad Request`:**

```json
{
  "error": "invalid job configuration",
  "details": [
    {"field": "sources", "message": "at least one source is required"},
    {"field": "aggregation.functions", "message": "at least one aggregation function is required"}
  ]
}
```

---

### List all jobs

```bash
GET /api/v1/pipelines
```

```json
{
  "jobs": [
    {"id": "a1b2c3d4", "status": "completed", "created_at": "2026-06-19T10:15:00Z"},
    {"id": "e5f6g7h8", "status": "running",   "created_at": "2026-06-19T10:42:30Z"}
  ]
}
```

---

### Get job details

```bash
GET /api/v1/pipelines/:id
```

```json
{
  "id": "a1b2c3d4",
  "status": "running",
  "created_at": "2026-06-19T10:15:00Z",
  "records_processed": 42800,
  "records_pending": 41431,
  "percent_complete": 51,
  "config": { "...full config..." }
}
```

---

### Get live progress & metrics

```bash
GET /api/v1/pipelines/:id/progress
```

```json
{
  "records_processed": 42800,
  "records_pending":   41431,
  "percent_complete":  51,
  "processing_rate":   1420.5,
  "stage_latencies": {
    "ingester":    2.1,
    "validator":   0.8,
    "transformer": 1.3,
    "aggregator":  0.4,
    "exporter":    3.7
  },
  "error_counts": {
    "validator":   312,
    "transformer": 14
  }
}
```

`processing_rate` is records/second. `stage_latencies` is average ms per record per stage.

---

### Get results

Available only after the job reaches `completed` status.

```bash
GET /api/v1/pipelines/:id/results
```

```json
{
  "results": [
    {"category": "electronics", "total_count": 150, "total_amount": 45000.50, "avg_amount": 300.00},
    {"category": "clothing",    "total_count": 80,  "total_amount": 12000.00, "avg_amount": 150.00}
  ],
  "metadata": {
    "total_input_records":  5000,
    "total_output_records": 2,
    "completed_at":         "2026-06-19T10:16:02Z"
  }
}
```

Returns `409 Conflict` if the job is still running, failed, or cancelled.

---

### Get errors (paginated)

```bash
GET /api/v1/pipelines/:id/errors?offset=0&limit=50
```

```json
{
  "errors": [
    {
      "id":        "err-001",
      "job_id":    "a1b2c3d4",
      "stage":     "validator",
      "message":   "field 'amount' value -5 is below minimum 0",
      "record":    {"name": "Alice", "amount": "-5", "email": "alice@example.com"},
      "timestamp": "2026-06-19T10:15:03Z"
    }
  ],
  "total":  326,
  "offset": 0,
  "limit":  50
}
```

Default: `offset=0`, `limit=50`, max `limit=200`. The `total` field gives the full count across all pages.

---

### Cancel a running job

```bash
PATCH /api/v1/pipelines/:id/cancel
```

```json
{"id": "a1b2c3d4", "status": "cancelled", "message": "Cancellation initiated"}
```

Returns `409 Conflict` if the job is not currently running.

---

### Delete a job

```bash
DELETE /api/v1/pipelines/:id
```

Returns `204 No Content`. Returns `409 Conflict` if the job is currently running (cancel it first).

---

### Using the ready-made job configs

The `testdata/job_configs/` directory contains five ready-to-use job specs:

```bash
# 200-record height/weight CSV from FSU (fast, good for smoke tests)
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" -d @testdata/job_configs/iris_csv.json

# 300K+ row COVID-19 CSV from Our World in Data (large dataset test)
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" -d @testdata/job_configs/covid_csv.json

# JSONPlaceholder posts API (100 records, group by userId)
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" -d @testdata/job_configs/jsonplaceholder_posts.json

# JSONPlaceholder users API (10 records)
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" -d @testdata/job_configs/randomuser_api.json

# Mixed-source Global Daily Report: CSV + 2 JSON APIs concurrently (800 total records)
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" -d @testdata/job_configs/global_daily_report.json
```

---

## 6. Running Jobs on Each Source Type

All three source types use the same pipeline stages — only the `sources` block changes.

---

### CSV File

Fields generated by `datagen`: `id`, `name`, `email`, `amount`, `date`, `category`, `active`, `quantity`, `rating`

```bash
# Generate test file first
go run ./cmd/datagen --rows 1000 --output testdata/medium.csv
```

```json
{
  "sources": [
    { "type": "csv", "path": "testdata/medium.csv" }
  ],
  "validation": {
    "fields": [
      { "name": "id",       "type": "string", "required": true },
      { "name": "name",     "type": "string", "required": true },
      { "name": "email",    "type": "string", "required": true },
      { "name": "amount",   "type": "string", "required": true },
      { "name": "category", "type": "string", "required": true }
    ]
  },
  "transformations": [
    { "field": "category", "operation": "uppercase" },
    { "field": "email",    "operation": "lowercase" }
  ],
  "aggregation": {
    "group_by": ["category"],
    "functions": [
      { "name": "count",   "field": "id",     "alias": "record_count" },
      { "name": "sum",     "field": "amount",  "alias": "total_amount" },
      { "name": "average", "field": "amount",  "alias": "avg_amount" }
    ]
  },
  "exports": [
    { "type": "json",   "path": "output/csv_results.json" },
    { "type": "sqlite", "path": "output/csv_results.db", "table_name": "summary" }
  ],
  "worker_pools": { "ingester": 1, "validator": 4, "transformer": 4, "aggregator": 1, "exporter": 2 },
  "timeout_seconds": 60
}
```

---

### JSON File

Fields generated by `datagen --format json`: same as CSV (`id`, `name`, `email`, `amount`, `date`, `category`, `active`, `quantity`, `rating`)

```bash
go run ./cmd/datagen --rows 500 --format json --output testdata/small.json
```

```json
{
  "sources": [
    { "type": "json", "path": "testdata/small.json" }
  ],
  "validation": {
    "fields": [
      { "name": "id",       "type": "string", "required": true },
      { "name": "amount",   "type": "string", "required": true },
      { "name": "category", "type": "string", "required": true }
    ]
  },
  "transformations": [
    { "field": "category", "operation": "uppercase" }
  ],
  "aggregation": {
    "group_by": ["category"],
    "functions": [
      { "name": "count",   "field": "id",     "alias": "record_count" },
      { "name": "sum",     "field": "amount",  "alias": "total_amount" }
    ]
  },
  "exports": [
    { "type": "json", "path": "output/json_results.json" }
  ],
  "worker_pools": { "ingester": 1, "validator": 2, "transformer": 2, "aggregator": 1, "exporter": 1 },
  "timeout_seconds": 30
}
```

---

### HTTP API Source

The pipeline fetches the URL, parses the JSON response array, and feeds records into the same stages. No file download needed.

**JSONPlaceholder Posts** — fields: `id`, `userId`, `title`, `body`

```json
{
  "sources": [
    { "type": "http", "path": "https://jsonplaceholder.typicode.com/posts", "timeout_seconds": 30 }
  ],
  "validation": {
    "fields": [
      { "name": "id",     "type": "string", "required": true },
      { "name": "title",  "type": "string", "required": true },
      { "name": "userId", "type": "string", "required": true }
    ]
  },
  "transformations": [
    { "field": "title", "operation": "uppercase" }
  ],
  "aggregation": {
    "group_by": ["userId"],
    "functions": [
      { "name": "count", "field": "id", "alias": "post_count" }
    ]
  },
  "exports": [
    { "type": "json", "path": "output/posts_by_user.json" }
  ],
  "worker_pools": { "ingester": 1, "validator": 2, "transformer": 2, "aggregator": 1, "exporter": 1 },
  "timeout_seconds": 30
}
```

**DummyJSON Products** — fields: `id`, `title`, `price`, `stock`, `category`, `rating`

```json
{
  "sources": [
    { "type": "http", "path": "https://dummyjson.com/products?limit=100", "timeout_seconds": 30 }
  ],
  "validation": {
    "fields": [
      { "name": "id",       "type": "string", "required": true },
      { "name": "title",    "type": "string", "required": true },
      { "name": "price",    "type": "string", "required": true },
      { "name": "category", "type": "string", "required": true }
    ]
  },
  "transformations": [
    { "field": "title",    "operation": "uppercase" },
    { "field": "category", "operation": "uppercase" }
  ],
  "aggregation": {
    "group_by": ["category"],
    "functions": [
      { "name": "count",   "field": "id",    "alias": "product_count" },
      { "name": "average", "field": "price", "alias": "avg_price" },
      { "name": "sum",     "field": "stock", "alias": "total_stock" }
    ]
  },
  "exports": [
    { "type": "json",   "path": "output/products_by_category.json" },
    { "type": "sqlite", "path": "output/products.db", "table_name": "product_summary" }
  ],
  "worker_pools": { "ingester": 1, "validator": 2, "transformer": 2, "aggregator": 1, "exporter": 1 },
  "timeout_seconds": 30
}
```

---

### Mixed Sources (CSV + JSON + HTTP in one job)

All three sources run concurrently. Records are merged into a single stream through all pipeline stages.

```json
{
  "sources": [
    { "type": "csv",  "path": "testdata/small.csv" },
    { "type": "json", "path": "testdata/small.json" },
    { "type": "http", "path": "https://jsonplaceholder.typicode.com/posts", "timeout_seconds": 30 }
  ],
  "validation": {
    "fields": [
      { "name": "id",   "type": "string", "required": true },
      { "name": "name", "type": "string", "required": false }
    ]
  },
  "transformations": [],
  "aggregation": {
    "functions": [
      { "name": "count", "field": "id", "alias": "total_records" }
    ]
  },
  "exports": [
    { "type": "json", "path": "output/mixed_results.json" }
  ],
  "worker_pools": { "ingester": 3, "validator": 4, "transformer": 2, "aggregator": 1, "exporter": 1 },
  "timeout_seconds": 60
}
```

> **Note on validation types:** All pipeline sources (CSV, JSON, HTTP) store field values as strings internally. Always use `"type": "string"` in validation for fields coming from any source. Use `"type": "number"` only with `"operation": "type_convert"` in transformations to convert first.

---

## 6b. Running on Different Dataset Sizes

### Dataset size guide

| Dataset size | Source | Worker pools | Timeout | Notes |
|-------------|--------|--------------|---------|-------|
| < 1K records | Local CSV/JSON | defaults (all 1) | 60s | No tuning needed |
| 1K – 10K | Local or HTTP | validator: 2, transformer: 2 | 120s | Light parallelism |
| 10K – 100K | Local files | validator: 4–8, transformer: 4–8 | 300s | Scale with CPU count |
| 100K – 1M | Local files | validator: 8–16, transformer: 8–16 | 900s | Monitor back-pressure via `/metrics` |
| 300K+ (streaming) | HTTP (e.g. COVID CSV) | validator: 4, transformer: 4 | 600s | Network is the bottleneck |

### Generating synthetic test data

```bash
# 1K records with 5% invalid data (CSV)
go run ./cmd/datagen -rows 1000 -format csv -output testdata/large/data_1k.csv

# 10K records (JSON)
go run ./cmd/datagen -rows 10000 -format json -output testdata/large/data_10k.json

# 100K records, no invalid data
go run ./cmd/datagen -rows 100000 -format csv -output testdata/large/data_100k.csv -invalid-pct 0

# Reproducible dataset (fixed seed)
go run ./cmd/datagen -rows 50000 -format csv -output testdata/large/data_50k.csv -seed 42
```

Generated records have these fields: `name`, `email`, `amount` (0–999.99), `date` (RFC3339), `category` (10 values), `active` (bool), `quantity` (1–100), `rating` (0.1–5.0).

### Worker pool sizing rules

```
ingester    — one goroutine per source; set to the number of sources
validator   — CPU-bound; set to runtime.NumCPU() or 2×NumCPU() for IO-heavy validation
transformer — CPU-bound; same as validator
aggregator  — always 1; it is a fan-in stage and must serialize writes
exporter    — set to the number of export targets (1–3 typically)
```

Example job body for a 100K-record local CSV on an 8-core machine:

```json
{
  "sources": [{"type": "csv", "path": "./testdata/large/data_100k.csv"}],
  "worker_pools": {
    "ingester":    1,
    "validator":   8,
    "transformer": 8,
    "aggregator":  1,
    "exporter":    2
  },
  "timeout_seconds": 300
}
```

### Monitoring throughput while running

Poll the progress endpoint while a large job runs:

```bash
JOB_ID="<id from POST response>"

watch -n 1 "curl -s http://localhost:8080/api/v1/pipelines/$JOB_ID/progress | \
  python3 -m json.tool"
```

Key fields to watch:
- `processing_rate` — records/sec; if this drops, a downstream stage is a bottleneck
- `stage_latencies.exporter` — if this is high, add more exporter workers or switch to a faster export target
- `error_counts.validator` — if this spikes, your data has quality issues; inspect `/errors`

---

## 7. Prometheus Metrics

The server exposes a Prometheus-compatible `/metrics` endpoint. It uses a **private registry** (no global default metrics conflicts) and serves three metric families:

1. **Pipeline-specific metrics** — collected live from the job store and progress tracker at each scrape
2. **Go runtime metrics** — goroutine count, GC pauses, heap usage, etc.
3. **Process metrics** — CPU time, open file descriptors, resident memory

### Scraping the endpoint

```bash
# One-shot: view all metrics in Prometheus text format
curl http://localhost:8080/metrics

# View only pipeline metrics
curl -s http://localhost:8080/metrics | grep ^pipeline_
```

### Pipeline metric reference

| Metric name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `pipeline_jobs_total` | Gauge | `status` | Number of jobs in each status (queued / running / completed / failed / cancelled) |
| `pipeline_records_processed_total` | Gauge | `job_id` | Records that completed all pipeline stages for this job |
| `pipeline_records_pending` | Gauge | `job_id` | Records not yet fully processed |
| `pipeline_percent_complete` | Gauge | `job_id` | Completion percentage 0–100 |
| `pipeline_processing_rate_records_per_sec` | Gauge | `job_id` | Current throughput in records/second |
| `pipeline_stage_latency_ms` | Gauge | `job_id`, `stage` | Average milliseconds per record at each stage |
| `pipeline_stage_errors_total` | Gauge | `job_id`, `stage` | Total errors produced by each stage |

### Example scrape output (with one running job)

```
# HELP pipeline_jobs_total Total number of pipeline jobs grouped by status.
# TYPE pipeline_jobs_total gauge
pipeline_jobs_total{status="cancelled"} 0
pipeline_jobs_total{status="completed"} 2
pipeline_jobs_total{status="failed"}    0
pipeline_jobs_total{status="queued"}    0
pipeline_jobs_total{status="running"}   1

# HELP pipeline_records_processed_total Total number of records processed by a pipeline job.
# TYPE pipeline_records_processed_total gauge
pipeline_records_processed_total{job_id="a1b2c3d4"} 42800

# HELP pipeline_records_pending Number of records still pending in a pipeline job.
# TYPE pipeline_records_pending gauge
pipeline_records_pending{job_id="a1b2c3d4"} 41431

# HELP pipeline_percent_complete Completion percentage (0-100) of a pipeline job.
# TYPE pipeline_percent_complete gauge
pipeline_percent_complete{job_id="a1b2c3d4"} 51

# HELP pipeline_processing_rate_records_per_sec Current processing rate in records per second.
# TYPE pipeline_processing_rate_records_per_sec gauge
pipeline_processing_rate_records_per_sec{job_id="a1b2c3d4"} 1420.5

# HELP pipeline_stage_latency_ms Average latency in milliseconds per record for a pipeline stage.
# TYPE pipeline_stage_latency_ms gauge
pipeline_stage_latency_ms{job_id="a1b2c3d4",stage="aggregator"}  0.4
pipeline_stage_latency_ms{job_id="a1b2c3d4",stage="exporter"}    3.7
pipeline_stage_latency_ms{job_id="a1b2c3d4",stage="ingester"}    2.1
pipeline_stage_latency_ms{job_id="a1b2c3d4",stage="transformer"} 1.3
pipeline_stage_latency_ms{job_id="a1b2c3d4",stage="validator"}   0.8

# HELP pipeline_stage_errors_total Total number of errors produced by a pipeline stage.
# TYPE pipeline_stage_errors_total gauge
pipeline_stage_errors_total{job_id="a1b2c3d4",stage="transformer"} 14
pipeline_stage_errors_total{job_id="a1b2c3d4",stage="validator"}   312
```

### Configuring Prometheus to scrape this server

Add this job to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: data-pipeline
    static_configs:
      - targets: ["localhost:8080"]
    metrics_path: /metrics
    scrape_interval: 5s
```

Then start Prometheus:

```bash
prometheus --config.file=prometheus.yml
```

Open `http://localhost:9090` and query:

```promql
# Throughput of all running jobs
pipeline_processing_rate_records_per_sec

# Error rate: validator errors as % of records processed
rate(pipeline_stage_errors_total{stage="validator"}[1m])
  / rate(pipeline_records_processed_total[1m])

# Which stage is slowest?
topk(1, pipeline_stage_latency_ms)
```

---

## 8. PostgreSQL Export Target

The `postgres` export type writes aggregated results to a PostgreSQL table. It uses the same `ExportTarget` interface as SQLite — the pipeline code is unaware of which database backs it.

### How it works

1. On first `Write`, the target issues `CREATE TABLE IF NOT EXISTS` with column types inferred from the result values (`TEXT`, `BIGINT`, `DOUBLE PRECISION`, `BOOLEAN`).
2. All rows are inserted inside a single transaction using a prepared statement with `$N` placeholders (libpq style).
3. Subsequent `Write` calls on the same table succeed because of `IF NOT EXISTS` — rows accumulate across calls.

### Configuration

Set `"type": "postgres"` in the exports array. The `"path"` field holds the connection string (DSN). `"table_name"` is the destination table (defaults to `"results"` if omitted).

```json
{
  "exports": [
    {
      "type":       "postgres",
      "path":       "postgres://user:pass@localhost:5432/mydb?sslmode=disable",
      "table_name": "pipeline_results"
    }
  ]
}
```

If `"path"` is left blank, the server falls back to the `POSTGRES_DSN` environment variable:

```bash
POSTGRES_DSN="postgres://user:pass@localhost:5432/mydb?sslmode=disable" go run ./cmd/server
```

### DSN formats accepted by `lib/pq`

```
# URL format
postgres://user:password@host:5432/dbname?sslmode=disable

# Key-value format
host=localhost port=5432 user=myuser password=mypass dbname=mydb sslmode=disable
```

### Combining export targets

You can write to multiple targets in the same job. Results are written to all of them in parallel:

```json
{
  "exports": [
    {"type": "postgres", "path": "postgres://user:pass@localhost:5432/mydb?sslmode=disable", "table_name": "covid_summary"},
    {"type": "sqlite",   "path": "./output/covid_summary.db",   "table_name": "covid_summary"},
    {"type": "json",     "path": "./output/covid_summary.json"}
  ]
}
```

### Running the integration tests

The PostgreSQL tests require a live database. Point `POSTGRES_DSN` at any reachable instance (local Docker, cloud, etc.) and run:

```bash
# Start a local Postgres with Docker
docker run -d --name pg-test \
  -e POSTGRES_USER=pipeline \
  -e POSTGRES_PASSWORD=pipeline \
  -e POSTGRES_DB=pipeline \
  -p 5432:5432 \
  postgres:16-alpine

# Run postgres tests only
POSTGRES_DSN="postgres://pipeline:pipeline@localhost:5432/pipeline?sslmode=disable" \
  go test -v ./internal/export/ -run TestPostgresTarget

# Run the full test suite with postgres enabled
POSTGRES_DSN="postgres://pipeline:pipeline@localhost:5432/pipeline?sslmode=disable" \
  go test -race ./...
```

Tests that require `POSTGRES_DSN` are automatically **skipped** when the variable is not set, so the standard `go test ./...` command always passes without a database.

---

## 9. Project Structure

```
data-pipeline/
│
├── cmd/
│   ├── server/main.go          # Entry point — wires all dependencies and starts HTTP server
│   └── datagen/main.go         # Synthetic dataset generator (CSV / JSON)
│
├── internal/
│   ├── api/
│   │   ├── router.go           # Route registration, middleware chain, Prometheus registry
│   │   ├── handler.go          # CreateJob, ListJobs, GetJob, DeleteJob, GetProgress, GetResults
│   │   ├── cancel_handler.go   # CancelJob — calls context.CancelFunc via sync.Map
│   │   ├── errors_handler.go   # GetErrors — paginated error listing
│   │   ├── results_handler.go  # InMemoryResultStore
│   │   ├── metrics.go          # pipelineCollector — Prometheus custom Collector
│   │   └── middleware.go       # LoggingMiddleware, RecoveryMiddleware
│   │
│   ├── config/
│   │   ├── config.go           # ApplyEnvOverrides
│   │   └── validate.go         # ValidateJobConfig — returns typed ValidationError slice
│   │
│   ├── pipeline/
│   │   ├── pipeline.go         # Pipeline.Run — orchestrates stage goroutines
│   │   ├── stage.go            # Stage interface
│   │   ├── ingester.go         # Ingester — fan-out per source, merges into ch1
│   │   ├── ingester_stage.go   # IngesterStage — wires Ingester into pipeline
│   │   ├── validator.go        # Validator — schema checks, emits to errorCh on failure
│   │   ├── transformer.go      # Transformer — type converts, trims, normalizes
│   │   ├── aggregator.go       # Aggregator — group-by, count/sum/average
│   │   ├── exporter.go         # Exporter — fans out to export targets
│   │   ├── worker_pool.go      # Generic fan-out worker pool
│   │   ├── source.go           # Source interface (CSVSource)
│   │   ├── json_source.go      # JSONSource — reads JSON file or HTTP response
│   │   └── http_source.go      # HTTPSource — fetches URL, parses JSON array
│   │
│   ├── model/
│   │   ├── job.go              # Job, JobConfig, SourceConfig, ExportConfig, etc.
│   │   ├── record.go           # Record (fields map + metadata)
│   │   ├── progress.go         # Progress (counters, rates, latencies)
│   │   └── error_entry.go      # ErrorEntry (stage, message, failed record)
│   │
│   ├── store/
│   │   ├── job_store.go        # JobStore interface + InMemoryJobStore
│   │   ├── error_store.go      # ErrorStore interface
│   │   ├── error_store_impl.go # InMemoryErrorStore — append-only, per-job
│   │   ├── progress_tracker.go # ProgressTracker interface
│   │   └── progress_tracker_impl.go # Atomic counters, sliding-window rate
│   │
│   └── export/
│       ├── target.go           # ExportTarget interface
│       ├── sqlite.go           # SQLiteTarget — CGO, creates table dynamically
│       ├── postgres.go         # PostgresTarget — lib/pq, $N placeholders, CREATE TABLE IF NOT EXISTS
│       ├── csv.go              # CSVTarget — streams rows to file
│       └── json.go             # JSONTarget — writes JSON array to file
│
├── testdata/
│   ├── sample.csv              # Small sample for unit tests
│   ├── sample.json             # Small sample for unit tests
│   ├── job_configs/            # Ready-to-POST job specifications
│   │   ├── covid_csv.json      # 300K+ record COVID-19 dataset
│   │   ├── iris_csv.json       # 200-record height/weight CSV
│   │   ├── jsonplaceholder_posts.json
│   │   ├── randomuser_api.json
│   │   └── global_daily_report.json  # Mixed CSV + 2 JSON APIs
│   ├── api_responses/          # Example API response files (all endpoints)
│   └── large/                  # Generated large datasets (gitignored)
│
├── DESIGN_REPORT.md            # ≤1 page design summary
├── go.mod
└── go.sum
```

---

## 10. Testing

```bash
# Run all tests
go test ./...

# With race detector (recommended before committing)
go test -race ./...

# Verbose output for a specific package
go test -v ./internal/pipeline/...

# Load tests — runs pipelines on 1K and 10K synthetic records, asserts throughput
go test -tags load -timeout 120s -v ./internal/pipeline/ -run TestLoad

# Real-world API integration tests — fetches live data from JSONPlaceholder and FSU CSV
# Auto-skips if the network is unreachable
go test -tags load -timeout 120s -v ./internal/pipeline/ -run TestRealWorld

# Property-based tests only
go test -v ./... -run TestProperty
```

### What the tests cover

| Test type | Location | What it verifies |
|-----------|----------|------------------|
| Unit tests | each `*_test.go` | Individual stage logic: parsing, validation rules, transform operations, aggregation math |
| Integration tests | `pipeline_integration_test.go` | Full pipeline on sample data: correct output, error count, progress tracking |
| Property-based tests | `*_property_test.go` | Invariants across random inputs (rapid): record conservation, no data races, channel closure |
| Load tests | `load_test.go` | Throughput on 1K–10K records; asserts > 500 records/sec |
| Real-world tests | `realworld_test.go` | Live pipeline runs against JSONPlaceholder and FSU CSV endpoints |
| API handler tests | `internal/api/*_test.go` | HTTP handlers: correct status codes, response shapes, error paths |
