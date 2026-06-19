# Data Processing Pipeline

A concurrent data processing pipeline system built in Go. It ingests data from multiple sources (CSV, JSON, HTTP APIs), validates, transforms, aggregates, and exports results through five sequential stages connected by buffered channels. A REST API exposes job lifecycle management, real-time progress tracking, error inspection, and result retrieval.

## Prerequisites

- **Go 1.21+** (project uses Go 1.23)
- **CGO enabled** (required for SQLite support via `go-sqlite3`)
  - On macOS: Xcode Command Line Tools (`xcode-select --install`)
  - On Linux: `gcc` or `build-essential` package
- **Git** (for dependency management)

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/mattn/go-sqlite3` | SQLite export target (requires CGO) |
| `github.com/stretchr/testify` | Test assertions |
| `pgregory.net/rapid` | Property-based testing |

## Build

```bash
# Build the server binary
go build ./cmd/server

# Build with CGO explicitly enabled (if needed)
CGO_ENABLED=1 go build ./cmd/server
```

## Run

```bash
# Run directly
go run ./cmd/server

# Or run the built binary
./server

# With custom port
PORT=9090 go run ./cmd/server
```

The server starts on port `:8080` by default. Set the `PORT` environment variable to override.

## Test

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run tests for a specific package
go test ./internal/pipeline/...

# Run with race detector
go test -race ./...
```

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `PORT` | HTTP server listen port | `8080` |

Source file paths and API URLs in job configurations can be overridden via environment variables.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              REST API (:8080)                                    │
│   POST /api/v1/pipelines  GET /progress  GET /results  GET /errors  PATCH /cancel│
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│                           Pipeline Engine                                        │
│                                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌────────────┐    ┌──────────┐    ┌──────────┐ │
│  │          │    │          │    │            │    │          │    │          │ │
│  │ Ingester ├───►│Validator ├───►│Transformer ├───►│Aggregator├───►│ Exporter │ │
│  │          │    │          │    │            │    │          │    │          │ │
│  └────┬─────┘    └──────────┘    └────────────┘    └──────────┘    └────┬─────┘ │
│       │         ch1 buf:100     ch2 buf:100      ch3 buf:100     ch4 buf:100    │
│       │                                                                  │       │
└───────┼──────────────────────────────────────────────────────────────────┼───────┘
        │                                                                  │
┌───────▼───────┐                                              ┌───────────▼──────┐
│  Data Sources │                                              │  Export Targets   │
│               │                                              │                   │
│  • CSV Files  │                                              │  • SQLite DB      │
│  • JSON Files │                                              │  • CSV Files      │
│  • HTTP APIs  │                                              │  • JSON Files     │
└───────────────┘                                              └───────────────────┘
```

**Data Flow:**

```
Data Sources → Ingester → [ch1 buf:100] → Validator → [ch2 buf:100] → Transformer → [ch3 buf:100] → Aggregator → [ch4 buf:100] → Exporter → Export Targets
```

Each stage runs as one or more goroutines with configurable worker pools (1–32 workers). Stages communicate through typed buffered channels (`chan *Record`, buffer size 100) providing back-pressure and decoupled processing rates.

## API Examples

### Create a Pipeline Job

```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [
      {"type": "csv", "path": "./testdata/sample.csv"},
      {"type": "json", "path": "./testdata/sample.json"}
    ],
    "validation": {
      "fields": [
        {"name": "amount", "type": "number", "required": true, "min": 0, "max": 1000000},
        {"name": "email", "type": "string", "required": true, "pattern": "^[\\w.]+@[\\w.]+$"},
        {"name": "name", "type": "string", "required": true}
      ]
    },
    "transformations": [
      {"field": "amount", "operation": "type_convert", "target_type": "number"},
      {"field": "email", "operation": "lowercase"},
      {"field": "name", "operation": "trim"}
    ],
    "aggregation": {
      "group_by": ["category"],
      "functions": [
        {"name": "count", "field": "*", "alias": "total_count"},
        {"name": "sum", "field": "amount", "alias": "total_amount"},
        {"name": "average", "field": "amount", "alias": "avg_amount"}
      ]
    },
    "exports": [
      {"type": "sqlite", "path": "./results.db", "table_name": "aggregated_results"},
      {"type": "csv", "path": "./results.csv"},
      {"type": "json", "path": "./results.json"}
    ],
    "worker_pools": {
      "ingester": 2,
      "validator": 4,
      "transformer": 4,
      "aggregator": 1,
      "exporter": 2
    },
    "timeout_seconds": 3600
  }'
```

**Response (201 Created):**

```json
{
  "id": "job-a1b2c3d4",
  "status": "queued",
  "created_at": "2024-01-15T10:30:00Z"
}
```

### List All Jobs

```bash
curl http://localhost:8080/api/v1/pipelines
```

**Response (200 OK):**

```json
{
  "jobs": [
    {"id": "job-a1b2c3d4", "status": "running", "created_at": "2024-01-15T10:30:00Z"},
    {"id": "job-e5f6g7h8", "status": "completed", "created_at": "2024-01-15T09:00:00Z"}
  ]
}
```

### Get Job Details

```bash
curl http://localhost:8080/api/v1/pipelines/job-a1b2c3d4
```

**Response (200 OK):**

```json
{
  "id": "job-a1b2c3d4",
  "config": { "..." },
  "status": "running",
  "created_at": "2024-01-15T10:30:00Z",
  "records_processed": 4500,
  "records_pending": 500,
  "percent_complete": 90
}
```

### Get Job Progress

```bash
curl http://localhost:8080/api/v1/pipelines/job-a1b2c3d4/progress
```

**Response (200 OK):**

```json
{
  "records_processed": 4500,
  "records_pending": 500,
  "percent_complete": 90,
  "processing_rate": 150.5,
  "stage_latencies": {
    "ingester": 2.1,
    "validator": 1.5,
    "transformer": 3.2,
    "aggregator": 0.8,
    "exporter": 5.0
  },
  "error_counts": {
    "validator": 12,
    "transformer": 3
  }
}
```

### Get Job Results

```bash
curl http://localhost:8080/api/v1/pipelines/job-a1b2c3d4/results
```

**Response (200 OK):**

```json
{
  "results": [
    {"category": "electronics", "total_count": 150, "total_amount": 45000.50, "avg_amount": 300.00},
    {"category": "clothing", "total_count": 80, "total_amount": 12000.00, "avg_amount": 150.00}
  ],
  "metadata": {
    "total_input_records": 5000,
    "total_output_records": 2,
    "completed_at": "2024-01-15T10:45:00Z"
  }
}
```

### Get Job Errors (with Pagination)

```bash
# Default pagination (offset=0, limit=50)
curl http://localhost:8080/api/v1/pipelines/job-a1b2c3d4/errors

# Custom pagination
curl "http://localhost:8080/api/v1/pipelines/job-a1b2c3d4/errors?offset=10&limit=25"
```

**Response (200 OK):**

```json
{
  "errors": [
    {
      "id": "err-001",
      "stage": "validator",
      "message": "field 'amount' failed numeric range check: value -5 is below minimum 0",
      "record": {"name": "Test", "amount": "-5", "email": "test@example.com"},
      "timestamp": "2024-01-15T10:31:05Z"
    }
  ],
  "total": 15,
  "offset": 0,
  "limit": 50
}
```

### Cancel a Running Job

```bash
curl -X PATCH http://localhost:8080/api/v1/pipelines/job-a1b2c3d4/cancel
```

**Response (202 Accepted):**

```json
{
  "id": "job-a1b2c3d4",
  "status": "cancelled",
  "message": "Cancellation initiated"
}
```

### Delete a Job

```bash
curl -X DELETE http://localhost:8080/api/v1/pipelines/job-a1b2c3d4
```

**Response:** `204 No Content`

## Design

### Concurrency Model

The pipeline uses Go's concurrency primitives to achieve parallel data processing:

- **Goroutines**: Each pipeline stage runs as one or more goroutines. Worker pools within a stage use a fan-out/fan-in pattern where a distributor sends records to N workers, and a collector merges results into the output channel.

- **Channels**: Stages are connected by typed buffered channels (`chan *Record`, buffer size 100). Channel buffering provides natural back-pressure — a slow downstream stage causes the upstream channel to fill, throttling the producer. Closing a channel signals end-of-stream to the next stage.

- **Worker Pools**: Each stage supports 1–32 configurable workers. Workers process records concurrently within a stage, coordinated by `sync.WaitGroup`. The output channel is closed only after all workers complete, ensuring no data loss.

- **Context Cancellation**: A `context.Context` propagates timeout and user-initiated cancellation to all goroutines. When cancelled, workers finish in-flight records (up to a 5-second grace period), then exit. This ensures clean shutdown without resource leaks.

### Project Structure

```
data-pipeline/
├── cmd/server/main.go          # Entry point, wires dependencies
├── internal/
│   ├── api/                    # REST handlers, router, middleware
│   ├── config/                 # Job configuration and validation
│   ├── pipeline/               # Pipeline orchestrator and stages
│   ├── model/                  # Core data types (Record, Job, Error, Progress)
│   ├── store/                  # In-memory stores (jobs, errors, progress)
│   └── export/                 # Export targets (SQLite, CSV, JSON)
├── testdata/                   # Sample CSV and JSON files for testing
├── go.mod
└── go.sum
```
