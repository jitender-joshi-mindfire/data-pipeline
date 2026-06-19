# Design Report — Data Processing Pipeline

## Overview

This system is a concurrent, five-stage data processing pipeline built in Go. It ingests records from CSV files, JSON files, and HTTP APIs; validates, transforms, and aggregates them; and exports results to SQLite, CSV, and JSON. A REST API manages job lifecycle, real-time metrics, and error inspection.

---

## Concurrency Model

The pipeline follows a **staged goroutine pipeline** pattern. Each stage runs as one or more goroutines, connected by typed buffered channels (`chan *model.Record`):

```
Ingester → [ch1] → Validator → [ch2] → Transformer → [ch3] → Aggregator → [ch4] → Exporter
```

**Ingestion fan-out**: The Ingester launches one goroutine per configured source (CSV, JSON, HTTP) concurrently. All records are merged into a single output channel via a `sync.WaitGroup`-coordinated close. This means a 3-source job uses 3 goroutines reading simultaneously.

**Worker pool fan-out/fan-in**: The Validator and Transformer stages use configurable worker pools (1–32 workers). A pool of N workers all read from the same input channel (Go's channel semantics provide natural load distribution — no explicit dispatcher needed) and write to the same output channel. A `sync.WaitGroup` ensures the output channel is closed only after all workers exit.

**Aggregator fan-in**: The Aggregator is intentionally single-goroutine. It collects all records before computing group-by aggregations, which requires seeing the full dataset. Parallelising partial aggregations would add complexity with minimal gain at the record counts this system targets.

**Context propagation**: Every goroutine receives a `context.Context`. On cancellation or timeout, goroutines exit their processing loop at the next `select` check. A 5-second grace period allows in-flight records to complete before force-termination.

---

## Worker Pool Sizing Rationale

- **Default: 1** — safe for all workloads; no goroutine overhead for small jobs.
- **Range: [1, 32]** — 32 matches a typical high-core-count server. Beyond 32, channel contention and scheduler overhead begin to outweigh concurrency gains for CPU-bound work.
- **Recommendation**: Validator and Transformer benefit most from N=4–8 workers on a 4-core machine. Ingester pool size has no effect today (one goroutine per source is already the model). Aggregator should remain at 1.

---

## Channel Buffering Strategy

All inter-stage channels use **buffer size 100**. This choice balances three concerns:

| Buffer size | Effect |
|-------------|--------|
| 0 (unbuffered) | Maximum back-pressure; stages block each other; high context-switch overhead |
| 100 | Decouples burst processing; absorbs 100-record speed differences between stages without blocking |
| 10,000+ | Unbounded memory growth; masks slow downstream stages instead of throttling them |

Buffer size 100 means a slow aggregator can receive 100 records before blocking the transformer, giving the aggregator time to catch up without wasting unbounded memory.

---

## Tradeoffs Considered

**In-memory stores vs. persistent stores**: All three stores (JobStore, ErrorStore, ProgressTracker) are in-memory. This means job state is lost on server restart. The tradeoff: zero deployment complexity (no database setup), zero latency on reads/writes, and no schema migration. For a production system, a SQLite or PostgreSQL-backed store would be the natural upgrade path.

**Single aggregator goroutine**: Parallelising aggregation requires either pre-sharding by group key (complex routing) or a merge phase (more channels, more synchronisation). For datasets up to ~1M records, a single goroutine aggregating in a `map[string]group` is faster than the coordination overhead of parallel partial aggregation.

**Error collection vs. error channels**: Errors are written directly to the `ErrorStore` (a thread-safe in-memory map) rather than through a dedicated `errorCh` goroutine. This eliminates a goroutine and a channel, reduces latency on error recording, and avoids the risk of the error channel becoming a bottleneck under high error rates.

**Graceful shutdown**: A 5-second grace period on cancellation was chosen to align with typical HTTP request timeouts. Shorter (1s) risks losing in-flight records under load; longer (30s) makes the API feel unresponsive to cancellation requests.
