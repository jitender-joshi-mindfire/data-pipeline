package store

import (
	"sync"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// jobProgress holds the internal tracking state for a single job.
type jobProgress struct {
	startTime       time.Time
	total           int64
	processed       int64
	stageLatencies  map[string][]time.Duration
	errorCounts     map[string]int64
}

// Compile-time check that InMemoryProgressTracker satisfies ProgressTracker.
var _ ProgressTracker = (*InMemoryProgressTracker)(nil)

// InMemoryProgressTracker implements ProgressTracker using in-memory maps
// protected by a sync.RWMutex for concurrent access.
type InMemoryProgressTracker struct {
	mu   sync.RWMutex
	jobs map[string]*jobProgress
}

// NewProgressTracker creates a new InMemoryProgressTracker.
func NewProgressTracker() *InMemoryProgressTracker {
	return &InMemoryProgressTracker{
		jobs: make(map[string]*jobProgress),
	}
}

// getOrCreate returns the jobProgress for a given jobID, creating it if needed.
// Caller must hold the write lock.
func (pt *InMemoryProgressTracker) getOrCreate(jobID string) *jobProgress {
	jp, ok := pt.jobs[jobID]
	if !ok {
		jp = &jobProgress{
			startTime:      time.Now(),
			stageLatencies: make(map[string][]time.Duration),
			errorCounts:    make(map[string]int64),
		}
		pt.jobs[jobID] = jp
	}
	return jp
}

// RecordProcessed records a successfully processed record with its latency.
func (pt *InMemoryProgressTracker) RecordProcessed(jobID string, stage string, latency time.Duration) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	jp := pt.getOrCreate(jobID)
	jp.processed++
	jp.stageLatencies[stage] = append(jp.stageLatencies[stage], latency)
}

// RecordFailed records a failed record for a stage.
func (pt *InMemoryProgressTracker) RecordFailed(jobID string, stage string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	jp := pt.getOrCreate(jobID)
	jp.errorCounts[stage]++
}

// SetTotal sets the total number of records expected for a job.
func (pt *InMemoryProgressTracker) SetTotal(jobID string, total int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	jp := pt.getOrCreate(jobID)
	jp.total = total
}

// GetProgress returns the current progress metrics for a job.
// Returns nil if the job has not been tracked.
func (pt *InMemoryProgressTracker) GetProgress(jobID string) *model.Progress {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	jp, ok := pt.jobs[jobID]
	if !ok {
		return nil
	}

	processed := jp.processed
	total := jp.total

	// Compute records pending.
	var pending int64
	if total > processed {
		pending = total - processed
	}

	// Compute percent complete (0-100), capped at 100.
	var percentComplete int
	if total > 0 {
		pct := (processed * 100) / total
		if pct > 100 {
			pct = 100
		}
		percentComplete = int(pct)
	}

	// Compute processing rate: records_processed / elapsed_seconds.
	var processingRate float64
	elapsed := time.Since(jp.startTime).Seconds()
	if elapsed > 0 {
		processingRate = float64(processed) / elapsed
	}

	// Compute per-stage average latency in milliseconds.
	stageLatencies := make(map[string]float64, len(jp.stageLatencies))
	for stage, latencies := range jp.stageLatencies {
		if len(latencies) == 0 {
			continue
		}
		var sum time.Duration
		for _, l := range latencies {
			sum += l
		}
		avgMs := float64(sum.Nanoseconds()) / float64(len(latencies)) / 1e6
		stageLatencies[stage] = avgMs
	}

	// Copy error counts.
	errorCounts := make(map[string]int64, len(jp.errorCounts))
	for stage, count := range jp.errorCounts {
		errorCounts[stage] = count
	}

	return &model.Progress{
		RecordsProcessed: processed,
		RecordsPending:   pending,
		PercentComplete:  percentComplete,
		ProcessingRate:   processingRate,
		StageLatencies:   stageLatencies,
		ErrorCounts:      errorCounts,
	}
}
