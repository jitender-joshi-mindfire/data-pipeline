package store

import (
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// ProgressTracker maintains real-time metrics for pipeline jobs.
type ProgressTracker interface {
	// RecordProcessed records a successfully processed record with its latency.
	RecordProcessed(jobID string, stage string, latency time.Duration)
	// RecordFailed records a failed record for a stage.
	RecordFailed(jobID string, stage string)
	// SetTotal sets the total number of records expected for a job.
	SetTotal(jobID string, total int64)
	// GetProgress returns the current progress metrics for a job.
	GetProgress(jobID string) *model.Progress
}
