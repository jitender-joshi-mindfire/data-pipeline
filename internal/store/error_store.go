package store

import "github.com/jitendraj/data-pipeline/internal/model"

// ErrorStore collects processing errors.
type ErrorStore interface {
	// Add stores an error entry for a job.
	Add(jobID string, entry model.ErrorEntry)
	// GetByJob returns paginated errors for a job and the total count.
	GetByJob(jobID string, offset, limit int) ([]model.ErrorEntry, int)
	// DeleteByJob removes all errors associated with a job.
	DeleteByJob(jobID string)
}
