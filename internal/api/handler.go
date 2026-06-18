package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/jitendraj/data-pipeline/internal/config"
	"github.com/jitendraj/data-pipeline/internal/model"
)

// JobStore is the interface for job CRUD operations used by the handler.
type JobStore interface {
	Create(config model.JobConfig) (*model.Job, error)
	Get(id string) (*model.Job, error)
	List() []*model.Job
	Delete(id string) error
	UpdateStatus(id string, status model.JobStatus, errMsg string) error
}

// ErrorStore is the interface for error operations used by the handler.
type ErrorStore interface {
	GetByJob(jobID string, offset, limit int) ([]model.ErrorEntry, int)
	DeleteByJob(jobID string)
}

// ProgressTracker is the interface for progress tracking used by the handler.
type ProgressTracker interface {
	GetProgress(jobID string) *model.Progress
}

// ResultStore provides access to pipeline results for completed jobs.
type ResultStore interface {
	GetResults(jobID string) ([]map[string]interface{}, bool)
	StoreResults(jobID string, results []map[string]interface{})
}

// createJobResponse is the response body for POST /api/v1/pipelines.
type createJobResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// jobListItem is a single item in the list jobs response.
type jobListItem struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// listJobsResponse is the response body for GET /api/v1/pipelines.
type listJobsResponse struct {
	Jobs []jobListItem `json:"jobs"`
}

// getJobResponse is the response body for GET /api/v1/pipelines/:id.
type getJobResponse struct {
	ID               string          `json:"id"`
	Config           model.JobConfig `json:"config"`
	Status           string          `json:"status"`
	CreatedAt        time.Time       `json:"created_at"`
	RecordsProcessed int64           `json:"records_processed"`
	RecordsPending   int64           `json:"records_pending"`
	PercentComplete  int             `json:"percent_complete"`
}

// errorResponse is a generic error response body.
type errorResponse struct {
	Error string `json:"error"`
}

// resultsResponseBody is the response body for GET /api/v1/pipelines/:id/results.
type resultsResponseBody struct {
	Results  []map[string]interface{} `json:"results"`
	Metadata resultsMetadataBody      `json:"metadata"`
}

// resultsMetadataBody holds metadata for the results response.
type resultsMetadataBody struct {
	TotalInputRecords  int64  `json:"total_input_records"`
	TotalOutputRecords int    `json:"total_output_records"`
	CompletedAt        string `json:"completed_at"`
}

// validationErrorResponse is the response body for 400 validation errors.
type validationErrorResponse struct {
	Error  string                   `json:"error"`
	Details []config.ValidationError `json:"details"`
}

// CreateJob handles POST /api/v1/pipelines.
// It validates the job configuration, creates the job, triggers async execution,
// and returns 201 with the job ID.
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var cfg model.JobConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Error: "invalid JSON body: " + err.Error(),
		})
		return
	}

	// Validate configuration
	validationErrors := config.ValidateJobConfig(cfg)
	if len(validationErrors) > 0 {
		writeJSON(w, http.StatusBadRequest, validationErrorResponse{
			Error:   "invalid job configuration",
			Details: validationErrors,
		})
		return
	}

	// Create the job
	job, err := h.JobStore.Create(cfg)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error: "failed to create job: " + err.Error(),
		})
		return
	}

	// Start async pipeline execution
	if h.Runner != nil {
		h.Runner.RunJob(job.ID)
	}

	writeJSON(w, http.StatusCreated, createJobResponse{
		ID:        job.ID,
		Status:    string(job.Status),
		CreatedAt: job.CreatedAt,
	})
}

// ListJobs handles GET /api/v1/pipelines.
// It returns all jobs with ID, status, and creation timestamp.
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := h.JobStore.List()

	items := make([]jobListItem, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, jobListItem{
			ID:        job.ID,
			Status:    string(job.Status),
			CreatedAt: job.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, listJobsResponse{Jobs: items})
}

// GetJob handles GET /api/v1/pipelines/:id.
// It returns full job details including config, status, timestamps, and record counts.
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, err := h.JobStore.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "job not found: " + id,
		})
		return
	}

	var recordsProcessed, recordsPending int64
	var percentComplete int

	if h.ProgressTracker != nil {
		progress := h.ProgressTracker.GetProgress(job.ID)
		if progress != nil {
			recordsProcessed = progress.RecordsProcessed
			recordsPending = progress.RecordsPending
			percentComplete = progress.PercentComplete
		}
	}

	writeJSON(w, http.StatusOK, getJobResponse{
		ID:               job.ID,
		Config:           job.Config,
		Status:           string(job.Status),
		CreatedAt:        job.CreatedAt,
		RecordsProcessed: recordsProcessed,
		RecordsPending:   recordsPending,
		PercentComplete:  percentComplete,
	})
}

// DeleteJob handles DELETE /api/v1/pipelines/:id.
// It deletes the job and all associated data. Returns 409 for running jobs.
func (h *Handler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	err := h.JobStore.Delete(id)
	if err != nil {
		switch err.Error() {
		case "job not found":
			writeJSON(w, http.StatusNotFound, errorResponse{
				Error: "job not found: " + id,
			})
		case "cannot delete a running job":
			writeJSON(w, http.StatusConflict, errorResponse{
				Error: "job is currently running; cancel it before deletion",
			})
		default:
			writeJSON(w, http.StatusInternalServerError, errorResponse{
				Error: "failed to delete job: " + err.Error(),
			})
		}
		return
	}

	// Clean up associated data
	if h.ErrorStore != nil {
		h.ErrorStore.DeleteByJob(id)
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetProgress handles GET /api/v1/pipelines/:id/progress.
// Returns current progress metrics for a running job, or final metrics
// for completed/failed/cancelled jobs.
func (h *Handler) GetProgress(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Verify the job exists
	_, err := h.JobStore.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "job not found: " + id,
		})
		return
	}

	// Get progress metrics from the tracker
	progress := h.ProgressTracker.GetProgress(id)
	if progress == nil {
		// No progress data yet — return zeroed metrics
		progress = &model.Progress{
			StageLatencies: make(map[string]float64),
			ErrorCounts:    make(map[string]int64),
		}
	}

	// Ensure maps are never nil in the response
	if progress.StageLatencies == nil {
		progress.StageLatencies = make(map[string]float64)
	}
	if progress.ErrorCounts == nil {
		progress.ErrorCounts = make(map[string]int64)
	}

	writeJSON(w, http.StatusOK, progress)
}

// GetResults handles GET /api/v1/pipelines/:id/results.
// Returns aggregated results with metadata for completed jobs.
// Returns 409 for jobs not in terminal status or for failed/cancelled jobs.
func (h *Handler) GetResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Look up the job
	job, err := h.JobStore.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "job not found: " + id,
		})
		return
	}

	// Check job status
	switch job.Status {
	case model.StatusCompleted:
		// Proceed to return results
	case model.StatusFailed, model.StatusCancelled:
		writeJSON(w, http.StatusConflict, errorResponse{
			Error: "job did not complete successfully, results are unavailable",
		})
		return
	default:
		// Job is still in progress (queued or running)
		writeJSON(w, http.StatusConflict, errorResponse{
			Error: "job is still in progress",
		})
		return
	}

	// Get results from the result store
	var results []map[string]interface{}
	if h.ResultStore != nil {
		storedResults, ok := h.ResultStore.GetResults(id)
		if ok {
			results = storedResults
		}
	}
	if results == nil {
		results = []map[string]interface{}{}
	}

	// Get progress for total input records
	var totalInputRecords int64
	if h.ProgressTracker != nil {
		progress := h.ProgressTracker.GetProgress(id)
		if progress != nil {
			totalInputRecords = progress.RecordsProcessed
		}
	}

	// Build completion timestamp in RFC 3339 format
	var completedAt string
	if job.CompletedAt != nil {
		completedAt = job.CompletedAt.Format(time.RFC3339)
	}

	// Build response
	resp := resultsResponseBody{
		Results: results,
		Metadata: resultsMetadataBody{
			TotalInputRecords:  totalInputRecords,
			TotalOutputRecords: len(results),
			CompletedAt:        completedAt,
		},
	}

	writeJSON(w, http.StatusOK, resp)
}

// GetErrors is implemented in errors_handler.go.

// CancelJob is implemented in cancel_handler.go.

// writeJSON encodes a value as JSON and writes it to the response writer.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
