package api

import (
	"context"
	"net/http"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// cancelJobResponse is the response body for PATCH /api/v1/pipelines/:id/cancel.
type cancelJobResponse struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// CancelJob handles PATCH /api/v1/pipelines/:id/cancel.
// It initiates cancellation for a running pipeline job by calling the stored
// context.CancelFunc and transitioning the job status to "cancelled".
// Returns 202 on success, 404 if the job doesn't exist, and 409 if the job
// is not in "running" state.
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Look up the job in the store
	job, err := h.JobStore.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "job not found: " + id,
		})
		return
	}

	// Only running jobs can be cancelled
	if job.Status != model.StatusRunning {
		writeJSON(w, http.StatusConflict, errorResponse{
			Error: "job cannot be cancelled in its current state: " + string(job.Status),
		})
		return
	}

	// Look up and call the cancel function for this job.
	// The CancelFuncs map stores context.CancelFunc values keyed by job ID.
	// Calling the cancel function propagates cancellation via context.Context
	// to all pipeline stage goroutines.
	if cancelVal, ok := h.CancelFuncs.LoadAndDelete(id); ok {
		if cancelFunc, valid := cancelVal.(context.CancelFunc); valid && cancelFunc != nil {
			cancelFunc()
		}
	}

	// Update job status to cancelled
	_ = h.JobStore.UpdateStatus(id, model.StatusCancelled, "")

	// Return 202 Accepted
	writeJSON(w, http.StatusAccepted, cancelJobResponse{
		ID:      id,
		Status:  string(model.StatusCancelled),
		Message: "Cancellation initiated",
	})
}
