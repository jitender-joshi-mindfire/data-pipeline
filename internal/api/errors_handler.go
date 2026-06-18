package api

import (
	"net/http"
	"strconv"

	"github.com/jitendraj/data-pipeline/internal/model"
)

const (
	defaultErrorOffset = 0
	defaultErrorLimit  = 50
	maxErrorLimit      = 200
)

// errorsResponse is the response body for GET /api/v1/pipelines/:id/errors.
type errorsResponse struct {
	Errors []model.ErrorEntry `json:"errors"`
	Total  int                `json:"total"`
	Offset int                `json:"offset"`
	Limit  int                `json:"limit"`
}

// GetErrors handles GET /api/v1/pipelines/:id/errors.
// It returns a paginated list of errors for a given job, with total count.
// Returns 404 if the job does not exist.
func (h *Handler) GetErrors(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Verify job exists
	_, err := h.JobStore.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: "job not found: " + id,
		})
		return
	}

	// Parse pagination query parameters
	offset := parseIntParam(r, "offset", defaultErrorOffset)
	limit := parseIntParam(r, "limit", defaultErrorLimit)

	// Clamp offset to non-negative
	if offset < 0 {
		offset = defaultErrorOffset
	}

	// Clamp limit to valid range
	if limit <= 0 {
		limit = defaultErrorLimit
	}
	if limit > maxErrorLimit {
		limit = maxErrorLimit
	}

	// Fetch paginated errors
	errors, total := h.ErrorStore.GetByJob(id, offset, limit)

	writeJSON(w, http.StatusOK, errorsResponse{
		Errors: errors,
		Total:  total,
		Offset: offset,
		Limit:  limit,
	})
}

// parseIntParam parses an integer query parameter, returning the default if
// the parameter is missing or unparseable.
func parseIntParam(r *http.Request, name string, defaultVal int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return defaultVal
	}
	return val
}
