package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Route Registration and HTTP Method Enforcement Tests
// =============================================================================

func TestRouter_MethodNotAllowed(t *testing.T) {
	h := &Handler{
		JobStore:        newMockJobStore(),
		ErrorStore:      newMockErrorStore(),
		ProgressTracker: newMockProgressTracker(),
	}
	router := NewRouterWithLimiter(h, nil)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"PUT on pipelines", "PUT", "/api/v1/pipelines"},
		{"DELETE on pipelines collection", "DELETE", "/api/v1/pipelines"},
		{"PATCH on pipelines collection", "PATCH", "/api/v1/pipelines"},
		{"POST on pipeline by id", "POST", "/api/v1/pipelines/test-id"},
		{"PUT on pipeline by id", "PUT", "/api/v1/pipelines/test-id"},
		{"POST on progress", "POST", "/api/v1/pipelines/test-id/progress"},
		{"DELETE on progress", "DELETE", "/api/v1/pipelines/test-id/progress"},
		{"POST on results", "POST", "/api/v1/pipelines/test-id/results"},
		{"DELETE on results", "DELETE", "/api/v1/pipelines/test-id/results"},
		{"POST on errors", "POST", "/api/v1/pipelines/test-id/errors"},
		{"DELETE on errors", "DELETE", "/api/v1/pipelines/test-id/errors"},
		{"GET on cancel", "GET", "/api/v1/pipelines/test-id/cancel"},
		{"POST on cancel", "POST", "/api/v1/pipelines/test-id/cancel"},
		{"DELETE on cancel", "DELETE", "/api/v1/pipelines/test-id/cancel"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			// Go 1.22+ ServeMux returns 405 for wrong methods on known paths
			assert.Equal(t, http.StatusMethodNotAllowed, w.Code,
				"expected 405 for %s %s", tc.method, tc.path)
		})
	}
}

func TestRouter_CorrectStatusCodes(t *testing.T) {
	js := newMockJobStore()
	completedAt := time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC)
	js.jobs["job-1"] = &model.Job{
		ID:          "job-1",
		Status:      model.StatusCompleted,
		CreatedAt:   time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		CompletedAt: &completedAt,
		Config: model.JobConfig{
			Sources: []model.SourceConfig{{Type: "csv", Path: "/data.csv"}},
			Exports: []model.ExportConfig{{Type: "json", Path: "/out.json"}},
		},
	}
	js.jobs["job-completed2"] = &model.Job{
		ID:          "job-completed2",
		Status:      model.StatusCompleted,
		CreatedAt:   time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		CompletedAt: &completedAt,
	}
	js.jobs["job-running"] = &model.Job{
		ID:        "job-running",
		Status:    model.StatusRunning,
		CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	es := newMockErrorStore()
	pt := newMockProgressTracker()
	pt.progress["job-1"] = &model.Progress{
		RecordsProcessed: 100,
		StageLatencies:   map[string]float64{},
		ErrorCounts:      map[string]int64{},
	}
	pt.progress["job-completed2"] = &model.Progress{
		RecordsProcessed: 100,
		StageLatencies:   map[string]float64{},
		ErrorCounts:      map[string]int64{},
	}

	rs := newMockResultStore()
	rs.results["job-completed2"] = []map[string]interface{}{{"key": "val"}}

	h := &Handler{
		JobStore:        js,
		ErrorStore:      es,
		ProgressTracker: pt,
		ResultStore:     rs,
	}
	router := NewRouterWithLimiter(h, nil)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"POST create job", "POST", "/api/v1/pipelines",
			`{"sources":[{"type":"csv","path":"/f.csv"}],"exports":[{"type":"json","path":"/o.json"}]}`,
			http.StatusCreated},
		{"GET list jobs", "GET", "/api/v1/pipelines", "", http.StatusOK},
		{"GET job by id", "GET", "/api/v1/pipelines/job-1", "", http.StatusOK},
		{"GET job not found", "GET", "/api/v1/pipelines/nonexistent", "", http.StatusNotFound},
		{"DELETE completed job", "DELETE", "/api/v1/pipelines/job-1", "", http.StatusNoContent},
		{"DELETE running job conflict", "DELETE", "/api/v1/pipelines/job-running", "", http.StatusConflict},
		{"GET progress", "GET", "/api/v1/pipelines/job-running/progress", "", http.StatusOK},
		{"GET results completed", "GET", "/api/v1/pipelines/job-completed2/results", "", http.StatusOK},
		{"GET results running 409", "GET", "/api/v1/pipelines/job-running/results", "", http.StatusConflict},
		{"GET errors", "GET", "/api/v1/pipelines/job-running/errors", "", http.StatusOK},
		{"PATCH cancel running", "PATCH", "/api/v1/pipelines/job-running/cancel", "", http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var body *bytes.Reader
			if tc.body != "" {
				body = bytes.NewReader([]byte(tc.body))
			} else {
				body = bytes.NewReader(nil)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code,
				"unexpected status for %s %s", tc.method, tc.path)
		})
	}
}

// =============================================================================
// Request Validation and Error Responses Tests
// =============================================================================

func TestCreateJob_EmptyBody_Returns400(t *testing.T) {
	h := &Handler{JobStore: newMockJobStore()}

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader([]byte("")))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.NotEmpty(t, resp.Error)
}

func TestCreateJob_InvalidExportType_Returns400(t *testing.T) {
	h := &Handler{JobStore: newMockJobStore()}

	cfg := model.JobConfig{
		Sources: []model.SourceConfig{{Type: "csv", Path: "/input.csv"}},
		Exports: []model.ExportConfig{{Type: "parquet", Path: "/out.parquet"}},
	}
	body, _ := json.Marshal(cfg)

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp validationErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "invalid job configuration", resp.Error)
	assert.NotEmpty(t, resp.Details)
}

func TestCreateJob_InvalidTimeoutSeconds_Returns400(t *testing.T) {
	h := &Handler{JobStore: newMockJobStore()}

	timeout := 0 // below minimum of 1
	cfg := model.JobConfig{
		Sources:        []model.SourceConfig{{Type: "csv", Path: "/input.csv"}},
		Exports:        []model.ExportConfig{{Type: "json", Path: "/out.json"}},
		TimeoutSeconds: &timeout,
	}
	body, _ := json.Marshal(cfg)

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp validationErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.NotEmpty(t, resp.Details)
}

func TestCreateJob_MultipleValidationErrors_Returns400WithDetails(t *testing.T) {
	h := &Handler{JobStore: newMockJobStore()}

	cfg := model.JobConfig{
		Sources: []model.SourceConfig{}, // empty
		Exports: []model.ExportConfig{}, // empty
	}
	body, _ := json.Marshal(cfg)

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp validationErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "invalid job configuration", resp.Error)
	// Should have at least 2 errors: missing sources and missing exports
	assert.GreaterOrEqual(t, len(resp.Details), 2)
}

func TestDeleteJob_CancelledJob_Returns204(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-c"] = &model.Job{
		ID:     "job-c",
		Status: model.StatusCancelled,
	}

	h := &Handler{
		JobStore:   js,
		ErrorStore: newMockErrorStore(),
	}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/job-c", nil)
	req.SetPathValue("id", "job-c")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestDeleteJob_FailedJob_Returns204(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-f"] = &model.Job{
		ID:     "job-f",
		Status: model.StatusFailed,
	}

	h := &Handler{
		JobStore:   js,
		ErrorStore: newMockErrorStore(),
	}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/job-f", nil)
	req.SetPathValue("id", "job-f")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

// =============================================================================
// Response Format Verification Tests
// =============================================================================

func TestCreateJob_ResponseFormat(t *testing.T) {
	js := newMockJobStore()
	h := &Handler{JobStore: js}

	body := validJobConfigJSON()
	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	// Verify JSON structure has exactly the expected fields
	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Must contain id, status, created_at
	assert.Contains(t, raw, "id")
	assert.Contains(t, raw, "status")
	assert.Contains(t, raw, "created_at")
	assert.IsType(t, "", raw["id"])
	assert.Equal(t, "queued", raw["status"])
	// created_at should be a valid RFC3339 timestamp
	_, parseErr := time.Parse(time.RFC3339, raw["created_at"].(string))
	assert.NoError(t, parseErr, "created_at should be RFC3339 format")
}

func TestListJobs_ResponseFormat(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{
		ID:        "job-1",
		Status:    model.StatusRunning,
		CreatedAt: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
	}

	h := &Handler{JobStore: js}

	req := httptest.NewRequest("GET", "/api/v1/pipelines", nil)
	w := httptest.NewRecorder()

	h.ListJobs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Must have "jobs" array
	assert.Contains(t, raw, "jobs")
	jobs := raw["jobs"].([]interface{})
	assert.Len(t, jobs, 1)

	// Each job item has id, status, created_at
	item := jobs[0].(map[string]interface{})
	assert.Contains(t, item, "id")
	assert.Contains(t, item, "status")
	assert.Contains(t, item, "created_at")
	assert.Equal(t, "job-1", item["id"])
	assert.Equal(t, "running", item["status"])
}

func TestGetJob_ResponseFormat(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-fmt"] = &model.Job{
		ID:     "job-fmt",
		Status: model.StatusRunning,
		Config: model.JobConfig{
			Sources: []model.SourceConfig{{Type: "csv", Path: "/data.csv"}},
			Exports: []model.ExportConfig{{Type: "json", Path: "/out.json"}},
		},
		CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	pt := newMockProgressTracker()
	pt.progress["job-fmt"] = &model.Progress{
		RecordsProcessed: 500,
		RecordsPending:   100,
		PercentComplete:  83,
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-fmt", nil)
	req.SetPathValue("id", "job-fmt")
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Verify all expected fields are present
	assert.Contains(t, raw, "id")
	assert.Contains(t, raw, "config")
	assert.Contains(t, raw, "status")
	assert.Contains(t, raw, "created_at")
	assert.Contains(t, raw, "records_processed")
	assert.Contains(t, raw, "records_pending")
	assert.Contains(t, raw, "percent_complete")

	assert.Equal(t, "job-fmt", raw["id"])
	assert.Equal(t, "running", raw["status"])
	assert.Equal(t, float64(500), raw["records_processed"])
	assert.Equal(t, float64(100), raw["records_pending"])
	assert.Equal(t, float64(83), raw["percent_complete"])

	// Config should be a nested object with sources and exports
	cfg := raw["config"].(map[string]interface{})
	assert.Contains(t, cfg, "sources")
	assert.Contains(t, cfg, "exports")
}

func TestDeleteJob_ResponseFormat_NoBody(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-del"] = &model.Job{
		ID:     "job-del",
		Status: model.StatusCompleted,
	}

	h := &Handler{
		JobStore:   js,
		ErrorStore: newMockErrorStore(),
	}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/job-del", nil)
	req.SetPathValue("id", "job-del")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	// 204 should have no body
	assert.Empty(t, w.Body.Bytes())
}

func TestGetProgress_ResponseFormat(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-p"] = &model.Job{ID: "job-p", Status: model.StatusRunning}

	pt := newMockProgressTracker()
	pt.progress["job-p"] = &model.Progress{
		RecordsProcessed: 4500,
		RecordsPending:   500,
		PercentComplete:  90,
		ProcessingRate:   150.5,
		StageLatencies: map[string]float64{
			"ingester":    2.1,
			"validator":   1.5,
			"transformer": 3.0,
		},
		ErrorCounts: map[string]int64{
			"validator": 12,
		},
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-p/progress", nil)
	req.SetPathValue("id", "job-p")
	w := httptest.NewRecorder()

	h.GetProgress(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Verify all expected fields
	assert.Contains(t, raw, "records_processed")
	assert.Contains(t, raw, "records_pending")
	assert.Contains(t, raw, "percent_complete")
	assert.Contains(t, raw, "processing_rate")
	assert.Contains(t, raw, "stage_latencies")
	assert.Contains(t, raw, "error_counts")

	assert.Equal(t, float64(4500), raw["records_processed"])
	assert.Equal(t, float64(500), raw["records_pending"])
	assert.Equal(t, float64(90), raw["percent_complete"])
	assert.Equal(t, 150.5, raw["processing_rate"])

	// stage_latencies is a map
	latencies := raw["stage_latencies"].(map[string]interface{})
	assert.Equal(t, 2.1, latencies["ingester"])
	assert.Equal(t, 1.5, latencies["validator"])

	// error_counts is a map
	errCounts := raw["error_counts"].(map[string]interface{})
	assert.Equal(t, float64(12), errCounts["validator"])
}

func TestGetResults_ResponseFormat(t *testing.T) {
	completedAt := time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC)

	js := newMockJobStore()
	js.jobs["job-r"] = &model.Job{
		ID:          "job-r",
		Status:      model.StatusCompleted,
		CompletedAt: &completedAt,
	}

	rs := newMockResultStore()
	rs.results["job-r"] = []map[string]interface{}{
		{"category": "electronics", "total_count": float64(150)},
	}

	pt := newMockProgressTracker()
	pt.progress["job-r"] = &model.Progress{RecordsProcessed: 5000}

	h := &Handler{
		JobStore:        js,
		ResultStore:     rs,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-r/results", nil)
	req.SetPathValue("id", "job-r")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Verify top-level structure: results array and metadata object
	assert.Contains(t, raw, "results")
	assert.Contains(t, raw, "metadata")

	results := raw["results"].([]interface{})
	assert.Len(t, results, 1)

	// Verify results contain expected data
	first := results[0].(map[string]interface{})
	assert.Equal(t, "electronics", first["category"])
	assert.Equal(t, float64(150), first["total_count"])

	// Verify metadata fields
	metadata := raw["metadata"].(map[string]interface{})
	assert.Contains(t, metadata, "total_input_records")
	assert.Contains(t, metadata, "total_output_records")
	assert.Contains(t, metadata, "completed_at")
	assert.Equal(t, float64(5000), metadata["total_input_records"])
	assert.Equal(t, float64(1), metadata["total_output_records"])
	assert.Equal(t, "2024-01-15T10:45:00Z", metadata["completed_at"])
}

func TestGetErrors_ResponseFormat_Full(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-e"] = &model.Job{ID: "job-e", Status: model.StatusFailed}

	ts := time.Date(2024, 1, 15, 10, 31, 5, 0, time.UTC)
	es := newMockErrorStoreWithData()
	es.errors["job-e"] = []model.ErrorEntry{
		{
			ID:        "err-001",
			JobID:     "job-e",
			Stage:     "validator",
			Message:   "field 'amount' out of range",
			Record:    map[string]interface{}{"name": "Test", "amount": "-5"},
			Timestamp: ts,
		},
	}

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-e/errors", nil)
	req.SetPathValue("id", "job-e")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Verify top-level: errors array, total, offset, limit
	assert.Contains(t, raw, "errors")
	assert.Contains(t, raw, "total")
	assert.Contains(t, raw, "offset")
	assert.Contains(t, raw, "limit")

	assert.Equal(t, float64(1), raw["total"])
	assert.Equal(t, float64(0), raw["offset"])
	assert.Equal(t, float64(50), raw["limit"])

	errors := raw["errors"].([]interface{})
	assert.Len(t, errors, 1)

	// Verify error entry fields
	entry := errors[0].(map[string]interface{})
	assert.Contains(t, entry, "id")
	assert.Contains(t, entry, "stage")
	assert.Contains(t, entry, "message")
	assert.Contains(t, entry, "record")
	assert.Contains(t, entry, "timestamp")
	assert.Equal(t, "err-001", entry["id"])
	assert.Equal(t, "validator", entry["stage"])
	assert.Equal(t, "field 'amount' out of range", entry["message"])
}

func TestCancelJob_ResponseFormat(t *testing.T) {
	js := newMockJobStoreForCancel()
	js.addJob(&model.Job{
		ID:        "job-cx",
		Status:    model.StatusRunning,
		CreatedAt: time.Now().UTC(),
	})

	h := &Handler{JobStore: js}

	req := httptest.NewRequest("PATCH", "/api/v1/pipelines/job-cx/cancel", nil)
	req.SetPathValue("id", "job-cx")
	w := httptest.NewRecorder()

	h.CancelJob(w, req)

	assert.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Verify response fields: id, status, message
	assert.Contains(t, raw, "id")
	assert.Contains(t, raw, "status")
	assert.Contains(t, raw, "message")
	assert.Equal(t, "job-cx", raw["id"])
	assert.Equal(t, "cancelled", raw["status"])
	assert.Equal(t, "Cancellation initiated", raw["message"])
}

// =============================================================================
// Error Response Format Tests
// =============================================================================

func TestNotFound_ErrorResponseFormat(t *testing.T) {
	h := &Handler{
		JobStore:        newMockJobStore(),
		ProgressTracker: newMockProgressTracker(),
	}

	// Test GetJob 404
	req := httptest.NewRequest("GET", "/api/v1/pipelines/no-such-id", nil)
	req.SetPathValue("id", "no-such-id")
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "no-such-id")
}

func TestConflict_ErrorResponseFormat(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-run"] = &model.Job{
		ID:     "job-run",
		Status: model.StatusRunning,
	}

	h := &Handler{JobStore: js}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/job-run", nil)
	req.SetPathValue("id", "job-run")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.NotEmpty(t, resp.Error)
}

func TestValidationError_ResponseFormat(t *testing.T) {
	h := &Handler{JobStore: newMockJobStore()}

	cfg := model.JobConfig{
		Sources: []model.SourceConfig{{Type: "invalid_type", Path: "/f.csv"}},
		Exports: []model.ExportConfig{{Type: "json", Path: "/o.json"}},
	}
	body, _ := json.Marshal(cfg)

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)

	// Must have "error" and "details"
	assert.Contains(t, raw, "error")
	assert.Contains(t, raw, "details")
	assert.Equal(t, "invalid job configuration", raw["error"])

	details := raw["details"].([]interface{})
	assert.NotEmpty(t, details)

	// Each detail has "field" and "message"
	detail := details[0].(map[string]interface{})
	assert.Contains(t, detail, "field")
	assert.Contains(t, detail, "message")
}

// =============================================================================
// Additional Edge Cases for Complete Coverage
// =============================================================================

func TestGetJob_ResponseIncludesAllJobStatuses(t *testing.T) {
	statuses := []model.JobStatus{
		model.StatusQueued,
		model.StatusRunning,
		model.StatusCompleted,
		model.StatusFailed,
		model.StatusCancelled,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			js := newMockJobStore()
			js.jobs["job-s"] = &model.Job{
				ID:        "job-s",
				Status:    status,
				CreatedAt: time.Now().UTC(),
			}

			pt := newMockProgressTracker()
			h := &Handler{
				JobStore:        js,
				ProgressTracker: pt,
			}

			req := httptest.NewRequest("GET", "/api/v1/pipelines/job-s", nil)
			req.SetPathValue("id", "job-s")
			w := httptest.NewRecorder()

			h.GetJob(w, req)

			assert.Equal(t, http.StatusOK, w.Code)

			var resp getJobResponse
			err := json.Unmarshal(w.Body.Bytes(), &resp)
			assert.NoError(t, err)
			assert.Equal(t, string(status), resp.Status)
		})
	}
}

func TestListJobs_ResponseEmptyArrayNotNull(t *testing.T) {
	h := &Handler{JobStore: newMockJobStore()}

	req := httptest.NewRequest("GET", "/api/v1/pipelines", nil)
	w := httptest.NewRecorder()

	h.ListJobs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify that "jobs" is an empty array, not null
	var raw map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &raw)
	require.NoError(t, err)
	assert.NotNil(t, raw["jobs"])
	jobs := raw["jobs"].([]interface{})
	assert.Len(t, jobs, 0)
}
