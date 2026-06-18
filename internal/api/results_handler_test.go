package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
)

// --- Mock ResultStore ---

type mockResultStore struct {
	results map[string][]map[string]interface{}
}

func newMockResultStore() *mockResultStore {
	return &mockResultStore{
		results: make(map[string][]map[string]interface{}),
	}
}

func (m *mockResultStore) GetResults(jobID string) ([]map[string]interface{}, bool) {
	r, ok := m.results[jobID]
	return r, ok
}

func (m *mockResultStore) StoreResults(jobID string, results []map[string]interface{}) {
	m.results[jobID] = results
}

// --- Tests ---

func TestGetResults_CompletedJob_ReturnsResults(t *testing.T) {
	completedAt := time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC)

	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{
		ID:          "job-1",
		Status:      model.StatusCompleted,
		CompletedAt: &completedAt,
		CreatedAt:   time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	rs := newMockResultStore()
	rs.results["job-1"] = []map[string]interface{}{
		{"category": "electronics", "total_count": float64(150), "total_amount": 45000.50},
		{"category": "clothing", "total_count": float64(80), "total_amount": 12000.00},
	}

	pt := newMockProgressTracker()
	pt.progress["job-1"] = &model.Progress{
		RecordsProcessed: 5000,
	}

	h := &Handler{
		JobStore:        js,
		ResultStore:     rs,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/results", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp resultsResponseBody
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Len(t, resp.Results, 2)
	assert.Equal(t, "electronics", resp.Results[0]["category"])
	assert.Equal(t, "clothing", resp.Results[1]["category"])
	assert.Equal(t, int64(5000), resp.Metadata.TotalInputRecords)
	assert.Equal(t, 2, resp.Metadata.TotalOutputRecords)
	assert.Equal(t, "2024-01-15T10:45:00Z", resp.Metadata.CompletedAt)
}

func TestGetResults_JobNotFound_Returns404(t *testing.T) {
	js := newMockJobStore()
	h := &Handler{
		JobStore: js,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/nonexistent/results", nil)
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "not found")
}

func TestGetResults_RunningJob_Returns409(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-run"] = &model.Job{
		ID:     "job-run",
		Status: model.StatusRunning,
	}

	h := &Handler{
		JobStore: js,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-run/results", nil)
	req.SetPathValue("id", "job-run")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "job is still in progress", resp.Error)
}

func TestGetResults_QueuedJob_Returns409(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-q"] = &model.Job{
		ID:     "job-q",
		Status: model.StatusQueued,
	}

	h := &Handler{
		JobStore: js,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-q/results", nil)
	req.SetPathValue("id", "job-q")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "job is still in progress", resp.Error)
}

func TestGetResults_FailedJob_Returns409(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-fail"] = &model.Job{
		ID:     "job-fail",
		Status: model.StatusFailed,
	}

	h := &Handler{
		JobStore: js,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-fail/results", nil)
	req.SetPathValue("id", "job-fail")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "job did not complete successfully, results are unavailable", resp.Error)
}

func TestGetResults_CancelledJob_Returns409(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-cancel"] = &model.Job{
		ID:     "job-cancel",
		Status: model.StatusCancelled,
	}

	h := &Handler{
		JobStore: js,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-cancel/results", nil)
	req.SetPathValue("id", "job-cancel")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "job did not complete successfully, results are unavailable", resp.Error)
}

func TestGetResults_CompletedJob_NoResults_ReturnsEmptyArray(t *testing.T) {
	completedAt := time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC)

	js := newMockJobStore()
	js.jobs["job-empty"] = &model.Job{
		ID:          "job-empty",
		Status:      model.StatusCompleted,
		CompletedAt: &completedAt,
	}

	rs := newMockResultStore()
	// No results stored for this job

	pt := newMockProgressTracker()

	h := &Handler{
		JobStore:        js,
		ResultStore:     rs,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-empty/results", nil)
	req.SetPathValue("id", "job-empty")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp resultsResponseBody
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.NotNil(t, resp.Results)
	assert.Len(t, resp.Results, 0)
	assert.Equal(t, int64(0), resp.Metadata.TotalInputRecords)
	assert.Equal(t, 0, resp.Metadata.TotalOutputRecords)
	assert.Equal(t, "2024-01-15T10:45:00Z", resp.Metadata.CompletedAt)
}

func TestGetResults_NilResultStore_ReturnsEmptyArray(t *testing.T) {
	completedAt := time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC)

	js := newMockJobStore()
	js.jobs["job-nil"] = &model.Job{
		ID:          "job-nil",
		Status:      model.StatusCompleted,
		CompletedAt: &completedAt,
	}

	pt := newMockProgressTracker()

	h := &Handler{
		JobStore:        js,
		ResultStore:     nil, // no result store
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-nil/results", nil)
	req.SetPathValue("id", "job-nil")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp resultsResponseBody
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.NotNil(t, resp.Results)
	assert.Len(t, resp.Results, 0)
}

func TestGetResults_NilProgressTracker_ReturnsZeroInputRecords(t *testing.T) {
	completedAt := time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC)

	js := newMockJobStore()
	js.jobs["job-nopt"] = &model.Job{
		ID:          "job-nopt",
		Status:      model.StatusCompleted,
		CompletedAt: &completedAt,
	}

	rs := newMockResultStore()
	rs.results["job-nopt"] = []map[string]interface{}{
		{"x": "y"},
	}

	h := &Handler{
		JobStore:        js,
		ResultStore:     rs,
		ProgressTracker: nil, // no progress tracker
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-nopt/results", nil)
	req.SetPathValue("id", "job-nopt")
	w := httptest.NewRecorder()

	h.GetResults(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp resultsResponseBody
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), resp.Metadata.TotalInputRecords)
	assert.Equal(t, 1, resp.Metadata.TotalOutputRecords)
}

func TestInMemoryResultStore_StoreAndGet(t *testing.T) {
	store := NewInMemoryResultStore()

	results := []map[string]interface{}{
		{"key": "value1"},
		{"key": "value2"},
	}

	store.StoreResults("job-1", results)

	got, ok := store.GetResults("job-1")
	assert.True(t, ok)
	assert.Equal(t, results, got)
}

func TestInMemoryResultStore_GetNonExistent(t *testing.T) {
	store := NewInMemoryResultStore()

	got, ok := store.GetResults("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestInMemoryResultStore_Delete(t *testing.T) {
	store := NewInMemoryResultStore()

	store.StoreResults("job-1", []map[string]interface{}{{"a": "b"}})
	store.DeleteResults("job-1")

	got, ok := store.GetResults("job-1")
	assert.False(t, ok)
	assert.Nil(t, got)
}
