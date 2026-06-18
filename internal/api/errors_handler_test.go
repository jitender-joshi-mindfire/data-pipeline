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

// mockErrorStoreWithData extends mock with configurable data for testing.
type mockErrorStoreWithData struct {
	errors      map[string][]model.ErrorEntry
	deletedJobs []string
}

func newMockErrorStoreWithData() *mockErrorStoreWithData {
	return &mockErrorStoreWithData{
		errors: make(map[string][]model.ErrorEntry),
	}
}

func (m *mockErrorStoreWithData) GetByJob(jobID string, offset, limit int) ([]model.ErrorEntry, int) {
	entries := m.errors[jobID]
	total := len(entries)

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset >= total {
		return []model.ErrorEntry{}, total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return entries[offset:end], total
}

func (m *mockErrorStoreWithData) DeleteByJob(jobID string) {
	m.deletedJobs = append(m.deletedJobs, jobID)
}

// --- GetErrors Tests ---

func TestGetErrors_JobNotFound(t *testing.T) {
	h := &Handler{
		JobStore:   newMockJobStore(),
		ErrorStore: newMockErrorStoreWithData(),
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/nonexistent/errors", nil)
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "not found")
}

func TestGetErrors_EmptyErrors(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusCompleted}

	es := newMockErrorStoreWithData()

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.Errors)
	assert.Equal(t, 0, resp.Total)
	assert.Equal(t, 0, resp.Offset)
	assert.Equal(t, 50, resp.Limit)
}

func TestGetErrors_DefaultPagination(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(15)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Len(t, resp.Errors, 15)
	assert.Equal(t, 15, resp.Total)
	assert.Equal(t, 0, resp.Offset)
	assert.Equal(t, 50, resp.Limit)
}

func TestGetErrors_WithOffsetAndLimit(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(20)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?offset=5&limit=10", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Len(t, resp.Errors, 10)
	assert.Equal(t, 20, resp.Total)
	assert.Equal(t, 5, resp.Offset)
	assert.Equal(t, 10, resp.Limit)
}

func TestGetErrors_LimitClampedToMax200(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusCompleted}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(10)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?limit=500", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.Limit)
}

func TestGetErrors_NegativeOffsetUsesDefault(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(5)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?offset=-10", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Offset)
	assert.Len(t, resp.Errors, 5)
}

func TestGetErrors_InvalidOffsetUsesDefault(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(3)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?offset=abc", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Offset)
}

func TestGetErrors_InvalidLimitUsesDefault(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(3)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?limit=xyz", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 50, resp.Limit)
}

func TestGetErrors_ZeroLimitUsesDefault(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(3)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?limit=0", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 50, resp.Limit)
}

func TestGetErrors_OffsetBeyondTotal(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(5)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?offset=100", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.Errors)
	assert.Equal(t, 5, resp.Total)
	assert.Equal(t, 100, resp.Offset)
}

func TestGetErrors_ResponseFormat(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusFailed}

	ts := time.Date(2024, 1, 15, 10, 31, 5, 0, time.UTC)
	es := newMockErrorStoreWithData()
	es.errors["job-1"] = []model.ErrorEntry{
		{
			ID:        "err-001",
			JobID:     "job-1",
			Stage:     "validator",
			Message:   "field 'amount' failed numeric range check",
			Record:    map[string]interface{}{"name": "Test", "amount": "-5"},
			Timestamp: ts,
		},
	}

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)

	assert.Len(t, resp.Errors, 1)
	assert.Equal(t, "err-001", resp.Errors[0].ID)
	assert.Equal(t, "validator", resp.Errors[0].Stage)
	assert.Equal(t, "field 'amount' failed numeric range check", resp.Errors[0].Message)
	assert.Equal(t, "Test", resp.Errors[0].Record["name"])
	assert.Equal(t, "-5", resp.Errors[0].Record["amount"])
	assert.Equal(t, ts, resp.Errors[0].Timestamp)
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 0, resp.Offset)
	assert.Equal(t, 50, resp.Limit)
}

func TestGetErrors_NegativeLimitUsesDefault(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{ID: "job-1", Status: model.StatusRunning}

	es := newMockErrorStoreWithData()
	es.errors["job-1"] = makeErrorEntries(3)

	h := &Handler{
		JobStore:   js,
		ErrorStore: es,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-1/errors?limit=-5", nil)
	req.SetPathValue("id", "job-1")
	w := httptest.NewRecorder()

	h.GetErrors(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp errorsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 50, resp.Limit)
}

// --- Helper ---

func makeErrorEntries(n int) []model.ErrorEntry {
	entries := make([]model.ErrorEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = model.ErrorEntry{
			ID:        "err-" + string(rune('0'+i%10)),
			JobID:     "job-1",
			Stage:     "validator",
			Message:   "test error",
			Record:    map[string]interface{}{"field": "value"},
			Timestamp: time.Date(2024, 1, 15, 10, 30, i, 0, time.UTC),
		}
	}
	return entries
}
