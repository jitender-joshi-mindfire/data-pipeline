package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
)

// --- Mock implementations ---

type mockJobStore struct {
	jobs      map[string]*model.Job
	createErr error
	deleteErr error
}

func newMockJobStore() *mockJobStore {
	return &mockJobStore{
		jobs: make(map[string]*model.Job),
	}
}

func (m *mockJobStore) Create(cfg model.JobConfig) (*model.Job, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	job := &model.Job{
		ID:        "job-test-123",
		Config:    cfg,
		Status:    model.StatusQueued,
		CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}
	m.jobs[job.ID] = job
	return job, nil
}

func (m *mockJobStore) Get(id string) (*model.Job, error) {
	job, ok := m.jobs[id]
	if !ok {
		return nil, errors.New("job not found")
	}
	return job, nil
}

func (m *mockJobStore) List() []*model.Job {
	jobs := make([]*model.Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

func (m *mockJobStore) Delete(id string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	job, ok := m.jobs[id]
	if !ok {
		return errors.New("job not found")
	}
	if job.Status == model.StatusRunning {
		return errors.New("cannot delete a running job")
	}
	delete(m.jobs, id)
	return nil
}

func (m *mockJobStore) UpdateStatus(id string, status model.JobStatus, errMsg string) error {
	job, ok := m.jobs[id]
	if !ok {
		return errors.New("job not found")
	}
	job.Status = status
	job.Error = errMsg
	if status == model.StatusCompleted || status == model.StatusFailed || status == model.StatusCancelled {
		now := time.Now().UTC()
		job.CompletedAt = &now
	}
	return nil
}

type mockErrorStore struct {
	deletedJobs []string
	errors      map[string][]model.ErrorEntry
}

func newMockErrorStore() *mockErrorStore {
	return &mockErrorStore{
		errors: make(map[string][]model.ErrorEntry),
	}
}

func (m *mockErrorStore) DeleteByJob(jobID string) {
	m.deletedJobs = append(m.deletedJobs, jobID)
	delete(m.errors, jobID)
}

func (m *mockErrorStore) GetByJob(jobID string, offset, limit int) ([]model.ErrorEntry, int) {
	entries := m.errors[jobID]
	total := len(entries)
	if offset >= total {
		return []model.ErrorEntry{}, total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return entries[offset:end], total
}

type mockProgressTracker struct {
	progress map[string]*model.Progress
}

func newMockProgressTracker() *mockProgressTracker {
	return &mockProgressTracker{
		progress: make(map[string]*model.Progress),
	}
}

func (m *mockProgressTracker) GetProgress(jobID string) *model.Progress {
	return m.progress[jobID]
}

type mockRunner struct {
	started []string
}

func (m *mockRunner) RunJob(jobID string) {
	m.started = append(m.started, jobID)
}

// --- Helper ---

func validJobConfigJSON() []byte {
	cfg := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/data/input.csv"},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: "/data/output.json"},
		},
	}
	data, _ := json.Marshal(cfg)
	return data
}

// --- CreateJob Tests ---

func TestCreateJob_Success(t *testing.T) {
	js := newMockJobStore()
	runner := &mockRunner{}
	h := &Handler{
		JobStore: js,
		Runner:   runner,
	}

	body := validJobConfigJSON()
	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	var resp createJobResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "job-test-123", resp.ID)
	assert.Equal(t, "queued", resp.Status)
	assert.Equal(t, "2024-01-15T10:30:00Z", resp.CreatedAt.Format(time.RFC3339))

	// Runner should have been invoked
	assert.Equal(t, []string{"job-test-123"}, runner.started)
}

func TestCreateJob_InvalidJSON(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "invalid JSON body")
}

func TestCreateJob_InvalidConfig_NoSources(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	cfg := model.JobConfig{
		Sources: []model.SourceConfig{},
		Exports: []model.ExportConfig{
			{Type: "json", Path: "/out.json"},
		},
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
	assert.Equal(t, "sources", resp.Details[0].Field)
}

func TestCreateJob_InvalidConfig_NoExports(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	cfg := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/input.csv"},
		},
		Exports: []model.ExportConfig{},
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
	assert.Equal(t, "exports", resp.Details[0].Field)
}

func TestCreateJob_InvalidConfig_BadSourceType(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	cfg := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "xml", Path: "/data.xml"},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: "/out.json"},
		},
	}
	body, _ := json.Marshal(cfg)

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateJob_InvalidConfig_WorkerPoolOutOfRange(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	cfg := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/input.csv"},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: "/out.json"},
		},
		WorkerPools: model.WorkerPoolConfig{
			Validator: 100, // out of range
		},
	}
	body, _ := json.Marshal(cfg)

	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateJob_NilRunner(t *testing.T) {
	js := newMockJobStore()
	h := &Handler{
		JobStore: js,
		Runner:   nil, // no runner configured
	}

	body := validJobConfigJSON()
	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	// Should still succeed — runner is optional
	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestCreateJob_ContentTypeJSON(t *testing.T) {
	js := newMockJobStore()
	h := &Handler{
		JobStore: js,
	}

	body := validJobConfigJSON()
	req := httptest.NewRequest("POST", "/api/v1/pipelines", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

// --- ListJobs Tests ---

func TestListJobs_Empty(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines", nil)
	w := httptest.NewRecorder()

	h.ListJobs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp listJobsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.Jobs)
}

func TestListJobs_WithJobs(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-1"] = &model.Job{
		ID:        "job-1",
		Status:    model.StatusRunning,
		CreatedAt: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
	}
	js.jobs["job-2"] = &model.Job{
		ID:        "job-2",
		Status:    model.StatusCompleted,
		CreatedAt: time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC),
	}

	h := &Handler{JobStore: js}

	req := httptest.NewRequest("GET", "/api/v1/pipelines", nil)
	w := httptest.NewRecorder()

	h.ListJobs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp listJobsResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Len(t, resp.Jobs, 2)
}

func TestListJobs_ContentTypeJSON(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines", nil)
	w := httptest.NewRecorder()

	h.ListJobs(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

// --- GetJob Tests ---

func TestGetJob_Success(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-abc"] = &model.Job{
		ID:     "job-abc",
		Status: model.StatusRunning,
		Config: model.JobConfig{
			Sources: []model.SourceConfig{{Type: "csv", Path: "/data.csv"}},
			Exports: []model.ExportConfig{{Type: "json", Path: "/out.json"}},
		},
		CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	pt := newMockProgressTracker()
	pt.progress["job-abc"] = &model.Progress{
		RecordsProcessed: 4500,
		RecordsPending:   500,
		PercentComplete:  90,
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-abc", nil)
	req.SetPathValue("id", "job-abc")
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp getJobResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "job-abc", resp.ID)
	assert.Equal(t, "running", resp.Status)
	assert.Equal(t, int64(4500), resp.RecordsProcessed)
	assert.Equal(t, int64(500), resp.RecordsPending)
	assert.Equal(t, 90, resp.PercentComplete)
	assert.NotEmpty(t, resp.Config.Sources)
}

func TestGetJob_NotFound(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/nonexistent", nil)
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "not found")
}

func TestGetJob_NoProgressTracker(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-abc"] = &model.Job{
		ID:        "job-abc",
		Status:    model.StatusQueued,
		CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: nil,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-abc", nil)
	req.SetPathValue("id", "job-abc")
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp getJobResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), resp.RecordsProcessed)
	assert.Equal(t, int64(0), resp.RecordsPending)
	assert.Equal(t, 0, resp.PercentComplete)
}

// --- DeleteJob Tests ---

func TestDeleteJob_Success(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-del"] = &model.Job{
		ID:     "job-del",
		Status: model.StatusCompleted,
	}

	errStore := newMockErrorStore()
	h := &Handler{
		JobStore:   js,
		ErrorStore: errStore,
	}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/job-del", nil)
	req.SetPathValue("id", "job-del")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Empty(t, w.Body.Bytes())

	// Verify the job was deleted from the store
	_, err := js.Get("job-del")
	assert.Error(t, err)

	// Verify error store cleanup was called
	assert.Equal(t, []string{"job-del"}, errStore.deletedJobs)
}

func TestDeleteJob_NotFound(t *testing.T) {
	h := &Handler{
		JobStore: newMockJobStore(),
	}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/nonexistent", nil)
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "not found")
}

func TestDeleteJob_RunningJob_Returns409(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-run"] = &model.Job{
		ID:     "job-run",
		Status: model.StatusRunning,
	}

	h := &Handler{
		JobStore: js,
	}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/job-run", nil)
	req.SetPathValue("id", "job-run")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "running")
}

func TestDeleteJob_NilErrorStore(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-del"] = &model.Job{
		ID:     "job-del",
		Status: model.StatusFailed,
	}

	h := &Handler{
		JobStore:   js,
		ErrorStore: nil, // no error store
	}

	req := httptest.NewRequest("DELETE", "/api/v1/pipelines/job-del", nil)
	req.SetPathValue("id", "job-del")
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	// Should still succeed without error store
	assert.Equal(t, http.StatusNoContent, w.Code)
}

// --- GetProgress Tests ---

// TestGetProgress_Success verifies the GetProgress handler returns progress metrics.
func TestGetProgress_Success(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-prog"] = &model.Job{
		ID:     "job-prog",
		Status: model.StatusRunning,
	}

	pt := newMockProgressTracker()
	pt.progress["job-prog"] = &model.Progress{
		RecordsProcessed: 4500,
		RecordsPending:   500,
		PercentComplete:  90,
		ProcessingRate:   150.5,
		StageLatencies: map[string]float64{
			"ingester":  2.1,
			"validator": 1.5,
		},
		ErrorCounts: map[string]int64{
			"validator": 12,
		},
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-prog/progress", nil)
	req.SetPathValue("id", "job-prog")
	w := httptest.NewRecorder()

	h.GetProgress(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp model.Progress
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(4500), resp.RecordsProcessed)
	assert.Equal(t, int64(500), resp.RecordsPending)
	assert.Equal(t, 90, resp.PercentComplete)
	assert.Equal(t, 150.5, resp.ProcessingRate)
	assert.Equal(t, 2.1, resp.StageLatencies["ingester"])
	assert.Equal(t, 1.5, resp.StageLatencies["validator"])
	assert.Equal(t, int64(12), resp.ErrorCounts["validator"])
}

// TestGetProgress_NotFound verifies 404 for non-existent job.
func TestGetProgress_NotFound(t *testing.T) {
	h := &Handler{
		JobStore:        newMockJobStore(),
		ProgressTracker: newMockProgressTracker(),
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/nonexistent/progress", nil)
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.GetProgress(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp errorResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp.Error, "not found")
}

// TestGetProgress_NoProgressData verifies zeroed metrics when no progress exists yet.
func TestGetProgress_NoProgressData(t *testing.T) {
	js := newMockJobStore()
	js.jobs["job-new"] = &model.Job{
		ID:     "job-new",
		Status: model.StatusQueued,
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: newMockProgressTracker(),
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-new/progress", nil)
	req.SetPathValue("id", "job-new")
	w := httptest.NewRecorder()

	h.GetProgress(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp model.Progress
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), resp.RecordsProcessed)
	assert.Equal(t, int64(0), resp.RecordsPending)
	assert.Equal(t, 0, resp.PercentComplete)
	assert.Equal(t, float64(0), resp.ProcessingRate)
	assert.NotNil(t, resp.StageLatencies)
	assert.NotNil(t, resp.ErrorCounts)
}

// TestGetProgress_CompletedJob verifies final metrics are returned for completed jobs.
func TestGetProgress_CompletedJob(t *testing.T) {
	js := newMockJobStore()
	completedAt := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)
	js.jobs["job-done"] = &model.Job{
		ID:          "job-done",
		Status:      model.StatusCompleted,
		CompletedAt: &completedAt,
	}

	pt := newMockProgressTracker()
	pt.progress["job-done"] = &model.Progress{
		RecordsProcessed: 10000,
		RecordsPending:   0,
		PercentComplete:  100,
		ProcessingRate:   200.0,
		StageLatencies: map[string]float64{
			"ingester":    1.0,
			"validator":   2.0,
			"transformer": 3.0,
			"aggregator":  0.5,
			"exporter":    4.0,
		},
		ErrorCounts: map[string]int64{
			"validator":   5,
			"transformer": 2,
		},
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-done/progress", nil)
	req.SetPathValue("id", "job-done")
	w := httptest.NewRecorder()

	h.GetProgress(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp model.Progress
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(10000), resp.RecordsProcessed)
	assert.Equal(t, int64(0), resp.RecordsPending)
	assert.Equal(t, 100, resp.PercentComplete)
	assert.Equal(t, 200.0, resp.ProcessingRate)
	assert.Len(t, resp.StageLatencies, 5)
	assert.Equal(t, int64(5), resp.ErrorCounts["validator"])
	assert.Equal(t, int64(2), resp.ErrorCounts["transformer"])
}

// TestGetProgress_FailedJob verifies final metrics are returned for failed jobs.
func TestGetProgress_FailedJob(t *testing.T) {
	js := newMockJobStore()
	failedAt := time.Date(2024, 1, 15, 10, 45, 0, 0, time.UTC)
	js.jobs["job-fail"] = &model.Job{
		ID:          "job-fail",
		Status:      model.StatusFailed,
		CompletedAt: &failedAt,
		Error:       "context deadline exceeded",
	}

	pt := newMockProgressTracker()
	pt.progress["job-fail"] = &model.Progress{
		RecordsProcessed: 3000,
		RecordsPending:   7000,
		PercentComplete:  30,
		ProcessingRate:   100.0,
		StageLatencies: map[string]float64{
			"ingester":  1.5,
			"validator": 2.5,
		},
		ErrorCounts: map[string]int64{
			"validator": 50,
		},
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-fail/progress", nil)
	req.SetPathValue("id", "job-fail")
	w := httptest.NewRecorder()

	h.GetProgress(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp model.Progress
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(3000), resp.RecordsProcessed)
	assert.Equal(t, int64(7000), resp.RecordsPending)
	assert.Equal(t, 30, resp.PercentComplete)
	assert.Equal(t, 100.0, resp.ProcessingRate)
}

// TestGetProgress_CancelledJob verifies final metrics are returned for cancelled jobs.
func TestGetProgress_CancelledJob(t *testing.T) {
	js := newMockJobStore()
	cancelledAt := time.Date(2024, 1, 15, 10, 40, 0, 0, time.UTC)
	js.jobs["job-cancel"] = &model.Job{
		ID:          "job-cancel",
		Status:      model.StatusCancelled,
		CompletedAt: &cancelledAt,
	}

	pt := newMockProgressTracker()
	pt.progress["job-cancel"] = &model.Progress{
		RecordsProcessed: 5000,
		RecordsPending:   5000,
		PercentComplete:  50,
		ProcessingRate:   125.0,
		StageLatencies: map[string]float64{
			"ingester":  1.0,
			"validator": 1.5,
		},
		ErrorCounts: map[string]int64{},
	}

	h := &Handler{
		JobStore:        js,
		ProgressTracker: pt,
	}

	req := httptest.NewRequest("GET", "/api/v1/pipelines/job-cancel/progress", nil)
	req.SetPathValue("id", "job-cancel")
	w := httptest.NewRecorder()

	h.GetProgress(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp model.Progress
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(5000), resp.RecordsProcessed)
	assert.Equal(t, int64(5000), resp.RecordsPending)
	assert.Equal(t, 50, resp.PercentComplete)
}
