package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockJobStoreForCancel implements the JobStore interface for cancel handler tests.
type mockJobStoreForCancel struct {
	mu   sync.RWMutex
	jobs map[string]*model.Job
}

func newMockJobStoreForCancel() *mockJobStoreForCancel {
	return &mockJobStoreForCancel{
		jobs: make(map[string]*model.Job),
	}
}

func (s *mockJobStoreForCancel) Create(config model.JobConfig) (*model.Job, error) {
	return nil, nil
}

func (s *mockJobStoreForCancel) Get(id string) (*model.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job not found")
	}
	return job, nil
}

func (s *mockJobStoreForCancel) List() []*model.Job {
	return nil
}

func (s *mockJobStoreForCancel) Delete(id string) error {
	return nil
}

func (s *mockJobStoreForCancel) UpdateStatus(id string, status model.JobStatus, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job not found")
	}
	job.Status = status
	job.Error = errMsg
	if status == model.StatusCancelled || status == model.StatusCompleted || status == model.StatusFailed {
		now := time.Now().UTC()
		job.CompletedAt = &now
	}
	return nil
}

func (s *mockJobStoreForCancel) addJob(job *model.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func TestCancelJob_RunningJob_Returns202(t *testing.T) {
	store := newMockJobStoreForCancel()
	store.addJob(&model.Job{
		ID:        "job-123",
		Status:    model.StatusRunning,
		CreatedAt: time.Now().UTC(),
	})

	cancelled := false
	ctx, cancel := context.WithCancel(context.Background())
	_ = ctx

	h := &Handler{JobStore: store}
	h.CancelFuncs.Store("job-123", context.CancelFunc(func() {
		cancelled = true
		cancel()
	}))

	req := httptest.NewRequest("PATCH", "/api/v1/pipelines/job-123/cancel", nil)
	req.SetPathValue("id", "job-123")
	w := httptest.NewRecorder()

	h.CancelJob(w, req)

	assert.Equal(t, http.StatusAccepted, w.Code)
	assert.True(t, cancelled, "cancel function should have been called")

	var resp cancelJobResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "job-123", resp.ID)
	assert.Equal(t, "cancelled", resp.Status)
	assert.Equal(t, "Cancellation initiated", resp.Message)

	// Verify job status was updated
	job, _ := store.Get("job-123")
	assert.Equal(t, model.StatusCancelled, job.Status)
}

func TestCancelJob_NonRunningJob_Returns409(t *testing.T) {
	statuses := []model.JobStatus{
		model.StatusQueued,
		model.StatusCompleted,
		model.StatusFailed,
		model.StatusCancelled,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			store := newMockJobStoreForCancel()
			store.addJob(&model.Job{
				ID:        "job-456",
				Status:    status,
				CreatedAt: time.Now().UTC(),
			})

			h := &Handler{JobStore: store}

			req := httptest.NewRequest("PATCH", "/api/v1/pipelines/job-456/cancel", nil)
			req.SetPathValue("id", "job-456")
			w := httptest.NewRecorder()

			h.CancelJob(w, req)

			assert.Equal(t, http.StatusConflict, w.Code)

			var resp errorResponse
			err := json.NewDecoder(w.Body).Decode(&resp)
			require.NoError(t, err)
			assert.Contains(t, resp.Error, "cannot be cancelled")
			assert.Contains(t, resp.Error, string(status))
		})
	}
}

func TestCancelJob_NonExistentJob_Returns404(t *testing.T) {
	store := newMockJobStoreForCancel()
	h := &Handler{JobStore: store}

	req := httptest.NewRequest("PATCH", "/api/v1/pipelines/nonexistent/cancel", nil)
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.CancelJob(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	var resp errorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp.Error, "not found")
}

func TestCancelJob_NoCancelFunc_StillUpdatesStatus(t *testing.T) {
	store := newMockJobStoreForCancel()
	store.addJob(&model.Job{
		ID:        "job-789",
		Status:    model.StatusRunning,
		CreatedAt: time.Now().UTC(),
	})

	h := &Handler{JobStore: store}
	// No cancel function stored — simulates an edge case

	req := httptest.NewRequest("PATCH", "/api/v1/pipelines/job-789/cancel", nil)
	req.SetPathValue("id", "job-789")
	w := httptest.NewRecorder()

	h.CancelJob(w, req)

	assert.Equal(t, http.StatusAccepted, w.Code)

	// Verify status was still updated
	job, _ := store.Get("job-789")
	assert.Equal(t, model.StatusCancelled, job.Status)
}

func TestCancelJob_CancelFuncRemovedAfterUse(t *testing.T) {
	store := newMockJobStoreForCancel()
	store.addJob(&model.Job{
		ID:        "job-abc",
		Status:    model.StatusRunning,
		CreatedAt: time.Now().UTC(),
	})

	_, cancel := context.WithCancel(context.Background())
	h := &Handler{JobStore: store}
	h.CancelFuncs.Store("job-abc", cancel)

	req := httptest.NewRequest("PATCH", "/api/v1/pipelines/job-abc/cancel", nil)
	req.SetPathValue("id", "job-abc")
	w := httptest.NewRecorder()

	h.CancelJob(w, req)

	assert.Equal(t, http.StatusAccepted, w.Code)

	// Verify the cancel func was removed from the map
	_, loaded := h.CancelFuncs.Load("job-abc")
	assert.False(t, loaded, "cancel function should be removed after cancellation")
}
