package store

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// Errors returned by JobStore operations.
var (
	ErrJobNotFound    = errors.New("job not found")
	ErrJobIsRunning   = errors.New("cannot delete a running job")
)

// JobStore manages the lifecycle of pipeline jobs.
type JobStore interface {
	// Create stores a new job with the given config, assigns a unique ID,
	// sets the status to "queued", and returns the created job.
	Create(config model.JobConfig) (*model.Job, error)
	// Get retrieves a job by its ID.
	Get(id string) (*model.Job, error)
	// List returns all jobs in the store.
	List() []*model.Job
	// Delete removes a job by its ID. Returns ErrJobIsRunning if the job
	// is currently running.
	Delete(id string) error
	// UpdateStatus changes the status of a job. For terminal statuses
	// (completed, failed, cancelled), it also sets CompletedAt.
	UpdateStatus(id string, status model.JobStatus, errMsg string) error
}

// InMemoryJobStore is a thread-safe in-memory implementation of JobStore.
type InMemoryJobStore struct {
	mu   sync.RWMutex
	jobs map[string]*model.Job
}

// NewInMemoryJobStore creates a new in-memory job store.
func NewInMemoryJobStore() *InMemoryJobStore {
	return &InMemoryJobStore{
		jobs: make(map[string]*model.Job),
	}
}

// Create stores a new job with a generated UUID, sets status to "queued",
// and returns the created job.
func (s *InMemoryJobStore) Create(config model.JobConfig) (*model.Job, error) {
	id, err := generateUUID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate job ID: %w", err)
	}

	job := &model.Job{
		ID:        id,
		Config:    config,
		Status:    model.StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	s.mu.Lock()
	s.jobs[id] = job
	s.mu.Unlock()

	return job, nil
}

// Get retrieves a job by ID. Returns ErrJobNotFound if the job does not exist.
func (s *InMemoryJobStore) Get(id string) (*model.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

// List returns all jobs in the store.
func (s *InMemoryJobStore) List() []*model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]*model.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// Delete removes a job by ID. Returns ErrJobNotFound if the job does not exist,
// or ErrJobIsRunning if the job is currently running.
func (s *InMemoryJobStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if job.Status == model.StatusRunning {
		return ErrJobIsRunning
	}

	delete(s.jobs, id)
	return nil
}

// UpdateStatus changes the status of a job. For terminal statuses (completed,
// failed, cancelled), it also sets CompletedAt. Returns ErrJobNotFound if the
// job does not exist.
func (s *InMemoryJobStore) UpdateStatus(id string, status model.JobStatus, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}

	// Prevent transitioning out of a terminal state.
	if job.Status == model.StatusCompleted || job.Status == model.StatusFailed || job.Status == model.StatusCancelled {
		return nil
	}

	job.Status = status
	job.Error = errMsg

	if status == model.StatusCompleted || status == model.StatusFailed || status == model.StatusCancelled {
		now := time.Now().UTC()
		job.CompletedAt = &now
	}

	return nil
}

// generateUUID generates a version 4 UUID string.
func generateUUID() (string, error) {
	var uuid [16]byte
	_, err := rand.Read(uuid[:])
	if err != nil {
		return "", err
	}

	// Set version 4 bits
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Set variant bits
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}
