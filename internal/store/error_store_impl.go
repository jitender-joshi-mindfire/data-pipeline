package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
)

const (
	maxMessageLength = 1000
	defaultLimit     = 50
	maxLimit         = 200
)

// idCounter is a package-level counter for generating unique error entry IDs.
var idCounter uint64
var idMu sync.Mutex

func nextID() string {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return fmt.Sprintf("err-%d", idCounter)
}

// InMemoryErrorStore is a thread-safe in-memory implementation of ErrorStore.
type InMemoryErrorStore struct {
	mu     sync.RWMutex
	errors map[string][]model.ErrorEntry // jobID -> errors
}

// NewInMemoryErrorStore creates a new InMemoryErrorStore.
func NewInMemoryErrorStore() *InMemoryErrorStore {
	return &InMemoryErrorStore{
		errors: make(map[string][]model.ErrorEntry),
	}
}

// Add stores an error entry for a job. Messages longer than 1000 characters
// are truncated. The entry is stamped with the current UTC time and assigned
// a unique ID.
func (s *InMemoryErrorStore) Add(jobID string, entry model.ErrorEntry) {
	entry.ID = nextID()
	entry.JobID = jobID
	entry.Timestamp = time.Now().UTC()

	if len(entry.Message) > maxMessageLength {
		entry.Message = entry.Message[:maxMessageLength]
	}

	s.mu.Lock()
	s.errors[jobID] = append(s.errors[jobID], entry)
	s.mu.Unlock()
}

// GetByJob returns paginated errors for a job and the total count.
// offset defaults to 0 if negative. limit defaults to 50 if <= 0 and is
// capped at 200.
func (s *InMemoryErrorStore) GetByJob(jobID string, offset, limit int) ([]model.ErrorEntry, int) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.errors[jobID]
	total := len(entries)

	if offset >= total {
		return []model.ErrorEntry{}, total
	}

	end := offset + limit
	if end > total {
		end = total
	}

	// Return a copy to avoid data races on the underlying slice.
	result := make([]model.ErrorEntry, end-offset)
	copy(result, entries[offset:end])

	return result, total
}

// DeleteByJob removes all errors associated with a job.
func (s *InMemoryErrorStore) DeleteByJob(jobID string) {
	s.mu.Lock()
	delete(s.errors, jobID)
	s.mu.Unlock()
}
