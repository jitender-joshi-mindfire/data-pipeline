package api

import "sync"

// InMemoryResultStore is a thread-safe in-memory implementation of ResultStore.
type InMemoryResultStore struct {
	mu      sync.RWMutex
	results map[string][]map[string]interface{}
}

// NewInMemoryResultStore creates a new in-memory result store.
func NewInMemoryResultStore() *InMemoryResultStore {
	return &InMemoryResultStore{
		results: make(map[string][]map[string]interface{}),
	}
}

// GetResults returns the stored results for a job, or false if no results exist.
func (s *InMemoryResultStore) GetResults(jobID string) ([]map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	results, ok := s.results[jobID]
	return results, ok
}

// StoreResults stores the results for a job.
func (s *InMemoryResultStore) StoreResults(jobID string, results []map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[jobID] = results
}

// DeleteResults removes stored results for a job.
func (s *InMemoryResultStore) DeleteResults(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.results, jobID)
}
