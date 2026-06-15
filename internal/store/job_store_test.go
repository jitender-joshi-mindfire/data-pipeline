package store

import (
	"sync"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
)

func TestInMemoryJobStore_Create(t *testing.T) {
	store := NewInMemoryJobStore()

	config := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/data/test.csv"},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: "/output/results.json"},
		},
	}

	job, err := store.Create(config)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if job.ID == "" {
		t.Fatal("expected non-empty job ID")
	}
	if job.Status != model.StatusQueued {
		t.Errorf("expected status %q, got %q", model.StatusQueued, job.Status)
	}
	if job.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}
	if job.CompletedAt != nil {
		t.Error("expected nil CompletedAt")
	}
	if len(job.Config.Sources) != 1 || job.Config.Sources[0].Path != "/data/test.csv" {
		t.Error("job config not stored correctly")
	}
}

func TestInMemoryJobStore_Create_UniqueIDs(t *testing.T) {
	store := NewInMemoryJobStore()
	config := model.JobConfig{}

	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		job, err := store.Create(config)
		if err != nil {
			t.Fatalf("iteration %d: expected no error, got %v", i, err)
		}
		if ids[job.ID] {
			t.Fatalf("duplicate job ID generated: %s", job.ID)
		}
		ids[job.ID] = true
	}
}

func TestInMemoryJobStore_Get(t *testing.T) {
	store := NewInMemoryJobStore()
	config := model.JobConfig{}

	job, _ := store.Create(config)

	retrieved, err := store.Get(job.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if retrieved.ID != job.ID {
		t.Errorf("expected ID %q, got %q", job.ID, retrieved.ID)
	}
}

func TestInMemoryJobStore_Get_NotFound(t *testing.T) {
	store := NewInMemoryJobStore()

	_, err := store.Get("non-existent-id")
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestInMemoryJobStore_List(t *testing.T) {
	store := NewInMemoryJobStore()
	config := model.JobConfig{}

	// Empty store
	jobs := store.List()
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs, got %d", len(jobs))
	}

	// Add jobs
	store.Create(config)
	store.Create(config)
	store.Create(config)

	jobs = store.List()
	if len(jobs) != 3 {
		t.Errorf("expected 3 jobs, got %d", len(jobs))
	}
}

func TestInMemoryJobStore_Delete(t *testing.T) {
	store := NewInMemoryJobStore()
	config := model.JobConfig{}

	job, _ := store.Create(config)

	err := store.Delete(job.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	_, err = store.Get(job.ID)
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound after delete, got %v", err)
	}
}

func TestInMemoryJobStore_Delete_NotFound(t *testing.T) {
	store := NewInMemoryJobStore()

	err := store.Delete("non-existent-id")
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestInMemoryJobStore_Delete_RunningJob(t *testing.T) {
	store := NewInMemoryJobStore()
	config := model.JobConfig{}

	job, _ := store.Create(config)
	store.UpdateStatus(job.ID, model.StatusRunning, "")

	err := store.Delete(job.ID)
	if err != ErrJobIsRunning {
		t.Errorf("expected ErrJobIsRunning, got %v", err)
	}

	// Verify job still exists
	_, err = store.Get(job.ID)
	if err != nil {
		t.Errorf("expected job to still exist, got %v", err)
	}
}

func TestInMemoryJobStore_UpdateStatus(t *testing.T) {
	store := NewInMemoryJobStore()
	config := model.JobConfig{}

	job, _ := store.Create(config)

	// Update to running
	err := store.UpdateStatus(job.ID, model.StatusRunning, "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	retrieved, _ := store.Get(job.ID)
	if retrieved.Status != model.StatusRunning {
		t.Errorf("expected status %q, got %q", model.StatusRunning, retrieved.Status)
	}
	if retrieved.CompletedAt != nil {
		t.Error("expected nil CompletedAt for running status")
	}
}

func TestInMemoryJobStore_UpdateStatus_Terminal(t *testing.T) {
	terminalStatuses := []model.JobStatus{
		model.StatusCompleted,
		model.StatusFailed,
		model.StatusCancelled,
	}

	for _, status := range terminalStatuses {
		t.Run(string(status), func(t *testing.T) {
			store := NewInMemoryJobStore()
			config := model.JobConfig{}

			job, _ := store.Create(config)
			errMsg := ""
			if status == model.StatusFailed {
				errMsg = "something went wrong"
			}

			err := store.UpdateStatus(job.ID, status, errMsg)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			retrieved, _ := store.Get(job.ID)
			if retrieved.Status != status {
				t.Errorf("expected status %q, got %q", status, retrieved.Status)
			}
			if retrieved.CompletedAt == nil {
				t.Error("expected non-nil CompletedAt for terminal status")
			}
			if status == model.StatusFailed && retrieved.Error != errMsg {
				t.Errorf("expected error %q, got %q", errMsg, retrieved.Error)
			}
		})
	}
}

func TestInMemoryJobStore_UpdateStatus_NotFound(t *testing.T) {
	store := NewInMemoryJobStore()

	err := store.UpdateStatus("non-existent-id", model.StatusRunning, "")
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestInMemoryJobStore_ConcurrentAccess(t *testing.T) {
	store := NewInMemoryJobStore()
	config := model.JobConfig{}

	// Create some jobs first
	var jobIDs []string
	for i := 0; i < 10; i++ {
		job, _ := store.Create(config)
		jobIDs = append(jobIDs, job.ID)
	}

	var wg sync.WaitGroup

	// Concurrent creates
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Create(config)
		}()
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			store.Get(id)
		}(jobIDs[i%len(jobIDs)])
	}

	// Concurrent lists
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.List()
		}()
	}

	// Concurrent status updates
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			store.UpdateStatus(id, model.StatusRunning, "")
		}(jobIDs[i])
	}

	wg.Wait()

	// Should have 10 original + 50 created concurrently = 60 jobs
	jobs := store.List()
	if len(jobs) != 60 {
		t.Errorf("expected 60 jobs, got %d", len(jobs))
	}
}

func TestGenerateUUID_Format(t *testing.T) {
	uuid, err := generateUUID()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// UUID format: 8-4-4-4-12 hex characters
	if len(uuid) != 36 {
		t.Errorf("expected UUID length 36, got %d: %s", len(uuid), uuid)
	}

	// Check dashes are in the right positions
	if uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
		t.Errorf("UUID format is incorrect: %s", uuid)
	}

	// Check version 4 indicator
	if uuid[14] != '4' {
		t.Errorf("expected version 4 UUID (char at index 14 should be '4'), got %c in %s", uuid[14], uuid)
	}
}
