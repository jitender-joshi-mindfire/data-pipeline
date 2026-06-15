package store

import (
	"strings"
	"sync"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
)

func TestAdd_AssignsIDAndTimestamp(t *testing.T) {
	s := NewInMemoryErrorStore()

	entry := model.ErrorEntry{
		Stage:   "validator",
		Message: "field 'amount' is required",
		Record:  map[string]interface{}{"name": "test"},
	}

	s.Add("job-1", entry)

	errors, total := s.GetByJob("job-1", 0, 50)
	if total != 1 {
		t.Fatalf("expected total=1, got %d", total)
	}
	if errors[0].ID == "" {
		t.Error("expected non-empty ID")
	}
	if errors[0].JobID != "job-1" {
		t.Errorf("expected JobID=job-1, got %s", errors[0].JobID)
	}
	if errors[0].Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
	if errors[0].Timestamp.Location().String() != "UTC" {
		t.Error("expected UTC timestamp")
	}
}

func TestAdd_TruncatesLongMessages(t *testing.T) {
	s := NewInMemoryErrorStore()

	longMsg := strings.Repeat("x", 1500)
	entry := model.ErrorEntry{
		Stage:   "transformer",
		Message: longMsg,
	}

	s.Add("job-1", entry)

	errors, _ := s.GetByJob("job-1", 0, 50)
	if len(errors[0].Message) != 1000 {
		t.Errorf("expected message length=1000, got %d", len(errors[0].Message))
	}
	if errors[0].Message != longMsg[:1000] {
		t.Error("expected truncated message to preserve first 1000 chars")
	}
}

func TestAdd_PreservesShortMessages(t *testing.T) {
	s := NewInMemoryErrorStore()

	msg := "short message"
	entry := model.ErrorEntry{
		Stage:   "validator",
		Message: msg,
	}

	s.Add("job-1", entry)

	errors, _ := s.GetByJob("job-1", 0, 50)
	if errors[0].Message != msg {
		t.Errorf("expected message=%q, got %q", msg, errors[0].Message)
	}
}

func TestAdd_ExactlyMaxLength(t *testing.T) {
	s := NewInMemoryErrorStore()

	msg := strings.Repeat("a", 1000)
	entry := model.ErrorEntry{
		Stage:   "validator",
		Message: msg,
	}

	s.Add("job-1", entry)

	errors, _ := s.GetByJob("job-1", 0, 50)
	if len(errors[0].Message) != 1000 {
		t.Errorf("expected message length=1000, got %d", len(errors[0].Message))
	}
}

func TestGetByJob_Pagination(t *testing.T) {
	s := NewInMemoryErrorStore()

	for i := 0; i < 10; i++ {
		s.Add("job-1", model.ErrorEntry{Stage: "validator", Message: "error"})
	}

	// First page
	errors, total := s.GetByJob("job-1", 0, 3)
	if total != 10 {
		t.Fatalf("expected total=10, got %d", total)
	}
	if len(errors) != 3 {
		t.Errorf("expected 3 errors, got %d", len(errors))
	}

	// Middle page
	errors, total = s.GetByJob("job-1", 3, 3)
	if total != 10 {
		t.Fatalf("expected total=10, got %d", total)
	}
	if len(errors) != 3 {
		t.Errorf("expected 3 errors, got %d", len(errors))
	}

	// Last partial page
	errors, total = s.GetByJob("job-1", 8, 3)
	if total != 10 {
		t.Fatalf("expected total=10, got %d", total)
	}
	if len(errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(errors))
	}
}

func TestGetByJob_DefaultLimit(t *testing.T) {
	s := NewInMemoryErrorStore()

	for i := 0; i < 60; i++ {
		s.Add("job-1", model.ErrorEntry{Stage: "validator", Message: "error"})
	}

	// limit=0 should default to 50
	errors, total := s.GetByJob("job-1", 0, 0)
	if total != 60 {
		t.Fatalf("expected total=60, got %d", total)
	}
	if len(errors) != 50 {
		t.Errorf("expected 50 errors (default limit), got %d", len(errors))
	}
}

func TestGetByJob_MaxLimit(t *testing.T) {
	s := NewInMemoryErrorStore()

	for i := 0; i < 250; i++ {
		s.Add("job-1", model.ErrorEntry{Stage: "validator", Message: "error"})
	}

	// limit=300 should be capped at 200
	errors, total := s.GetByJob("job-1", 0, 300)
	if total != 250 {
		t.Fatalf("expected total=250, got %d", total)
	}
	if len(errors) != 200 {
		t.Errorf("expected 200 errors (max limit), got %d", len(errors))
	}
}

func TestGetByJob_OffsetBeyondTotal(t *testing.T) {
	s := NewInMemoryErrorStore()

	for i := 0; i < 5; i++ {
		s.Add("job-1", model.ErrorEntry{Stage: "validator", Message: "error"})
	}

	errors, total := s.GetByJob("job-1", 10, 50)
	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}
	if len(errors) != 0 {
		t.Errorf("expected 0 errors, got %d", len(errors))
	}
}

func TestGetByJob_NonExistentJob(t *testing.T) {
	s := NewInMemoryErrorStore()

	errors, total := s.GetByJob("non-existent", 0, 50)
	if total != 0 {
		t.Fatalf("expected total=0, got %d", total)
	}
	if len(errors) != 0 {
		t.Errorf("expected 0 errors, got %d", len(errors))
	}
}

func TestGetByJob_NegativeOffset(t *testing.T) {
	s := NewInMemoryErrorStore()

	for i := 0; i < 5; i++ {
		s.Add("job-1", model.ErrorEntry{Stage: "validator", Message: "error"})
	}

	errors, total := s.GetByJob("job-1", -1, 50)
	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}
	if len(errors) != 5 {
		t.Errorf("expected 5 errors, got %d", len(errors))
	}
}

func TestDeleteByJob(t *testing.T) {
	s := NewInMemoryErrorStore()

	s.Add("job-1", model.ErrorEntry{Stage: "validator", Message: "error1"})
	s.Add("job-1", model.ErrorEntry{Stage: "transformer", Message: "error2"})
	s.Add("job-2", model.ErrorEntry{Stage: "validator", Message: "error3"})

	s.DeleteByJob("job-1")

	errors, total := s.GetByJob("job-1", 0, 50)
	if total != 0 {
		t.Fatalf("expected total=0 after delete, got %d", total)
	}
	if len(errors) != 0 {
		t.Errorf("expected 0 errors after delete, got %d", len(errors))
	}

	// job-2 should be unaffected
	errors, total = s.GetByJob("job-2", 0, 50)
	if total != 1 {
		t.Fatalf("expected job-2 total=1, got %d", total)
	}
	if len(errors) != 1 {
		t.Errorf("expected 1 error for job-2, got %d", len(errors))
	}
}

func TestErrorStore_ConcurrentAccess(t *testing.T) {
	s := NewInMemoryErrorStore()

	var wg sync.WaitGroup
	const goroutines = 50
	const entriesPerGoroutine = 20

	// Concurrent writes
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				s.Add("job-1", model.ErrorEntry{
					Stage:   "validator",
					Message: "concurrent error",
				})
			}
		}()
	}

	// Concurrent reads
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				s.GetByJob("job-1", 0, 50)
			}
		}()
	}

	wg.Wait()

	_, total := s.GetByJob("job-1", 0, 50)
	expected := goroutines * entriesPerGoroutine
	if total != expected {
		t.Errorf("expected total=%d, got %d", expected, total)
	}
}

func TestStoresStageAndRecordData(t *testing.T) {
	s := NewInMemoryErrorStore()

	record := map[string]interface{}{
		"name":   "Alice",
		"amount": -5.0,
	}

	s.Add("job-1", model.ErrorEntry{
		Stage:   "validator",
		Message: "amount below minimum",
		Record:  record,
	})

	errors, _ := s.GetByJob("job-1", 0, 50)
	if errors[0].Stage != "validator" {
		t.Errorf("expected stage=validator, got %s", errors[0].Stage)
	}
	if errors[0].Record["name"] != "Alice" {
		t.Errorf("expected record name=Alice, got %v", errors[0].Record["name"])
	}
	if errors[0].Record["amount"] != -5.0 {
		t.Errorf("expected record amount=-5, got %v", errors[0].Record["amount"])
	}
}
