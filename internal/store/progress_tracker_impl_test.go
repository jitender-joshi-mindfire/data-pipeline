package store

import (
	"sync"
	"testing"
	"time"
)

func TestNewProgressTracker(t *testing.T) {
	pt := NewProgressTracker()
	if pt == nil {
		t.Fatal("expected non-nil ProgressTracker")
	}
	if pt.jobs == nil {
		t.Fatal("expected jobs map to be initialized")
	}
}

func TestGetProgress_UnknownJob(t *testing.T) {
	pt := NewProgressTracker()
	progress := pt.GetProgress("nonexistent")
	if progress != nil {
		t.Fatal("expected nil for unknown job")
	}
}

func TestSetTotal(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 100)

	progress := pt.GetProgress("job1")
	if progress == nil {
		t.Fatal("expected non-nil progress after SetTotal")
	}
	if progress.RecordsPending != 100 {
		t.Errorf("expected RecordsPending=100, got %d", progress.RecordsPending)
	}
	if progress.RecordsProcessed != 0 {
		t.Errorf("expected RecordsProcessed=0, got %d", progress.RecordsProcessed)
	}
	if progress.PercentComplete != 0 {
		t.Errorf("expected PercentComplete=0, got %d", progress.PercentComplete)
	}
}

func TestRecordProcessed(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 10)

	pt.RecordProcessed("job1", "validator", 5*time.Millisecond)
	pt.RecordProcessed("job1", "validator", 15*time.Millisecond)
	pt.RecordProcessed("job1", "transformer", 10*time.Millisecond)

	progress := pt.GetProgress("job1")
	if progress.RecordsProcessed != 3 {
		t.Errorf("expected RecordsProcessed=3, got %d", progress.RecordsProcessed)
	}
	if progress.RecordsPending != 7 {
		t.Errorf("expected RecordsPending=7, got %d", progress.RecordsPending)
	}
	if progress.PercentComplete != 30 {
		t.Errorf("expected PercentComplete=30, got %d", progress.PercentComplete)
	}

	// Validator average latency: (5+15)/2 = 10ms
	valLatency, ok := progress.StageLatencies["validator"]
	if !ok {
		t.Fatal("expected validator stage latency")
	}
	if valLatency < 9.9 || valLatency > 10.1 {
		t.Errorf("expected validator latency ~10ms, got %f", valLatency)
	}

	// Transformer average latency: 10ms
	transLatency, ok := progress.StageLatencies["transformer"]
	if !ok {
		t.Fatal("expected transformer stage latency")
	}
	if transLatency < 9.9 || transLatency > 10.1 {
		t.Errorf("expected transformer latency ~10ms, got %f", transLatency)
	}
}

func TestRecordFailed(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 10)

	pt.RecordFailed("job1", "validator")
	pt.RecordFailed("job1", "validator")
	pt.RecordFailed("job1", "transformer")

	progress := pt.GetProgress("job1")
	if progress.ErrorCounts["validator"] != 2 {
		t.Errorf("expected validator error count=2, got %d", progress.ErrorCounts["validator"])
	}
	if progress.ErrorCounts["transformer"] != 1 {
		t.Errorf("expected transformer error count=1, got %d", progress.ErrorCounts["transformer"])
	}
}

func TestPercentComplete_CappedAt100(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 5)

	// Process more than total (edge case)
	for i := 0; i < 10; i++ {
		pt.RecordProcessed("job1", "validator", time.Millisecond)
	}

	progress := pt.GetProgress("job1")
	if progress.PercentComplete > 100 {
		t.Errorf("expected PercentComplete capped at 100, got %d", progress.PercentComplete)
	}
}

func TestPercentComplete_ZeroTotal(t *testing.T) {
	pt := NewProgressTracker()
	// Don't set total (defaults to 0)
	pt.RecordProcessed("job1", "validator", time.Millisecond)

	progress := pt.GetProgress("job1")
	if progress.PercentComplete != 0 {
		t.Errorf("expected PercentComplete=0 when total is 0, got %d", progress.PercentComplete)
	}
}

func TestProcessingRate(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 100)

	// Process some records
	for i := 0; i < 10; i++ {
		pt.RecordProcessed("job1", "validator", time.Millisecond)
	}

	// Allow a small amount of time to pass
	time.Sleep(10 * time.Millisecond)

	progress := pt.GetProgress("job1")
	// Rate should be > 0 since time has elapsed and records are processed
	if progress.ProcessingRate <= 0 {
		t.Errorf("expected ProcessingRate > 0, got %f", progress.ProcessingRate)
	}
}

func TestRecordsPending_NeverNegative(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 5)

	// Process more than total
	for i := 0; i < 10; i++ {
		pt.RecordProcessed("job1", "validator", time.Millisecond)
	}

	progress := pt.GetProgress("job1")
	if progress.RecordsPending < 0 {
		t.Errorf("expected RecordsPending >= 0, got %d", progress.RecordsPending)
	}
}

func TestConcurrentAccess(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 1000)

	var wg sync.WaitGroup
	// Simulate concurrent record processing
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pt.RecordProcessed("job1", "validator", time.Millisecond)
		}()
	}
	// Simulate concurrent failures
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pt.RecordFailed("job1", "validator")
		}()
	}
	// Simulate concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pt.GetProgress("job1")
		}()
	}

	wg.Wait()

	progress := pt.GetProgress("job1")
	if progress.RecordsProcessed != 100 {
		t.Errorf("expected RecordsProcessed=100, got %d", progress.RecordsProcessed)
	}
	if progress.ErrorCounts["validator"] != 50 {
		t.Errorf("expected validator error count=50, got %d", progress.ErrorCounts["validator"])
	}
}

func TestMultipleJobs(t *testing.T) {
	pt := NewProgressTracker()
	pt.SetTotal("job1", 100)
	pt.SetTotal("job2", 200)

	pt.RecordProcessed("job1", "ingester", 2*time.Millisecond)
	pt.RecordProcessed("job2", "ingester", 4*time.Millisecond)
	pt.RecordProcessed("job2", "ingester", 6*time.Millisecond)

	p1 := pt.GetProgress("job1")
	p2 := pt.GetProgress("job2")

	if p1.RecordsProcessed != 1 {
		t.Errorf("job1: expected RecordsProcessed=1, got %d", p1.RecordsProcessed)
	}
	if p2.RecordsProcessed != 2 {
		t.Errorf("job2: expected RecordsProcessed=2, got %d", p2.RecordsProcessed)
	}
	if p1.RecordsPending != 99 {
		t.Errorf("job1: expected RecordsPending=99, got %d", p1.RecordsPending)
	}
	if p2.RecordsPending != 198 {
		t.Errorf("job2: expected RecordsPending=198, got %d", p2.RecordsPending)
	}
}
