package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// mockProcessor implements Processor for testing.
type mockProcessor struct {
	processFunc func(ctx context.Context, record *model.Record) (*model.Record, error)
}

func (m *mockProcessor) Process(ctx context.Context, record *model.Record) (*model.Record, error) {
	return m.processFunc(ctx, record)
}

// mockErrorStore implements store.ErrorStore for testing.
type mockErrorStore struct {
	mu      sync.Mutex
	entries []model.ErrorEntry
}

func (m *mockErrorStore) Add(jobID string, entry model.ErrorEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
}

func (m *mockErrorStore) GetByJob(jobID string, offset, limit int) ([]model.ErrorEntry, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries, len(m.entries)
}

func (m *mockErrorStore) DeleteByJob(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = nil
}

// mockProgressTracker implements store.ProgressTracker for testing.
type mockProgressTracker struct {
	mu        sync.Mutex
	processed int
	failed    int
	latencies []time.Duration
}

func (m *mockProgressTracker) RecordProcessed(jobID string, stage string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed++
	m.latencies = append(m.latencies, latency)
}

func (m *mockProgressTracker) RecordFailed(jobID string, stage string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed++
}

func (m *mockProgressTracker) SetTotal(jobID string, total int64) {}

func (m *mockProgressTracker) GetProgress(jobID string) *model.Progress {
	return nil
}

func TestWorkerPool_ProcessesAllRecords(t *testing.T) {
	processor := &mockProcessor{
		processFunc: func(ctx context.Context, record *model.Record) (*model.Record, error) {
			return record, nil
		},
	}
	errStore := &mockErrorStore{}
	progress := &mockProgressTracker{}

	wp := &WorkerPool{
		Size:      3,
		Processor: processor,
		Stage:     "test-stage",
		ErrStore:  errStore,
		Progress:  progress,
		JobID:     "job-1",
	}

	in := make(chan *model.Record, 10)
	out := make(chan *model.Record, 10)

	// Send 5 records
	for i := 0; i < 5; i++ {
		in <- &model.Record{ID: "rec-" + string(rune('a'+i)), Fields: map[string]interface{}{"val": i}}
	}
	close(in)

	err := wp.Run(context.Background(), in, out)
	assert.NoError(t, err)

	// Collect output
	var results []*model.Record
	for r := range out {
		results = append(results, r)
	}

	assert.Equal(t, 5, len(results))
	assert.Equal(t, 5, progress.processed)
	assert.Equal(t, 0, progress.failed)
	assert.Equal(t, 0, len(errStore.entries))
}

func TestWorkerPool_HandlesErrors(t *testing.T) {
	processor := &mockProcessor{
		processFunc: func(ctx context.Context, record *model.Record) (*model.Record, error) {
			if record.ID == "bad" {
				return nil, errors.New("processing failed")
			}
			return record, nil
		},
	}
	errStore := &mockErrorStore{}
	progress := &mockProgressTracker{}

	wp := &WorkerPool{
		Size:      2,
		Processor: processor,
		Stage:     "validator",
		ErrStore:  errStore,
		Progress:  progress,
		JobID:     "job-2",
	}

	in := make(chan *model.Record, 5)
	out := make(chan *model.Record, 5)

	in <- &model.Record{ID: "good-1", Fields: map[string]interface{}{"x": 1}}
	in <- &model.Record{ID: "bad", Fields: map[string]interface{}{"x": 2}}
	in <- &model.Record{ID: "good-2", Fields: map[string]interface{}{"x": 3}}
	close(in)

	err := wp.Run(context.Background(), in, out)
	assert.NoError(t, err)

	var results []*model.Record
	for r := range out {
		results = append(results, r)
	}

	assert.Equal(t, 2, len(results))
	assert.Equal(t, 2, progress.processed)
	assert.Equal(t, 1, progress.failed)
	assert.Equal(t, 1, len(errStore.entries))
	assert.Equal(t, "validator", errStore.entries[0].Stage)
	assert.Equal(t, "job-2", errStore.entries[0].JobID)
	assert.Equal(t, "processing failed", errStore.entries[0].Message)
}

func TestWorkerPool_RespectsContextCancellation(t *testing.T) {
	processor := &mockProcessor{
		processFunc: func(ctx context.Context, record *model.Record) (*model.Record, error) {
			// Simulate some work
			time.Sleep(50 * time.Millisecond)
			return record, nil
		},
	}
	errStore := &mockErrorStore{}
	progress := &mockProgressTracker{}

	wp := &WorkerPool{
		Size:      2,
		Processor: processor,
		Stage:     "slow-stage",
		ErrStore:  errStore,
		Progress:  progress,
		JobID:     "job-3",
	}

	in := make(chan *model.Record, 100)
	out := make(chan *model.Record, 100)

	// Send many records
	for i := 0; i < 50; i++ {
		in <- &model.Record{ID: "rec", Fields: map[string]interface{}{"i": i}}
	}
	close(in)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := wp.Run(ctx, in, out)
	assert.Equal(t, context.Canceled, err)

	// Not all records should have been processed
	var results []*model.Record
	for r := range out {
		results = append(results, r)
	}
	assert.Less(t, len(results), 50)
}

func TestWorkerPool_ClosesOutChannelAfterCompletion(t *testing.T) {
	processor := &mockProcessor{
		processFunc: func(ctx context.Context, record *model.Record) (*model.Record, error) {
			return record, nil
		},
	}
	errStore := &mockErrorStore{}
	progress := &mockProgressTracker{}

	wp := &WorkerPool{
		Size:      4,
		Processor: processor,
		Stage:     "stage",
		ErrStore:  errStore,
		Progress:  progress,
		JobID:     "job-4",
	}

	in := make(chan *model.Record)
	out := make(chan *model.Record, 10)

	// Close input immediately
	close(in)

	err := wp.Run(context.Background(), in, out)
	assert.NoError(t, err)

	// out channel should be closed - reading should return zero value
	_, ok := <-out
	assert.False(t, ok, "out channel should be closed after Run completes")
}

func TestWorkerPool_Name(t *testing.T) {
	wp := &WorkerPool{
		Stage: "transformer",
	}
	assert.Equal(t, "transformer", wp.Name())
}

func TestWorkerPool_SingleWorker(t *testing.T) {
	var order []string
	var mu sync.Mutex

	processor := &mockProcessor{
		processFunc: func(ctx context.Context, record *model.Record) (*model.Record, error) {
			mu.Lock()
			order = append(order, record.ID)
			mu.Unlock()
			return record, nil
		},
	}
	errStore := &mockErrorStore{}
	progress := &mockProgressTracker{}

	wp := &WorkerPool{
		Size:      1,
		Processor: processor,
		Stage:     "ordered-stage",
		ErrStore:  errStore,
		Progress:  progress,
		JobID:     "job-5",
	}

	in := make(chan *model.Record, 3)
	out := make(chan *model.Record, 3)

	in <- &model.Record{ID: "a", Fields: map[string]interface{}{}}
	in <- &model.Record{ID: "b", Fields: map[string]interface{}{}}
	in <- &model.Record{ID: "c", Fields: map[string]interface{}{}}
	close(in)

	err := wp.Run(context.Background(), in, out)
	assert.NoError(t, err)

	// With a single worker, order should be preserved
	assert.Equal(t, []string{"a", "b", "c"}, order)
}

func TestTruncateMessage(t *testing.T) {
	short := "hello"
	assert.Equal(t, "hello", truncateMessage(short, 1000))

	long := ""
	for i := 0; i < 200; i++ {
		long += "abcdefghij" // 2000 chars total
	}
	result := truncateMessage(long, 1000)
	assert.Equal(t, 1000, len(result))
	assert.Equal(t, long[:1000], result)
}

// Feature: data-processing-pipeline, Property 1: Record Conservation Through Pipeline Stages
// Validates: Requirements 1.3, 2.2, 2.6, 4.5, 5.7
//
// For any input record set and any pipeline stage with N workers, the count of
// records emitted on the output channel plus the count of records logged to the
// Error_Store must equal the count of records received on the input channel.
func TestProperty1_RecordConservationThroughPipelineStages(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random worker pool size between 1 and 32
		poolSize := rapid.IntRange(1, 32).Draw(t, "poolSize")

		// Generate random number of input records (0 to 200)
		numRecords := rapid.IntRange(0, 200).Draw(t, "numRecords")

		// Generate a set of record IDs that should fail processing
		failSet := make(map[int]bool)
		if numRecords > 0 {
			numFailures := rapid.IntRange(0, numRecords).Draw(t, "numFailures")
			// Pick which indices should fail
			for i := 0; i < numFailures; i++ {
				idx := rapid.IntRange(0, numRecords-1).Draw(t, fmt.Sprintf("failIdx_%d", i))
				failSet[idx] = true
			}
		}

		// Create processor that fails for records in failSet
		processor := &mockProcessor{
			processFunc: func(ctx context.Context, record *model.Record) (*model.Record, error) {
				// Extract index from record ID
				var idx int
				fmt.Sscanf(record.ID, "rec-%d", &idx)
				if failSet[idx] {
					return nil, errors.New("simulated failure")
				}
				return record, nil
			},
		}

		errStore := &mockErrorStore{}
		progress := &mockProgressTracker{}

		wp := &WorkerPool{
			Size:      poolSize,
			Processor: processor,
			Stage:     "test-stage",
			ErrStore:  errStore,
			Progress:  progress,
			JobID:     "job-prop1",
		}

		in := make(chan *model.Record, numRecords+1)
		out := make(chan *model.Record, numRecords+1)

		// Send all records to the input channel
		for i := 0; i < numRecords; i++ {
			in <- &model.Record{
				ID:     fmt.Sprintf("rec-%d", i),
				Fields: map[string]interface{}{"index": i},
			}
		}
		close(in)

		// Run the worker pool
		err := wp.Run(context.Background(), in, out)
		if err != nil {
			t.Fatalf("unexpected error from Run: %v", err)
		}

		// Count output records
		outputCount := 0
		for range out {
			outputCount++
		}

		// Count error records
		errStore.mu.Lock()
		errorCount := len(errStore.entries)
		errStore.mu.Unlock()

		// Property: output records + error records == input records
		if outputCount+errorCount != numRecords {
			t.Fatalf("record conservation violated: output(%d) + errors(%d) = %d, want %d (poolSize=%d)",
				outputCount, errorCount, outputCount+errorCount, numRecords, poolSize)
		}
	})
}
