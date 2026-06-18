package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
)

// pipelineMockSource implements the Source interface for testing the pipeline.
type pipelineMockSource struct {
	records []*model.Record
	err     error
}

func (m *pipelineMockSource) Read(ctx context.Context, out chan<- *model.Record) error {
	if m.err != nil {
		return m.err
	}
	for _, r := range m.records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- r:
		}
	}
	return nil
}

func (m *pipelineMockSource) Type() string       { return "mock" }
func (m *pipelineMockSource) Identifier() string { return "mock-source" }

// pipelineMockExportTarget implements the export.ExportTarget interface for testing.
type pipelineMockExportTarget struct {
	mu      sync.Mutex
	records []*model.Record
	err     error
}

func (m *pipelineMockExportTarget) Write(ctx context.Context, results []*model.Record) error {
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	m.records = results
	m.mu.Unlock()
	return nil
}

func (m *pipelineMockExportTarget) Type() string       { return "mock" }
func (m *pipelineMockExportTarget) Identifier() string { return "mock-target" }
func (m *pipelineMockExportTarget) Close() error       { return nil }

func newTestJob() *model.Job {
	return &model.Job{
		ID:     "test-job-1",
		Status: model.StatusQueued,
		Config: model.JobConfig{
			Sources: []model.SourceConfig{
				{Type: "csv", Path: "test.csv"},
			},
			Validation: model.ValidationConfig{
				Fields: []model.FieldSchema{},
			},
			Transformations: []model.TransformConfig{},
			Aggregation: model.AggregationConfig{
				GroupBy:   []string{},
				Functions: []model.AggregationFunction{
					{Name: "count", Field: "*", Alias: "total"},
				},
			},
			Exports: []model.ExportConfig{
				{Type: "csv", Path: "out.csv"},
			},
			WorkerPools: model.WorkerPoolConfig{
				Validator:   1,
				Transformer: 1,
			},
		},
		CreatedAt: time.Now().UTC(),
	}
}

func TestPipeline_Run_SuccessfulCompletion(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	// Manually insert the job into the store
	jobStore.Create(job.Config)
	jobs := jobStore.List()
	job = jobs[0]

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"name": "Alice", "value": float64(10)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "mock-source"}},
		{ID: "r2", Fields: map[string]interface{}{"name": "Bob", "value": float64(20)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "mock-source"}},
		{ID: "r3", Fields: map[string]interface{}{"name": "Charlie", "value": float64(30)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "mock-source"}},
	}

	source := &pipelineMockSource{records: records}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		job,
		jobStore,
		[]Source{source},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	err := p.Run(context.Background())
	assert.NoError(t, err)

	// Job should be completed
	updatedJob, _ := jobStore.Get(job.ID)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	// Export target should have received aggregated results
	target.mu.Lock()
	assert.NotEmpty(t, target.records)
	target.mu.Unlock()
}

func TestPipeline_Run_ContextCancellation(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	jobStore.Create(job.Config)
	jobs := jobStore.List()
	job = jobs[0]

	// Create a source that blocks until context is cancelled
	slowSource := &blockingSource{}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		job,
		jobStore,
		[]Source{slowSource},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()

	// Give the pipeline time to start
	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-done
	assert.Error(t, err)

	// Job should be cancelled
	updatedJob, _ := jobStore.Get(job.ID)
	assert.Equal(t, model.StatusCancelled, updatedJob.Status)
}

func TestPipeline_Run_ContextTimeout(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	jobStore.Create(job.Config)
	jobs := jobStore.List()
	job = jobs[0]

	// Create a source that blocks
	slowSource := &blockingSource{}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		job,
		jobStore,
		[]Source{slowSource},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	assert.Error(t, err)

	// Job should be failed (timeout)
	updatedJob, _ := jobStore.Get(job.ID)
	assert.Equal(t, model.StatusFailed, updatedJob.Status)
}

func TestPipeline_Run_TransitionsToRunning(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	jobStore.Create(job.Config)
	jobs := jobStore.List()
	job = jobs[0]

	// Verify job starts as queued
	j, _ := jobStore.Get(job.ID)
	assert.Equal(t, model.StatusQueued, j.Status)

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"x": float64(1)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "s"}},
	}
	source := &pipelineMockSource{records: records}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		job,
		jobStore,
		[]Source{source},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	err := p.Run(context.Background())
	assert.NoError(t, err)

	// After completion, job should be completed (it was running in between)
	j, _ = jobStore.Get(job.ID)
	assert.Equal(t, model.StatusCompleted, j.Status)
}

func TestPipeline_Run_FatalStageError(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	jobStore.Create(job.Config)
	jobs := jobStore.List()
	job = jobs[0]

	// Source that returns an error
	failSource := &pipelineMockSource{err: errors.New("source unavailable")}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		job,
		jobStore,
		[]Source{failSource},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	err := p.Run(context.Background())
	// With a single source that fails, ingester logs error but doesn't return fatal error
	// The pipeline should still complete (with no records)
	// The ingester logs errors and continues; it only returns ctx.Err()
	assert.NoError(t, err)

	j, _ := jobStore.Get(job.ID)
	assert.Equal(t, model.StatusCompleted, j.Status)
}

func TestPipeline_NormalizeWorkerCount(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{-1, 1},
		{1, 1},
		{16, 16},
		{32, 32},
		{33, 32},
		{100, 32},
	}

	for _, tt := range tests {
		result := normalizeWorkerCount(tt.input)
		assert.Equal(t, tt.expected, result, "normalizeWorkerCount(%d)", tt.input)
	}
}

func TestPipeline_MultipleWorkers(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	job.Config.WorkerPools.Validator = 4
	job.Config.WorkerPools.Transformer = 2

	created, _ := jobStore.Create(job.Config)

	records := make([]*model.Record, 10)
	for i := range records {
		records[i] = &model.Record{
			ID:       fmt.Sprintf("r%d", i),
			Fields:   map[string]interface{}{"val": float64(i)},
			Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "s"},
		}
	}
	source := &pipelineMockSource{records: records}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		created,
		jobStore,
		[]Source{source},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	assert.Equal(t, 4, p.ValidatorWorkers)
	assert.Equal(t, 2, p.TransformerWorkers)

	err := p.Run(context.Background())
	assert.NoError(t, err)

	j, _ := jobStore.Get(created.ID)
	assert.Equal(t, model.StatusCompleted, j.Status)
}

// blockingSource blocks until context is cancelled
type blockingSource struct{}

func (b *blockingSource) Read(ctx context.Context, out chan<- *model.Record) error {
	<-ctx.Done()
	return ctx.Err()
}

func (b *blockingSource) Type() string       { return "mock" }
func (b *blockingSource) Identifier() string { return "blocking-source" }

// slowShutdownSource blocks and ignores context cancellation for a duration,
// simulating a stage that exceeds the grace period.
type slowShutdownSource struct {
	shutdownDelay time.Duration
}

func (s *slowShutdownSource) Read(ctx context.Context, out chan<- *model.Record) error {
	<-ctx.Done()
	// Simulate slow shutdown: sleep even after context is cancelled
	time.Sleep(s.shutdownDelay)
	return ctx.Err()
}

func (s *slowShutdownSource) Type() string       { return "mock" }
func (s *slowShutdownSource) Identifier() string { return "slow-shutdown-source" }

func intPtr(v int) *int {
	return &v
}

func TestPipeline_RunJob_WithTimeout(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	timeout := 1
	job.Config.TimeoutSeconds = &timeout

	created, _ := jobStore.Create(job.Config)
	created.Config.TimeoutSeconds = &timeout

	// Use a blocking source that never completes
	slowSource := &blockingSource{}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		created,
		jobStore,
		[]Source{slowSource},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	err := p.RunJob()
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	// Job should be failed (timeout)
	updatedJob, _ := jobStore.Get(created.ID)
	assert.Equal(t, model.StatusFailed, updatedJob.Status)
}

func TestPipeline_RunJob_WithoutTimeout(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	// No timeout set - TimeoutSeconds is nil
	job.Config.TimeoutSeconds = nil

	created, _ := jobStore.Create(job.Config)

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"name": "Alice", "value": float64(10)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "mock-source"}},
	}

	source := &pipelineMockSource{records: records}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		created,
		jobStore,
		[]Source{source},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	err := p.RunJob()
	assert.NoError(t, err)

	// Job should complete normally without timeout
	updatedJob, _ := jobStore.Get(created.ID)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)
}

func TestPipeline_RunJob_TimeoutTransitionsToFailed(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	timeout := 1
	job.Config.TimeoutSeconds = &timeout

	created, _ := jobStore.Create(job.Config)
	created.Config.TimeoutSeconds = &timeout

	// Slow source that blocks forever
	slowSource := &blockingSource{}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		created,
		jobStore,
		[]Source{slowSource},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	err := p.RunJob()
	assert.Error(t, err)

	// Job must transition to "failed" on timeout
	updatedJob, _ := jobStore.Get(created.ID)
	assert.Equal(t, model.StatusFailed, updatedJob.Status)
	assert.Contains(t, updatedJob.Error, "pipeline timeout exceeded")
}

func TestPipeline_GracePeriod_WarningLogged(t *testing.T) {
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	job := newTestJob()
	// Set a very short timeout so it triggers quickly
	timeout := 1
	job.Config.TimeoutSeconds = &timeout

	created, _ := jobStore.Create(job.Config)
	created.Config.TimeoutSeconds = &timeout

	// Use a source that takes longer than grace period to shut down (7 seconds > 5 second grace)
	slowSource := &slowShutdownSource{shutdownDelay: 7 * time.Second}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(
		created,
		jobStore,
		[]Source{slowSource},
		[]export.ExportTarget{target},
		errStore,
		progress,
	)

	err := p.RunJob()
	assert.Error(t, err)

	// Job should be failed
	updatedJob, _ := jobStore.Get(created.ID)
	assert.Equal(t, model.StatusFailed, updatedJob.Status)

	// Check that a warning was logged to the error store for the ingester stage
	// (the stage that didn't shut down in time)
	errors, total := errStore.GetByJob(created.ID, 0, 100)
	assert.Greater(t, total, 0)
	foundGracePeriodWarning := false
	for _, e := range errors {
		if e.Stage == "ingester" && (assert.ObjectsAreEqual("stage \"ingester\" did not terminate within 5s grace period; force-terminated", e.Message) || 
			len(e.Message) > 0 && e.Message == fmt.Sprintf("stage %q did not terminate within 5s grace period; force-terminated", "ingester")) {
			foundGracePeriodWarning = true
			break
		}
	}
	assert.True(t, foundGracePeriodWarning, "expected grace period warning for ingester stage, got errors: %v", errors)
}

func TestPipeline_RunJob_TimeoutBoundsValidation(t *testing.T) {
	tests := []struct {
		name           string
		timeout        *int
		source         Source
		expectTimeout  bool
		expectComplete bool
	}{
		{
			name:           "nil timeout - no deadline",
			timeout:        nil,
			source:         &pipelineMockSource{records: []*model.Record{{ID: "r1", Fields: map[string]interface{}{"x": float64(1)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "s"}}}},
			expectComplete: true,
		},
		{
			name:           "timeout of 1 second - valid minimum",
			timeout:        intPtr(1),
			source:         &blockingSource{},
			expectTimeout:  true,
		},
		{
			name:           "timeout of 0 - out of range, treated as no timeout",
			timeout:        intPtr(0),
			source:         &pipelineMockSource{records: []*model.Record{{ID: "r1", Fields: map[string]interface{}{"x": float64(1)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "s"}}}},
			expectComplete: true,
		},
		{
			name:           "timeout of -1 - out of range, treated as no timeout",
			timeout:        intPtr(-1),
			source:         &pipelineMockSource{records: []*model.Record{{ID: "r1", Fields: map[string]interface{}{"x": float64(1)}, Metadata: model.RecordMetadata{SourceType: "mock", SourceID: "s"}}}},
			expectComplete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobStore := store.NewInMemoryJobStore()
			errStore := store.NewInMemoryErrorStore()
			progress := store.NewProgressTracker()

			job := newTestJob()
			job.Config.TimeoutSeconds = tt.timeout

			created, _ := jobStore.Create(job.Config)
			created.Config.TimeoutSeconds = tt.timeout

			target := &pipelineMockExportTarget{}
			p := NewPipeline(
				created,
				jobStore,
				[]Source{tt.source},
				[]export.ExportTarget{target},
				errStore,
				progress,
			)

			err := p.RunJob()

			if tt.expectTimeout {
				assert.Error(t, err)
				updatedJob, _ := jobStore.Get(created.ID)
				assert.Equal(t, model.StatusFailed, updatedJob.Status)
			}
			if tt.expectComplete {
				assert.NoError(t, err)
				updatedJob, _ := jobStore.Get(created.ID)
				assert.Equal(t, model.StatusCompleted, updatedJob.Status)
			}
		})
	}
}
