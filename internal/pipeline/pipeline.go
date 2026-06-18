package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

const (
	// channelBufferSize is the default buffer size for inter-stage channels.
	channelBufferSize = 100

	// maxWorkers is the maximum allowed worker pool size per stage.
	maxWorkers = 32

	// defaultWorkers is the default worker pool size when not specified.
	defaultWorkers = 1

	// gracePeriod is the duration to wait for stages to shut down after context cancellation.
	gracePeriod = 5 * time.Second

	// minTimeoutSeconds is the minimum allowed timeout in seconds.
	minTimeoutSeconds = 1

	// maxTimeoutSeconds is the maximum allowed timeout in seconds (24 hours).
	maxTimeoutSeconds = 86400
)

// Pipeline orchestrates the five-stage data processing pipeline.
// It connects Ingester → Validator → Transformer → Aggregator → Exporter
// with buffered channels, manages goroutine lifecycle, and transitions job status.
type Pipeline struct {
	Job      *model.Job
	JobStore store.JobStore

	// Stage dependencies
	Sources      []Source
	ExportTargets []export.ExportTarget
	ErrStore     store.ErrorStore
	Progress     store.ProgressTracker

	// Worker pool configuration
	ValidatorWorkers   int
	TransformerWorkers int
}

// NewPipeline creates a new Pipeline with the given configuration and dependencies.
// It normalizes worker pool sizes to be within [1, 32].
func NewPipeline(
	job *model.Job,
	jobStore store.JobStore,
	sources []Source,
	exportTargets []export.ExportTarget,
	errStore store.ErrorStore,
	progress store.ProgressTracker,
) *Pipeline {
	validatorWorkers := normalizeWorkerCount(job.Config.WorkerPools.Validator)
	transformerWorkers := normalizeWorkerCount(job.Config.WorkerPools.Transformer)

	return &Pipeline{
		Job:                job,
		JobStore:           jobStore,
		Sources:            sources,
		ExportTargets:      exportTargets,
		ErrStore:           errStore,
		Progress:           progress,
		ValidatorWorkers:   validatorWorkers,
		TransformerWorkers: transformerWorkers,
	}
}

// RunJob creates the appropriate context based on the job's timeout configuration
// and executes the pipeline. If TimeoutSeconds is set and within [1, 86400], a
// context with that deadline is used. Otherwise, a cancellable context with no
// deadline is created.
func (p *Pipeline) RunJob() error {
	var ctx context.Context
	var cancel context.CancelFunc

	if p.Job.Config.TimeoutSeconds != nil {
		timeout := *p.Job.Config.TimeoutSeconds
		if timeout >= minTimeoutSeconds && timeout <= maxTimeoutSeconds {
			ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
		} else {
			// Out of range: treat as no timeout
			ctx, cancel = context.WithCancel(context.Background())
		}
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	return p.Run(ctx)
}

// Run executes the pipeline end-to-end. It transitions the job through its
// lifecycle: queued → running → completed/failed/cancelled.
// The provided context controls timeout and cancellation propagation.
func (p *Pipeline) Run(ctx context.Context) error {
	// Transition job to running
	if err := p.JobStore.UpdateStatus(p.Job.ID, model.StatusRunning, ""); err != nil {
		return fmt.Errorf("failed to transition job to running: %w", err)
	}

	// Create a cancellable context derived from the parent
	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create buffered channels connecting the five stages
	ingesterOut := make(chan *model.Record, channelBufferSize)   // ingester → validator
	validatorOut := make(chan *model.Record, channelBufferSize)  // validator → transformer
	transformerOut := make(chan *model.Record, channelBufferSize) // transformer → aggregator
	aggregatorOut := make(chan *model.Record, channelBufferSize) // aggregator → exporter

	// Build stages
	ingester := NewIngester(p.Sources, p.ErrStore, p.Job.ID)

	validatorPool := &WorkerPool{
		Size:      p.ValidatorWorkers,
		Processor: NewValidator(p.Job.Config.Validation, p.ErrStore, p.Progress, p.Job.ID),
		Stage:     "validator",
		ErrStore:  p.ErrStore,
		Progress:  p.Progress,
		JobID:     p.Job.ID,
	}

	transformerPool := &WorkerPool{
		Size:      p.TransformerWorkers,
		Processor: NewTransformer(p.Job.Config.Transformations),
		Stage:     "transformer",
		ErrStore:  p.ErrStore,
		Progress:  p.Progress,
		JobID:     p.Job.ID,
	}

	aggregator := NewAggregator(p.Job.Config.Aggregation, p.Job.ID, p.ErrStore, p.Progress)

	exporter := NewExporterStage(p.Job.ID, p.ExportTargets, p.ErrStore)

	// Track errors from each stage
	var (
		mu       sync.Mutex
		fatalErr error
	)

	setFatal := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if fatalErr == nil && err != nil {
			fatalErr = err
			cancel() // Cancel all stages on first fatal error
		}
	}

	// Track stage completion for grace period reporting
	stageNames := []string{"ingester", "validator", "transformer", "aggregator", "exporter"}
	stageCompleted := make([]bool, len(stageNames))
	var stageMu sync.Mutex

	markDone := func(idx int) {
		stageMu.Lock()
		stageCompleted[idx] = true
		stageMu.Unlock()
	}

	// Launch all stages as goroutines
	var wg sync.WaitGroup

	// Stage 1: Ingester (no input channel, produces to ingesterOut)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer markDone(0)
		if err := ingester.Run(pipelineCtx, nil, ingesterOut); err != nil {
			if pipelineCtx.Err() == nil {
				// Only set fatal if it's not due to context cancellation
				setFatal(fmt.Errorf("ingester: %w", err))
			}
		}
	}()

	// Stage 2: Validator (reads from ingesterOut, writes to validatorOut)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer markDone(1)
		if err := validatorPool.Run(pipelineCtx, ingesterOut, validatorOut); err != nil {
			if pipelineCtx.Err() == nil {
				setFatal(fmt.Errorf("validator: %w", err))
			}
		}
	}()

	// Stage 3: Transformer (reads from validatorOut, writes to transformerOut)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer markDone(2)
		if err := transformerPool.Run(pipelineCtx, validatorOut, transformerOut); err != nil {
			if pipelineCtx.Err() == nil {
				setFatal(fmt.Errorf("transformer: %w", err))
			}
		}
	}()

	// Stage 4: Aggregator (reads from transformerOut, writes to aggregatorOut)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer markDone(3)
		if err := aggregator.Run(pipelineCtx, transformerOut, aggregatorOut); err != nil {
			if pipelineCtx.Err() == nil {
				setFatal(fmt.Errorf("aggregator: %w", err))
			}
		}
	}()

	// Stage 5: Exporter (reads from aggregatorOut, writes to nil out channel)
	// The exporter is the terminal stage; we use a dummy output channel.
	exporterOut := make(chan *model.Record, channelBufferSize)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer markDone(4)
		if err := exporter.Run(pipelineCtx, aggregatorOut, exporterOut); err != nil {
			if pipelineCtx.Err() == nil {
				setFatal(fmt.Errorf("exporter: %w", err))
			}
		}
	}()

	// Wait for all stages with grace period handling
	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	// If context is already done (timeout/cancellation), apply grace period
	gracePeriodExceeded := false
	select {
	case <-allDone:
		// All stages completed normally
	case <-ctx.Done():
		// Context was cancelled or timed out. Wait for grace period.
		select {
		case <-allDone:
			// Stages finished within grace period
		case <-time.After(gracePeriod):
			// Grace period exceeded - log warnings for stages that didn't finish
			gracePeriodExceeded = true
			stageMu.Lock()
			for i, name := range stageNames {
				if !stageCompleted[i] {
					p.ErrStore.Add(p.Job.ID, model.ErrorEntry{
						JobID:     p.Job.ID,
						Stage:     name,
						Message:   fmt.Sprintf("stage %q did not terminate within 5s grace period; force-terminated", name),
						Timestamp: time.Now().UTC(),
					})
				}
			}
			stageMu.Unlock()
		}
	}

	// Determine final job status
	mu.Lock()
	finalErr := fatalErr
	mu.Unlock()

	if finalErr != nil {
		// Check if the error was due to parent context cancellation (user cancel)
		if ctx.Err() == context.Canceled {
			_ = p.JobStore.UpdateStatus(p.Job.ID, model.StatusCancelled, "")
			return ctx.Err()
		}
		// Fatal error from a stage
		errMsg := finalErr.Error()
		_ = p.JobStore.UpdateStatus(p.Job.ID, model.StatusFailed, errMsg)
		return finalErr
	}

	// Check if the parent context was cancelled (timeout or user cancel)
	if ctx.Err() != nil {
		if ctx.Err() == context.DeadlineExceeded {
			msg := "pipeline timeout exceeded"
			if gracePeriodExceeded {
				msg = "pipeline timeout exceeded; some stages did not terminate within grace period"
			}
			_ = p.JobStore.UpdateStatus(p.Job.ID, model.StatusFailed, msg)
			return ctx.Err()
		}
		_ = p.JobStore.UpdateStatus(p.Job.ID, model.StatusCancelled, "")
		return ctx.Err()
	}

	// All stages completed successfully
	_ = p.JobStore.UpdateStatus(p.Job.ID, model.StatusCompleted, "")
	return nil
}

// normalizeWorkerCount clamps a worker count to [1, maxWorkers].
func normalizeWorkerCount(n int) int {
	if n < 1 {
		return defaultWorkers
	}
	if n > maxWorkers {
		return maxWorkers
	}
	return n
}
