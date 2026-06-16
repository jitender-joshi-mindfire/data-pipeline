package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

// WorkerPool runs N workers for a given Processor using fan-out/fan-in concurrency.
type WorkerPool struct {
	Size      int
	Processor Processor
	Stage     string
	ErrStore  store.ErrorStore
	Progress  store.ProgressTracker
	JobID     string
}

// Run executes the worker pool, distributing incoming records across N worker
// goroutines (fan-out) and merging outputs into the out channel (fan-in).
// It closes the out channel only after all workers have finished.
func (wp *WorkerPool) Run(ctx context.Context, in <-chan *model.Record, out chan<- *model.Record) error {
	var wg sync.WaitGroup

	for i := 0; i < wp.Size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for record := range in {
				// Check context before processing
				select {
				case <-ctx.Done():
					return
				default:
				}

				start := time.Now()
				result, err := wp.Processor.Process(ctx, record)
				latency := time.Since(start)

				if err != nil {
					wp.ErrStore.Add(wp.JobID, model.ErrorEntry{
						JobID:     wp.JobID,
						Stage:     wp.Stage,
						Message:   truncateMessage(err.Error(), 1000),
						Record:    record.Fields,
						Timestamp: time.Now().UTC(),
					})
					wp.Progress.RecordFailed(wp.JobID, wp.Stage)
					continue
				}

				wp.Progress.RecordProcessed(wp.JobID, wp.Stage, latency)

				// Check context before sending to out channel
				select {
				case out <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()
	close(out)

	return ctx.Err()
}

// Name returns the stage name for this worker pool.
func (wp *WorkerPool) Name() string {
	return wp.Stage
}

// truncateMessage truncates a message to maxLen characters.
func truncateMessage(msg string, maxLen int) string {
	if len(msg) <= maxLen {
		return msg
	}
	return msg[:maxLen]
}
