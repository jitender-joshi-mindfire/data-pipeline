package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

// Ingester implements the Stage interface. It launches one goroutine per
// configured Source, merges all emitted Records into a single output channel,
// and continues processing remaining sources if one fails.
type Ingester struct {
	Sources    []Source
	ErrorStore store.ErrorStore
	JobID      string
}

// NewIngester creates a new Ingester with the given sources, error store, and job ID.
func NewIngester(sources []Source, errorStore store.ErrorStore, jobID string) *Ingester {
	return &Ingester{
		Sources:    sources,
		ErrorStore: errorStore,
		JobID:      jobID,
	}
}

// Name returns the stage name.
func (ing *Ingester) Name() string {
	return "ingester"
}

// Run launches one goroutine per configured source, merging all Records into
// the out channel. The in channel is ignored because the Ingester is the first
// stage in the pipeline. The out channel is closed only after all source
// goroutines have finished. If a source fails, the error is logged to the
// ErrorStore and remaining sources continue processing.
func (ing *Ingester) Run(ctx context.Context, in <-chan *model.Record, out chan<- *model.Record) error {
	var wg sync.WaitGroup

	for _, src := range ing.Sources {
		wg.Add(1)
		go func(s Source) {
			defer wg.Done()

			if err := s.Read(ctx, out); err != nil {
				// If the context was cancelled, don't log as a source error
				if ctx.Err() != nil {
					return
				}
				ing.logSourceError(s, err)
			}
		}(src)
	}

	wg.Wait()
	close(out)
	return ctx.Err()
}

// logSourceError logs a source-level error to the ErrorStore.
func (ing *Ingester) logSourceError(src Source, err error) {
	if ing.ErrorStore == nil {
		return
	}
	ing.ErrorStore.Add(ing.JobID, model.ErrorEntry{
		Stage:     "ingester",
		Message:   fmt.Sprintf("source %s (%s) failed: %v", src.Identifier(), src.Type(), err),
		Timestamp: time.Now().UTC(),
	})
}
