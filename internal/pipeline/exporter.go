package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

// ExporterStage implements the Stage interface and writes aggregated results
// to all configured export targets independently.
type ExporterStage struct {
	jobID    string
	targets  []export.ExportTarget
	errStore store.ErrorStore
}

// NewExporterStage creates a new ExporterStage with the given targets and error store.
func NewExporterStage(jobID string, targets []export.ExportTarget, errStore store.ErrorStore) *ExporterStage {
	return &ExporterStage{
		jobID:    jobID,
		targets:  targets,
		errStore: errStore,
	}
}

// Name returns the stage name.
func (e *ExporterStage) Name() string {
	return "exporter"
}

// Run executes the exporter stage. It collects all records from the input channel,
// then writes them to all configured export targets independently. A failure in one
// target does not prevent writes to other targets.
func (e *ExporterStage) Run(ctx context.Context, in <-chan *model.Record, out chan<- *model.Record) error {
	defer close(out)

	// Collect all records from input
	var records []*model.Record
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-in:
			if !ok {
				goto export
			}
			records = append(records, record)
		}
	}

export:
	// Write to all targets independently
	for _, target := range e.targets {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := target.Write(ctx, records); err != nil {
			// Log error to ErrorStore with target identifier and reason
			e.errStore.Add(e.jobID, model.ErrorEntry{
				JobID:     e.jobID,
				Stage:     "exporter",
				Message:   fmt.Sprintf("export to target '%s' failed: %s", target.Identifier(), err.Error()),
				Record:    nil,
				Timestamp: time.Now().UTC(),
			})
			// Continue with remaining targets
			continue
		}
	}

	// Forward all records downstream so the pipeline can capture them for the
	// in-memory result store (used by the GET /results API endpoint).
	for _, r := range records {
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
