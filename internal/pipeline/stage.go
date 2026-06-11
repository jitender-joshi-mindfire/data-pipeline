package pipeline

import (
	"context"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// Stage defines the contract for all pipeline stages.
type Stage interface {
	// Run executes the stage, reading from in and writing to out.
	// It must respect ctx cancellation and close out when done.
	Run(ctx context.Context, in <-chan *model.Record, out chan<- *model.Record) error
	// Name returns the name of the stage.
	Name() string
}

// Processor is the unit of work within a stage.
type Processor interface {
	// Process applies the stage logic to a single record.
	Process(ctx context.Context, record *model.Record) (*model.Record, error)
}
