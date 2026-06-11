package pipeline

import (
	"context"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// Source reads data and emits records.
type Source interface {
	// Read reads data from the source and sends records to the out channel.
	Read(ctx context.Context, out chan<- *model.Record) error
	// Type returns the source type (e.g., "csv", "json", "http").
	Type() string
	// Identifier returns the source identifier (e.g., file path or URL).
	Identifier() string
}
