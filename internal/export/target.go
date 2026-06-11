package export

import (
	"context"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// ExportTarget writes aggregated results to a target.
type ExportTarget interface {
	// Write writes the aggregated results to the export target.
	Write(ctx context.Context, results []*model.Record) error
	// Type returns the export target type (e.g., "sqlite", "csv", "json").
	Type() string
	// Identifier returns the export target identifier (e.g., file path or DB path).
	Identifier() string
	// Close releases any resources held by the export target.
	Close() error
}
