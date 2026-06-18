package export

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sort"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// CSVTarget implements ExportTarget for writing aggregated results to a CSV file.
type CSVTarget struct {
	filePath string
}

// NewCSVTarget creates a new CSVTarget that writes to the specified file path.
func NewCSVTarget(filePath string) *CSVTarget {
	return &CSVTarget{filePath: filePath}
}

// Write writes the aggregated results to a CSV file with a header row.
// It overwrites the file if it already exists.
func (c *CSVTarget) Write(ctx context.Context, results []*model.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Collect all field names from results for a consistent header
	fieldSet := make(map[string]struct{})
	for _, r := range results {
		for k := range r.Fields {
			fieldSet[k] = struct{}{}
		}
	}

	// Sort field names for deterministic column ordering
	fields := make([]string, 0, len(fieldSet))
	for k := range fieldSet {
		fields = append(fields, k)
	}
	sort.Strings(fields)

	// Create or overwrite the file
	file, err := os.Create(c.filePath)
	if err != nil {
		return fmt.Errorf("csv export: failed to create file %s: %w", c.filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header row
	if err := writer.Write(fields); err != nil {
		return fmt.Errorf("csv export: failed to write header: %w", err)
	}

	// Write each record as a row
	for _, r := range results {
		if err := ctx.Err(); err != nil {
			return err
		}

		row := make([]string, len(fields))
		for i, field := range fields {
			row[i] = fmt.Sprintf("%v", r.Fields[field])
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("csv export: failed to write row: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("csv export: flush error: %w", err)
	}

	return nil
}

// Type returns the export target type.
func (c *CSVTarget) Type() string {
	return "csv"
}

// Identifier returns the file path used by this export target.
func (c *CSVTarget) Identifier() string {
	return c.filePath
}

// Close is a no-op for CSVTarget since the file is written and closed in Write.
func (c *CSVTarget) Close() error {
	return nil
}
