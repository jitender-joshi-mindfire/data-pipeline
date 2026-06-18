package export

import (
	"context"
	"encoding/json"
	"os"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// JSONTarget implements ExportTarget by writing aggregated results as a JSON array to a file.
type JSONTarget struct {
	filePath string
}

// NewJSONTarget creates a new JSONTarget that writes to the specified file path.
func NewJSONTarget(filePath string) *JSONTarget {
	return &JSONTarget{filePath: filePath}
}

// Write serializes the results' Fields as a JSON array and writes to the file,
// overwriting the file if it already exists.
func (j *JSONTarget) Write(ctx context.Context, results []*model.Record) error {
	// Extract Fields from each record for export
	output := make([]map[string]interface{}, 0, len(results))
	for _, r := range results {
		output = append(output, r.Fields)
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(j.filePath, data, 0644)
}

// Type returns "json" identifying this export target type.
func (j *JSONTarget) Type() string {
	return "json"
}

// Identifier returns the file path for this export target.
func (j *JSONTarget) Identifier() string {
	return j.filePath
}

// Close is a no-op since the file is written and closed in Write.
func (j *JSONTarget) Close() error {
	return nil
}
