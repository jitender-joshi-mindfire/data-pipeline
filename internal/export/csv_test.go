package export

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVTarget_Type(t *testing.T) {
	target := NewCSVTarget("/tmp/test.csv")
	assert.Equal(t, "csv", target.Type())
}

func TestCSVTarget_Identifier(t *testing.T) {
	path := "/tmp/output.csv"
	target := NewCSVTarget(path)
	assert.Equal(t, path, target.Identifier())
}

func TestCSVTarget_Close(t *testing.T) {
	target := NewCSVTarget("/tmp/test.csv")
	err := target.Close()
	assert.NoError(t, err)
}

func TestCSVTarget_Write_EmptyResults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.csv")
	target := NewCSVTarget(path)

	err := target.Write(context.Background(), []*model.Record{})
	require.NoError(t, err)

	// File should exist but have no content (no fields to write header for)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "\n", string(data)) // Just the empty header line
}

func TestCSVTarget_Write_SingleRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "single.csv")
	target := NewCSVTarget(path)

	records := []*model.Record{
		{
			ID: "r1",
			Fields: map[string]interface{}{
				"name":  "Alice",
				"score": 95.5,
			},
		},
	}

	err := target.Write(context.Background(), records)
	require.NoError(t, err)

	// Read back and verify
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	require.NoError(t, err)

	// Header + 1 data row
	require.Len(t, rows, 2)
	// Fields are sorted: "name", "score"
	assert.Equal(t, []string{"name", "score"}, rows[0])
	assert.Equal(t, []string{"Alice", "95.5"}, rows[1])
}

func TestCSVTarget_Write_MultipleRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.csv")
	target := NewCSVTarget(path)

	records := []*model.Record{
		{
			ID: "r1",
			Fields: map[string]interface{}{
				"category": "electronics",
				"count":    150,
				"total":    45000.5,
			},
		},
		{
			ID: "r2",
			Fields: map[string]interface{}{
				"category": "clothing",
				"count":    80,
				"total":    12000.0,
			},
		},
	}

	err := target.Write(context.Background(), records)
	require.NoError(t, err)

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	require.NoError(t, err)

	require.Len(t, rows, 3)
	// Fields sorted: "category", "count", "total"
	assert.Equal(t, []string{"category", "count", "total"}, rows[0])
	assert.Equal(t, []string{"electronics", "150", "45000.5"}, rows[1])
	assert.Equal(t, []string{"clothing", "80", "12000"}, rows[2])
}

func TestCSVTarget_Write_OverwritesExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "overwrite.csv")

	// Write initial content
	err := os.WriteFile(path, []byte("old,content\n1,2\n3,4\n"), 0644)
	require.NoError(t, err)

	target := NewCSVTarget(path)
	records := []*model.Record{
		{
			ID:     "r1",
			Fields: map[string]interface{}{"x": "new"},
		},
	}

	err = target.Write(context.Background(), records)
	require.NoError(t, err)

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	require.NoError(t, err)

	require.Len(t, rows, 2)
	assert.Equal(t, []string{"x"}, rows[0])
	assert.Equal(t, []string{"new"}, rows[1])
}

func TestCSVTarget_Write_CancelledContext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cancelled.csv")
	target := NewCSVTarget(path)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := target.Write(ctx, []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"a": "1"}},
	})
	assert.Error(t, err)
}

func TestCSVTarget_Write_SortedHeaders(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sorted.csv")
	target := NewCSVTarget(path)

	records := []*model.Record{
		{
			ID: "r1",
			Fields: map[string]interface{}{
				"zebra":    1,
				"alpha":    2,
				"middle":   3,
			},
		},
	}

	err := target.Write(context.Background(), records)
	require.NoError(t, err)

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	require.NoError(t, err)

	// Verify alphabetical sorting
	assert.Equal(t, []string{"alpha", "middle", "zebra"}, rows[0])
}

func TestCSVTarget_ImplementsExportTarget(t *testing.T) {
	var _ ExportTarget = (*CSVTarget)(nil)
}
