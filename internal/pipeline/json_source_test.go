package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestJSONSource_Type(t *testing.T) {
	src := NewJSONSource("/path/to/file.json")
	assert.Equal(t, "json", src.Type())
}

func TestJSONSource_Identifier(t *testing.T) {
	src := NewJSONSource("/path/to/file.json")
	assert.Equal(t, "/path/to/file.json", src.Identifier())
}

func TestJSONSource_ReadArray(t *testing.T) {
	// Create a temp file with JSON array content
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.json")
	content := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35}
	]`
	err := os.WriteFile(filePath, []byte(content), 0644)
	assert.NoError(t, err)

	src := NewJSONSource(filePath)
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 3)

	// Verify first record
	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, "30", records[0].Fields["age"])
	assert.Equal(t, "json", records[0].Metadata.SourceType)
	assert.Equal(t, filePath, records[0].Metadata.SourceID)
	assert.Equal(t, 1, records[0].Metadata.LineNumber)

	// Verify second record
	assert.Equal(t, "Bob", records[1].Fields["name"])
	assert.Equal(t, "25", records[1].Fields["age"])
	assert.Equal(t, 2, records[1].Metadata.LineNumber)

	// Verify third record
	assert.Equal(t, "Charlie", records[2].Fields["name"])
	assert.Equal(t, "35", records[2].Fields["age"])
	assert.Equal(t, 3, records[2].Metadata.LineNumber)

	// Verify unique IDs
	assert.NotEqual(t, records[0].ID, records[1].ID)
	assert.NotEqual(t, records[1].ID, records[2].ID)
}

func TestJSONSource_ReadNDJSON(t *testing.T) {
	// Create a temp file with newline-delimited JSON
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.ndjson")
	content := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}`
	err := os.WriteFile(filePath, []byte(content), 0644)
	assert.NoError(t, err)

	src := NewJSONSource(filePath)
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 3)

	// Verify records
	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, "30", records[0].Fields["age"])
	assert.Equal(t, "json", records[0].Metadata.SourceType)
	assert.Equal(t, filePath, records[0].Metadata.SourceID)
	assert.Equal(t, 1, records[0].Metadata.LineNumber)

	assert.Equal(t, "Bob", records[1].Fields["name"])
	assert.Equal(t, 2, records[1].Metadata.LineNumber)

	assert.Equal(t, "Charlie", records[2].Fields["name"])
	assert.Equal(t, 3, records[2].Metadata.LineNumber)
}

func TestJSONSource_ReadEmptyArray(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "empty.json")
	err := os.WriteFile(filePath, []byte("[]"), 0644)
	assert.NoError(t, err)

	src := NewJSONSource(filePath)
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 0)
}

func TestJSONSource_ReadNDJSONWithEmptyLines(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.ndjson")
	content := `{"name": "Alice"}

{"name": "Bob"}
`
	err := os.WriteFile(filePath, []byte(content), 0644)
	assert.NoError(t, err)

	src := NewJSONSource(filePath)
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 2)
	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, "Bob", records[1].Fields["name"])
}

func TestJSONSource_ReadFileNotFound(t *testing.T) {
	src := NewJSONSource("/nonexistent/path/file.json")
	out := make(chan *model.Record, 10)

	err := src.Read(context.Background(), out)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open JSON file")
}

func TestJSONSource_ReadCancellation(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.json")
	content := `[{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]`
	err := os.WriteFile(filePath, []byte(content), 0644)
	assert.NoError(t, err)

	src := NewJSONSource(filePath)
	out := make(chan *model.Record, 1) // Small buffer to test blocking

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = src.Read(ctx, out)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestJSONSource_SingleObject(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "single.json")
	content := `[{"name": "Alice", "email": "alice@example.com", "active": true}]`
	err := os.WriteFile(filePath, []byte(content), 0644)
	assert.NoError(t, err)

	src := NewJSONSource(filePath)
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 1)
	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, "alice@example.com", records[0].Fields["email"])
	assert.Equal(t, "true", records[0].Fields["active"])
}

func TestJSONSource_ImplementsSourceInterface(t *testing.T) {
	var _ Source = (*JSONSource)(nil)
}
