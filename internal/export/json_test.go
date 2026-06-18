package export

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestJSONTarget_Write(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "output.json")
	target := NewJSONTarget(filePath)

	records := []*model.Record{
		{
			ID:     "r1",
			Fields: map[string]interface{}{"category": "electronics", "total": float64(100)},
		},
		{
			ID:     "r2",
			Fields: map[string]interface{}{"category": "clothing", "total": float64(50)},
		},
	}

	err := target.Write(context.Background(), records)
	assert.NoError(t, err)

	// Read back and verify
	data, err := os.ReadFile(filePath)
	assert.NoError(t, err)

	var result []map[string]interface{}
	err = json.Unmarshal(data, &result)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "electronics", result[0]["category"])
	assert.Equal(t, float64(100), result[0]["total"])
	assert.Equal(t, "clothing", result[1]["category"])
	assert.Equal(t, float64(50), result[1]["total"])
}

func TestJSONTarget_WriteOverwritesExistingFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "output.json")

	// Write initial content
	err := os.WriteFile(filePath, []byte(`[{"old": "data"}]`), 0644)
	assert.NoError(t, err)

	target := NewJSONTarget(filePath)
	records := []*model.Record{
		{
			ID:     "r1",
			Fields: map[string]interface{}{"new": "data"},
		},
	}

	err = target.Write(context.Background(), records)
	assert.NoError(t, err)

	data, err := os.ReadFile(filePath)
	assert.NoError(t, err)

	var result []map[string]interface{}
	err = json.Unmarshal(data, &result)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "data", result[0]["new"])
	// Old data should not be present
	assert.Nil(t, result[0]["old"])
}

func TestJSONTarget_WriteEmptyResults(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "output.json")
	target := NewJSONTarget(filePath)

	err := target.Write(context.Background(), []*model.Record{})
	assert.NoError(t, err)

	data, err := os.ReadFile(filePath)
	assert.NoError(t, err)

	var result []map[string]interface{}
	err = json.Unmarshal(data, &result)
	assert.NoError(t, err)
	assert.Len(t, result, 0)
}

func TestJSONTarget_Type(t *testing.T) {
	target := NewJSONTarget("/tmp/test.json")
	assert.Equal(t, "json", target.Type())
}

func TestJSONTarget_Identifier(t *testing.T) {
	target := NewJSONTarget("/tmp/test.json")
	assert.Equal(t, "/tmp/test.json", target.Identifier())
}

func TestJSONTarget_Close(t *testing.T) {
	target := NewJSONTarget("/tmp/test.json")
	err := target.Close()
	assert.NoError(t, err)
}

func TestJSONTarget_ImplementsExportTarget(t *testing.T) {
	// Compile-time check that JSONTarget implements ExportTarget
	var _ ExportTarget = (*JSONTarget)(nil)
}
