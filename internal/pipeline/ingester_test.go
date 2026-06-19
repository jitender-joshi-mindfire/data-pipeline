package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- CSV Source Unit Tests ---
// Validates: Requirements 15.1, 15.7

func TestCSVSource_SingleRow(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "single.csv")
	content := "name,age,email\nAlice,30,alice@example.com\n"
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	src := &CSVSource{FilePath: filePath, JobID: "job-1", ErrorStore: store.NewInMemoryErrorStore()}
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
	assert.Equal(t, "30", records[0].Fields["age"])
	assert.Equal(t, "alice@example.com", records[0].Fields["email"])
	assert.Equal(t, "csv", records[0].Metadata.SourceType)
	assert.Equal(t, filePath, records[0].Metadata.SourceID)
	assert.Equal(t, 2, records[0].Metadata.LineNumber)
}

func TestCSVSource_MultiRow(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "multi.csv")
	content := "id,name,score\n1,Alice,95\n2,Bob,87\n3,Charlie,92\n4,Diana,88\n"
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	src := &CSVSource{FilePath: filePath, JobID: "job-1", ErrorStore: store.NewInMemoryErrorStore()}
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 4)

	// Verify each record has correct fields
	assert.Equal(t, "1", records[0].Fields["id"])
	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, "95", records[0].Fields["score"])
	assert.Equal(t, 2, records[0].Metadata.LineNumber)

	assert.Equal(t, "2", records[1].Fields["id"])
	assert.Equal(t, "Bob", records[1].Fields["name"])
	assert.Equal(t, "87", records[1].Fields["score"])
	assert.Equal(t, 3, records[1].Metadata.LineNumber)

	assert.Equal(t, "3", records[2].Fields["id"])
	assert.Equal(t, "Charlie", records[2].Fields["name"])
	assert.Equal(t, "92", records[2].Fields["score"])
	assert.Equal(t, 4, records[2].Metadata.LineNumber)

	assert.Equal(t, "4", records[3].Fields["id"])
	assert.Equal(t, "Diana", records[3].Fields["name"])
	assert.Equal(t, "88", records[3].Fields["score"])
	assert.Equal(t, 5, records[3].Metadata.LineNumber)

	// All records should have unique IDs
	ids := make(map[string]bool)
	for _, r := range records {
		assert.NotEmpty(t, r.ID)
		assert.False(t, ids[r.ID], "duplicate record ID found")
		ids[r.ID] = true
	}
}

func TestCSVSource_QuotedFieldsWithCommas(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "quoted.csv")
	// CSV with quoted fields that contain commas
	content := `name,address,note
"Smith, John","123 Main St, Suite 4","Has comma, in value"
"Doe, Jane","456 Oak Ave, Apt 2","Another, comma"
`
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	src := &CSVSource{FilePath: filePath, JobID: "job-1", ErrorStore: store.NewInMemoryErrorStore()}
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}

	assert.Len(t, records, 2)

	// Verify quoted fields with commas are parsed correctly
	assert.Equal(t, "Smith, John", records[0].Fields["name"])
	assert.Equal(t, "123 Main St, Suite 4", records[0].Fields["address"])
	assert.Equal(t, "Has comma, in value", records[0].Fields["note"])

	assert.Equal(t, "Doe, Jane", records[1].Fields["name"])
	assert.Equal(t, "456 Oak Ave, Apt 2", records[1].Fields["address"])
	assert.Equal(t, "Another, comma", records[1].Fields["note"])
}

func TestCSVSource_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "empty.csv")
	err := os.WriteFile(filePath, []byte(""), 0644)
	require.NoError(t, err)

	src := &CSVSource{FilePath: filePath, JobID: "job-1", ErrorStore: store.NewInMemoryErrorStore()}
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

func TestCSVSource_HeaderOnlyNoData(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "headeronly.csv")
	content := "name,age,email\n"
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	src := &CSVSource{FilePath: filePath, JobID: "job-1", ErrorStore: store.NewInMemoryErrorStore()}
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

func TestCSVSource_Metadata(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "meta.csv")
	content := "x\n1\n"
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

	src := &CSVSource{FilePath: filePath, JobID: "job-1", ErrorStore: store.NewInMemoryErrorStore()}
	out := make(chan *model.Record, 10)

	err = src.Read(context.Background(), out)
	assert.NoError(t, err)
	close(out)

	r := <-out
	assert.Equal(t, "csv", r.Metadata.SourceType)
	assert.Equal(t, filePath, r.Metadata.SourceID)
	assert.Equal(t, 2, r.Metadata.LineNumber)
}

func TestCSVSource_FileNotFound(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	src := &CSVSource{FilePath: "/nonexistent/path/missing.csv", JobID: "job-1", ErrorStore: errStore}
	out := make(chan *model.Record, 10)

	err := src.Read(context.Background(), out)
	assert.Error(t, err)

	// Error should be logged to ErrorStore
	errs, total := errStore.GetByJob("job-1", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "failed to open CSV source")
	assert.Equal(t, "ingester", errs[0].Stage)
}

// --- JSON Source Unit Tests ---
// Validates: Requirements 15.1

func TestJSONSource_SingleObjectInArray(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "single.json")
	content := `[{"name": "Alice", "age": 30, "active": true}]`
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

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
	assert.Equal(t, "30", records[0].Fields["age"])
	assert.Equal(t, "true", records[0].Fields["active"])
	assert.Equal(t, "json", records[0].Metadata.SourceType)
	assert.Equal(t, filePath, records[0].Metadata.SourceID)
	assert.Equal(t, 1, records[0].Metadata.LineNumber)
}

func TestJSONSource_MultiObjectArray(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "multi.json")
	content := `[
		{"id": 1, "name": "Alice", "score": 95.5},
		{"id": 2, "name": "Bob", "score": 87.3},
		{"id": 3, "name": "Charlie", "score": 91.0}
	]`
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)

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

	assert.Equal(t, "1", records[0].Fields["id"])
	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, "95.5", records[0].Fields["score"])
	assert.Equal(t, 1, records[0].Metadata.LineNumber)

	assert.Equal(t, "2", records[1].Fields["id"])
	assert.Equal(t, "Bob", records[1].Fields["name"])
	assert.Equal(t, "87.3", records[1].Fields["score"])
	assert.Equal(t, 2, records[1].Metadata.LineNumber)

	assert.Equal(t, "3", records[2].Fields["id"])
	assert.Equal(t, "Charlie", records[2].Fields["name"])
	assert.Equal(t, "91", records[2].Fields["score"])
	assert.Equal(t, 3, records[2].Metadata.LineNumber)

	// Verify unique IDs
	ids := make(map[string]bool)
	for _, r := range records {
		assert.NotEmpty(t, r.ID)
		assert.False(t, ids[r.ID], "duplicate record ID")
		ids[r.ID] = true
	}
}

func TestJSONSource_EmptyArray(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "empty.json")
	err := os.WriteFile(filePath, []byte("[]"), 0644)
	require.NoError(t, err)

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

// --- Error Handling Tests ---
// Validates: Requirements 15.7

func TestIngester_UnreachableSourceLogged(t *testing.T) {
	// Test that when a source is unreachable, the error is logged to ErrorStore
	errStore := store.NewInMemoryErrorStore()

	// Use a CSV source with a non-existent file path
	unreachableSrc := &CSVSource{
		FilePath:   "/nonexistent/path/data.csv",
		JobID:      "job-err",
		ErrorStore: errStore,
	}

	ing := NewIngester([]Source{unreachableSrc}, errStore, "job-err")
	out := make(chan *model.Record, 100)

	err := ing.Run(context.Background(), nil, out)
	assert.NoError(t, err) // Run itself should not error - the error is logged

	// Channel should be closed with no records
	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}
	assert.Empty(t, records)

	// Error should be logged to ErrorStore
	errs, total := errStore.GetByJob("job-err", 0, 50)
	assert.Greater(t, total, 0)
	assert.Greater(t, len(errs), 0)

	// Verify the error references the source
	foundSourceError := false
	for _, e := range errs {
		if e.Stage == "ingester" {
			foundSourceError = true
			break
		}
	}
	assert.True(t, foundSourceError, "expected an ingester error logged for unreachable source")
}

func TestIngester_RemainingSourcesContinueAfterFailure(t *testing.T) {
	// Test that remaining sources continue processing when one fails
	dir := t.TempDir()

	// Create a valid CSV file
	validPath := filepath.Join(dir, "valid.csv")
	content := "name,value\nAlice,100\nBob,200\n"
	err := os.WriteFile(validPath, []byte(content), 0644)
	require.NoError(t, err)

	errStore := store.NewInMemoryErrorStore()

	// One unreachable source and one valid source
	badSrc := &CSVSource{FilePath: "/nonexistent/bad.csv", JobID: "job-mix", ErrorStore: errStore}
	goodSrc := &CSVSource{FilePath: validPath, JobID: "job-mix", ErrorStore: errStore}

	ing := NewIngester([]Source{badSrc, goodSrc}, errStore, "job-mix")
	out := make(chan *model.Record, 100)

	err = ing.Run(context.Background(), nil, out)
	assert.NoError(t, err)

	// Records from the good source should still be received
	var records []*model.Record
	for r := range out {
		records = append(records, r)
	}
	assert.Len(t, records, 2)
	assert.Equal(t, "Alice", records[0].Fields["name"])
	assert.Equal(t, "Bob", records[1].Fields["name"])

	// The bad source error should be logged
	errs, total := errStore.GetByJob("job-mix", 0, 50)
	assert.Greater(t, total, 0)
	foundBadSource := false
	for _, e := range errs {
		if e.Stage == "ingester" {
			foundBadSource = true
			break
		}
	}
	assert.True(t, foundBadSource, "expected error logged for the unreachable source")
}

func TestIngester_MultipleFailedSourcesAllLogged(t *testing.T) {
	// Test that multiple failing sources are all logged independently
	errStore := store.NewInMemoryErrorStore()

	badSrc1 := &mockSource{id: "bad1.csv", srcType: "csv", records: nil, err: errors.New("connection refused")}
	badSrc2 := &mockSource{id: "bad2.json", srcType: "json", records: nil, err: errors.New("timeout")}

	records := makeRecords(2, "csv", "good.csv")
	goodSrc := &mockSource{id: "good.csv", srcType: "csv", records: records}

	ing := NewIngester([]Source{badSrc1, badSrc2, goodSrc}, errStore, "job-multi-err")
	out := make(chan *model.Record, 100)

	err := ing.Run(context.Background(), nil, out)
	assert.NoError(t, err)

	var received []*model.Record
	for r := range out {
		received = append(received, r)
	}

	// Good source records are received
	assert.Len(t, received, 2)

	// Both bad sources should have errors logged
	errs, total := errStore.GetByJob("job-multi-err", 0, 50)
	assert.Equal(t, 2, total)
	assert.Len(t, errs, 2)

	// Verify both source errors are present
	messages := make([]string, len(errs))
	for i, e := range errs {
		messages[i] = e.Message
	}
	assert.Contains(t, joinMessages(messages), "bad1.csv")
	assert.Contains(t, joinMessages(messages), "bad2.json")
}

// joinMessages concatenates error messages for assertion convenience.
func joinMessages(msgs []string) string {
	result := ""
	for _, m := range msgs {
		result += m + " "
	}
	return result
}
