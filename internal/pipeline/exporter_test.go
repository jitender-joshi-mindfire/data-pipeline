package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockExportTarget is a test double for export.ExportTarget.
type mockExportTarget struct {
	mu         sync.Mutex
	written    []*model.Record
	identifier string
	targetType string
	writeErr   error
}

func newMockTarget(identifier, targetType string) *mockExportTarget {
	return &mockExportTarget{
		identifier: identifier,
		targetType: targetType,
	}
}

func (m *mockExportTarget) Write(ctx context.Context, results []*model.Record) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.written = append(m.written, results...)
	return nil
}

func (m *mockExportTarget) Type() string {
	return m.targetType
}

func (m *mockExportTarget) Identifier() string {
	return m.identifier
}

func (m *mockExportTarget) Close() error {
	return nil
}

// Compile-time check that mockExportTarget implements ExportTarget.
var _ export.ExportTarget = (*mockExportTarget)(nil)

func TestExporterStage_Name(t *testing.T) {
	stage := NewExporterStage("job-1", nil, nil)
	assert.Equal(t, "exporter", stage.Name())
}

func TestExporterStage_WritesToAllTargets(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	target1 := newMockTarget("/tmp/out1.csv", "csv")
	target2 := newMockTarget("/tmp/out2.json", "json")

	targets := []export.ExportTarget{target1, target2}
	stage := NewExporterStage("job-1", targets, errStore)

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"name": "Alice", "score": 90.0}},
		{ID: "r2", Fields: map[string]interface{}{"name": "Bob", "score": 85.0}},
	}

	in := make(chan *model.Record, len(records))
	out := make(chan *model.Record)
	for _, r := range records {
		in <- r
	}
	close(in)

	err := stage.Run(context.Background(), in, out)
	require.NoError(t, err)

	// Both targets should have received all records
	assert.Len(t, target1.written, 2)
	assert.Len(t, target2.written, 2)

	// No errors should have been logged
	errors, total := errStore.GetByJob("job-1", 0, 50)
	assert.Equal(t, 0, total)
	assert.Empty(t, errors)
}

func TestExporterStage_FailureInOneTargetDoesNotPreventOthers(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	target1 := newMockTarget("/tmp/out1.csv", "csv")
	target1.writeErr = errors.New("disk full")

	target2 := newMockTarget("/tmp/out2.json", "json")
	target3 := newMockTarget("/tmp/out3.db", "sqlite")

	targets := []export.ExportTarget{target1, target2, target3}
	stage := NewExporterStage("job-1", targets, errStore)

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"name": "Alice"}},
	}

	in := make(chan *model.Record, len(records))
	out := make(chan *model.Record)
	for _, r := range records {
		in <- r
	}
	close(in)

	err := stage.Run(context.Background(), in, out)
	require.NoError(t, err)

	// target1 failed, but target2 and target3 should still receive records
	assert.Empty(t, target1.written)
	assert.Len(t, target2.written, 1)
	assert.Len(t, target3.written, 1)
}

func TestExporterStage_ErrorsLoggedToErrorStore(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	target1 := newMockTarget("/tmp/failing.csv", "csv")
	target1.writeErr = errors.New("permission denied")

	targets := []export.ExportTarget{target1}
	stage := NewExporterStage("job-42", targets, errStore)

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"x": 1}},
	}

	in := make(chan *model.Record, len(records))
	out := make(chan *model.Record)
	for _, r := range records {
		in <- r
	}
	close(in)

	err := stage.Run(context.Background(), in, out)
	require.NoError(t, err)

	// Verify error was logged
	entries, total := errStore.GetByJob("job-42", 0, 50)
	assert.Equal(t, 1, total)
	require.Len(t, entries, 1)
	assert.Equal(t, "exporter", entries[0].Stage)
	assert.Contains(t, entries[0].Message, "/tmp/failing.csv")
	assert.Contains(t, entries[0].Message, "permission denied")
}

func TestExporterStage_EmptyInput(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	target := newMockTarget("/tmp/out.csv", "csv")

	targets := []export.ExportTarget{target}
	stage := NewExporterStage("job-1", targets, errStore)

	in := make(chan *model.Record)
	out := make(chan *model.Record)
	close(in)

	err := stage.Run(context.Background(), in, out)
	require.NoError(t, err)

	// Target receives an empty slice (still called with empty records)
	assert.Empty(t, target.written)
}

func TestExporterStage_ContextCancellation(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	target := newMockTarget("/tmp/out.csv", "csv")

	targets := []export.ExportTarget{target}
	stage := NewExporterStage("job-1", targets, errStore)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	in := make(chan *model.Record)
	out := make(chan *model.Record)

	// Since context is already cancelled, Run should return ctx.Err()
	err := stage.Run(ctx, in, out)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestExporterStage_MultipleTargetFailures(t *testing.T) {
	errStore := store.NewInMemoryErrorStore()
	target1 := newMockTarget("/tmp/fail1.csv", "csv")
	target1.writeErr = errors.New("error 1")

	target2 := newMockTarget("/tmp/fail2.json", "json")
	target2.writeErr = errors.New("error 2")

	target3 := newMockTarget("/tmp/success.db", "sqlite")

	targets := []export.ExportTarget{target1, target2, target3}
	stage := NewExporterStage("job-1", targets, errStore)

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"val": 42}},
	}

	in := make(chan *model.Record, len(records))
	out := make(chan *model.Record)
	for _, r := range records {
		in <- r
	}
	close(in)

	err := stage.Run(context.Background(), in, out)
	require.NoError(t, err)

	// Only target3 should succeed
	assert.Empty(t, target1.written)
	assert.Empty(t, target2.written)
	assert.Len(t, target3.written, 1)

	// Two errors should be logged
	entries, total := errStore.GetByJob("job-1", 0, 50)
	assert.Equal(t, 2, total)
	require.Len(t, entries, 2)
	assert.Contains(t, entries[0].Message, "/tmp/fail1.csv")
	assert.Contains(t, entries[1].Message, "/tmp/fail2.json")
}
