//go:build integration

package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EndToEnd_CSVAndJSON tests a full pipeline run with 10+ records
// from CSV and JSON sources, verifying aggregated output values (count, sum, average).
func TestIntegration_EndToEnd_CSVAndJSON(t *testing.T) {
	// Create a temp directory for test files
	tmpDir := t.TempDir()

	// Create CSV source file with 6 records
	csvPath := filepath.Join(tmpDir, "input.csv")
	csvContent := `name,category,amount
Alice,electronics,100.50
Bob,clothing,200.00
Charlie,electronics,300.00
Diana,food,150.25
Eve,clothing,50.75
Frank,electronics,400.00
`
	require.NoError(t, os.WriteFile(csvPath, []byte(csvContent), 0644))

	// Create JSON source file with 5 records
	jsonPath := filepath.Join(tmpDir, "input.json")
	jsonRecords := []map[string]interface{}{
		{"name": "Grace", "category": "food", "amount": 75.50},
		{"name": "Henry", "category": "electronics", "amount": 250.00},
		{"name": "Iris", "category": "clothing", "amount": 125.00},
		{"name": "Jack", "category": "food", "amount": 200.00},
		{"name": "Kate", "category": "electronics", "amount": 180.00},
	}
	jsonData, err := json.Marshal(jsonRecords)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(jsonPath, jsonData, 0644))

	// Export output file
	outputPath := filepath.Join(tmpDir, "output.json")

	// Set up stores
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	// Create job config with aggregation by category
	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: csvPath},
			{Type: "json", Path: jsonPath},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "amount", Type: "number", Required: true},
				{Name: "category", Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{
			{Field: "amount", Operation: "type_convert", TargetType: "number"},
		},
		Aggregation: model.AggregationConfig{
			GroupBy: []string{"category"},
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "total_count"},
				{Name: "sum", Field: "amount", Alias: "total_amount"},
				{Name: "average", Field: "amount", Alias: "avg_amount"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: outputPath},
		},
		WorkerPools: model.WorkerPoolConfig{
			Validator:   2,
			Transformer: 2,
		},
	}

	// Create the job
	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// Build sources
	sources := []Source{
		&CSVSource{FilePath: csvPath, JobID: job.ID, ErrorStore: errStore},
		NewJSONSource(jsonPath),
	}

	// Build export target
	targets := []export.ExportTarget{
		export.NewJSONTarget(outputPath),
	}

	// Create and run pipeline
	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)
	err = p.Run(context.Background())
	require.NoError(t, err)

	// Verify job completed
	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	// Read and verify the output
	outputData, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))

	// We expect 3 groups: electronics, clothing, food
	assert.Len(t, results, 3)

	// Build a map of category -> result for easy lookup
	resultMap := make(map[string]map[string]interface{})
	for _, r := range results {
		cat, ok := r["category"].(string)
		require.True(t, ok, "category should be a string")
		resultMap[cat] = r
	}

	// Verify electronics: Alice(100.50) + Charlie(300) + Frank(400) + Henry(250) + Kate(180) = 5 records, sum=1230.50, avg=246.10
	elec, ok := resultMap["electronics"]
	require.True(t, ok, "electronics group should exist")
	assert.Equal(t, float64(5), elec["total_count"])
	assert.InDelta(t, 1230.50, elec["total_amount"], 0.01)
	assert.InDelta(t, 246.10, elec["avg_amount"], 0.01)

	// Verify clothing: Bob(200) + Eve(50.75) + Iris(125) = 3 records, sum=375.75, avg=125.25
	cloth, ok := resultMap["clothing"]
	require.True(t, ok, "clothing group should exist")
	assert.Equal(t, float64(3), cloth["total_count"])
	assert.InDelta(t, 375.75, cloth["total_amount"], 0.01)
	assert.InDelta(t, 125.25, cloth["avg_amount"], 0.01)

	// Verify food: Diana(150.25) + Grace(75.50) + Jack(200) = 3 records, sum=425.75, avg=141.9167
	food, ok := resultMap["food"]
	require.True(t, ok, "food group should exist")
	assert.Equal(t, float64(3), food["total_count"])
	assert.InDelta(t, 425.75, food["total_amount"], 0.01)
	assert.InDelta(t, 141.9167, food["avg_amount"], 0.01)
}

// TestIntegration_Cancellation_StopsWithin5Seconds tests that cancellation stops
// pipeline processing within 5 seconds.
func TestIntegration_Cancellation_StopsWithin5Seconds(t *testing.T) {
	// Set up stores
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	// Create job config
	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "dummy.csv"},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{},
		},
		Transformations: []model.TransformConfig{},
		Aggregation: model.AggregationConfig{
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "total"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: "dummy_output.json"},
		},
		WorkerPools: model.WorkerPoolConfig{
			Validator:   1,
			Transformer: 1,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// Use a blocking source that never produces records
	slowSource := &blockingSource{}
	target := &pipelineMockExportTarget{}

	p := NewPipeline(job, jobStore, []Source{slowSource}, []export.ExportTarget{target}, errStore, progress)

	// Create context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	startTime := time.Now()

	// Run pipeline in background
	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()

	// Allow pipeline to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the pipeline
	cancel()

	// Wait for pipeline to finish
	pipelineErr := <-done
	elapsed := time.Since(startTime)

	// Verify it stopped within 5 seconds
	assert.Less(t, elapsed, 5*time.Second, "pipeline should stop within 5 seconds after cancellation")
	assert.Error(t, pipelineErr)

	// Verify job status is cancelled
	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCancelled, updatedJob.Status)
}

// TestIntegration_MultiSource_MergesCorrectly tests that records from multiple
// sources (CSV and JSON) are all merged and processed correctly.
func TestIntegration_MultiSource_MergesCorrectly(t *testing.T) {
	tmpDir := t.TempDir()

	// Create CSV source with 4 records
	csvPath := filepath.Join(tmpDir, "source1.csv")
	csvContent := `id,value
csv1,10
csv2,20
csv3,30
csv4,40
`
	require.NoError(t, os.WriteFile(csvPath, []byte(csvContent), 0644))

	// Create JSON source with 4 records
	jsonPath := filepath.Join(tmpDir, "source2.json")
	jsonRecords := []map[string]interface{}{
		{"id": "json1", "value": 50.0},
		{"id": "json2", "value": 60.0},
		{"id": "json3", "value": 70.0},
		{"id": "json4", "value": 80.0},
	}
	jsonData, err := json.Marshal(jsonRecords)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(jsonPath, jsonData, 0644))

	// Output file
	outputPath := filepath.Join(tmpDir, "output.json")

	// Set up stores
	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	// No validation (accept all), no transformations, aggregate all records together
	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: csvPath},
			{Type: "json", Path: jsonPath},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{},
		},
		Transformations: []model.TransformConfig{},
		Aggregation: model.AggregationConfig{
			// No group-by: aggregate all records into a single group
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "total_count"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: outputPath},
		},
		WorkerPools: model.WorkerPoolConfig{
			Validator:   1,
			Transformer: 1,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// Build sources
	sources := []Source{
		&CSVSource{FilePath: csvPath, JobID: job.ID, ErrorStore: errStore},
		NewJSONSource(jsonPath),
	}

	targets := []export.ExportTarget{
		export.NewJSONTarget(outputPath),
	}

	// Run the pipeline
	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)
	err = p.Run(context.Background())
	require.NoError(t, err)

	// Verify job completed
	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	// Read output and verify total count = 8 (4 from CSV + 4 from JSON)
	outputData, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))

	// Since no group-by, we should have a single aggregation result
	require.Len(t, results, 1)
	assert.Equal(t, float64(8), results[0]["total_count"],
		"all 8 records from both sources should be counted")
}
