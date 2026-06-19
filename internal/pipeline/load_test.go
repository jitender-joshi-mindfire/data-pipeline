//go:build load

// Package pipeline contains load/stress tests for the data processing pipeline.
// Run with: go test -tags load -timeout 300s -v ./internal/pipeline/ -run TestLoad
package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var loadTestCategories = []string{"electronics", "clothing", "food", "furniture", "automotive", "sports", "books", "toys", "health", "garden"}

// generateLargeCSV creates a CSV file with the specified number of rows.
// ~5% of records are intentionally invalid to exercise validation.
func generateLargeCSV(t *testing.T, path string, rows int) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	writer.Write([]string{"name", "email", "amount", "date", "category", "quantity", "rating"})

	faker := gofakeit.New(42)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < rows; i++ {
		name := faker.Name()
		email := faker.Email()
		amount := fmt.Sprintf("%.2f", float64(rng.Intn(100000))/100.0)
		date := time.Now().AddDate(0, 0, -rng.Intn(730)).Format(time.RFC3339)
		category := loadTestCategories[rng.Intn(len(loadTestCategories))]
		quantity := fmt.Sprintf("%d", rng.Intn(100)+1)
		rating := fmt.Sprintf("%.1f", float64(rng.Intn(50)+1)/10.0)

		// Corrupt ~5% of records
		if rng.Float64() < 0.05 {
			switch rng.Intn(4) {
			case 0:
				name = ""
			case 1:
				email = "invalid"
			case 2:
				amount = "-999.99"
			case 3:
				category = ""
			}
		}

		writer.Write([]string{name, email, amount, date, category, quantity, rating})
	}
}

// generateLargeJSON creates a JSON file with the specified number of records.
func generateLargeJSON(t *testing.T, path string, rows int) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	faker := gofakeit.New(99)
	rng := rand.New(rand.NewSource(99))

	records := make([]map[string]interface{}, 0, rows)
	for i := 0; i < rows; i++ {
		rec := map[string]interface{}{
			"name":     faker.Name(),
			"email":    faker.Email(),
			"amount":   float64(rng.Intn(100000)) / 100.0,
			"date":     time.Now().AddDate(0, 0, -rng.Intn(730)).Format(time.RFC3339),
			"category": loadTestCategories[rng.Intn(len(loadTestCategories))],
			"quantity": rng.Intn(100) + 1,
			"rating":   float64(rng.Intn(50)+1) / 10.0,
		}

		// Corrupt ~5% of records
		if rng.Float64() < 0.05 {
			switch rng.Intn(4) {
			case 0:
				delete(rec, "name")
			case 1:
				rec["email"] = "bad-email"
			case 2:
				rec["amount"] = "not_a_number"
			case 3:
				rec["category"] = nil
			}
		}

		records = append(records, rec)
	}

	encoder := json.NewEncoder(f)
	require.NoError(t, encoder.Encode(records))
}

// TestLoad_10K_CSV_Pipeline runs the full pipeline on 10K CSV records.
func TestLoad_10K_CSV_Pipeline(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "input_10k.csv")
	outputPath := filepath.Join(tmpDir, "output.json")

	generateLargeCSV(t, csvPath, 10000)

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	minAmount := float64(0)
	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: csvPath},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "amount", Type: "number", Required: true, Min: &minAmount},
				{Name: "category", Type: "string", Required: true, Pattern: "^.+$"},
				{Name: "name", Type: "string", Required: true, Pattern: "^.+$"},
				{Name: "email", Type: "string", Required: true, Pattern: "^[\\w.]+@[\\w.]+$"},
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
			Validator:   4,
			Transformer: 4,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{
		&CSVSource{FilePath: csvPath, JobID: job.ID, ErrorStore: errStore},
	}
	targets := []export.ExportTarget{export.NewJSONTarget(outputPath)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)

	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	t.Logf("10K CSV pipeline completed in %v", elapsed)
	t.Logf("Throughput: %.0f records/sec", 10000.0/elapsed.Seconds())

	// Verify job completed
	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	// Verify output exists and has aggregation results
	outputData, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))
	assert.Greater(t, len(results), 0, "should produce aggregated results")
	t.Logf("Produced %d aggregation groups", len(results))

	// Verify some errors were captured (from the ~5% invalid records)
	errors, total := errStore.GetByJob(job.ID, 0, 200)
	t.Logf("Captured %d errors (showing first %d)", total, len(errors))
	assert.Greater(t, total, 0, "should capture validation errors from invalid records")
}

// TestLoad_10K_JSON_Pipeline runs the full pipeline on 10K JSON records.
func TestLoad_10K_JSON_Pipeline(t *testing.T) {
	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "input_10k.json")
	outputPath := filepath.Join(tmpDir, "output.json")

	generateLargeJSON(t, jsonPath, 10000)

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "json", Path: jsonPath},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "amount", Type: "number", Required: true},
				{Name: "category", Type: "string", Required: true},
				{Name: "name", Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{},
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
			Validator:   4,
			Transformer: 4,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{NewJSONSource(jsonPath)}
	targets := []export.ExportTarget{export.NewJSONTarget(outputPath)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)

	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	t.Logf("10K JSON pipeline completed in %v", elapsed)
	t.Logf("Throughput: %.0f records/sec", 10000.0/elapsed.Seconds())

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	outputData, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))
	assert.Greater(t, len(results), 0)
	t.Logf("Produced %d aggregation groups", len(results))
}

// TestLoad_MultiSource_CSV_JSON_20K runs the pipeline with combined 20K records
// from multiple sources (10K CSV + 10K JSON) to stress multi-source ingestion.
func TestLoad_MultiSource_CSV_JSON_20K(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "input_10k.csv")
	jsonPath := filepath.Join(tmpDir, "input_10k.json")
	outputPath := filepath.Join(tmpDir, "output.json")

	generateLargeCSV(t, csvPath, 10000)
	generateLargeJSON(t, jsonPath, 10000)

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

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
			Validator:   8,
			Transformer: 8,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{
		&CSVSource{FilePath: csvPath, JobID: job.ID, ErrorStore: errStore},
		NewJSONSource(jsonPath),
	}
	targets := []export.ExportTarget{export.NewJSONTarget(outputPath)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)

	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	t.Logf("20K multi-source pipeline completed in %v", elapsed)
	t.Logf("Throughput: %.0f records/sec", 20000.0/elapsed.Seconds())

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	outputData, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))
	// Should have at least 10 category groups (may include empty category from invalid records)
	assert.GreaterOrEqual(t, len(results), 10, "should have at least 10 category groups")
	t.Logf("Produced %d aggregation groups from 20K records", len(results))
}

// TestLoad_HTTPSource_DummyJSON tests ingestion from an external HTTP API.
// Uses a local mock server that simulates the DummyJSON products API response
// structure with 500 products to avoid network dependency in CI.
func TestLoad_HTTPSource_DummyJSON(t *testing.T) {
	// Create a mock server that simulates DummyJSON products API
	products := generateMockProducts(500)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(products)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "http", Path: server.URL, TimeoutSeconds: 30},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "price", Type: "number", Required: true},
				{Name: "category", Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{},
		Aggregation: model.AggregationConfig{
			GroupBy: []string{"category"},
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "product_count"},
				{Name: "sum", Field: "price", Alias: "total_price"},
				{Name: "average", Field: "price", Alias: "avg_price"},
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

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{NewHTTPSource(server.URL, 30)}
	targets := []export.ExportTarget{export.NewJSONTarget(outputPath)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)

	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	t.Logf("HTTP source (500 products) pipeline completed in %v", elapsed)

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	outputData, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))
	assert.Greater(t, len(results), 0)
	t.Logf("Produced %d category groups from HTTP source", len(results))
}

// TestLoad_LiveHTTP_DummyJSON_Products tests against the real DummyJSON API.
// This test requires internet access and is skipped if the API is unreachable.
func TestLoad_LiveHTTP_DummyJSON_Products(t *testing.T) {
	// Check if the API is reachable
	resp, err := http.Get("https://dummyjson.com/products?limit=0&select=title,price,category,rating,stock")
	if err != nil {
		t.Skipf("DummyJSON API unreachable (skipping live HTTP test): %v", err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Skipf("DummyJSON API returned %d (skipping)", resp.StatusCode)
		return
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	// DummyJSON /products?limit=0 returns ~194 products in a wrapper object.
	// Our HTTP source expects a JSON array at the top level.
	// We'll use a proxy server to unwrap the "products" array.
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiResp, err := http.Get("https://dummyjson.com/products?limit=0&select=title,price,category,rating,stock")
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer apiResp.Body.Close()

		var wrapper struct {
			Products []map[string]interface{} `json:"products"`
		}
		if err := json.NewDecoder(apiResp.Body).Decode(&wrapper); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(wrapper.Products)
	}))
	defer proxyServer.Close()

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "http", Path: proxyServer.URL, TimeoutSeconds: 30},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "price", Type: "number", Required: true},
				{Name: "category", Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{},
		Aggregation: model.AggregationConfig{
			GroupBy: []string{"category"},
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "product_count"},
				{Name: "average", Field: "price", Alias: "avg_price"},
				{Name: "sum", Field: "price", Alias: "total_price"},
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

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{NewHTTPSource(proxyServer.URL, 30)}
	targets := []export.ExportTarget{export.NewJSONTarget(outputPath)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)

	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	t.Logf("Live DummyJSON pipeline completed in %v", elapsed)

	outputData, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))
	t.Logf("Produced %d category groups from %d live DummyJSON products", len(results), 194)
	assert.Greater(t, len(results), 5, "DummyJSON has many categories")
}

// TestLoad_50K_HighConcurrency runs a stress test with 50K records and maximum
// concurrency to test pipeline stability under heavy load.
func TestLoad_50K_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 50K load test in short mode")
	}

	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "input_50k.csv")
	outputCSV := filepath.Join(tmpDir, "output.csv")
	outputJSON := filepath.Join(tmpDir, "output.json")

	generateLargeCSV(t, csvPath, 50000)

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: csvPath},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "amount", Type: "number", Required: true},
				{Name: "category", Type: "string", Required: true},
				{Name: "name", Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{
			{Field: "amount", Operation: "type_convert", TargetType: "number"},
			{Field: "name", Operation: "trim"},
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
			{Type: "csv", Path: outputCSV},
			{Type: "json", Path: outputJSON},
		},
		WorkerPools: model.WorkerPoolConfig{
			Validator:   16,
			Transformer: 16,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{
		&CSVSource{FilePath: csvPath, JobID: job.ID, ErrorStore: errStore},
	}
	targets := []export.ExportTarget{
		export.NewCSVTarget(outputCSV),
		export.NewJSONTarget(outputJSON),
	}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)

	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	t.Logf("50K high-concurrency pipeline completed in %v", elapsed)
	t.Logf("Throughput: %.0f records/sec", 50000.0/elapsed.Seconds())

	// Verify both outputs exist
	assert.FileExists(t, outputCSV)
	assert.FileExists(t, outputJSON)

	// Verify JSON output
	outputData, err := os.ReadFile(outputJSON)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(outputData, &results))
	assert.GreaterOrEqual(t, len(results), 10, "should have at least 10 categories")

	// Verify total count adds up to approximately 50K minus invalid records
	var totalCount float64
	for _, r := range results {
		if tc, ok := r["total_count"].(float64); ok {
			totalCount += tc
		}
	}
	t.Logf("Total valid records processed: %.0f (out of 50000)", totalCount)
	assert.Greater(t, totalCount, float64(40000), "most records should pass validation")
}

// TestLoad_CancellationUnderLoad tests that cancellation works correctly
// even when the pipeline is processing a large dataset.
func TestLoad_CancellationUnderLoad(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "slow_source"},
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
			{Type: "json", Path: outputPath},
		},
		WorkerPools: model.WorkerPoolConfig{
			Validator:   1,
			Transformer: 1,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// Use a slow source that emits records with delays
	slowSource := &slowRecordSource{delayPerRecord: 10 * time.Millisecond, totalRecords: 10000}
	targets := []export.ExportTarget{export.NewJSONTarget(outputPath)}

	p := NewPipeline(job, jobStore, []Source{slowSource}, targets, errStore, progress)

	ctx, cancel := context.WithCancel(context.Background())
	startTime := time.Now()

	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()

	// Let it process for 500ms then cancel
	time.Sleep(500 * time.Millisecond)
	cancel()

	pipelineErr := <-done
	elapsed := time.Since(startTime)

	assert.Error(t, pipelineErr)
	assert.Less(t, elapsed, 6*time.Second, "should stop within grace period after cancel")
	t.Logf("Slow pipeline cancelled after %v", elapsed)

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCancelled, updatedJob.Status)
}

// TestLoad_TimeoutUnderLoad tests that timeout correctly stops a long pipeline.
func TestLoad_TimeoutUnderLoad(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	timeout := 1 // 1 second timeout
	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "slow_source"},
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
			{Type: "json", Path: outputPath},
		},
		TimeoutSeconds: &timeout,
		WorkerPools: model.WorkerPoolConfig{
			Validator:   1,
			Transformer: 1,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// Use a slow source that will take longer than the timeout
	slowSource := &slowRecordSource{delayPerRecord: 50 * time.Millisecond, totalRecords: 10000}
	targets := []export.ExportTarget{export.NewJSONTarget(outputPath)}

	p := NewPipeline(job, jobStore, []Source{slowSource}, targets, errStore, progress)

	start := time.Now()
	pipelineErr := p.RunJob()
	elapsed := time.Since(start)

	assert.Error(t, pipelineErr)
	// Should timeout after ~1s + up to 5s grace period
	assert.Less(t, elapsed, 7*time.Second, "should respect timeout + grace period")
	t.Logf("Slow pipeline timed out after %v", elapsed)

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusFailed, updatedJob.Status)
}

// TestLoad_MultiExportTarget tests that multiple export targets all receive
// data correctly under load.
func TestLoad_MultiExportTarget(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "input.csv")
	outCSV := filepath.Join(tmpDir, "results.csv")
	outJSON := filepath.Join(tmpDir, "results.json")
	outSQLite := filepath.Join(tmpDir, "results.db")

	generateLargeCSV(t, csvPath, 5000)

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: csvPath},
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
			},
		},
		Exports: []model.ExportConfig{
			{Type: "csv", Path: outCSV},
			{Type: "json", Path: outJSON},
			{Type: "sqlite", Path: outSQLite, TableName: "results"},
		},
		WorkerPools: model.WorkerPoolConfig{
			Validator:   4,
			Transformer: 4,
		},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{
		&CSVSource{FilePath: csvPath, JobID: job.ID, ErrorStore: errStore},
	}

	sqliteTarget, err := export.NewSQLiteTarget(outSQLite, "results")
	require.NoError(t, err)
	defer sqliteTarget.Close()

	targets := []export.ExportTarget{
		export.NewCSVTarget(outCSV),
		export.NewJSONTarget(outJSON),
		sqliteTarget,
	}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)
	err = p.Run(context.Background())
	require.NoError(t, err)

	// Verify all three output files exist and have content
	assert.FileExists(t, outCSV)
	assert.FileExists(t, outJSON)
	assert.FileExists(t, outSQLite)

	csvInfo, _ := os.Stat(outCSV)
	jsonInfo, _ := os.Stat(outJSON)
	sqliteInfo, _ := os.Stat(outSQLite)

	assert.Greater(t, csvInfo.Size(), int64(0))
	assert.Greater(t, jsonInfo.Size(), int64(0))
	assert.Greater(t, sqliteInfo.Size(), int64(0))

	t.Logf("Multi-export: CSV=%d bytes, JSON=%d bytes, SQLite=%d bytes",
		csvInfo.Size(), jsonInfo.Size(), sqliteInfo.Size())
}

// generateMockProducts generates a slice of mock product objects simulating
// the DummyJSON products API structure.
func generateMockProducts(count int) []map[string]interface{} {
	faker := gofakeit.New(77)
	rng := rand.New(rand.NewSource(77))
	categories := []string{"beauty", "fragrances", "furniture", "groceries",
		"laptops", "smartphones", "mens-shirts", "womens-dresses",
		"sports-accessories", "sunglasses", "tablets", "vehicle"}

	products := make([]map[string]interface{}, 0, count)
	for i := 0; i < count; i++ {
		products = append(products, map[string]interface{}{
			"id":                 i + 1,
			"title":              faker.ProductName(),
			"description":        faker.Sentence(10),
			"category":           categories[rng.Intn(len(categories))],
			"price":              float64(rng.Intn(200000)+100) / 100.0,
			"discountPercentage": float64(rng.Intn(3000)) / 100.0,
			"rating":             float64(rng.Intn(50)+1) / 10.0,
			"stock":              rng.Intn(500),
			"brand":              faker.Company(),
			"sku":                faker.LetterN(8),
		})
	}
	return products
}

// slowRecordSource is a Source that produces records with a configurable delay
// between each record. Used to simulate slow data ingestion for cancellation/timeout tests.
type slowRecordSource struct {
	delayPerRecord time.Duration
	totalRecords   int
}

func (s *slowRecordSource) Read(ctx context.Context, out chan<- *model.Record) error {
	for i := 0; i < s.totalRecords; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.delayPerRecord):
		}

		id, _ := generateRecordID()
		record := &model.Record{
			ID: id,
			Fields: map[string]interface{}{
				"index": float64(i),
				"value": float64(i * 10),
			},
			Metadata: model.RecordMetadata{
				SourceType: "test",
				SourceID:   "slow_source",
				LineNumber: i + 1,
			},
		}

		select {
		case out <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *slowRecordSource) Type() string       { return "test" }
func (s *slowRecordSource) Identifier() string { return "slow_source" }
