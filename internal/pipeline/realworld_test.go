//go:build load

// Package pipeline contains real-world API integration tests that use the
// assignment-specified data sources: COVID CSV over HTTP, JSONPlaceholder,
// and the mixed "Global Daily Report" scenario.
//
// Run with: go test -tags load -timeout 300s -v ./internal/pipeline/ -run TestRealWorld
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// skipIfUnreachable skips the test if the given URL returns an error or non-200.
// Used to make live-API tests resilient to network unavailability in CI.
func skipIfUnreachable(t *testing.T, url string) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Skipf("skipping: %s is unreachable (%v)", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("skipping: %s returned HTTP %d", url, resp.StatusCode)
	}
}

// -----------------------------------------------------------------------------
// Test 1: CSV over HTTP — FSU height/weight dataset (200 rows)
// Assignment source: https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv
// -----------------------------------------------------------------------------

// TestRealWorld_CSV_OverHTTP_FSU_HeightWeight ingests the FSU height/weight CSV
// directly over HTTP, and counts the 200 data rows.
// Note: The CSV has malformed headers ("Index", Height(Inches)", "Weight(Pounds)")
// so we validate and count without numeric transformations.
func TestRealWorld_CSV_OverHTTP_FSU_HeightWeight(t *testing.T) {
	const url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
	skipIfUnreachable(t, url)

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "hw_summary.json")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	// The CSV has columns: "Index", Height(Inches)", "Weight(Pounds)"
	// (the second column has a malformed header — missing leading quote).
	// We accept all records without strict validation to count all 200 rows.
	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: url, TimeoutSeconds: 30},
		},
		Validation:      model.ValidationConfig{Fields: []model.FieldSchema{}},
		Transformations: []model.TransformConfig{},
		Aggregation: model.AggregationConfig{
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "total_people"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: outJSON},
		},
		WorkerPools: model.WorkerPoolConfig{Validator: 1, Transformer: 1},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// CSVSource with HTTP URL — the key feature being tested
	sources := []Source{
		&CSVSource{
			FilePath:    url,
			JobID:       job.ID,
			ErrorStore:  errStore,
			HTTPTimeout: 30 * time.Second,
		},
	}
	targets := []export.ExportTarget{export.NewJSONTarget(outJSON)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)
	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)

	t.Logf("FSU CSV-over-HTTP pipeline completed in %v", elapsed)

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	data, err := os.ReadFile(outJSON)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &results))
	require.Len(t, results, 1)

	totalPeople := results[0]["total_people"].(float64)
	// hw_200.csv has 200 data rows
	assert.Equal(t, float64(200), totalPeople, "expected 200 rows in hw_200.csv")
	t.Logf("Ingested %v people from FSU CSV over HTTP", totalPeople)
}

// -----------------------------------------------------------------------------
// Test 2: JSON API — JSONPlaceholder posts (100 records, group-by userId)
// Assignment source: https://jsonplaceholder.typicode.com/posts
// -----------------------------------------------------------------------------

// TestRealWorld_JSONPlaceholder_Posts ingests 100 posts from JSONPlaceholder,
// validates structure, and aggregates post counts per userId (10 users × 10 posts).
func TestRealWorld_JSONPlaceholder_Posts(t *testing.T) {
	const url = "https://jsonplaceholder.typicode.com/posts"
	skipIfUnreachable(t, url)

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "posts_summary.json")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "http", Path: url, TimeoutSeconds: 30},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "id",     Type: "number", Required: true},
				{Name: "userId", Type: "number", Required: true},
				{Name: "title",  Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{
			{Field: "title", Operation: "trim"},
			{Field: "title", Operation: "lowercase"},
		},
		Aggregation: model.AggregationConfig{
			GroupBy: []string{"userId"},
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "posts_per_user"},
				{Name: "sum",   Field: "id", Alias: "id_sum"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: outJSON},
		},
		WorkerPools: model.WorkerPoolConfig{Validator: 2, Transformer: 2},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{NewHTTPSource(url, 30)}
	targets := []export.ExportTarget{export.NewJSONTarget(outJSON)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)
	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)

	t.Logf("JSONPlaceholder posts pipeline completed in %v", elapsed)

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	data, err := os.ReadFile(outJSON)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &results))

	// JSONPlaceholder has 10 users, each with 10 posts
	assert.Equal(t, 10, len(results), "should have 10 user groups")

	for _, r := range results {
		assert.Equal(t, float64(10), r["posts_per_user"],
			"each user should have exactly 10 posts")
	}
	t.Logf("Produced %d user groups from 100 JSONPlaceholder posts", len(results))
}

// -----------------------------------------------------------------------------
// Test 3: JSON API — JSONPlaceholder comments (500 records)
// Demonstrates a larger JSON API payload with email validation
// -----------------------------------------------------------------------------

// TestRealWorld_JSONPlaceholder_Comments ingests 500 comments from JSONPlaceholder,
// validates email format, and counts comments per post.
func TestRealWorld_JSONPlaceholder_Comments(t *testing.T) {
	const url = "https://jsonplaceholder.typicode.com/comments"
	skipIfUnreachable(t, url)

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "comments_summary.json")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "http", Path: url, TimeoutSeconds: 30},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "postId", Type: "number", Required: true},
				{Name: "name",   Type: "string", Required: true},
				{Name: "email",  Type: "string", Required: true},
				{Name: "body",   Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{
			{Field: "email", Operation: "lowercase"},
			{Field: "name",  Operation: "trim"},
		},
		Aggregation: model.AggregationConfig{
			GroupBy: []string{"postId"},
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "comments_per_post"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: outJSON},
		},
		WorkerPools: model.WorkerPoolConfig{Validator: 2, Transformer: 2},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	sources := []Source{NewHTTPSource(url, 30)}
	targets := []export.ExportTarget{export.NewJSONTarget(outJSON)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)
	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)
	require.NoError(t, err)

	t.Logf("JSONPlaceholder comments pipeline completed in %v (500 records)", elapsed)

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	data, err := os.ReadFile(outJSON)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &results))

	// 500 comments across 100 posts = 5 comments per post
	assert.Equal(t, 100, len(results), "should have 100 post groups")
	for _, r := range results {
		assert.Equal(t, float64(5), r["comments_per_post"],
			"each post should have exactly 5 comments")
	}
	t.Logf("Produced %d post groups from 500 comments", len(results))
}

// -----------------------------------------------------------------------------
// Test 4: Mixed sources — the "Global Daily Report" scenario from the assignment.
// Sources: CSV over HTTP + two JSON HTTP APIs, all ingested concurrently.
// This is the exact example in the assignment brief.
// -----------------------------------------------------------------------------

// TestRealWorld_GlobalDailyReport_MixedSources is the assignment's "Global Daily Report"
// scenario: three concurrent sources of different types (CSV-over-HTTP + 2 JSON APIs)
// are ingested simultaneously, demonstrating fan-out across source types.
func TestRealWorld_GlobalDailyReport_MixedSources(t *testing.T) {
	// Check all three sources are reachable before starting
	sources_to_check := []string{
		"https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv",
		"https://jsonplaceholder.typicode.com/posts",
		"https://jsonplaceholder.typicode.com/comments",
	}
	for _, u := range sources_to_check {
		skipIfUnreachable(t, u)
	}

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "global_report.json")
	outCSV := filepath.Join(tmpDir, "global_report.csv")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv",  Path: "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv", TimeoutSeconds: 30},
			{Type: "http", Path: "https://jsonplaceholder.typicode.com/posts",    TimeoutSeconds: 30},
			{Type: "http", Path: "https://jsonplaceholder.typicode.com/comments", TimeoutSeconds: 30},
		},
		Validation:      model.ValidationConfig{Fields: []model.FieldSchema{}},
		Transformations: []model.TransformConfig{},
		Aggregation: model.AggregationConfig{
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "total_records_ingested"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: outJSON},
			{Type: "csv",  Path: outCSV},
		},
		WorkerPools: model.WorkerPoolConfig{Validator: 4, Transformer: 4},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// 3 concurrent ingestion goroutines: 1 CSV-over-HTTP + 2 JSON APIs
	pipelineSources := []Source{
		&CSVSource{
			FilePath:    "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv",
			JobID:       job.ID,
			ErrorStore:  errStore,
			HTTPTimeout: 30 * time.Second,
		},
		NewHTTPSource("https://jsonplaceholder.typicode.com/posts",    30),
		NewHTTPSource("https://jsonplaceholder.typicode.com/comments", 30),
	}
	targets := []export.ExportTarget{
		export.NewJSONTarget(outJSON),
		export.NewCSVTarget(outCSV),
	}

	p := NewPipeline(job, jobStore, pipelineSources, targets, errStore, progress)

	start := time.Now()
	err = p.Run(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	t.Logf("Global Daily Report pipeline completed in %v", elapsed)
	t.Logf("  Sources: CSV-over-HTTP (200) + posts (100) + comments (500) = 800 records")

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	data, err := os.ReadFile(outJSON)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &results))
	require.Len(t, results, 1)

	total := results[0]["total_records_ingested"].(float64)
	// 200 (CSV) + 100 (posts) + 500 (comments) = 800
	assert.Equal(t, float64(800), total,
		"should ingest all 800 records from 3 concurrent sources")
	t.Logf("Total records ingested from 3 mixed sources: %.0f", total)

	// Verify both export files exist
	assert.FileExists(t, outJSON)
	assert.FileExists(t, outCSV)
}

// -----------------------------------------------------------------------------
// Test 5: CSV-over-HTTP using a local mock server (no network required)
// Validates the CSVSource URL-detection and HTTP streaming logic in isolation.
// -----------------------------------------------------------------------------

// TestRealWorld_CSV_OverHTTP_MockServer tests the CSV-over-HTTP capability
// without any external network dependency, using an in-process mock server.
func TestRealWorld_CSV_OverHTTP_MockServer(t *testing.T) {
	// Serve a small CSV with known content
	csvData := "id,product,price,category\n" +
		"1,Widget,9.99,tools\n" +
		"2,Gadget,24.99,electronics\n" +
		"3,Doohickey,4.99,tools\n" +
		"4,Thingamajig,14.99,electronics\n" +
		"5,Whatsit,2.99,tools\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, csvData)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "products_summary.json")

	jobStore := store.NewInMemoryJobStore()
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()

	minPrice := 0.0
	jobConfig := model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: server.URL, TimeoutSeconds: 10},
		},
		Validation: model.ValidationConfig{
			Fields: []model.FieldSchema{
				{Name: "price",    Type: "number", Required: true, Min: &minPrice},
				{Name: "category", Type: "string", Required: true},
			},
		},
		Transformations: []model.TransformConfig{
			{Field: "price",    Operation: "type_convert", TargetType: "number"},
			{Field: "category", Operation: "lowercase"},
		},
		Aggregation: model.AggregationConfig{
			GroupBy: []string{"category"},
			Functions: []model.AggregationFunction{
				{Name: "count",   Field: "*",     Alias: "product_count"},
				{Name: "sum",     Field: "price", Alias: "total_price"},
				{Name: "average", Field: "price", Alias: "avg_price"},
			},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: outJSON},
		},
		WorkerPools: model.WorkerPoolConfig{Validator: 1, Transformer: 1},
	}

	job, err := jobStore.Create(jobConfig)
	require.NoError(t, err)

	// CSVSource pointing to a URL — core functionality being tested
	sources := []Source{
		&CSVSource{
			FilePath:    server.URL,
			JobID:       job.ID,
			ErrorStore:  errStore,
			HTTPTimeout: 10 * time.Second,
		},
	}
	targets := []export.ExportTarget{export.NewJSONTarget(outJSON)}

	p := NewPipeline(job, jobStore, sources, targets, errStore, progress)
	err = p.Run(context.Background())
	require.NoError(t, err)

	updatedJob, err := jobStore.Get(job.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCompleted, updatedJob.Status)

	data, err := os.ReadFile(outJSON)
	require.NoError(t, err)

	var results []map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &results))
	require.Len(t, results, 2, "should have 2 categories: tools and electronics")

	resultMap := make(map[string]map[string]interface{})
	for _, r := range results {
		cat := r["category"].(string)
		resultMap[cat] = r
	}

	// tools: 1(9.99) + 3(4.99) + 5(2.99) = 3 items, sum=17.97, avg=5.99
	tools := resultMap["tools"]
	assert.Equal(t, float64(3), tools["product_count"])
	assert.InDelta(t, 17.97, tools["total_price"], 0.01)
	assert.InDelta(t, 5.99, tools["avg_price"], 0.01)

	// electronics: 2(24.99) + 4(14.99) = 2 items, sum=39.98, avg=19.99
	elec := resultMap["electronics"]
	assert.Equal(t, float64(2), elec["product_count"])
	assert.InDelta(t, 39.98, elec["total_price"], 0.01)
	assert.InDelta(t, 19.99, elec["avg_price"], 0.01)
}
