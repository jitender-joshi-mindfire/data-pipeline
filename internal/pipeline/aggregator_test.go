package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
)

func newTestAggregator(config model.AggregationConfig) (*Aggregator, *store.InMemoryErrorStore, *store.InMemoryProgressTracker) {
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()
	agg := NewAggregator(config, "test-job", errStore, progress)
	return agg, errStore, progress
}

func runAggregator(t *testing.T, agg *Aggregator, records []*model.Record) []*model.Record {
	t.Helper()
	in := make(chan *model.Record, len(records))
	out := make(chan *model.Record, 100)

	for _, r := range records {
		in <- r
	}
	close(in)

	err := agg.Run(context.Background(), in, out)
	assert.NoError(t, err)

	var results []*model.Record
	for r := range out {
		results = append(results, r)
	}
	return results
}

func TestAggregator_Name(t *testing.T) {
	agg, _, _ := newTestAggregator(model.AggregationConfig{})
	assert.Equal(t, "aggregator", agg.Name())
}

func TestAggregator_CountSumAverage_NoGroupBy(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
			{Name: "sum", Field: "amount", Alias: "total_amount"},
			{Name: "average", Field: "amount", Alias: "avg_amount"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"amount": float64(10)}},
		{ID: "2", Fields: map[string]interface{}{"amount": float64(20)}},
		{ID: "3", Fields: map[string]interface{}{"amount": float64(30)}},
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	assert.Equal(t, float64(3), results[0].Fields["total_count"])
	assert.Equal(t, float64(60), results[0].Fields["total_amount"])
	assert.Equal(t, float64(20), results[0].Fields["avg_amount"])
	assert.Equal(t, float64(3), results[0].Fields["_count"])
}

func TestAggregator_GroupBy(t *testing.T) {
	config := model.AggregationConfig{
		GroupBy: []string{"category"},
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
			{Name: "sum", Field: "amount", Alias: "total_amount"},
			{Name: "average", Field: "amount", Alias: "avg_amount"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"category": "electronics", "amount": float64(100)}},
		{ID: "2", Fields: map[string]interface{}{"category": "clothing", "amount": float64(50)}},
		{ID: "3", Fields: map[string]interface{}{"category": "electronics", "amount": float64(200)}},
		{ID: "4", Fields: map[string]interface{}{"category": "clothing", "amount": float64(75)}},
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 2)

	// Build a map of results by category for easy assertion
	resultMap := make(map[string]*model.Record)
	for _, r := range results {
		cat := r.Fields["category"].(string)
		resultMap[cat] = r
	}

	// Electronics group
	elec := resultMap["electronics"]
	assert.NotNil(t, elec)
	assert.Equal(t, float64(2), elec.Fields["total_count"])
	assert.Equal(t, float64(300), elec.Fields["total_amount"])
	assert.Equal(t, float64(150), elec.Fields["avg_amount"])
	assert.Equal(t, float64(2), elec.Fields["_count"])

	// Clothing group
	cloth := resultMap["clothing"]
	assert.NotNil(t, cloth)
	assert.Equal(t, float64(2), cloth.Fields["total_count"])
	assert.Equal(t, float64(125), cloth.Fields["total_amount"])
	assert.Equal(t, float64(62.5), cloth.Fields["avg_amount"])
	assert.Equal(t, float64(2), cloth.Fields["_count"])
}

func TestAggregator_MultipleGroupByFields(t *testing.T) {
	config := model.AggregationConfig{
		GroupBy: []string{"region", "category"},
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
			{Name: "sum", Field: "amount", Alias: "total_amount"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"region": "US", "category": "A", "amount": float64(10)}},
		{ID: "2", Fields: map[string]interface{}{"region": "US", "category": "A", "amount": float64(20)}},
		{ID: "3", Fields: map[string]interface{}{"region": "US", "category": "B", "amount": float64(30)}},
		{ID: "4", Fields: map[string]interface{}{"region": "EU", "category": "A", "amount": float64(40)}},
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 3)
}

func TestAggregator_ZeroRecords(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
		},
	}
	agg, errStore, _ := newTestAggregator(config)

	results := runAggregator(t, agg, []*model.Record{})

	assert.Len(t, results, 0)

	// Should have logged a warning
	errors, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errors[0].Message, "zero records")
}

func TestAggregator_MissingGroupByField(t *testing.T) {
	config := model.AggregationConfig{
		GroupBy: []string{"category"},
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
		},
	}
	agg, errStore, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"category": "A", "amount": float64(10)}},
		{ID: "2", Fields: map[string]interface{}{"amount": float64(20)}}, // Missing category
		{ID: "3", Fields: map[string]interface{}{"category": "A", "amount": float64(30)}},
	}

	results := runAggregator(t, agg, records)

	// Only one group (A) since record 2 is excluded
	assert.Len(t, results, 1)
	assert.Equal(t, float64(2), results[0].Fields["total_count"])

	// Error should be logged for the excluded record
	errors, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errors[0].Message, "group-by field")
}

func TestAggregator_NullGroupByField(t *testing.T) {
	config := model.AggregationConfig{
		GroupBy: []string{"category"},
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
		},
	}
	agg, errStore, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"category": "A", "amount": float64(10)}},
		{ID: "2", Fields: map[string]interface{}{"category": nil, "amount": float64(20)}}, // Null category
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	assert.Equal(t, float64(1), results[0].Fields["total_count"])

	errors, _ := errStore.GetByJob("test-job", 0, 50)
	assert.Len(t, errors, 1)
	assert.Contains(t, errors[0].Message, "group-by field")
}

func TestAggregator_NonNumericAggregationField(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "sum", Field: "amount", Alias: "total_amount"},
		},
	}
	agg, errStore, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"amount": float64(10)}},
		{ID: "2", Fields: map[string]interface{}{"amount": "not_a_number"}},
		{ID: "3", Fields: map[string]interface{}{"amount": float64(30)}},
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	assert.Equal(t, float64(40), results[0].Fields["total_amount"])

	// Error logged for the non-numeric record
	errors, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errors[0].Message, "non-numeric")
}

func TestAggregator_MissingAggregationField(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "sum", Field: "amount", Alias: "total_amount"},
		},
	}
	agg, errStore, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"amount": float64(10)}},
		{ID: "2", Fields: map[string]interface{}{"name": "test"}}, // Missing amount
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	assert.Equal(t, float64(10), results[0].Fields["total_amount"])

	// Error logged for the missing field record
	errors, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errors[0].Message, "missing or null field")
}

func TestAggregator_CountWithSpecificField(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "amount", Alias: "amount_count"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"amount": float64(10)}},
		{ID: "2", Fields: map[string]interface{}{"amount": float64(20)}},
		{ID: "3", Fields: map[string]interface{}{"name": "test"}}, // Missing amount
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	// Count should only count records with valid numeric amount field
	assert.Equal(t, float64(2), results[0].Fields["amount_count"])
}

func TestAggregator_AverageWithZeroValidRecords(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "average", Field: "amount", Alias: "avg_amount"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"amount": "not_a_number"}},
		{ID: "2", Fields: map[string]interface{}{"name": "test"}}, // Missing amount
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	// Average of zero valid records should be 0
	assert.Equal(t, float64(0), results[0].Fields["avg_amount"])
}

func TestAggregator_ContextCancellation(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan *model.Record)
	out := make(chan *model.Record, 100)

	// Cancel immediately
	cancel()

	err := agg.Run(ctx, in, out)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestAggregator_StringNumericConversion(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "sum", Field: "amount", Alias: "total_amount"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	// String values that are numeric should be converted
	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"amount": "10.5"}},
		{ID: "2", Fields: map[string]interface{}{"amount": "20.5"}},
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	assert.Equal(t, float64(31), results[0].Fields["total_amount"])
}

func TestAggregator_IntegerFields(t *testing.T) {
	config := model.AggregationConfig{
		Functions: []model.AggregationFunction{
			{Name: "sum", Field: "count", Alias: "total_count"},
			{Name: "average", Field: "count", Alias: "avg_count"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"count": 10}},
		{ID: "2", Fields: map[string]interface{}{"count": int64(20)}},
		{ID: "3", Fields: map[string]interface{}{"count": int32(30)}},
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	assert.Equal(t, float64(60), results[0].Fields["total_count"])
	assert.Equal(t, float64(20), results[0].Fields["avg_count"])
}

func TestAggregator_RecordMetadata(t *testing.T) {
	config := model.AggregationConfig{
		GroupBy: []string{"category"},
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
		},
	}
	agg, _, _ := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"category": "A", "amount": float64(10)}},
	}

	results := runAggregator(t, agg, records)

	assert.Len(t, results, 1)
	assert.Equal(t, "aggregator", results[0].Metadata.SourceType)
	assert.NotEmpty(t, results[0].ID)
}

func TestAggregator_ProgressTracking(t *testing.T) {
	config := model.AggregationConfig{
		GroupBy: []string{"category"},
		Functions: []model.AggregationFunction{
			{Name: "count", Field: "*", Alias: "total_count"},
		},
	}
	agg, _, progress := newTestAggregator(config)

	records := []*model.Record{
		{ID: "1", Fields: map[string]interface{}{"category": "A"}},
		{ID: "2", Fields: map[string]interface{}{"category": "B"}},
	}

	_ = runAggregator(t, agg, records)

	// Allow some time for progress updates
	time.Sleep(10 * time.Millisecond)

	p := progress.GetProgress("test-job")
	assert.NotNil(t, p)
	// 2 groups emitted = 2 records processed
	assert.Equal(t, int64(2), p.RecordsProcessed)
}
