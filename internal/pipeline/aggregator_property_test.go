package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 9: Aggregation Correctness
// Validates: Requirements 6.1, 6.2, 6.3, 6.4
//
// For any set of records with numeric fields and a given aggregation configuration
// (with or without group-by), the aggregator shall produce one result per unique group
// where: count equals the number of records in that group, sum equals the arithmetic
// sum of the target field values, and average equals sum divided by count.
func TestProperty9_AggregationCorrectness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Decide whether to test with group-by or without group-by
		useGroupBy := rapid.Bool().Draw(t, "useGroupBy")

		// Generate number of records (1 to 50)
		numRecords := rapid.IntRange(1, 50).Draw(t, "numRecords")

		// Generate the numeric field name for aggregation
		aggField := "amount"

		// Generate group-by categories when group-by is enabled
		var groupByField string
		var categories []string
		if useGroupBy {
			groupByField = "category"
			numCategories := rapid.IntRange(1, 5).Draw(t, "numCategories")
			categories = make([]string, numCategories)
			for i := 0; i < numCategories; i++ {
				categories[i] = fmt.Sprintf("cat_%d", i)
			}
		}

		// Generate records with valid numeric fields
		records := make([]*model.Record, numRecords)
		for i := 0; i < numRecords; i++ {
			fields := make(map[string]interface{})

			// Generate a numeric value for the aggregation field
			val := rapid.Float64Range(-10000, 10000).Draw(t, fmt.Sprintf("amount_%d", i))
			fields[aggField] = val

			// Assign a category if group-by is enabled
			if useGroupBy {
				catIdx := rapid.IntRange(0, len(categories)-1).Draw(t, fmt.Sprintf("catIdx_%d", i))
				fields[groupByField] = categories[catIdx]
			}

			records[i] = &model.Record{
				ID:     fmt.Sprintf("record-%d", i),
				Fields: fields,
				Metadata: model.RecordMetadata{
					SourceType: "csv",
					SourceID:   "test.csv",
					LineNumber: i + 1,
				},
			}
		}

		// Build aggregation config
		var groupBy []string
		if useGroupBy {
			groupBy = []string{groupByField}
		}

		config := model.AggregationConfig{
			GroupBy: groupBy,
			Functions: []model.AggregationFunction{
				{Name: "count", Field: "*", Alias: "total_count"},
				{Name: "sum", Field: aggField, Alias: "total_sum"},
				{Name: "average", Field: aggField, Alias: "avg_value"},
			},
		}

		// Run the aggregator
		errStore := store.NewInMemoryErrorStore()
		progress := store.NewProgressTracker()
		agg := NewAggregator(config, "prop9-job", errStore, progress)

		in := make(chan *model.Record, len(records))
		out := make(chan *model.Record, 100)

		for _, r := range records {
			in <- r
		}
		close(in)

		err := agg.Run(context.Background(), in, out)
		if err != nil {
			t.Fatalf("aggregator returned error: %v", err)
		}

		// Collect results
		results := make([]*model.Record, 0)
		for r := range out {
			results = append(results, r)
		}

		// Independently compute expected values
		type groupStats struct {
			count int
			sum   float64
		}
		expected := make(map[string]*groupStats)

		for _, r := range records {
			var key string
			if useGroupBy {
				key = fmt.Sprintf("%v", r.Fields[groupByField])
			} else {
				key = "__all__"
			}

			if _, ok := expected[key]; !ok {
				expected[key] = &groupStats{}
			}
			expected[key].count++
			expected[key].sum += r.Fields[aggField].(float64)
		}

		// Verify: one result per unique group
		if len(results) != len(expected) {
			t.Fatalf("expected %d groups, got %d results", len(expected), len(results))
		}

		// Build result map by group key
		resultMap := make(map[string]*model.Record)
		for _, r := range results {
			var key string
			if useGroupBy {
				catVal, ok := r.Fields[groupByField]
				if !ok {
					t.Fatalf("result record missing group-by field %q: %v", groupByField, r.Fields)
				}
				key = fmt.Sprintf("%v", catVal)
			} else {
				key = "__all__"
			}
			resultMap[key] = r
		}

		// Verify each group's aggregation values
		for key, stats := range expected {
			result, ok := resultMap[key]
			if !ok {
				t.Fatalf("missing result for group %q", key)
			}

			// Verify count
			gotCount, ok := result.Fields["total_count"].(float64)
			if !ok {
				t.Fatalf("group %q: total_count is not float64: %T", key, result.Fields["total_count"])
			}
			expectedCount := float64(stats.count)
			if gotCount != expectedCount {
				t.Fatalf("group %q: count mismatch: expected %v, got %v", key, expectedCount, gotCount)
			}

			// Verify sum
			gotSum, ok := result.Fields["total_sum"].(float64)
			if !ok {
				t.Fatalf("group %q: total_sum is not float64: %T", key, result.Fields["total_sum"])
			}
			// Use a small tolerance for floating point comparison
			expectedSum := stats.sum
			if absDiff(gotSum, expectedSum) > 1e-9 {
				t.Fatalf("group %q: sum mismatch: expected %v, got %v (diff: %v)",
					key, expectedSum, gotSum, absDiff(gotSum, expectedSum))
			}

			// Verify average = sum / count
			gotAvg, ok := result.Fields["avg_value"].(float64)
			if !ok {
				t.Fatalf("group %q: avg_value is not float64: %T", key, result.Fields["avg_value"])
			}
			expectedAvg := stats.sum / float64(stats.count)
			if absDiff(gotAvg, expectedAvg) > 1e-9 {
				t.Fatalf("group %q: average mismatch: expected %v, got %v (diff: %v)",
					key, expectedAvg, gotAvg, absDiff(gotAvg, expectedAvg))
			}

			// Verify _count field equals count of input records in group
			gotInputCount, ok := result.Fields["_count"].(float64)
			if !ok {
				t.Fatalf("group %q: _count is not float64: %T", key, result.Fields["_count"])
			}
			if gotInputCount != expectedCount {
				t.Fatalf("group %q: _count mismatch: expected %v, got %v", key, expectedCount, gotInputCount)
			}
		}
	})
}

// absDiff returns the absolute difference between two float64 values.
func absDiff(a, b float64) float64 {
	d := a - b
	if d < 0 {
		return -d
	}
	return d
}

// Feature: data-processing-pipeline, Property 10: Aggregation Excludes Invalid Records
// Validates: Requirements 6.5, 6.7
//
// For any record with a missing, null, or non-numeric value in a field targeted for
// sum/average aggregation (or a missing/null group-by field), that record shall be
// excluded from the numeric computation and an error shall be logged.
// count(field) counts all non-null field values regardless of type (SQL standard behaviour).
func TestProperty10_AggregationExcludesInvalidRecords(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Decide test scenario: invalid aggregation field vs invalid group-by field
		testGroupByInvalid := rapid.Bool().Draw(t, "testGroupByInvalid")

		// Generate number of valid records (1 to 20)
		numValid := rapid.IntRange(1, 20).Draw(t, "numValid")
		// Generate number of invalid records (1 to 10)
		numInvalid := rapid.IntRange(1, 10).Draw(t, "numInvalid")

		aggField := "amount"
		groupByField := "category"

		// Generate categories for valid records
		categories := []string{"cat_A", "cat_B", "cat_C"}

		// Build valid records
		var allRecords []*model.Record
		var validSum float64
		validCount := 0
		// count(field) counts non-null values regardless of type; track how many
		// invalid records have the field present (not missing, not null)
		numInvalidWithField := 0

		for i := 0; i < numValid; i++ {
			val := rapid.Float64Range(1, 1000).Draw(t, fmt.Sprintf("validVal_%d", i))
			catIdx := rapid.IntRange(0, len(categories)-1).Draw(t, fmt.Sprintf("validCat_%d", i))
			fields := map[string]interface{}{
				aggField:     val,
				groupByField: categories[catIdx],
			}
			allRecords = append(allRecords, &model.Record{
				ID:     fmt.Sprintf("valid-%d", i),
				Fields: fields,
				Metadata: model.RecordMetadata{
					SourceType: "csv",
					SourceID:   "test.csv",
					LineNumber: i + 1,
				},
			})
			validSum += val
			validCount++
		}

		// Build invalid records based on the scenario
		for i := 0; i < numInvalid; i++ {
			fields := make(map[string]interface{})

			if testGroupByInvalid {
				// Invalid group-by field: missing or null
				fields[aggField] = rapid.Float64Range(1, 1000).Draw(t, fmt.Sprintf("invalidGBVal_%d", i))
				// Choose between missing group-by field or null group-by field
				useNull := rapid.Bool().Draw(t, fmt.Sprintf("useNullGB_%d", i))
				if useNull {
					fields[groupByField] = nil
				}
				// else: field is not set at all (missing)
			} else {
				// Invalid aggregation field: missing, null, or non-numeric
				fields[groupByField] = categories[0] // valid group-by

				invalidType := rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("invalidType_%d", i))
				switch invalidType {
				case 0:
					// Missing field: don't set aggField at all
				case 1:
					// Null field
					fields[aggField] = nil
				case 2:
					// Non-numeric field — field is present with non-null value,
					// so count(field) will include it even though sum/avg won't.
					nonNumeric := rapid.StringMatching(`[a-zA-Z]{3,10}`).Draw(t, fmt.Sprintf("nonNumeric_%d", i))
					fields[aggField] = nonNumeric
					numInvalidWithField++
				}
			}

			allRecords = append(allRecords, &model.Record{
				ID:     fmt.Sprintf("invalid-%d", i),
				Fields: fields,
				Metadata: model.RecordMetadata{
					SourceType: "csv",
					SourceID:   "test.csv",
					LineNumber: numValid + i + 1,
				},
			})
		}

		// Shuffle records to ensure order independence
		shuffleOrder := rapid.SliceOfN(rapid.IntRange(0, 1000), len(allRecords), len(allRecords)).Draw(t, "shuffle")
		shuffled := make([]*model.Record, len(allRecords))
		indices := make([]int, len(allRecords))
		for i := range indices {
			indices[i] = i
		}
		// Simple deterministic shuffle using drawn values as sort keys
		for i := 0; i < len(indices); i++ {
			j := shuffleOrder[i] % len(indices)
			indices[i], indices[j] = indices[j], indices[i]
		}
		for i, idx := range indices {
			shuffled[i] = allRecords[idx]
		}

		// Configure aggregation with group-by
		config := model.AggregationConfig{
			GroupBy: []string{groupByField},
			Functions: []model.AggregationFunction{
				{Name: "sum", Field: aggField, Alias: "total_sum"},
				{Name: "count", Field: aggField, Alias: "field_count"},
				{Name: "average", Field: aggField, Alias: "avg_value"},
			},
		}

		// Run aggregator
		errStore := store.NewInMemoryErrorStore()
		progress := store.NewProgressTracker()
		agg := NewAggregator(config, "prop10-job", errStore, progress)

		in := make(chan *model.Record, len(shuffled))
		out := make(chan *model.Record, 100)

		for _, r := range shuffled {
			in <- r
		}
		close(in)

		err := agg.Run(context.Background(), in, out)
		if err != nil {
			t.Fatalf("aggregator returned error: %v", err)
		}

		// Collect results
		var results []*model.Record
		for r := range out {
			results = append(results, r)
		}

		// Verify: invalid records are excluded from results
		if testGroupByInvalid {
			// Invalid group-by records should not form any group of their own.
			// All results should only have groups from valid records' categories.
			for _, r := range results {
				catVal, exists := r.Fields[groupByField]
				if !exists || catVal == nil {
					t.Fatalf("result record has missing/nil group-by field; invalid records should be excluded")
				}
				catStr, ok := catVal.(string)
				if !ok {
					t.Fatalf("result group-by field is not a string: %T", catVal)
				}
				found := false
				for _, c := range categories {
					if c == catStr {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("result contains unexpected category %q", catStr)
				}
			}

			// Total sum across all groups should match validSum
			var totalSum float64
			for _, r := range results {
				if s, ok := r.Fields["total_sum"].(float64); ok {
					totalSum += s
				}
			}
			if absDiff(totalSum, validSum) > 1e-9 {
				t.Fatalf("total sum mismatch: expected %v, got %v (invalid group-by records should be excluded)",
					validSum, totalSum)
			}
		} else {
			// Invalid aggregation field records have a valid group-by, so they still
			// participate in group formation but are excluded from sum/avg/count(field).
			// Sum across all groups should match validSum (only valid numeric records contribute)
			var totalSum float64
			var totalFieldCount float64
			for _, r := range results {
				if s, ok := r.Fields["total_sum"].(float64); ok {
					totalSum += s
				}
				if c, ok := r.Fields["field_count"].(float64); ok {
					totalFieldCount += c
				}
			}
			if absDiff(totalSum, validSum) > 1e-9 {
				t.Fatalf("total sum mismatch: expected %v, got %v (invalid agg field records should be excluded)",
					validSum, totalSum)
			}
			// count(field) = numeric records + non-null non-numeric records
			// (only missing/null records are excluded from count)
			expectedFieldCount := validCount + numInvalidWithField
			if int(totalFieldCount) != expectedFieldCount {
				t.Fatalf("total field count mismatch: expected %d (valid=%d + non-null-invalid=%d), got %v",
					expectedFieldCount, validCount, numInvalidWithField, totalFieldCount)
			}
		}

		// Verify: errors are logged for records that are invalid for sum/avg.
		// count(field) no longer errors on non-numeric values — only missing/null trigger errors for count.
		// sum/avg error on missing, null, and non-numeric. So min errors = numInvalid - numInvalidWithField
		// (non-numeric records only produce errors for the sum and average functions, not count).
		minExpectedErrors := numInvalid - numInvalidWithField // missing/null records → always errors
		errors, totalErrors := errStore.GetByJob("prop10-job", 0, 200)
		if !testGroupByInvalid && totalErrors < minExpectedErrors {
			t.Fatalf("expected at least %d errors for invalid records, got %d total errors",
				minExpectedErrors, totalErrors)
		}
		if testGroupByInvalid && totalErrors < numInvalid {
			t.Fatalf("expected at least %d errors for invalid group-by records, got %d total errors",
				numInvalid, totalErrors)
		}

		// Verify error messages are meaningful
		for _, e := range errors {
			if e.Stage != "aggregator" {
				t.Fatalf("error logged with unexpected stage: %q (expected 'aggregator')", e.Stage)
			}
			if e.Message == "" {
				t.Fatalf("error entry has empty message")
			}
		}
	})
}
