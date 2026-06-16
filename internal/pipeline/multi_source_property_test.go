package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 5: Multi-Source Record Completeness
// Validates: Requirements 3.4, 3.5, 3.6
//
// For any set of N valid sources, each containing a known number of records,
// the Ingester shall emit a total record count equal to the sum of all individual
// source record counts, regardless of ingestion concurrency or ordering.
func TestProperty5_MultiSourceRecordCompleteness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate 1 to 8 sources, each with 0 to 50 records
		numSources := rapid.IntRange(1, 8).Draw(t, "numSources")

		sources := make([]Source, numSources)
		expectedTotal := 0

		for i := 0; i < numSources; i++ {
			numRecords := rapid.IntRange(0, 50).Draw(t, fmt.Sprintf("numRecords_%d", i))
			sourceID := fmt.Sprintf("source-%d", i)
			sourceType := rapid.SampledFrom([]string{"csv", "json", "http"}).Draw(t, fmt.Sprintf("sourceType_%d", i))

			records := make([]*model.Record, numRecords)
			for j := 0; j < numRecords; j++ {
				records[j] = &model.Record{
					ID:     fmt.Sprintf("rec-%d-%d", i, j),
					Fields: map[string]interface{}{"value": j, "source": sourceID},
					Metadata: model.RecordMetadata{
						SourceType: sourceType,
						SourceID:   sourceID,
						LineNumber: j + 1,
					},
				}
			}

			sources[i] = &mockSource{
				id:      sourceID,
				srcType: sourceType,
				records: records,
			}
			expectedTotal += numRecords
		}

		// Run the Ingester stage with the generated sources
		errStore := store.NewInMemoryErrorStore()
		ingester := NewIngester(sources, errStore, "property-test-job")

		out := make(chan *model.Record, expectedTotal+100)
		err := ingester.Run(context.Background(), nil, out)
		if err != nil {
			t.Fatalf("Ingester.Run returned error: %v", err)
		}

		// Collect all emitted records
		var emitted []*model.Record
		for rec := range out {
			emitted = append(emitted, rec)
		}

		// Property: total emitted records equals sum of all individual source record counts
		if len(emitted) != expectedTotal {
			t.Fatalf("total emitted records mismatch: expected %d, got %d (numSources=%d)",
				expectedTotal, len(emitted), numSources)
		}

		// Property: each emitted record has source metadata set (validates requirement 3.6)
		for i, rec := range emitted {
			if rec.Metadata.SourceType == "" {
				t.Fatalf("record %d: missing source type in metadata", i)
			}
			if rec.Metadata.SourceID == "" {
				t.Fatalf("record %d: missing source identifier in metadata", i)
			}
		}

		// Verify no errors were logged (all sources are valid)
		_, errCount := errStore.GetByJob("property-test-job", 0, 100)
		if errCount != 0 {
			t.Fatalf("expected no errors for valid sources, got %d", errCount)
		}
	})
}

// TestProperty5_MultiSourceWithFailures tests that when some sources fail,
// the total emitted records equals the sum of records from successful sources,
// and errors are logged for failed sources.
// Feature: data-processing-pipeline, Property 5: Multi-Source Record Completeness
// Validates: Requirements 3.4, 3.5, 3.6
func TestProperty5_MultiSourceWithFailures(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate 2 to 6 total sources
		numSources := rapid.IntRange(2, 6).Draw(t, "numSources")
		// At least 1 good source and at least 1 failing source
		numFailing := rapid.IntRange(1, numSources-1).Draw(t, "numFailing")
		numGood := numSources - numFailing

		sources := make([]Source, 0, numSources)
		expectedTotal := 0

		// Create good sources
		for i := 0; i < numGood; i++ {
			numRecords := rapid.IntRange(1, 30).Draw(t, fmt.Sprintf("goodRecords_%d", i))
			sourceID := fmt.Sprintf("good-source-%d", i)
			sourceType := rapid.SampledFrom([]string{"csv", "json", "http"}).Draw(t, fmt.Sprintf("goodType_%d", i))

			records := make([]*model.Record, numRecords)
			for j := 0; j < numRecords; j++ {
				records[j] = &model.Record{
					ID:     fmt.Sprintf("good-%d-%d", i, j),
					Fields: map[string]interface{}{"value": j},
					Metadata: model.RecordMetadata{
						SourceType: sourceType,
						SourceID:   sourceID,
						LineNumber: j + 1,
					},
				}
			}

			sources = append(sources, &mockSource{
				id:      sourceID,
				srcType: sourceType,
				records: records,
			})
			expectedTotal += numRecords
		}

		// Create failing sources (return error, emit no records)
		for i := 0; i < numFailing; i++ {
			sourceID := fmt.Sprintf("bad-source-%d", i)
			sourceType := rapid.SampledFrom([]string{"csv", "json", "http"}).Draw(t, fmt.Sprintf("badType_%d", i))

			sources = append(sources, &mockSource{
				id:      sourceID,
				srcType: sourceType,
				records: nil,
				err:     fmt.Errorf("source %s unreachable", sourceID),
			})
		}

		// Run the Ingester
		errStore := store.NewInMemoryErrorStore()
		ingester := NewIngester(sources, errStore, "property-test-failures")

		out := make(chan *model.Record, expectedTotal+100)
		err := ingester.Run(context.Background(), nil, out)
		if err != nil {
			t.Fatalf("Ingester.Run returned error: %v", err)
		}

		// Collect emitted records
		var emitted []*model.Record
		for rec := range out {
			emitted = append(emitted, rec)
		}

		// Property: total emitted equals sum of good source records only
		if len(emitted) != expectedTotal {
			t.Fatalf("emitted records mismatch: expected %d from good sources, got %d",
				expectedTotal, len(emitted))
		}

		// Property: errors logged for each failing source (validates requirement 3.5)
		_, errCount := errStore.GetByJob("property-test-failures", 0, 200)
		if errCount != numFailing {
			t.Fatalf("expected %d errors for failing sources, got %d", numFailing, errCount)
		}

		// Property: each emitted record has source metadata (validates requirement 3.6)
		for i, rec := range emitted {
			if rec.Metadata.SourceType == "" {
				t.Fatalf("record %d: missing source type in metadata", i)
			}
			if rec.Metadata.SourceID == "" {
				t.Fatalf("record %d: missing source identifier in metadata", i)
			}
		}
	})
}
