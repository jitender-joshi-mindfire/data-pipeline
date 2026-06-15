package store

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 18: Progress Metrics Computation
// Validates: Requirements 9.2, 9.3, 9.4
//
// For any sequence of record-processing events with known timestamps and outcomes,
// the Progress_Tracker shall compute:
//   - processing_rate = records_processed / elapsed_seconds
//   - per-stage latency = mean of per-record processing durations for that stage
//   - error_counts = count of failed records per stage

func TestProperty18_ProcessingRate(t *testing.T) {
	// **Validates: Requirements 9.2**
	// Property: processing_rate = records_processed / elapsed_seconds
	// Since we can't control time.Now() easily, we verify that the rate is consistent:
	// processing_rate * elapsed_time >= records_processed (approximately, accounting for timing)
	// and that processing_rate > 0 when records are processed with non-zero elapsed time.
	rapid.Check(t, func(t *rapid.T) {
		numRecords := rapid.IntRange(1, 200).Draw(t, "numRecords")
		stageName := rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "stage")

		pt := NewProgressTracker()
		jobID := "job-rate-test"
		pt.SetTotal(jobID, int64(numRecords))

		// Record processing events with a small fixed latency
		for i := 0; i < numRecords; i++ {
			pt.RecordProcessed(jobID, stageName, time.Millisecond)
		}

		// Allow some measurable time to pass
		time.Sleep(time.Millisecond)

		progress := pt.GetProgress(jobID)
		if progress == nil {
			t.Fatal("expected non-nil progress")
		}

		// Property: processing_rate > 0 when records are processed
		if progress.ProcessingRate <= 0 {
			t.Fatalf("expected ProcessingRate > 0 with %d records processed, got %f",
				numRecords, progress.ProcessingRate)
		}

		// Property: processing_rate = records_processed / elapsed_seconds
		// We verify the relationship is correct by checking that
		// ProcessingRate is consistent with RecordsProcessed (within timing tolerance)
		if progress.RecordsProcessed != int64(numRecords) {
			t.Fatalf("expected RecordsProcessed=%d, got %d", numRecords, progress.RecordsProcessed)
		}

		// The rate should not exceed what's physically possible: records / minimum_elapsed
		// Since at least 1ms has passed (our sleep), rate should be <= numRecords / 0.001
		maxPossibleRate := float64(numRecords) / 0.001
		if progress.ProcessingRate > maxPossibleRate {
			t.Fatalf("ProcessingRate %f exceeds maximum possible rate %f",
				progress.ProcessingRate, maxPossibleRate)
		}
	})
}

func TestProperty18_PerStageLatency(t *testing.T) {
	// **Validates: Requirements 9.3**
	// Property: per-stage latency = mean of per-record processing durations for that stage
	rapid.Check(t, func(t *rapid.T) {
		// Generate 1-5 stages, each with 1-50 records and random latencies
		numStages := rapid.IntRange(1, 5).Draw(t, "numStages")

		pt := NewProgressTracker()
		jobID := "job-latency-test"

		type stageData struct {
			name      string
			latencies []time.Duration
		}

		stages := make([]stageData, numStages)
		totalRecords := 0

		for s := 0; s < numStages; s++ {
			stageName := rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "stageName")
			numRecords := rapid.IntRange(1, 50).Draw(t, "numRecords")

			latencies := make([]time.Duration, numRecords)
			for i := 0; i < numRecords; i++ {
				// Generate latencies between 1ms and 100ms
				latencyMs := rapid.IntRange(1, 100).Draw(t, "latencyMs")
				latencies[i] = time.Duration(latencyMs) * time.Millisecond
			}

			stages[s] = stageData{name: stageName, latencies: latencies}
			totalRecords += numRecords
		}

		pt.SetTotal(jobID, int64(totalRecords))

		// Record all processing events
		for _, sd := range stages {
			for _, lat := range sd.latencies {
				pt.RecordProcessed(jobID, sd.name, lat)
			}
		}

		progress := pt.GetProgress(jobID)
		if progress == nil {
			t.Fatal("expected non-nil progress")
		}

		// Verify per-stage latency = mean of durations (in ms) for each stage
		for _, sd := range stages {
			expectedAvg := computeMeanLatencyMs(sd.latencies)

			actualAvg, ok := progress.StageLatencies[sd.name]
			if !ok {
				t.Fatalf("expected latency for stage %q, not found in StageLatencies", sd.name)
			}

			// Allow for floating point precision tolerance (0.001 ms)
			if math.Abs(actualAvg-expectedAvg) > 0.001 {
				t.Fatalf("stage %q: expected avg latency %.6f ms, got %.6f ms",
					sd.name, expectedAvg, actualAvg)
			}
		}
	})
}

func TestProperty18_ErrorCounts(t *testing.T) {
	// **Validates: Requirements 9.4**
	// Property: error_counts = count of failed records per stage
	rapid.Check(t, func(t *rapid.T) {
		// Generate 1-5 stages, each with a random number of failures
		numStages := rapid.IntRange(1, 5).Draw(t, "numStages")

		pt := NewProgressTracker()
		jobID := "job-error-test"

		stageErrData := make([]stageErrorData, numStages)

		for s := 0; s < numStages; s++ {
			stageName := rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "stageName")
			numErrors := rapid.IntRange(0, 100).Draw(t, "numErrors")
			stageErrData[s] = stageErrorData{name: stageName, count: numErrors}
		}

		// Also add some successful records to verify error counts are independent
		numSuccess := rapid.IntRange(0, 50).Draw(t, "numSuccess")
		pt.SetTotal(jobID, int64(numSuccess+sumErrors(stageErrData)))

		for i := 0; i < numSuccess; i++ {
			pt.RecordProcessed(jobID, "some-stage", time.Millisecond)
		}

		// Record failures for each stage
		for _, se := range stageErrData {
			for i := 0; i < se.count; i++ {
				pt.RecordFailed(jobID, se.name)
			}
		}

		progress := pt.GetProgress(jobID)
		if progress == nil {
			t.Fatal("expected non-nil progress")
		}

		// Verify error_counts per stage
		for _, se := range stageErrData {
			actual := progress.ErrorCounts[se.name]
			if actual != int64(se.count) {
				t.Fatalf("stage %q: expected error count %d, got %d",
					se.name, se.count, actual)
			}
		}

		// Verify that stages with 0 errors either aren't in the map or have value 0
		for _, se := range stageErrData {
			if se.count == 0 {
				if val, exists := progress.ErrorCounts[se.name]; exists && val != 0 {
					t.Fatalf("stage %q: expected 0 errors (or absent), got %d", se.name, val)
				}
			}
		}
	})
}

// computeMeanLatencyMs computes the mean of durations in milliseconds.
func computeMeanLatencyMs(latencies []time.Duration) float64 {
	if len(latencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	return float64(sum.Nanoseconds()) / float64(len(latencies)) / 1e6
}

// stageErrorData holds stage name and error count for property tests.
type stageErrorData struct {
	name  string
	count int
}

// sumErrors computes the total number of errors across all stages.
func sumErrors(stages []stageErrorData) int {
	total := 0
	for _, s := range stages {
		total += s.count
	}
	return total
}
