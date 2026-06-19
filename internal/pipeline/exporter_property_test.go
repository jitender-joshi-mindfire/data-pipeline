package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/export"
	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 13: Export Target Independence
// Validates: Requirements 7.4, 7.5
//
// For any set of export targets where one or more targets fail, all non-failing
// targets shall still receive the complete set of aggregated results.
func TestProperty13_ExportTargetIndependence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random aggregated result records (1 to 20 records)
		numRecords := rapid.IntRange(1, 20).Draw(t, "numRecords")
		numFields := rapid.IntRange(1, 5).Draw(t, "numFields")

		// Generate unique field names
		fieldNames := make([]string, numFields)
		usedNames := make(map[string]bool)
		for i := 0; i < numFields; i++ {
			var name string
			for {
				name = rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,9}`).Draw(t, fmt.Sprintf("fieldName_%d", i))
				if !usedNames[name] {
					usedNames[name] = true
					break
				}
			}
			fieldNames[i] = name
		}

		// Generate records
		records := make([]*model.Record, numRecords)
		for r := 0; r < numRecords; r++ {
			fields := make(map[string]interface{}, numFields)
			for _, name := range fieldNames {
				fields[name] = rapid.StringMatching(`[a-zA-Z0-9 ]{1,20}`).Draw(t, fmt.Sprintf("val_%d_%s", r, name))
			}
			records[r] = &model.Record{
				ID:     fmt.Sprintf("rec-%d", r),
				Fields: fields,
			}
		}

		// Generate a random number of export targets (2 to 6)
		numTargets := rapid.IntRange(2, 6).Draw(t, "numTargets")

		// Randomly decide which targets should fail.
		// Ensure at least 1 fails and at least 1 succeeds for property to be meaningful.
		failFlags := make([]bool, numTargets)
		hasFailing := false
		hasSucceeding := false
		for i := 0; i < numTargets; i++ {
			failFlags[i] = rapid.Bool().Draw(t, fmt.Sprintf("targetFails_%d", i))
			if failFlags[i] {
				hasFailing = true
			} else {
				hasSucceeding = true
			}
		}
		// Force at least one failing and one succeeding
		if !hasFailing {
			idx := rapid.IntRange(0, numTargets-1).Draw(t, "forceFail")
			failFlags[idx] = true
		}
		if !hasSucceeding {
			idx := rapid.IntRange(0, numTargets-1).Draw(t, "forceSucceed")
			failFlags[idx] = false
		}

		// Create targets with some configured to fail
		targets := make([]export.ExportTarget, numTargets)
		for i := 0; i < numTargets; i++ {
			identifier := fmt.Sprintf("target-%d", i)
			mock := newMockTarget(identifier, "mock")
			if failFlags[i] {
				mock.writeErr = errors.New(fmt.Sprintf("simulated failure for %s", identifier))
			}
			targets[i] = mock
		}

		// Run the exporter stage
		errStore := store.NewInMemoryErrorStore()
		stage := NewExporterStage("job-prop13", targets, errStore)

		in := make(chan *model.Record, numRecords)
		out := make(chan *model.Record, numRecords+1) // buffered so exporter never blocks
		for _, r := range records {
			in <- r
		}
		close(in)

		err := stage.Run(context.Background(), in, out)
		if err != nil {
			t.Fatalf("ExporterStage.Run returned unexpected error: %v", err)
		}

		// Count expected failures
		expectedErrors := 0
		for _, fails := range failFlags {
			if fails {
				expectedErrors++
			}
		}

		// Verify: all non-failing targets received the complete set of records
		for i := 0; i < numTargets; i++ {
			mock := targets[i].(*mockExportTarget)
			if failFlags[i] {
				// Failing target should not have received records
				mock.mu.Lock()
				writtenCount := len(mock.written)
				mock.mu.Unlock()
				if writtenCount != 0 {
					t.Fatalf("failing target %d (%s) should not have received records, but got %d",
						i, mock.Identifier(), writtenCount)
				}
			} else {
				// Non-failing target must receive ALL records
				mock.mu.Lock()
				writtenCount := len(mock.written)
				mock.mu.Unlock()
				if writtenCount != numRecords {
					t.Fatalf("non-failing target %d (%s) should have received %d records, but got %d",
						i, mock.Identifier(), numRecords, writtenCount)
				}

				// Verify the records match by ID
				mock.mu.Lock()
				for r := 0; r < numRecords; r++ {
					if mock.written[r].ID != records[r].ID {
						t.Fatalf("non-failing target %d: record %d ID mismatch: expected %q, got %q",
							i, r, records[r].ID, mock.written[r].ID)
					}
				}
				mock.mu.Unlock()
			}
		}

		// Verify: errors were logged for each failing target
		_, totalErrors := errStore.GetByJob("job-prop13", 0, 200)
		if totalErrors != expectedErrors {
			t.Fatalf("expected %d errors logged (one per failing target), got %d", expectedErrors, totalErrors)
		}

		// Verify all logged errors are from the exporter stage
		storedErrors, _ := errStore.GetByJob("job-prop13", 0, 200)
		for _, entry := range storedErrors {
			if entry.Stage != "exporter" {
				t.Fatalf("expected error stage 'exporter', got %q", entry.Stage)
			}
		}
	})
}
