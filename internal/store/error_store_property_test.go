package store

import (
	"fmt"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 16: Error Store Message Truncation
// Validates: Requirements 11.1
//
// For any error message of arbitrary length stored in the Error_Store,
// the stored message shall be at most 1000 characters, truncating longer
// messages while preserving the first 1000 characters.
func TestProperty16_ErrorStoreMessageTruncation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate an arbitrary length string (0 to 5000 chars)
		msg := rapid.StringOfN(rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !@#")),
			0, 5000, -1).Draw(t, "message")

		store := NewInMemoryErrorStore()
		entry := model.ErrorEntry{
			Stage:   "validator",
			Message: msg,
			Record:  map[string]interface{}{"field": "value"},
		}

		store.Add("job-test", entry)

		errors, total := store.GetByJob("job-test", 0, 50)
		if total != 1 {
			t.Fatalf("expected 1 error stored, got %d", total)
		}

		storedMsg := errors[0].Message

		// Property: stored message is at most 1000 characters
		if len(storedMsg) > 1000 {
			t.Fatalf("stored message length %d exceeds 1000 characters", len(storedMsg))
		}

		// Property: messages <= 1000 chars are stored unchanged
		if len(msg) <= 1000 {
			if storedMsg != msg {
				t.Fatalf("message of length %d was modified: got %q, want %q", len(msg), storedMsg, msg)
			}
		}

		// Property: messages > 1000 chars are truncated to exactly the first 1000 characters
		if len(msg) > 1000 {
			if len(storedMsg) != 1000 {
				t.Fatalf("expected truncated message length=1000, got %d", len(storedMsg))
			}
			if storedMsg != msg[:1000] {
				t.Fatal("truncated message does not match first 1000 characters of original")
			}
		}
	})
}

// Feature: data-processing-pipeline, Property 17: Error Pagination Correctness
// Validates: Requirements 11.2, 11.3
//
// For any job with N total errors and pagination parameters (offset, limit),
// the returned error list shall contain exactly min(limit, N−offset) errors
// (or 0 if offset ≥ N), the total count shall equal N, and the default limit
// shall be 50 with a maximum of 200.
func TestProperty17_ErrorPaginationCorrectness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random number of errors to store (0 to 300)
		n := rapid.IntRange(0, 300).Draw(t, "numErrors")
		// Generate offset (including negative values to test default behavior, and beyond N)
		offset := rapid.IntRange(-5, 350).Draw(t, "offset")
		// Generate limit (including 0 for default, negative for default, and > 200 for max cap)
		limit := rapid.IntRange(-5, 500).Draw(t, "limit")

		store := NewInMemoryErrorStore()
		jobID := fmt.Sprintf("job-prop17-%d", n)

		// Add N errors to the store
		for i := 0; i < n; i++ {
			store.Add(jobID, model.ErrorEntry{
				Stage:   "validator",
				Message: fmt.Sprintf("error %d", i),
				Record:  map[string]interface{}{"index": i},
			})
		}

		// Query with the generated pagination parameters
		errors, total := store.GetByJob(jobID, offset, limit)

		// Property 1: total count always equals N
		if total != n {
			t.Fatalf("expected total=%d, got %d (offset=%d, limit=%d)", n, total, offset, limit)
		}

		// Compute effective offset (negative offset is treated as 0)
		effectiveOffset := offset
		if effectiveOffset < 0 {
			effectiveOffset = 0
		}

		// Compute effective limit (<=0 defaults to 50, >200 caps at 200)
		effectiveLimit := limit
		if effectiveLimit <= 0 {
			effectiveLimit = 50
		}
		if effectiveLimit > 200 {
			effectiveLimit = 200
		}

		// Compute expected count: min(effectiveLimit, N - effectiveOffset), or 0 if offset >= N
		var expectedCount int
		if effectiveOffset >= n {
			expectedCount = 0
		} else {
			remaining := n - effectiveOffset
			if effectiveLimit < remaining {
				expectedCount = effectiveLimit
			} else {
				expectedCount = remaining
			}
		}

		// Property 2: returned error count matches expected pagination result
		if len(errors) != expectedCount {
			t.Fatalf("expected %d errors, got %d (N=%d, offset=%d, limit=%d, effectiveOffset=%d, effectiveLimit=%d)",
				expectedCount, len(errors), n, offset, limit, effectiveOffset, effectiveLimit)
		}
	})
}
