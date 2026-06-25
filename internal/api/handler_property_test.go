package api

// Feature: data-processing-pipeline, Property 15: Non-Existent Job Returns 404

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// TestProperty15_NonExistentJobReturns404 verifies that all API endpoints
// return HTTP 404 for any UUID not assigned to a created job.
//
// **Validates: Requirements 8.5, 11.5**
func TestProperty15_NonExistentJobReturns404(t *testing.T) {
	// Set up the handler with empty stores (no jobs exist)
	js := newMockJobStore()
	es := newMockErrorStore()
	pt := newMockProgressTracker()
	rs := newMockResultStore()

	h := &Handler{
		JobStore:        js,
		ErrorStore:      es,
		ProgressTracker: pt,
		ResultStore:     rs,
	}

	router := NewRouterWithLimiter(h, nil)

	rapid.Check(t, func(t *rapid.T) {
		// Generate a random UUID-like string that is guaranteed to not be in the store.
		// We use rapid to generate random hex segments in UUID format.
		a := rapid.Uint32().Draw(t, "uuid-a")
		b := rapid.Uint16().Draw(t, "uuid-b")
		c := rapid.Uint16().Draw(t, "uuid-c")
		d := rapid.Uint16().Draw(t, "uuid-d")
		e := rapid.Uint64().Draw(t, "uuid-e")

		randomID := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", a, b, c, d, e&0xffffffffffff)

		// Define all endpoints that should return 404 for non-existent job IDs
		endpoints := []struct {
			method string
			path   string
		}{
			{"GET", "/api/v1/pipelines/" + randomID},
			{"GET", "/api/v1/pipelines/" + randomID + "/progress"},
			{"GET", "/api/v1/pipelines/" + randomID + "/results"},
			{"GET", "/api/v1/pipelines/" + randomID + "/errors"},
			{"PATCH", "/api/v1/pipelines/" + randomID + "/cancel"},
			{"DELETE", "/api/v1/pipelines/" + randomID},
		}

		for _, ep := range endpoints {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusNotFound, w.Code,
				"Expected 404 for %s %s with non-existent ID %s, got %d",
				ep.method, ep.path, randomID, w.Code)
		}
	})
}
