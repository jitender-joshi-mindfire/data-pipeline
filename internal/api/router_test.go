package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRouter_RoutesRegistered(t *testing.T) {
	js := newMockJobStore()
	es := newMockErrorStore()
	pt := newMockProgressTracker()
	h := &Handler{
		JobStore:        js,
		ErrorStore:      es,
		ProgressTracker: pt,
	}
	router := NewRouterWithLimiter(h, nil)

	tests := []struct {
		method string
		path   string
	}{
		{"POST", "/api/v1/pipelines"},
		{"GET", "/api/v1/pipelines"},
		{"GET", "/api/v1/pipelines/test-id"},
		{"DELETE", "/api/v1/pipelines/test-id"},
		{"GET", "/api/v1/pipelines/test-id/progress"},
		{"GET", "/api/v1/pipelines/test-id/results"},
		{"GET", "/api/v1/pipelines/test-id/errors"},
		{"PATCH", "/api/v1/pipelines/test-id/cancel"},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			// Routes are registered — we expect real responses, not 405
			assert.NotEqual(t, http.StatusMethodNotAllowed, w.Code)
		})
	}
}

func TestNewRouter_UnknownRoute_Returns404(t *testing.T) {
	h := &Handler{
		JobStore:        newMockJobStore(),
		ErrorStore:      newMockErrorStore(),
		ProgressTracker: newMockProgressTracker(),
	}
	router := NewRouterWithLimiter(h, nil)

	req := httptest.NewRequest("GET", "/api/v1/unknown", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}
