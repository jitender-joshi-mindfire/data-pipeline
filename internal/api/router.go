package api

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PipelineRunner is the interface for starting pipeline job execution.
type PipelineRunner interface {
	// RunJob starts asynchronous execution of a pipeline job by ID.
	RunJob(jobID string)
}

// Handler holds the dependencies needed to serve API requests.
type Handler struct {
	JobStore        JobStore
	ErrorStore      ErrorStore
	ProgressTracker ProgressTracker
	ResultStore     ResultStore
	Runner          PipelineRunner

	// CancelFuncs maps jobID → context.CancelFunc for running pipelines.
	// When a job is started, its cancel function is stored here.
	// When cancellation is requested, the function is called and removed.
	CancelFuncs sync.Map
}

// NewRouter creates an http.Handler with all API routes registered using the
// default production rate limiter. Use NewRouterWithLimiter(h, nil) in tests
// to disable rate limiting.
func NewRouter(h *Handler) http.Handler {
	return NewRouterWithLimiter(h, DefaultRateLimiter)
}

// NewRouterWithLimiter is like NewRouter but accepts an explicit rate limiter.
// Pass nil to disable rate limiting (e.g. in tests).
func NewRouterWithLimiter(h *Handler, limiter *ipRateLimiter) http.Handler {
	mux := http.NewServeMux()

	// Job lifecycle endpoints
	mux.HandleFunc("POST /api/v1/pipelines", h.CreateJob)
	mux.HandleFunc("GET /api/v1/pipelines", h.ListJobs)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", h.GetJob)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", h.DeleteJob)

	// Progress, results, errors, cancellation
	mux.HandleFunc("GET /api/v1/pipelines/{id}/progress", h.GetProgress)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/results", h.GetResults)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/errors", h.GetErrors)
	mux.HandleFunc("PATCH /api/v1/pipelines/{id}/cancel", h.CancelJob)

	// Prometheus metrics endpoint
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		newPipelineCollector(h.JobStore, h.ProgressTracker),
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	// Apply middleware (outermost first): recovery → rate-limit → CORS → logging → mux
	var handler http.Handler = mux
	handler = LoggingMiddleware(handler)
	handler = CORSMiddleware(handler)
	handler = RateLimitMiddleware(limiter, handler)
	handler = RecoveryMiddleware(handler)

	return handler
}
