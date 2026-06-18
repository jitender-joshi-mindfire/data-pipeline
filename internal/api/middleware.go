package api

import (
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"time"
)

// statusRecorder wraps http.ResponseWriter to capture the status code.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}

// LoggingMiddleware logs each request with method, path, status code, and duration.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		recorder := &statusRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, recorder.statusCode, duration)
	})
}

// RecoveryMiddleware catches panics from downstream handlers, logs the stack
// trace, and returns a 500 Internal Server Error response.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				stack := debug.Stack()
				log.Printf("PANIC: %v\n%s", err, stack)
				http.Error(w, fmt.Sprintf(`{"error":"internal server error"}`), http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
