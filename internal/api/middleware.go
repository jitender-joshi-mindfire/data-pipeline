package api

import (
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"sync"
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

// CORSMiddleware adds permissive CORS headers so browser-based clients can
// reach the API. Preflight OPTIONS requests are answered immediately with 204.
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ipRateLimiter is a simple token-bucket rate limiter keyed by remote IP.
// Each IP is allowed burst requests immediately, then refillRate per second.
type ipRateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*tokenBucket
	burst    int
	rate     float64 // tokens per second
}

type tokenBucket struct {
	tokens   float64
	lastSeen time.Time
}

func newIPRateLimiter(burst int, ratePerSecond float64) *ipRateLimiter {
	return &ipRateLimiter{
		visitors: make(map[string]*tokenBucket),
		burst:    burst,
		rate:     ratePerSecond,
	}
}

func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b, ok := l.visitors[ip]
	if !ok {
		l.visitors[ip] = &tokenBucket{tokens: float64(l.burst) - 1, lastSeen: now}
		return true
	}

	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens += elapsed * l.rate
	if b.tokens > float64(l.burst) {
		b.tokens = float64(l.burst)
	}
	b.lastSeen = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// DefaultRateLimiter is the production rate limiter: 200 burst, 50 req/s per IP.
// Tests that go through NewRouter directly get this limiter unless they call
// NewRouterWithLimiter(h, nil) to disable it.
var DefaultRateLimiter = newIPRateLimiter(200, 50)

// remoteIP extracts the IP address (without port) from r.RemoteAddr.
func remoteIP(r *http.Request) string {
	addr := r.RemoteAddr
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}

// RateLimitMiddleware rejects requests that exceed the per-IP rate limit with 429.
// If limiter is nil the middleware is a no-op (useful in tests).
func RateLimitMiddleware(limiter *ipRateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limiter != nil && !limiter.allow(remoteIP(r)) {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
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
