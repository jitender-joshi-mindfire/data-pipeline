package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/uid"
)

// DefaultHTTPTimeout is the default timeout for HTTP API requests.
const DefaultHTTPTimeout = 30 * time.Second

// HTTPSource reads data from an HTTP API endpoint and emits Records.
type HTTPSource struct {
	URL            string
	TimeoutSeconds int
}

// NewHTTPSource creates a new HTTPSource with the given URL and timeout.
// If timeoutSeconds is 0 or negative, DefaultHTTPTimeout is used.
func NewHTTPSource(url string, timeoutSeconds int) *HTTPSource {
	return &HTTPSource{
		URL:            url,
		TimeoutSeconds: timeoutSeconds,
	}
}

// Read fetches data from the HTTP endpoint via GET, parses the response body
// as a JSON array of objects, and emits one Record per object to the out channel.
func (h *HTTPSource) Read(ctx context.Context, out chan<- *model.Record) error {
	timeout := DefaultHTTPTimeout
	if h.TimeoutSeconds > 0 {
		timeout = time.Duration(h.TimeoutSeconds) * time.Second
	}

	client := &http.Client{
		Timeout: timeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP GET failed for %s: %w", h.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP GET %s returned status %d", h.URL, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body from %s: %w", h.URL, err)
	}

	var objects []map[string]interface{}
	if err := json.Unmarshal(body, &objects); err != nil {
		return fmt.Errorf("failed to parse JSON response from %s: %w", h.URL, err)
	}

	for i, obj := range objects {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		id, err := generateRecordUUID()
		if err != nil {
			return fmt.Errorf("failed to generate record ID: %w", err)
		}

		record := &model.Record{
			ID:     id,
			Fields: stringifyFields(obj),
			Metadata: model.RecordMetadata{
				SourceType: "http",
				SourceID:   h.URL,
				LineNumber: i + 1,
			},
		}

		select {
		case out <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// Type returns the source type identifier.
func (h *HTTPSource) Type() string {
	return "http"
}

// Identifier returns the source identifier (the URL).
func (h *HTTPSource) Identifier() string {
	return h.URL
}

func generateRecordUUID() (string, error) { return uid.New() }
