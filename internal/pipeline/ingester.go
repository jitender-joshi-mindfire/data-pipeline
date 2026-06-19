package pipeline

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

// CSVSource reads records from a CSV file or a remote CSV URL.
// When FilePath starts with "http://" or "https://", the CSV is fetched via
// HTTP GET and streamed directly into the parser — no local file is written.
// The first row is treated as the header defining field names.
type CSVSource struct {
	FilePath    string
	JobID       string
	ErrorStore  store.ErrorStore
	HTTPTimeout time.Duration // timeout for remote fetches (default 30s)
}

// isRemoteURL returns true if the path is an HTTP or HTTPS URL.
func isRemoteURL(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

// openCSVReader returns an io.ReadCloser for the CSV data, whether local or remote.
func (s *CSVSource) openCSVReader(ctx context.Context) (io.ReadCloser, error) {
	if isRemoteURL(s.FilePath) {
		timeout := s.HTTPTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		client := &http.Client{Timeout: timeout}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.FilePath, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to build HTTP request for %s: %w", s.FilePath, err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("HTTP GET failed for CSV %s: %w", s.FilePath, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP GET %s returned status %d", s.FilePath, resp.StatusCode)
		}
		return resp.Body, nil
	}
	return os.Open(s.FilePath)
}

// Read fetches or opens the CSV source, parses it using encoding/csv, and
// sends records to the out channel. The first row is treated as the header.
// Errors are logged to the ErrorStore and processing continues with remaining rows.
func (s *CSVSource) Read(ctx context.Context, out chan<- *model.Record) error {
	rc, err := s.openCSVReader(ctx)
	if err != nil {
		s.logError(fmt.Sprintf("failed to open CSV source %s: %v", s.FilePath, err), nil)
		return err
	}
	defer rc.Close()

	reader := csv.NewReader(rc)
	reader.LazyQuotes = true     // tolerate bare quotes in fields (real-world CSVs)
	reader.TrimLeadingSpace = true // trim spaces after field separators

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		s.logError(fmt.Sprintf("failed to read CSV header from %s: %v", s.FilePath, err), nil)
		return err
	}

	lineNumber := 1 // Header is line 1, data starts at line 2
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		lineNumber++

		if err != nil {
			s.logError(fmt.Sprintf("failed to read CSV row at line %d from %s: %v", lineNumber, s.FilePath, err), nil)
			continue
		}


		// Map row values to header names
		fields := make(map[string]interface{}, len(header))
		for i, name := range header {
			if i < len(row) {
				fields[name] = row[i]
			} else {
				fields[name] = ""
			}
		}

		id, err := generateRecordID()
		if err != nil {
			s.logError(fmt.Sprintf("failed to generate record ID at line %d: %v", lineNumber, err), fields)
			continue
		}

		record := &model.Record{
			ID:     id,
			Fields: fields,
			Metadata: model.RecordMetadata{
				SourceType: "csv",
				SourceID:   s.FilePath,
				LineNumber: lineNumber,
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
func (s *CSVSource) Type() string {
	return "csv"
}

// Identifier returns the file path of this CSV source.
func (s *CSVSource) Identifier() string {
	return s.FilePath
}

// logError logs an error entry to the ErrorStore for this source.
func (s *CSVSource) logError(message string, record map[string]interface{}) {
	if s.ErrorStore == nil {
		return
	}
	s.ErrorStore.Add(s.JobID, model.ErrorEntry{
		Stage:     "ingester",
		Message:   message,
		Record:    record,
		Timestamp: time.Now().UTC(),
	})
}


