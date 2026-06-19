package pipeline

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// JSONSource reads JSON data from a file and emits records.
// It supports both JSON array of objects and newline-delimited JSON (NDJSON).
type JSONSource struct {
	filePath string
}

// NewJSONSource creates a new JSONSource for the given file path.
func NewJSONSource(filePath string) *JSONSource {
	return &JSONSource{filePath: filePath}
}

// Type returns "json".
func (s *JSONSource) Type() string {
	return "json"
}

// Identifier returns the file path.
func (s *JSONSource) Identifier() string {
	return s.filePath
}

// Read reads JSON data from the file and sends records to the out channel.
// It first attempts to parse the file as a JSON array of objects. If that fails,
// it falls back to newline-delimited JSON (one JSON object per line).
func (s *JSONSource) Read(ctx context.Context, out chan<- *model.Record) error {
	f, err := os.Open(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to open JSON file %s: %w", s.filePath, err)
	}
	defer f.Close()

	// Try parsing as a JSON array first
	if err := s.readArray(ctx, f, out); err != nil {
		// If array parsing fails, seek back to start and try NDJSON
		if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
			return fmt.Errorf("failed to seek JSON file %s: %w", s.filePath, seekErr)
		}
		return s.readNDJSON(ctx, f, out)
	}
	return nil
}

// readArray attempts to parse the file as a JSON array of objects.
func (s *JSONSource) readArray(ctx context.Context, r io.Reader, out chan<- *model.Record) error {
	dec := json.NewDecoder(r)

	// Read the opening bracket
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("failed to read JSON token: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		return fmt.Errorf("expected JSON array, got %v", tok)
	}

	index := 0
	for dec.More() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var obj map[string]interface{}
		if err := dec.Decode(&obj); err != nil {
			return fmt.Errorf("failed to decode JSON object at index %d: %w", index, err)
		}

		id, err := generateRecordID()
		if err != nil {
			return fmt.Errorf("failed to generate record ID: %w", err)
		}

		record := &model.Record{
			ID:     id,
			Fields: stringifyFields(obj),
			Metadata: model.RecordMetadata{
				SourceType: "json",
				SourceID:   s.filePath,
				LineNumber: index + 1,
			},
		}

		select {
		case out <- record:
		case <-ctx.Done():
			return ctx.Err()
		}

		index++
	}

	// Read the closing bracket
	_, err = dec.Token()
	if err != nil {
		return fmt.Errorf("failed to read closing bracket: %w", err)
	}

	return nil
}

// readNDJSON parses the file as newline-delimited JSON (one JSON object per line).
func (s *JSONSource) readNDJSON(ctx context.Context, r io.Reader, out chan<- *model.Record) error {
	scanner := bufio.NewScanner(r)
	lineNum := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lineNum++
		line := scanner.Bytes()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		var obj map[string]interface{}
		if err := json.Unmarshal(line, &obj); err != nil {
			return fmt.Errorf("failed to parse NDJSON at line %d: %w", lineNum, err)
		}

		id, err := generateRecordID()
		if err != nil {
			return fmt.Errorf("failed to generate record ID: %w", err)
		}

		record := &model.Record{
			ID:     id,
			Fields: stringifyFields(obj),
			Metadata: model.RecordMetadata{
				SourceType: "json",
				SourceID:   s.filePath,
				LineNumber: lineNum,
			},
		}

		select {
		case out <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading NDJSON file %s: %w", s.filePath, err)
	}

	return nil
}

// stringifyFields converts all scalar JSON values to strings so that records
// from JSON sources are consistent with CSV records (which are always strings).
// This lets the validator apply the same type rules regardless of source type.
func stringifyFields(obj map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(obj))
	for k, v := range obj {
		switch val := v.(type) {
		case string:
			out[k] = val
		case float64:
			// Use strconv to avoid scientific notation for large integers
			if val == float64(int64(val)) {
				out[k] = strconv.FormatInt(int64(val), 10)
			} else {
				out[k] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		case bool:
			out[k] = strconv.FormatBool(val)
		case nil:
			out[k] = nil
		default:
			out[k] = fmt.Sprintf("%v", val)
		}
	}
	return out
}

// generateRecordID generates a version 4 UUID string for record identification.
func generateRecordID() (string, error) {
	var uuid [16]byte
	_, err := rand.Read(uuid[:])
	if err != nil {
		return "", err
	}

	// Set version 4 bits
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Set variant bits
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}
