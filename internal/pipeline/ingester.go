package pipeline

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

// CSVSource reads records from a CSV file. The first row is treated as the
// header defining field names. Each subsequent row emits one Record with
// fields mapped to the corresponding header names.
type CSVSource struct {
	FilePath   string
	JobID      string
	ErrorStore store.ErrorStore
}

// Read opens the CSV file, parses it using encoding/csv, and sends records
// to the out channel. The first row is treated as the header. Errors are
// logged to the ErrorStore and processing continues with remaining rows.
func (s *CSVSource) Read(ctx context.Context, out chan<- *model.Record) error {
	file, err := os.Open(s.FilePath)
	if err != nil {
		s.logError(fmt.Sprintf("failed to open CSV file %s: %v", s.FilePath, err), nil)
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			// Empty file, no records to emit
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


