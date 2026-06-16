package pipeline

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 3: CSV Parsing Round-Trip
// Validates: Requirements 3.1, 15.5
//
// For any valid tabular dataset (list of records with consistent field names),
// writing the dataset as CSV (with header row) and parsing it back through the
// Ingester shall produce records with identical field names and field values.
func TestProperty3_CSVParsingRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a set of field names (1 to 10 columns)
		numFields := rapid.IntRange(1, 10).Draw(t, "numFields")
		fieldNames := make([]string, numFields)
		usedNames := make(map[string]bool)
		for i := 0; i < numFields; i++ {
			// Generate unique, non-empty field names using safe CSV characters
			var name string
			for {
				name = rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,19}`).Draw(t, fmt.Sprintf("fieldName_%d", i))
				if !usedNames[name] {
					usedNames[name] = true
					break
				}
			}
			fieldNames[i] = name
		}

		// Generate rows of data (1 to 50 rows)
		numRows := rapid.IntRange(1, 50).Draw(t, "numRows")
		rows := make([]map[string]string, numRows)
		for r := 0; r < numRows; r++ {
			row := make(map[string]string, numFields)
			for fi, name := range fieldNames {
				// Generate values safe for CSV (no newlines).
				// For single-column CSVs, the last row's empty value is indistinguishable
				// from EOF in encoding/csv, so we ensure at least one non-empty value per row
				// when there's only one column.
				var value string
				if numFields == 1 {
					// Single column: must have non-empty values for CSV round-trip fidelity
					value = rapid.StringMatching(`[a-zA-Z0-9 ,."'!@#$%^&*()_+=\-\[\]{}|;:<>?/~]{1,50}`).Draw(t, fmt.Sprintf("val_%d_%d", fi, r))
				} else {
					// Multiple columns: empty values are safe (produce distinguishable CSV)
					value = rapid.StringMatching(`[a-zA-Z0-9 ,."'!@#$%^&*()_+=\-\[\]{}|;:<>?/~]{0,50}`).Draw(t, fmt.Sprintf("val_%d_%d", fi, r))
				}
				row[name] = value
			}
			rows[r] = row
		}

		// Write the dataset as a CSV file
		tmpDir, tmpErr := os.MkdirTemp("", "csv-roundtrip-*")
		if tmpErr != nil {
			t.Fatalf("failed to create temp dir: %v", tmpErr)
		}
		defer os.RemoveAll(tmpDir)
		csvPath := filepath.Join(tmpDir, "roundtrip.csv")

		file, err := os.Create(csvPath)
		if err != nil {
			t.Fatalf("failed to create temp CSV file: %v", err)
		}

		writer := csv.NewWriter(file)

		// Write header
		if err := writer.Write(fieldNames); err != nil {
			file.Close()
			t.Fatalf("failed to write CSV header: %v", err)
		}

		// Write data rows
		for _, row := range rows {
			record := make([]string, numFields)
			for i, name := range fieldNames {
				record[i] = row[name]
			}
			if err := writer.Write(record); err != nil {
				file.Close()
				t.Fatalf("failed to write CSV row: %v", err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			file.Close()
			t.Fatalf("CSV writer flush error: %v", err)
		}
		file.Close()

		// Parse back through CSVSource
		errStore := store.NewInMemoryErrorStore()
		source := &CSVSource{
			FilePath:   csvPath,
			JobID:      "roundtrip-test",
			ErrorStore: errStore,
		}

		out := make(chan *model.Record, numRows+10)
		readErr := source.Read(context.Background(), out)
		close(out)

		if readErr != nil {
			t.Fatalf("CSVSource.Read returned error: %v", readErr)
		}

		// Check no errors were logged
		errs, errCount := errStore.GetByJob("roundtrip-test", 0, 100)
		if errCount > 0 {
			t.Fatalf("expected no errors during CSV parsing, got %d: %v", errCount, errs)
		}

		// Collect parsed records
		var parsed []*model.Record
		for rec := range out {
			parsed = append(parsed, rec)
		}

		// Property: number of parsed records equals number of written rows
		if len(parsed) != numRows {
			t.Fatalf("record count mismatch: wrote %d rows, parsed %d records", numRows, len(parsed))
		}

		// Property: each parsed record has identical field names and values
		for i, rec := range parsed {
			// Check field names match
			parsedFieldNames := make([]string, 0, len(rec.Fields))
			for name := range rec.Fields {
				parsedFieldNames = append(parsedFieldNames, name)
			}
			sort.Strings(parsedFieldNames)

			expectedFieldNames := make([]string, len(fieldNames))
			copy(expectedFieldNames, fieldNames)
			sort.Strings(expectedFieldNames)

			if len(parsedFieldNames) != len(expectedFieldNames) {
				t.Fatalf("row %d: field count mismatch: expected %d, got %d (expected: %v, got: %v)",
					i, len(expectedFieldNames), len(parsedFieldNames), expectedFieldNames, parsedFieldNames)
			}

			for j := range expectedFieldNames {
				if parsedFieldNames[j] != expectedFieldNames[j] {
					t.Fatalf("row %d: field name mismatch at index %d: expected %q, got %q",
						i, j, expectedFieldNames[j], parsedFieldNames[j])
				}
			}

			// Check field values match
			for _, name := range fieldNames {
				expectedValue := rows[i][name]
				actualValue, ok := rec.Fields[name]
				if !ok {
					t.Fatalf("row %d: missing field %q in parsed record", i, name)
				}
				actualStr, ok := actualValue.(string)
				if !ok {
					t.Fatalf("row %d: field %q has non-string type %T", i, name, actualValue)
				}
				if actualStr != expectedValue {
					t.Fatalf("row %d: field %q value mismatch: expected %q, got %q",
						i, name, expectedValue, actualStr)
				}
			}

			// Check source metadata
			if rec.Metadata.SourceType != "csv" {
				t.Fatalf("row %d: expected source type 'csv', got %q", i, rec.Metadata.SourceType)
			}
			if rec.Metadata.SourceID != csvPath {
				t.Fatalf("row %d: expected source ID %q, got %q", i, csvPath, rec.Metadata.SourceID)
			}
		}
	})
}
