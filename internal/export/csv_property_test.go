package export

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 11: CSV Export Round-Trip
// Validates: Requirements 7.2
//
// For any set of aggregated result records, writing them to a CSV file via the
// Exporter and then parsing the resulting CSV file shall produce records with
// identical field names and values.
func TestProperty11_CSVExportRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a set of field names (1 to 8 columns)
		numFields := rapid.IntRange(1, 8).Draw(t, "numFields")
		fieldNames := make([]string, numFields)
		usedNames := make(map[string]bool)
		for i := 0; i < numFields; i++ {
			var name string
			for {
				name = rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,14}`).Draw(t, fmt.Sprintf("fieldName_%d", i))
				if !usedNames[name] {
					usedNames[name] = true
					break
				}
			}
			fieldNames[i] = name
		}

		// Generate records (1 to 30 records)
		numRecords := rapid.IntRange(1, 30).Draw(t, "numRecords")
		records := make([]*model.Record, numRecords)
		for r := 0; r < numRecords; r++ {
			fields := make(map[string]interface{}, numFields)
			for fi, name := range fieldNames {
				// Generate string values that are safe for CSV round-trip.
				// The CSVTarget uses fmt.Sprintf("%v", val) which for strings
				// produces the string itself. We use strings to ensure clean
				// round-trip since CSV is inherently a text format.
				value := rapid.StringMatching(`[a-zA-Z0-9 .!@#%^&*()_+=\-]{1,30}`).Draw(t, fmt.Sprintf("val_%d_%d", r, fi))
				fields[name] = value
			}
			records[r] = &model.Record{
				ID:     fmt.Sprintf("rec-%d", r),
				Fields: fields,
			}
		}

		// Write records using CSVTarget
		tmpDir, tmpErr := os.MkdirTemp("", "csv-export-roundtrip-*")
		if tmpErr != nil {
			t.Fatalf("failed to create temp dir: %v", tmpErr)
		}
		defer os.RemoveAll(tmpDir)
		csvPath := filepath.Join(tmpDir, "roundtrip.csv")
		target := NewCSVTarget(csvPath)

		err := target.Write(context.Background(), records)
		if err != nil {
			t.Fatalf("CSVTarget.Write failed: %v", err)
		}

		// Read the CSV file back
		file, err := os.Open(csvPath)
		if err != nil {
			t.Fatalf("failed to open CSV file: %v", err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		rows, err := reader.ReadAll()
		if err != nil {
			t.Fatalf("failed to read CSV file: %v", err)
		}

		// Property: there should be a header + numRecords data rows
		if len(rows) != numRecords+1 {
			t.Fatalf("row count mismatch: expected %d (header + %d data), got %d",
				numRecords+1, numRecords, len(rows))
		}

		// The header should contain sorted field names
		sortedFields := make([]string, len(fieldNames))
		copy(sortedFields, fieldNames)
		sort.Strings(sortedFields)

		header := rows[0]
		if len(header) != len(sortedFields) {
			t.Fatalf("header field count mismatch: expected %d, got %d", len(sortedFields), len(header))
		}
		for i, name := range sortedFields {
			if header[i] != name {
				t.Fatalf("header field mismatch at index %d: expected %q, got %q", i, name, header[i])
			}
		}

		// Property: each parsed row has identical field values to the original record
		for r := 0; r < numRecords; r++ {
			row := rows[r+1] // skip header
			if len(row) != len(sortedFields) {
				t.Fatalf("row %d: column count mismatch: expected %d, got %d", r, len(sortedFields), len(row))
			}
			for i, fieldName := range sortedFields {
				expectedValue := fmt.Sprintf("%v", records[r].Fields[fieldName])
				actualValue := row[i]
				if actualValue != expectedValue {
					t.Fatalf("row %d, field %q: value mismatch: expected %q, got %q",
						r, fieldName, expectedValue, actualValue)
				}
			}
		}
	})
}
