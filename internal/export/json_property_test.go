package export

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 12: JSON Export Round-Trip
// Validates: Requirements 7.3
//
// For any set of aggregated result records, writing them to a JSON file via the
// Exporter and then parsing the resulting JSON file shall produce records with
// identical field names and values.
func TestProperty12_JSONExportRoundTrip(t *testing.T) {
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
				// Choose between string and numeric values.
				// JSON round-trip preserves strings exactly and numbers as float64.
				useString := rapid.Bool().Draw(t, fmt.Sprintf("useString_%d_%d", r, fi))
				if useString {
					value := rapid.StringMatching(`[a-zA-Z0-9 .!@#%^&*()_+=\-]{1,30}`).Draw(t, fmt.Sprintf("strVal_%d_%d", r, fi))
					fields[name] = value
				} else {
					// Use float64 values since JSON unmarshals numbers as float64
					value := rapid.Float64Range(-1e6, 1e6).Draw(t, fmt.Sprintf("numVal_%d_%d", r, fi))
					fields[name] = value
				}
			}
			records[r] = &model.Record{
				ID:     fmt.Sprintf("rec-%d", r),
				Fields: fields,
			}
		}

		// Write records using JSONTarget
		tmpDir, tmpErr := os.MkdirTemp("", "json-export-roundtrip-*")
		if tmpErr != nil {
			t.Fatalf("failed to create temp dir: %v", tmpErr)
		}
		defer os.RemoveAll(tmpDir)
		jsonPath := filepath.Join(tmpDir, "roundtrip.json")
		target := NewJSONTarget(jsonPath)

		err := target.Write(context.Background(), records)
		if err != nil {
			t.Fatalf("JSONTarget.Write failed: %v", err)
		}

		// Read the JSON file back
		data, err := os.ReadFile(jsonPath)
		if err != nil {
			t.Fatalf("failed to read JSON file: %v", err)
		}

		var parsed []map[string]interface{}
		err = json.Unmarshal(data, &parsed)
		if err != nil {
			t.Fatalf("failed to parse JSON file: %v", err)
		}

		// Property: number of parsed records matches number of written records
		if len(parsed) != numRecords {
			t.Fatalf("record count mismatch: expected %d, got %d", numRecords, len(parsed))
		}

		// Property: each parsed record has identical field names and values
		for r := 0; r < numRecords; r++ {
			parsedRecord := parsed[r]
			originalFields := records[r].Fields

			// Check field count matches
			if len(parsedRecord) != len(originalFields) {
				t.Fatalf("record %d: field count mismatch: expected %d, got %d",
					r, len(originalFields), len(parsedRecord))
			}

			// Check each field name exists and value matches
			for fieldName, originalValue := range originalFields {
				parsedValue, exists := parsedRecord[fieldName]
				if !exists {
					t.Fatalf("record %d: missing field %q in parsed output", r, fieldName)
				}

				// Compare values accounting for JSON type handling:
				// - strings remain strings
				// - numbers become float64
				switch orig := originalValue.(type) {
				case string:
					parsedStr, ok := parsedValue.(string)
					if !ok {
						t.Fatalf("record %d, field %q: expected string type, got %T",
							r, fieldName, parsedValue)
					}
					if parsedStr != orig {
						t.Fatalf("record %d, field %q: string value mismatch: expected %q, got %q",
							r, fieldName, orig, parsedStr)
					}
				case float64:
					parsedNum, ok := parsedValue.(float64)
					if !ok {
						t.Fatalf("record %d, field %q: expected float64 type, got %T",
							r, fieldName, parsedValue)
					}
					if parsedNum != orig {
						t.Fatalf("record %d, field %q: numeric value mismatch: expected %v, got %v",
							r, fieldName, orig, parsedNum)
					}
				default:
					t.Fatalf("record %d, field %q: unexpected original value type %T",
						r, fieldName, originalValue)
				}
			}
		}
	})
}
