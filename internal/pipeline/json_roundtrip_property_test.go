package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 4: JSON Parsing Round-Trip
// Validates: Requirements 3.2, 15.5
//
// For any valid list of records (maps of string keys to JSON-compatible values),
// serializing as a JSON array and parsing through the Ingester shall produce
// records with identical field names and field values.
func TestProperty4_JSONParsingRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a set of field names (1 to 10 columns)
		numFields := rapid.IntRange(1, 10).Draw(t, "numFields")
		fieldNames := make([]string, numFields)
		usedNames := make(map[string]bool)
		for i := 0; i < numFields; i++ {
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

		// Generate rows of data (1 to 50 rows) with JSON-compatible values
		numRows := rapid.IntRange(1, 50).Draw(t, "numRows")
		rows := make([]map[string]interface{}, numRows)
		for r := 0; r < numRows; r++ {
			row := make(map[string]interface{}, numFields)
			for fi, name := range fieldNames {
				// Choose a JSON-compatible value type: string, float64, or bool
				valueType := rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("valType_%d_%d", fi, r))
				switch valueType {
				case 0:
					// String value
					row[name] = rapid.StringMatching(`[a-zA-Z0-9 _\-]{0,50}`).Draw(t, fmt.Sprintf("strVal_%d_%d", fi, r))
				case 1:
					// Number value (JSON numbers decode as float64)
					row[name] = rapid.Float64Range(-1e6, 1e6).Draw(t, fmt.Sprintf("numVal_%d_%d", fi, r))
				case 2:
					// Boolean value
					row[name] = rapid.Bool().Draw(t, fmt.Sprintf("boolVal_%d_%d", fi, r))
				}
			}
			rows[r] = row
		}

		// Serialize as a JSON array to a temp file
		tmpDir, tmpErr := os.MkdirTemp("", "json-roundtrip-*")
		if tmpErr != nil {
			t.Fatalf("failed to create temp dir: %v", tmpErr)
		}
		defer os.RemoveAll(tmpDir)
		jsonPath := filepath.Join(tmpDir, "roundtrip.json")

		data, err := json.Marshal(rows)
		if err != nil {
			t.Fatalf("failed to marshal JSON: %v", err)
		}

		if err := os.WriteFile(jsonPath, data, 0644); err != nil {
			t.Fatalf("failed to write JSON file: %v", err)
		}

		// Parse back through JSONSource
		source := NewJSONSource(jsonPath)
		out := make(chan *model.Record, numRows+10)

		readErr := source.Read(context.Background(), out)
		close(out)

		if readErr != nil {
			t.Fatalf("JSONSource.Read returned error: %v", readErr)
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

				// All values are stringified at ingestion time for consistency with CSV sources.
				// Compare the string representation of the expected value with the actual string.
				av, ok := actualValue.(string)
				if !ok {
					t.Fatalf("row %d: field %q expected string after ingestion, got %T", i, name, actualValue)
				}
				var expectedStr string
				switch ev := expectedValue.(type) {
				case string:
					expectedStr = ev
				case float64:
					if ev == float64(int64(ev)) {
						expectedStr = fmt.Sprintf("%d", int64(ev))
					} else {
						expectedStr = strconv.FormatFloat(ev, 'f', -1, 64)
					}
				case bool:
					expectedStr = strconv.FormatBool(ev)
				default:
					t.Fatalf("row %d: field %q has unexpected type %T", i, name, expectedValue)
				}
				if av != expectedStr {
					t.Fatalf("row %d: field %q value mismatch: expected %q, got %q", i, name, expectedStr, av)
				}
			}

			// Check source metadata
			if rec.Metadata.SourceType != "json" {
				t.Fatalf("row %d: expected source type 'json', got %q", i, rec.Metadata.SourceType)
			}
			if rec.Metadata.SourceID != jsonPath {
				t.Fatalf("row %d: expected source ID %q, got %q", i, jsonPath, rec.Metadata.SourceID)
			}
		}
	})
}
