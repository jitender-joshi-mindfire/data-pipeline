package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 6: Validation Correctness
// Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.6
//
// For any record and validation schema, the record shall appear in the
// Validator's output channel if and only if it satisfies all validation rules
// defined in the schema; otherwise it shall be excluded and all violations
// shall be individually logged to the Error_Store.
func TestProperty6_ValidationCorrectness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a validation schema with 1 to 5 fields
		numFields := rapid.IntRange(1, 5).Draw(t, "numFields")
		schemas := make([]model.FieldSchema, numFields)
		fieldNames := make([]string, numFields)

		usedNames := make(map[string]bool)
		for i := 0; i < numFields; i++ {
			var name string
			for {
				name = rapid.StringMatching(`[a-z][a-z0-9_]{1,9}`).Draw(t, fmt.Sprintf("fieldName_%d", i))
				if !usedNames[name] {
					usedNames[name] = true
					break
				}
			}
			fieldNames[i] = name

			fieldType := rapid.SampledFrom([]string{"string", "number", "date", "boolean"}).Draw(t, fmt.Sprintf("fieldType_%d", i))
			required := rapid.Bool().Draw(t, fmt.Sprintf("required_%d", i))

			schema := model.FieldSchema{
				Name:     name,
				Type:     fieldType,
				Required: required,
			}

			// Add numeric range constraints for number fields
			if fieldType == "number" {
				hasMin := rapid.Bool().Draw(t, fmt.Sprintf("hasMin_%d", i))
				hasMax := rapid.Bool().Draw(t, fmt.Sprintf("hasMax_%d", i))
				if hasMin {
					minVal := rapid.Float64Range(-1000, 500).Draw(t, fmt.Sprintf("min_%d", i))
					schema.Min = &minVal
				}
				if hasMax {
					maxVal := rapid.Float64Range(500, 2000).Draw(t, fmt.Sprintf("max_%d", i))
					schema.Max = &maxVal
				}
			}

			// Add pattern constraint for string fields
			if fieldType == "string" {
				hasPattern := rapid.Bool().Draw(t, fmt.Sprintf("hasPattern_%d", i))
				if hasPattern {
					// Use simple, valid regex patterns
					schema.Pattern = rapid.SampledFrom([]string{
						`^[a-z]+$`,
						`^[A-Z][a-z]+$`,
						`^\d+$`,
						`^[a-zA-Z0-9]+$`,
						`^.{1,10}$`,
					}).Draw(t, fmt.Sprintf("pattern_%d", i))
				}
			}

			schemas[i] = schema
		}

		// Generate a record that may or may not satisfy the schema
		fields := generateRecordFields(t, schemas)

		record := &model.Record{
			ID:     "test-record",
			Fields: fields,
			Metadata: model.RecordMetadata{
				SourceType: "csv",
				SourceID:   "test.csv",
				LineNumber: 1,
			},
		}

		// Run through the Validator
		errStore := store.NewInMemoryErrorStore()
		progress := store.NewProgressTracker()
		jobID := "property6-test"

		validationConfig := model.ValidationConfig{Fields: schemas}
		validator := NewValidator(validationConfig, errStore, progress, jobID)

		result, err := validator.Process(context.Background(), record)

		// Independently compute expected violations
		expectedViolations := computeExpectedViolations(record, schemas)

		if len(expectedViolations) == 0 {
			// Property: valid records are forwarded (Requirement 4.2)
			if err != nil {
				t.Fatalf("record should be valid but got error: %v\nfields: %v\nschema: %+v",
					err, fields, schemas)
			}
			if result == nil {
				t.Fatal("record should be valid but result is nil")
			}
			// The returned record should be the same record
			if result.ID != record.ID {
				t.Fatalf("returned record ID mismatch: expected %q, got %q", record.ID, result.ID)
			}
		} else {
			// Property: invalid records are excluded (Requirement 4.3)
			if err == nil {
				t.Fatalf("record should be invalid (%d violations) but no error returned\nfields: %v\nschema: %+v\nexpected violations: %v",
					len(expectedViolations), fields, schemas, expectedViolations)
			}
			if result != nil {
				t.Fatalf("record should be invalid but result is not nil")
			}

			// Property: all violations are individually logged to ErrorStore (Requirements 4.1, 4.3)
			errors, errCount := errStore.GetByJob(jobID, 0, 200)
			if errCount != len(expectedViolations) {
				t.Fatalf("expected %d violation(s) logged, got %d\nfields: %v\nschema: %+v\nexpected violations: %v\nactual errors: %v",
					len(expectedViolations), errCount, fields, schemas, expectedViolations, errMessages(errors))
			}

			// All errors should be in the "validator" stage
			for i, e := range errors {
				if e.Stage != "validator" {
					t.Fatalf("error %d: expected stage 'validator', got %q", i, e.Stage)
				}
			}
		}
	})
}

// generateRecordFields creates field values for a record that may or may not
// conform to the schema, exercising all validation rule types.
func generateRecordFields(t *rapid.T, schemas []model.FieldSchema) map[string]interface{} {
	fields := make(map[string]interface{})

	for i, schema := range schemas {
		// Decide whether to include this field at all
		includeField := rapid.Bool().Draw(t, fmt.Sprintf("include_%d", i))

		if !includeField {
			// Field absent - tests required-field check (Requirement 4.6)
			continue
		}

		// Decide whether to set the field to nil (null)
		setNull := rapid.Bool().Draw(t, fmt.Sprintf("setNull_%d", i))
		if setNull {
			fields[schema.Name] = nil
			continue
		}

		// Decide whether to generate a conforming or non-conforming value
		conforming := rapid.Bool().Draw(t, fmt.Sprintf("conforming_%d", i))

		if conforming {
			fields[schema.Name] = generateConformingValue(t, schema, i)
		} else {
			fields[schema.Name] = generateNonConformingValue(t, schema, i)
		}
	}

	return fields
}

// generateConformingValue creates a value that satisfies the schema constraints.
func generateConformingValue(t *rapid.T, schema model.FieldSchema, idx int) interface{} {
	switch schema.Type {
	case "string":
		if schema.Pattern != "" {
			// Generate a string matching the pattern
			return generateMatchingString(t, schema.Pattern, idx)
		}
		return rapid.StringMatching(`[a-zA-Z0-9]{1,20}`).Draw(t, fmt.Sprintf("strVal_%d", idx))

	case "number":
		minVal := -1000.0
		maxVal := 2000.0
		if schema.Min != nil {
			minVal = *schema.Min
		}
		if schema.Max != nil {
			maxVal = *schema.Max
		}
		if minVal > maxVal {
			// Schema is impossible to satisfy; just generate within min bounds
			return minVal
		}
		return rapid.Float64Range(minVal, maxVal).Draw(t, fmt.Sprintf("numVal_%d", idx))

	case "date":
		// Generate a valid RFC 3339 date
		year := rapid.IntRange(2000, 2030).Draw(t, fmt.Sprintf("year_%d", idx))
		month := rapid.IntRange(1, 12).Draw(t, fmt.Sprintf("month_%d", idx))
		day := rapid.IntRange(1, 28).Draw(t, fmt.Sprintf("day_%d", idx))
		hour := rapid.IntRange(0, 23).Draw(t, fmt.Sprintf("hour_%d", idx))
		min := rapid.IntRange(0, 59).Draw(t, fmt.Sprintf("min_%d", idx))
		sec := rapid.IntRange(0, 59).Draw(t, fmt.Sprintf("sec_%d", idx))
		return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", year, month, day, hour, min, sec)

	case "boolean":
		if rapid.Bool().Draw(t, fmt.Sprintf("boolNative_%d", idx)) {
			return rapid.Bool().Draw(t, fmt.Sprintf("boolVal_%d", idx))
		}
		// Generate as string representation
		return rapid.SampledFrom([]string{"true", "false", "1", "0", "TRUE", "FALSE"}).Draw(t, fmt.Sprintf("boolStr_%d", idx))

	default:
		return "unknown"
	}
}

// generateNonConformingValue creates a value that does NOT satisfy the type constraint.
func generateNonConformingValue(t *rapid.T, schema model.FieldSchema, idx int) interface{} {
	switch schema.Type {
	case "string":
		// Return a non-string type
		return rapid.IntRange(0, 1000).Draw(t, fmt.Sprintf("nonStr_%d", idx))

	case "number":
		// Return a non-numeric string that cannot be parsed as a number
		return rapid.SampledFrom([]string{"abc", "not-a-number", "NaN!", "twelve"}).Draw(t, fmt.Sprintf("nonNum_%d", idx))

	case "date":
		// Return a string that is not a valid RFC 3339 date
		return rapid.SampledFrom([]string{"not-a-date", "2024-13-45", "yesterday", "01/01/2024"}).Draw(t, fmt.Sprintf("nonDate_%d", idx))

	case "boolean":
		// Return something that isn't a boolean or parseable as one
		return rapid.SampledFrom([]string{"maybe", "yep", "nah", "unknown"}).Draw(t, fmt.Sprintf("nonBool_%d", idx))

	default:
		return "unknown"
	}
}

// generateMatchingString generates a string that matches the given regex pattern.
func generateMatchingString(t *rapid.T, pattern string, idx int) string {
	switch pattern {
	case `^[a-z]+$`:
		return rapid.StringMatching(`[a-z]{1,10}`).Draw(t, fmt.Sprintf("patStr_%d", idx))
	case `^[A-Z][a-z]+$`:
		return rapid.StringMatching(`[A-Z][a-z]{1,9}`).Draw(t, fmt.Sprintf("patStr_%d", idx))
	case `^\d+$`:
		return rapid.StringMatching(`[0-9]{1,10}`).Draw(t, fmt.Sprintf("patStr_%d", idx))
	case `^[a-zA-Z0-9]+$`:
		return rapid.StringMatching(`[a-zA-Z0-9]{1,10}`).Draw(t, fmt.Sprintf("patStr_%d", idx))
	case `^.{1,10}$`:
		return rapid.StringMatching(`[a-zA-Z0-9]{1,10}`).Draw(t, fmt.Sprintf("patStr_%d", idx))
	default:
		return "default"
	}
}

// computeExpectedViolations independently determines which violations a record has
// against the given schema, mirroring the validator's logic.
func computeExpectedViolations(record *model.Record, schemas []model.FieldSchema) []string {
	var violations []string

	for _, schema := range schemas {
		value, exists := record.Fields[schema.Name]
		fieldMissing := !exists || value == nil

		// Required field check (Requirement 4.6)
		if schema.Required && fieldMissing {
			violations = append(violations, fmt.Sprintf("field '%s' is required but missing or null", schema.Name))
			continue // No further checks if field is missing
		}

		// If field is not present and not required, skip remaining checks
		if fieldMissing {
			continue
		}

		// Type checking (Requirement 4.4)
		typeViolation := checkTypeIndependent(schema, value)
		if typeViolation != "" {
			violations = append(violations, typeViolation)
			continue // If type check fails, skip range/pattern checks
		}

		// Numeric range check (Requirement 4.4)
		if schema.Type == "number" {
			numVal, ok := toFloat64Independent(value)
			if ok {
				if schema.Min != nil && numVal < *schema.Min {
					violations = append(violations, fmt.Sprintf("field '%s' value %v is below minimum %v", schema.Name, numVal, *schema.Min))
				}
				if schema.Max != nil && numVal > *schema.Max {
					violations = append(violations, fmt.Sprintf("field '%s' value %v exceeds maximum %v", schema.Name, numVal, *schema.Max))
				}
			}
		}

		// Pattern check (Requirement 4.4)
		if schema.Pattern != "" && schema.Type == "string" {
			strVal, ok := value.(string)
			if ok {
				re, err := regexp.Compile(schema.Pattern)
				if err == nil && !re.MatchString(strVal) {
					violations = append(violations, fmt.Sprintf("field '%s' value '%s' does not match pattern '%s'", schema.Name, strVal, schema.Pattern))
				}
			}
		}
	}

	return violations
}

// checkTypeIndependent validates that a value matches the expected type (independent implementation).
func checkTypeIndependent(schema model.FieldSchema, value interface{}) string {
	switch schema.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Sprintf("field '%s' expected type string, got %T", schema.Name, value)
		}
	case "number":
		if !isNumericIndependent(value) {
			return fmt.Sprintf("field '%s' expected type number, got %T with value %v", schema.Name, value, value)
		}
	case "date":
		strVal, ok := value.(string)
		if !ok {
			return fmt.Sprintf("field '%s' expected type date (RFC 3339 string), got %T", schema.Name, value)
		}
		if _, err := time.Parse(time.RFC3339, strVal); err != nil {
			return fmt.Sprintf("field '%s' value '%s' is not a valid RFC 3339 date", schema.Name, strVal)
		}
	case "boolean":
		if !isBooleanIndependent(value) {
			return fmt.Sprintf("field '%s' expected type boolean, got %T with value %v", schema.Name, value, value)
		}
	}
	return ""
}

// isNumericIndependent checks if a value is numeric (independent implementation).
func isNumericIndependent(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
	case string:
		_, err := strconv.ParseFloat(value.(string), 64)
		return err == nil
	default:
		return false
	}
}

// isBooleanIndependent checks if a value is boolean (independent implementation).
func isBooleanIndependent(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return true
	case string:
		_, err := strconv.ParseBool(v)
		return err == nil
	default:
		return false
	}
}

// toFloat64Independent converts a value to float64 (independent implementation).
func toFloat64Independent(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// errMessages extracts error messages from a slice of ErrorEntry for debugging output.
func errMessages(entries []model.ErrorEntry) []string {
	msgs := make([]string, len(entries))
	for i, e := range entries {
		msgs[i] = e.Message
	}
	return msgs
}
