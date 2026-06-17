package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
)

// Validator implements the Processor interface and validates records against a schema.
type Validator struct {
	Config   model.ValidationConfig
	ErrStore store.ErrorStore
	Progress store.ProgressTracker
	JobID    string

	// compiledPatterns caches compiled regexes by field name
	compiledPatterns map[string]*regexp.Regexp
}

// NewValidator creates a new Validator processor.
// It pre-compiles regex patterns from the validation config.
func NewValidator(config model.ValidationConfig, errStore store.ErrorStore, progress store.ProgressTracker, jobID string) *Validator {
	compiled := make(map[string]*regexp.Regexp)
	for _, field := range config.Fields {
		if field.Pattern != "" {
			re, err := regexp.Compile(field.Pattern)
			if err == nil {
				compiled[field.Name] = re
			}
		}
	}
	return &Validator{
		Config:           config,
		ErrStore:         errStore,
		Progress:         progress,
		JobID:            jobID,
		compiledPatterns: compiled,
	}
}

// Process validates a single record against the configured schema.
// It evaluates all rules and collects all violations.
// If valid, the record is returned. If invalid, violations are logged and an error is returned.
func (v *Validator) Process(ctx context.Context, record *model.Record) (*model.Record, error) {
	start := time.Now()
	var violations []string

	for _, schema := range v.Config.Fields {
		fieldViolations := v.validateField(record, schema)
		violations = append(violations, fieldViolations...)
	}

	if len(violations) > 0 {
		// Log each violation as a separate error entry
		for _, violation := range violations {
			v.ErrStore.Add(v.JobID, model.ErrorEntry{
				JobID:     v.JobID,
				Stage:     "validator",
				Message:   violation,
				Record:    record.Fields,
				Timestamp: time.Now().UTC(),
			})
		}
		v.Progress.RecordFailed(v.JobID, "validator")
		return nil, fmt.Errorf("validation failed: %d violation(s)", len(violations))
	}

	latency := time.Since(start)
	v.Progress.RecordProcessed(v.JobID, "validator", latency)
	return record, nil
}

// validateField checks a single field schema against the record and returns all violations.
func (v *Validator) validateField(record *model.Record, schema model.FieldSchema) []string {
	var violations []string

	value, exists := record.Fields[schema.Name]
	fieldMissing := !exists || value == nil

	// Required field check
	if schema.Required && fieldMissing {
		violations = append(violations, fmt.Sprintf("field '%s' is required but missing or null", schema.Name))
		return violations // No further checks if field is missing
	}

	// If field is not present and not required, skip remaining checks
	if fieldMissing {
		return violations
	}

	// Type checking
	if schema.Type != "" {
		typeViolation := v.checkType(schema, value)
		if typeViolation != "" {
			violations = append(violations, typeViolation)
			// If type check fails, skip range/pattern checks since they depend on the type
			return violations
		}
	}

	// Numeric range check (only if type is number and field has numeric value)
	if schema.Type == "number" {
		numVal, ok := toFloat64(value)
		if ok {
			if schema.Min != nil && numVal < *schema.Min {
				violations = append(violations, fmt.Sprintf("field '%s' value %v is below minimum %v", schema.Name, numVal, *schema.Min))
			}
			if schema.Max != nil && numVal > *schema.Max {
				violations = append(violations, fmt.Sprintf("field '%s' value %v exceeds maximum %v", schema.Name, numVal, *schema.Max))
			}
		}
	}

	// Pattern check (only if type is string and pattern is set)
	if schema.Pattern != "" && schema.Type == "string" {
		strVal, ok := value.(string)
		if ok {
			re, exists := v.compiledPatterns[schema.Name]
			if exists && !re.MatchString(strVal) {
				violations = append(violations, fmt.Sprintf("field '%s' value '%s' does not match pattern '%s'", schema.Name, strVal, schema.Pattern))
			}
		}
	}

	return violations
}

// checkType validates that a value matches the expected type.
func (v *Validator) checkType(schema model.FieldSchema, value interface{}) string {
	switch schema.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Sprintf("field '%s' expected type string, got %T", schema.Name, value)
		}
	case "number":
		if !isNumeric(value) {
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
		if !isBoolean(value) {
			return fmt.Sprintf("field '%s' expected type boolean, got %T with value %v", schema.Name, value, value)
		}
	}
	return ""
}

// isNumeric checks if a value is a numeric type or a string parseable as a number.
func isNumeric(value interface{}) bool {
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

// isBoolean checks if a value is a bool or a string parseable as boolean.
func isBoolean(value interface{}) bool {
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

// toFloat64 converts a value to float64 if possible.
func toFloat64(value interface{}) (float64, bool) {
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
