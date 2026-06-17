package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// Transformer implements the Processor interface and applies transformation rules to records.
type Transformer struct {
	Config []model.TransformConfig
}

// NewTransformer creates a new Transformer processor with the given configuration.
func NewTransformer(config []model.TransformConfig) *Transformer {
	return &Transformer{
		Config: config,
	}
}

// Process applies all configured transformations to a record in order:
// type conversions first, then normalization, then enrichment.
// Returns the transformed record or an error if any transformation fails.
func (t *Transformer) Process(ctx context.Context, record *model.Record) (*model.Record, error) {
	// Create a copy of the record fields to avoid mutating the original
	transformed := make(map[string]interface{}, len(record.Fields))
	for k, v := range record.Fields {
		transformed[k] = v
	}

	// Group transformations by category and apply in order
	var typeConversions, normalizations, enrichments []model.TransformConfig
	for _, tc := range t.Config {
		switch tc.Operation {
		case "type_convert":
			typeConversions = append(typeConversions, tc)
		case "trim", "lowercase", "uppercase":
			normalizations = append(normalizations, tc)
		case "enrich":
			enrichments = append(enrichments, tc)
		}
	}

	// 1. Apply type conversions
	for _, tc := range typeConversions {
		val, exists := transformed[tc.Field]
		if !exists {
			return nil, fmt.Errorf("type_convert: field '%s' not found", tc.Field)
		}
		converted, err := applyTypeConversion(val, tc.TargetType)
		if err != nil {
			return nil, fmt.Errorf("type_convert: field '%s' to %s: %w", tc.Field, tc.TargetType, err)
		}
		transformed[tc.Field] = converted
	}

	// 2. Apply normalizations
	for _, tc := range normalizations {
		val, exists := transformed[tc.Field]
		if !exists {
			return nil, fmt.Errorf("%s: field '%s' not found", tc.Operation, tc.Field)
		}
		normalized, err := applyNormalization(val, tc.Operation)
		if err != nil {
			return nil, fmt.Errorf("%s: field '%s': %w", tc.Operation, tc.Field, err)
		}
		transformed[tc.Field] = normalized
	}

	// 3. Apply enrichments
	for _, tc := range enrichments {
		enriched, err := applyEnrichment(transformed, tc)
		if err != nil {
			return nil, fmt.Errorf("enrich: field '%s': %w", tc.Field, err)
		}
		transformed[tc.Field] = enriched
	}

	return &model.Record{
		ID:       record.ID,
		Fields:   transformed,
		Metadata: record.Metadata,
	}, nil
}

// applyTypeConversion converts a value to the specified target type.
func applyTypeConversion(value interface{}, targetType string) (interface{}, error) {
	switch targetType {
	case "number":
		return convertToNumber(value)
	case "date":
		return convertToDate(value)
	case "string":
		return convertToString(value)
	default:
		return nil, fmt.Errorf("unsupported target type '%s'", targetType)
	}
}

// convertToNumber converts a value to a float64.
func convertToNumber(value interface{}) (float64, error) {
	switch v := value.(type) {
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to number", v)
		}
		return f, nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to number", value)
	}
}

// convertToDate converts a string value to RFC 3339 date format (validates the format).
func convertToDate(value interface{}) (string, error) {
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("cannot convert %T to date, expected string", value)
	}
	str = strings.TrimSpace(str)
	_, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return "", fmt.Errorf("cannot parse '%s' as RFC 3339 date: %w", str, err)
	}
	return str, nil
}

// convertToString converts a value to its string representation.
func convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case bool:
		return strconv.FormatBool(v), nil
	case nil:
		return "", nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// applyNormalization applies a normalization operation to a field value.
func applyNormalization(value interface{}, operation string) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string value, got %T", value)
	}

	switch operation {
	case "trim":
		return strings.TrimSpace(str), nil
	case "lowercase":
		return strings.ToLower(str), nil
	case "uppercase":
		return strings.ToUpper(str), nil
	default:
		return nil, fmt.Errorf("unsupported normalization operation '%s'", operation)
	}
}

// applyEnrichment computes a derived field value from an expression.
// Supported expressions:
//   - "concat(field1, field2, ...)" — concatenates field values with space separator
//   - "field1 + field2" — adds two numeric fields
//   - "field1 - field2" — subtracts two numeric fields
//   - "field1 * field2" — multiplies two numeric fields
//   - "field1 / field2" — divides two numeric fields
func applyEnrichment(fields map[string]interface{}, tc model.TransformConfig) (interface{}, error) {
	expr := strings.TrimSpace(tc.Expression)
	if expr == "" {
		return nil, fmt.Errorf("expression is empty")
	}

	// Handle concat expression
	if strings.HasPrefix(expr, "concat(") && strings.HasSuffix(expr, ")") {
		inner := expr[7 : len(expr)-1]
		fieldNames := strings.Split(inner, ",")
		var parts []string
		for _, name := range fieldNames {
			name = strings.TrimSpace(name)
			val, exists := fields[name]
			if !exists {
				return nil, fmt.Errorf("referenced field '%s' not found", name)
			}
			parts = append(parts, fmt.Sprintf("%v", val))
		}
		return strings.Join(parts, " "), nil
	}

	// Handle arithmetic expressions: field1 op field2
	for _, op := range []string{" + ", " - ", " * ", " / "} {
		if idx := strings.Index(expr, op); idx > 0 {
			leftField := strings.TrimSpace(expr[:idx])
			rightField := strings.TrimSpace(expr[idx+len(op):])

			leftVal, exists := fields[leftField]
			if !exists {
				return nil, fmt.Errorf("referenced field '%s' not found", leftField)
			}
			rightVal, exists := fields[rightField]
			if !exists {
				return nil, fmt.Errorf("referenced field '%s' not found", rightField)
			}

			leftNum, err := toNumeric(leftVal)
			if err != nil {
				return nil, fmt.Errorf("field '%s' is not numeric: %w", leftField, err)
			}
			rightNum, err := toNumeric(rightVal)
			if err != nil {
				return nil, fmt.Errorf("field '%s' is not numeric: %w", rightField, err)
			}

			switch strings.TrimSpace(op) {
			case "+":
				return leftNum + rightNum, nil
			case "-":
				return leftNum - rightNum, nil
			case "*":
				return leftNum * rightNum, nil
			case "/":
				if rightNum == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return leftNum / rightNum, nil
			}
		}
	}

	// If expression is just a field reference, copy the value
	if val, exists := fields[expr]; exists {
		return val, nil
	}

	return nil, fmt.Errorf("unsupported expression: '%s'", expr)
}

// toNumeric converts a value to float64 for arithmetic operations.
func toNumeric(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert '%s' to number", v)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to number", value)
	}
}
