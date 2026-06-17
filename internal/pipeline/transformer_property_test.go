package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jitendraj/data-pipeline/internal/model"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 7: Transformation Preserves Untargeted Fields
// Validates: Requirements 5.6
//
// For any record with N fields and a transformation configuration targeting M specific fields
// (where M ≤ N), all N−M fields not named in any transformation rule shall have identical
// values in the output record as in the input record.
func TestProperty7_TransformationPreservesUntargetedFields(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a record with 2 to 10 fields
		numFields := rapid.IntRange(2, 10).Draw(t, "numFields")

		// Generate unique field names
		usedNames := make(map[string]bool)
		fieldNames := make([]string, 0, numFields)
		for i := 0; i < numFields; i++ {
			var name string
			for {
				name = rapid.StringMatching(`[a-z][a-z0-9_]{1,9}`).Draw(t, fmt.Sprintf("fieldName_%d", i))
				if !usedNames[name] {
					usedNames[name] = true
					break
				}
			}
			fieldNames = append(fieldNames, name)
		}

		// Generate field values - use string values so transformations can apply
		fields := make(map[string]interface{}, numFields)
		for i, name := range fieldNames {
			// Mix of string values (some numeric-looking for type_convert compatibility)
			valType := rapid.IntRange(0, 3).Draw(t, fmt.Sprintf("valType_%d", i))
			switch valType {
			case 0:
				// Plain string
				fields[name] = rapid.StringMatching(`[a-zA-Z ]{1,20}`).Draw(t, fmt.Sprintf("strVal_%d", i))
			case 1:
				// Numeric string (can be type-converted)
				fields[name] = fmt.Sprintf("%f", rapid.Float64Range(-1000, 1000).Draw(t, fmt.Sprintf("numStr_%d", i)))
			case 2:
				// Already a float64
				fields[name] = rapid.Float64Range(-1000, 1000).Draw(t, fmt.Sprintf("floatVal_%d", i))
			case 3:
				// Boolean
				fields[name] = rapid.Bool().Draw(t, fmt.Sprintf("boolVal_%d", i))
			}
		}

		// Choose a subset of fields to target with transformation rules (at least 1, at most numFields-1)
		// We need at least one untargeted field to verify preservation
		maxTargeted := numFields - 1
		if maxTargeted < 1 {
			maxTargeted = 1
		}
		numTargeted := rapid.IntRange(1, maxTargeted).Draw(t, "numTargeted")

		// Select which fields to target by drawing a boolean per field, then taking numTargeted
		targetedFields := make(map[string]bool, numTargeted)
		configs := make([]model.TransformConfig, 0, numTargeted)

		// Generate a permutation of indices by shuffling field names and picking first numTargeted
		allIndices := make([]int, numFields)
		for i := range allIndices {
			allIndices[i] = i
		}
		shuffled := rapid.Permutation(allIndices).Draw(t, "shuffledIndices")
		targetedIndices := shuffled[:numTargeted]

		// Build transformation rules only for targeted fields
		// Use only normalizations (trim, lowercase, uppercase) on string-valued targeted fields
		// since those don't fail and allow us to focus on the preservation property
		for _, idx := range targetedIndices {
			fieldName := fieldNames[idx]
			targetedFields[fieldName] = true

			// Ensure the targeted field has a string value for normalization operations
			strVal := rapid.StringMatching(`[a-zA-Z ]{1,20}`).Draw(t, fmt.Sprintf("targetVal_%d", idx))
			fields[fieldName] = strVal

			// Pick a normalization operation that won't fail on strings
			op := rapid.SampledFrom([]string{"trim", "lowercase", "uppercase"}).Draw(t, fmt.Sprintf("op_%d", idx))
			configs = append(configs, model.TransformConfig{
				Field:     fieldName,
				Operation: op,
			})
		}

		// Create the record
		record := &model.Record{
			ID:     "prop7-test",
			Fields: fields,
			Metadata: model.RecordMetadata{
				SourceType: "csv",
				SourceID:   "test.csv",
				LineNumber: 1,
			},
		}

		// Save the original values of untargeted fields for comparison
		untargetedOriginals := make(map[string]interface{})
		for name, val := range fields {
			if !targetedFields[name] {
				untargetedOriginals[name] = val
			}
		}

		// Apply the transformer
		transformer := NewTransformer(configs)
		result, err := transformer.Process(context.Background(), record)

		if err != nil {
			t.Fatalf("transformer should not fail on valid normalization inputs, got error: %v\nfields: %v\nconfigs: %+v",
				err, fields, configs)
		}
		if result == nil {
			t.Fatal("transformer returned nil result without error")
		}

		// Verify all untargeted fields are preserved identically
		for name, originalVal := range untargetedOriginals {
			resultVal, exists := result.Fields[name]
			if !exists {
				t.Fatalf("untargeted field %q is missing from result\noriginal fields: %v\ntargeted: %v\nresult fields: %v",
					name, fields, targetedFields, result.Fields)
			}
			if resultVal != originalVal {
				t.Fatalf("untargeted field %q was modified: original=%v (%T), result=%v (%T)\ntargeted: %v\nconfigs: %+v",
					name, originalVal, originalVal, resultVal, resultVal, targetedFields, configs)
			}
		}

		// Also verify that record ID and metadata are preserved
		if result.ID != record.ID {
			t.Fatalf("record ID was modified: expected %q, got %q", record.ID, result.ID)
		}
		if result.Metadata != record.Metadata {
			t.Fatalf("record metadata was modified: expected %+v, got %+v", record.Metadata, result.Metadata)
		}
	})
}

// Feature: data-processing-pipeline, Property 8: Normalization Idempotence
// Validates: Requirements 5.3
//
// For any string value and normalization operation (trim, lowercase, uppercase),
// applying the operation twice shall produce the same result as applying it once:
// normalize(normalize(x)) == normalize(x).
func TestProperty8_NormalizationIdempotence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate an arbitrary string value including whitespace, mixed case, etc.
		value := rapid.String().Draw(t, "value")

		// Pick a normalization operation
		operation := rapid.SampledFrom([]string{"trim", "lowercase", "uppercase"}).Draw(t, "operation")

		// Build a transformer config targeting a single field with the chosen operation
		fieldName := "target_field"
		config := []model.TransformConfig{
			{
				Field:     fieldName,
				Operation: operation,
			},
		}

		// Create a record with the generated value
		record := &model.Record{
			ID: "prop8-test",
			Fields: map[string]interface{}{
				fieldName: value,
			},
			Metadata: model.RecordMetadata{
				SourceType: "csv",
				SourceID:   "test.csv",
				LineNumber: 1,
			},
		}

		// Apply normalization once
		transformer := NewTransformer(config)
		firstResult, err := transformer.Process(context.Background(), record)
		if err != nil {
			t.Fatalf("first normalization pass failed: %v (value=%q, operation=%s)", err, value, operation)
		}

		firstVal := fmt.Sprintf("%v", firstResult.Fields[fieldName])

		// Apply normalization a second time on the result
		secondResult, err := transformer.Process(context.Background(), firstResult)
		if err != nil {
			t.Fatalf("second normalization pass failed: %v (value=%q, operation=%s)", err, firstVal, operation)
		}

		secondVal := fmt.Sprintf("%v", secondResult.Fields[fieldName])

		// Idempotence: normalize(normalize(x)) == normalize(x)
		assert.Equal(t, firstVal, secondVal,
			"normalization should be idempotent: operation=%s, original=%q, first=%q, second=%q",
			operation, value, firstVal, secondVal)
	})
}
