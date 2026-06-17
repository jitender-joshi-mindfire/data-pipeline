package pipeline

import (
	"context"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestTransformer_TypeConvert_StringToNumber(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "amount", Operation: "type_convert", TargetType: "number"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"amount": "42.5", "name": "test"},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, 42.5, result.Fields["amount"])
	assert.Equal(t, "test", result.Fields["name"]) // untargeted field preserved
}

func TestTransformer_TypeConvert_StringToDate(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "created_at", Operation: "type_convert", TargetType: "date"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"created_at": "2024-01-15T10:30:00Z"},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "2024-01-15T10:30:00Z", result.Fields["created_at"])
}

func TestTransformer_TypeConvert_StringToDate_Invalid(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "created_at", Operation: "type_convert", TargetType: "date"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"created_at": "not-a-date"},
	}

	_, err := transformer.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type_convert")
	assert.Contains(t, err.Error(), "created_at")
}

func TestTransformer_TypeConvert_NumberToString(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "code", Operation: "type_convert", TargetType: "string"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"code": float64(123)},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "123", result.Fields["code"])
}

func TestTransformer_TypeConvert_StringToNumber_Invalid(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "amount", Operation: "type_convert", TargetType: "number"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"amount": "not-a-number"},
	}

	_, err := transformer.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type_convert")
	assert.Contains(t, err.Error(), "amount")
}

func TestTransformer_Normalization_Trim(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "name", Operation: "trim"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"name": "  hello world  "},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", result.Fields["name"])
}

func TestTransformer_Normalization_Lowercase(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "email", Operation: "lowercase"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"email": "User@Example.COM"},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "user@example.com", result.Fields["email"])
}

func TestTransformer_Normalization_Uppercase(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "country", Operation: "uppercase"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"country": "united states"},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "UNITED STATES", result.Fields["country"])
}

func TestTransformer_Enrichment_Concat(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "full_name", Operation: "enrich", Expression: "concat(first, last)"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"first": "John", "last": "Doe"},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "John Doe", result.Fields["full_name"])
}

func TestTransformer_Enrichment_Addition(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "total", Operation: "enrich", Expression: "price + tax"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"price": float64(100), "tax": float64(8.5)},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, 108.5, result.Fields["total"])
}

func TestTransformer_Enrichment_Subtraction(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "profit", Operation: "enrich", Expression: "revenue - cost"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"revenue": float64(500), "cost": float64(200)},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, 300.0, result.Fields["profit"])
}

func TestTransformer_Enrichment_Multiplication(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "area", Operation: "enrich", Expression: "width * height"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"width": float64(5), "height": float64(3)},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, 15.0, result.Fields["area"])
}

func TestTransformer_Enrichment_Division(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "avg", Operation: "enrich", Expression: "total / count"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"total": float64(100), "count": float64(4)},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, 25.0, result.Fields["avg"])
}

func TestTransformer_Enrichment_DivisionByZero(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "avg", Operation: "enrich", Expression: "total / count"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"total": float64(100), "count": float64(0)},
	}

	_, err := transformer.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "division by zero")
}

func TestTransformer_PreservesUntargetedFields(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "amount", Operation: "type_convert", TargetType: "number"},
	})

	record := &model.Record{
		ID: "r1",
		Fields: map[string]interface{}{
			"amount":   "42",
			"name":     "Alice",
			"category": "electronics",
			"active":   true,
		},
		Metadata: model.RecordMetadata{
			SourceType: "csv",
			SourceID:   "/data/input.csv",
			LineNumber: 5,
		},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, 42.0, result.Fields["amount"])
	assert.Equal(t, "Alice", result.Fields["name"])
	assert.Equal(t, "electronics", result.Fields["category"])
	assert.Equal(t, true, result.Fields["active"])
	assert.Equal(t, record.Metadata, result.Metadata)
	assert.Equal(t, record.ID, result.ID)
}

func TestTransformer_OrderOfOperations(t *testing.T) {
	// Type conversion first, then normalization, then enrichment
	// Even if they're defined in a different order in config
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "email", Operation: "lowercase"},
		{Field: "total", Operation: "enrich", Expression: "price + tax"},
		{Field: "price", Operation: "type_convert", TargetType: "number"},
		{Field: "tax", Operation: "type_convert", TargetType: "number"},
		{Field: "name", Operation: "trim"},
	})

	record := &model.Record{
		ID: "r1",
		Fields: map[string]interface{}{
			"price": "100",
			"tax":   "8.5",
			"email": "USER@Test.COM",
			"name":  "  Alice  ",
		},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	// Type conversions applied first
	assert.Equal(t, 100.0, result.Fields["price"])
	assert.Equal(t, 8.5, result.Fields["tax"])
	// Normalization second
	assert.Equal(t, "user@test.com", result.Fields["email"])
	assert.Equal(t, "Alice", result.Fields["name"])
	// Enrichment last (uses converted numeric values)
	assert.Equal(t, 108.5, result.Fields["total"])
}

func TestTransformer_FieldNotFound(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "missing", Operation: "type_convert", TargetType: "number"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"name": "test"},
	}

	_, err := transformer.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestTransformer_NormalizationOnNonString(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "count", Operation: "lowercase"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"count": float64(42)},
	}

	_, err := transformer.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected string")
}

func TestTransformer_EmptyConfig(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"name": "Alice", "age": float64(30)},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "Alice", result.Fields["name"])
	assert.Equal(t, float64(30), result.Fields["age"])
}

func TestTransformer_DoesNotMutateOriginalRecord(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "name", Operation: "uppercase"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"name": "alice"},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "ALICE", result.Fields["name"])
	// Original record unchanged
	assert.Equal(t, "alice", record.Fields["name"])
}

func TestTransformer_Enrichment_FieldReference(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "backup_name", Operation: "enrich", Expression: "name"},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"name": "Alice"},
	}

	result, err := transformer.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.Equal(t, "Alice", result.Fields["backup_name"])
}

func TestTransformer_Enrichment_EmptyExpression(t *testing.T) {
	transformer := NewTransformer([]model.TransformConfig{
		{Field: "derived", Operation: "enrich", Expression: ""},
	})

	record := &model.Record{
		ID:     "r1",
		Fields: map[string]interface{}{"name": "Alice"},
	}

	_, err := transformer.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expression is empty")
}
