package pipeline

import (
	"context"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/jitendraj/data-pipeline/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Validator Unit Tests ---
// Validates: Requirements 15.2

func ptrFloat64(v float64) *float64 {
	return &v
}

// newTestValidator creates a Validator with the given schema, fresh ErrorStore and ProgressTracker.
func newTestValidator(fields []model.FieldSchema) (*Validator, *store.InMemoryErrorStore, *store.InMemoryProgressTracker) {
	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()
	config := model.ValidationConfig{Fields: fields}
	v := NewValidator(config, errStore, progress, "test-job")
	return v, errStore, progress
}

// --- Test acceptance of valid records ---

func TestValidator_AcceptsValidRecord_AllFieldsPresent(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
		{Name: "age", Type: "number", Required: true, Min: ptrFloat64(0), Max: ptrFloat64(150)},
		{Name: "email", Type: "string", Required: true, Pattern: `^[\w.]+@[\w.]+$`},
		{Name: "active", Type: "boolean", Required: false},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID: "rec-1",
		Fields: map[string]interface{}{
			"name":   "Alice",
			"age":    float64(30),
			"email":  "alice@example.com",
			"active": true,
		},
	}

	result, err := v.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, record, result)

	// No errors should be logged
	_, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 0, total)
}

func TestValidator_AcceptsValidRecord_OptionalFieldMissing(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
		{Name: "nickname", Type: "string", Required: false},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID: "rec-2",
		Fields: map[string]interface{}{
			"name": "Bob",
		},
	}

	result, err := v.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	_, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 0, total)
}

func TestValidator_AcceptsValidRecord_NumberAtBoundary(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "score", Type: "number", Required: true, Min: ptrFloat64(0), Max: ptrFloat64(100)},
	}

	v, errStore, _ := newTestValidator(schema)

	// Test at minimum boundary
	record := &model.Record{
		ID:     "rec-min",
		Fields: map[string]interface{}{"score": float64(0)},
	}
	result, err := v.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test at maximum boundary
	record = &model.Record{
		ID:     "rec-max",
		Fields: map[string]interface{}{"score": float64(100)},
	}
	result, err = v.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	_, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 0, total)
}

func TestValidator_AcceptsValidRecord_DateField(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "created_at", Type: "date", Required: true},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-date",
		Fields: map[string]interface{}{"created_at": "2024-01-15T10:30:00Z"},
	}

	result, err := v.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	_, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 0, total)
}

func TestValidator_AcceptsValidRecord_NumericString(t *testing.T) {
	// The validator treats numeric strings as valid numbers
	schema := []model.FieldSchema{
		{Name: "amount", Type: "number", Required: true, Min: ptrFloat64(0), Max: ptrFloat64(1000)},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-numstr",
		Fields: map[string]interface{}{"amount": "42.5"},
	}

	result, err := v.Process(context.Background(), record)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	_, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 0, total)
}

// --- Test rejection: missing required field ---

func TestValidator_RejectsMissingRequiredField(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
		{Name: "email", Type: "string", Required: true},
	}

	v, errStore, _ := newTestValidator(schema)

	// Record missing "email" field
	record := &model.Record{
		ID:     "rec-missing",
		Fields: map[string]interface{}{"name": "Alice"},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	// Error should be logged
	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "email")
	assert.Contains(t, errs[0].Message, "required")
	assert.Equal(t, "validator", errs[0].Stage)
}

func TestValidator_RejectsNullRequiredField(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
	}

	v, errStore, _ := newTestValidator(schema)

	// Field is present but nil
	record := &model.Record{
		ID:     "rec-null",
		Fields: map[string]interface{}{"name": nil},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "name")
	assert.Contains(t, errs[0].Message, "required")
}

// --- Test rejection: type mismatch ---

func TestValidator_RejectsTypeMismatch_ExpectedStringGotNumber(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-type",
		Fields: map[string]interface{}{"name": float64(123)},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "name")
	assert.Contains(t, errs[0].Message, "string")
}

func TestValidator_RejectsTypeMismatch_ExpectedNumberGotString(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "age", Type: "number", Required: true},
	}

	v, errStore, _ := newTestValidator(schema)

	// Non-numeric string
	record := &model.Record{
		ID:     "rec-type2",
		Fields: map[string]interface{}{"age": "not-a-number"},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "age")
	assert.Contains(t, errs[0].Message, "number")
}

func TestValidator_RejectsTypeMismatch_ExpectedBooleanGotString(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "active", Type: "boolean", Required: true},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-bool",
		Fields: map[string]interface{}{"active": "not-a-bool"},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "active")
	assert.Contains(t, errs[0].Message, "boolean")
}

func TestValidator_RejectsTypeMismatch_ExpectedDateGotInvalidFormat(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "created_at", Type: "date", Required: true},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-date-bad",
		Fields: map[string]interface{}{"created_at": "2024-01-15"},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "created_at")
	assert.Contains(t, errs[0].Message, "date")
}

// --- Test rejection: numeric value out of range ---

func TestValidator_RejectsNumberBelowMinimum(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "score", Type: "number", Required: true, Min: ptrFloat64(0), Max: ptrFloat64(100)},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-below",
		Fields: map[string]interface{}{"score": float64(-5)},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "score")
	assert.Contains(t, errs[0].Message, "below minimum")
}

func TestValidator_RejectsNumberAboveMaximum(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "score", Type: "number", Required: true, Min: ptrFloat64(0), Max: ptrFloat64(100)},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-above",
		Fields: map[string]interface{}{"score": float64(150)},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "score")
	assert.Contains(t, errs[0].Message, "exceeds maximum")
}

// --- Test rejection: string pattern mismatch ---

func TestValidator_RejectsPatternMismatch(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "email", Type: "string", Required: true, Pattern: `^[\w.]+@[\w.]+$`},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-pattern",
		Fields: map[string]interface{}{"email": "invalid-email"},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "email")
	assert.Contains(t, errs[0].Message, "pattern")
}

func TestValidator_RejectsPatternMismatch_PhoneNumber(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "phone", Type: "string", Required: true, Pattern: `^\d{3}-\d{3}-\d{4}$`},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID:     "rec-phone",
		Fields: map[string]interface{}{"phone": "123-45-6789"},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 1, total)
	assert.Contains(t, errs[0].Message, "phone")
	assert.Contains(t, errs[0].Message, "pattern")
}

// --- Test that rejected records are stored in ErrorStore ---

func TestValidator_RejectedRecordStoredInErrorStore(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
		{Name: "age", Type: "number", Required: true, Min: ptrFloat64(0), Max: ptrFloat64(150)},
	}

	v, errStore, _ := newTestValidator(schema)

	record := &model.Record{
		ID: "rec-err-store",
		Fields: map[string]interface{}{
			"name": "Alice",
			"age":  float64(200), // Out of range
		},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	errs, total := errStore.GetByJob("test-job", 0, 50)
	require.Equal(t, 1, total)

	// Verify error entry fields
	assert.Equal(t, "test-job", errs[0].JobID)
	assert.Equal(t, "validator", errs[0].Stage)
	assert.Contains(t, errs[0].Message, "age")
	assert.NotNil(t, errs[0].Record)
	assert.Equal(t, "Alice", errs[0].Record["name"])
	assert.Equal(t, float64(200), errs[0].Record["age"])
}

func TestValidator_MultipleViolationsAllStoredInErrorStore(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
		{Name: "age", Type: "number", Required: true},
		{Name: "email", Type: "string", Required: true, Pattern: `^[\w.]+@[\w.]+$`},
	}

	v, errStore, _ := newTestValidator(schema)

	// Record missing name (required), age is wrong type, email fails pattern
	// Note: the validator collects ALL violations per record
	record := &model.Record{
		ID: "rec-multi-err",
		Fields: map[string]interface{}{
			"age":   "not-a-number",
			"email": "bad-email",
		},
	}

	result, err := v.Process(context.Background(), record)
	assert.Error(t, err)
	assert.Nil(t, result)

	// Should have one error for missing "name", one for type mismatch on "age",
	// and one for pattern mismatch on "email"
	errs, total := errStore.GetByJob("test-job", 0, 50)
	assert.Equal(t, 3, total)

	// Verify each violation is stored separately
	messages := make([]string, len(errs))
	for i, e := range errs {
		messages[i] = e.Message
		assert.Equal(t, "validator", e.Stage)
		assert.Equal(t, "test-job", e.JobID)
		assert.NotNil(t, e.Record)
	}

	// Check that each violation type is represented
	foundName := false
	foundAge := false
	foundEmail := false
	for _, msg := range messages {
		if containsAll(msg, "name", "required") {
			foundName = true
		}
		if containsAll(msg, "age", "number") {
			foundAge = true
		}
		if containsAll(msg, "email", "pattern") {
			foundEmail = true
		}
	}
	assert.True(t, foundName, "expected violation for missing required field 'name'")
	assert.True(t, foundAge, "expected violation for type mismatch on field 'age'")
	assert.True(t, foundEmail, "expected violation for pattern mismatch on field 'email'")
}

// --- Test that progress tracker is updated ---

func TestValidator_UpdatesProgressTracker_ValidRecord(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
	}

	_, _, progress := newTestValidator(schema)
	v, _, _ := newTestValidator(schema)
	// Use the same progress tracker
	errStore := store.NewInMemoryErrorStore()
	v = NewValidator(model.ValidationConfig{Fields: schema}, errStore, progress, "test-job")

	record := &model.Record{
		ID:     "rec-prog",
		Fields: map[string]interface{}{"name": "Alice"},
	}

	_, err := v.Process(context.Background(), record)
	assert.NoError(t, err)

	prog := progress.GetProgress("test-job")
	assert.NotNil(t, prog)
	assert.Equal(t, int64(1), prog.RecordsProcessed)
}

func TestValidator_UpdatesProgressTracker_InvalidRecord(t *testing.T) {
	schema := []model.FieldSchema{
		{Name: "name", Type: "string", Required: true},
	}

	errStore := store.NewInMemoryErrorStore()
	progress := store.NewProgressTracker()
	v := NewValidator(model.ValidationConfig{Fields: schema}, errStore, progress, "test-job")

	record := &model.Record{
		ID:     "rec-fail-prog",
		Fields: map[string]interface{}{},
	}

	_, err := v.Process(context.Background(), record)
	assert.Error(t, err)

	prog := progress.GetProgress("test-job")
	assert.NotNil(t, prog)
	assert.Equal(t, int64(1), prog.ErrorCounts["validator"])
}

// containsAll checks that s contains all of the given substrings.
func containsAll(s string, substrs ...string) bool {
	for _, sub := range substrs {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
