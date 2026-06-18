package export

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jitendraj/data-pipeline/internal/model"
)

func TestSQLiteTarget_Type(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "results")
	require.NoError(t, err)
	defer target.Close()

	assert.Equal(t, "sqlite", target.Type())
}

func TestSQLiteTarget_Identifier(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "results")
	require.NoError(t, err)
	defer target.Close()

	assert.Equal(t, dbPath, target.Identifier())
}

func TestSQLiteTarget_WriteCreatesTableAndInserts(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "aggregated_results")
	require.NoError(t, err)
	defer target.Close()

	records := []*model.Record{
		{
			ID: "rec-1",
			Fields: map[string]interface{}{
				"category":    "electronics",
				"total_count": int64(150),
				"total_amount": 45000.50,
			},
		},
		{
			ID: "rec-2",
			Fields: map[string]interface{}{
				"category":    "clothing",
				"total_count": int64(80),
				"total_amount": 12000.00,
			},
		},
	}

	err = target.Write(context.Background(), records)
	require.NoError(t, err)

	// Verify the data was written
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query(`SELECT "category", "total_amount", "total_count" FROM "aggregated_results" ORDER BY "category"`)
	require.NoError(t, err)
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var category string
		var totalAmount float64
		var totalCount int64
		err := rows.Scan(&category, &totalAmount, &totalCount)
		require.NoError(t, err)
		results = append(results, map[string]interface{}{
			"category":     category,
			"total_amount": totalAmount,
			"total_count":  totalCount,
		})
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, "clothing", results[0]["category"])
	assert.Equal(t, 12000.00, results[0]["total_amount"])
	assert.Equal(t, int64(80), results[0]["total_count"])
	assert.Equal(t, "electronics", results[1]["category"])
	assert.Equal(t, 45000.50, results[1]["total_amount"])
	assert.Equal(t, int64(150), results[1]["total_count"])
}

func TestSQLiteTarget_WriteEmptyRecords(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "results")
	require.NoError(t, err)
	defer target.Close()

	err = target.Write(context.Background(), []*model.Record{})
	assert.NoError(t, err)
}

func TestSQLiteTarget_WriteWithMixedFieldTypes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "mixed_results")
	require.NoError(t, err)
	defer target.Close()

	records := []*model.Record{
		{
			ID: "rec-1",
			Fields: map[string]interface{}{
				"name":  "item-a",
				"count": int64(10),
				"price": 9.99,
			},
		},
		{
			ID: "rec-2",
			Fields: map[string]interface{}{
				"name":  "item-b",
				"count": int64(20),
				"price": 19.99,
			},
		},
	}

	err = target.Write(context.Background(), records)
	require.NoError(t, err)

	// Verify by querying
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM "mixed_results"`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestSQLiteTarget_Close(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "results")
	require.NoError(t, err)

	err = target.Close()
	assert.NoError(t, err)

	// Verify the database file was created
	_, err = os.Stat(dbPath)
	assert.NoError(t, err)
}

func TestSQLiteTarget_WriteWithNilFieldValues(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "nullable_results")
	require.NoError(t, err)
	defer target.Close()

	records := []*model.Record{
		{
			ID: "rec-1",
			Fields: map[string]interface{}{
				"name":     "item-a",
				"optional": nil,
			},
		},
		{
			ID: "rec-2",
			Fields: map[string]interface{}{
				"name":     "item-b",
				"optional": "has-value",
			},
		},
	}

	err = target.Write(context.Background(), records)
	require.NoError(t, err)

	// Verify data
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM "nullable_results"`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestSQLiteTarget_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	target, err := NewSQLiteTarget(dbPath, "results")
	require.NoError(t, err)
	defer target.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	records := []*model.Record{
		{
			ID:     "rec-1",
			Fields: map[string]interface{}{"name": "test"},
		},
	}

	err = target.Write(ctx, records)
	// Should return an error since context is already cancelled
	assert.Error(t, err)
}
