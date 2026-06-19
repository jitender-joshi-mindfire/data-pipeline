package export

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// postgresDSN returns the DSN to use for postgres tests.
// Tests are skipped when POSTGRES_DSN is not set.
func postgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping postgres integration tests")
	}
	return dsn
}

// uniqueTable returns a table name that is unique per test to avoid collisions.
func uniqueTable(t *testing.T) string {
	t.Helper()
	// Replace characters invalid in identifiers
	safe := ""
	for _, c := range t.Name() {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			safe += string(c)
		} else {
			safe += "_"
		}
	}
	return fmt.Sprintf("test_%s", safe)
}

// dropTable removes the table created by a test.
func dropTable(t *testing.T, dsn, table string) {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return
	}
	defer db.Close()
	db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, table))
}

func TestPostgresTarget_Type(t *testing.T) {
	dsn := postgresDSN(t)
	table := uniqueTable(t)
	defer dropTable(t, dsn, table)

	target, err := NewPostgresTarget(dsn, table)
	require.NoError(t, err)
	defer target.Close()

	assert.Equal(t, "postgres", target.Type())
}

func TestPostgresTarget_Identifier(t *testing.T) {
	dsn := postgresDSN(t)
	table := uniqueTable(t)
	defer dropTable(t, dsn, table)

	target, err := NewPostgresTarget(dsn, table)
	require.NoError(t, err)
	defer target.Close()

	assert.Equal(t, dsn, target.Identifier())
}

func TestPostgresTarget_WriteCreatesTableAndInserts(t *testing.T) {
	dsn := postgresDSN(t)
	table := uniqueTable(t)
	defer dropTable(t, dsn, table)

	target, err := NewPostgresTarget(dsn, table)
	require.NoError(t, err)
	defer target.Close()

	records := []*model.Record{
		{
			ID: "rec-1",
			Fields: map[string]interface{}{
				"category":     "electronics",
				"total_count":  int64(150),
				"total_amount": 45000.50,
			},
		},
		{
			ID: "rec-2",
			Fields: map[string]interface{}{
				"category":     "clothing",
				"total_count":  int64(80),
				"total_amount": 12000.00,
			},
		},
	}

	err = target.Write(context.Background(), records)
	require.NoError(t, err)

	// Verify the data was written
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query(
		fmt.Sprintf(`SELECT "category", "total_amount", "total_count" FROM "%s" ORDER BY "category"`, table),
	)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		category    string
		totalAmount float64
		totalCount  int64
	}
	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.category, &r.totalAmount, &r.totalCount))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, got, 2)
	assert.Equal(t, "clothing", got[0].category)
	assert.Equal(t, 12000.00, got[0].totalAmount)
	assert.Equal(t, int64(80), got[0].totalCount)
	assert.Equal(t, "electronics", got[1].category)
	assert.Equal(t, 45000.50, got[1].totalAmount)
	assert.Equal(t, int64(150), got[1].totalCount)
}

func TestPostgresTarget_WriteEmptyRecords(t *testing.T) {
	dsn := postgresDSN(t)
	table := uniqueTable(t)
	defer dropTable(t, dsn, table)

	target, err := NewPostgresTarget(dsn, table)
	require.NoError(t, err)
	defer target.Close()

	err = target.Write(context.Background(), []*model.Record{})
	assert.NoError(t, err)
}

func TestPostgresTarget_WriteIdempotent(t *testing.T) {
	dsn := postgresDSN(t)
	table := uniqueTable(t)
	defer dropTable(t, dsn, table)

	target, err := NewPostgresTarget(dsn, table)
	require.NoError(t, err)
	defer target.Close()

	records := []*model.Record{
		{ID: "rec-1", Fields: map[string]interface{}{"name": "alpha", "score": float64(1.5)}},
	}

	// Writing twice must not error (CREATE TABLE IF NOT EXISTS).
	require.NoError(t, target.Write(context.Background(), records))
	require.NoError(t, target.Write(context.Background(), records))

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()

	var count int
	require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table)).Scan(&count))
	assert.Equal(t, 2, count)
}

func TestPostgresTarget_WriteWithMixedFieldTypes(t *testing.T) {
	dsn := postgresDSN(t)
	table := uniqueTable(t)
	defer dropTable(t, dsn, table)

	target, err := NewPostgresTarget(dsn, table)
	require.NoError(t, err)
	defer target.Close()

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"label": "x", "count": int64(3), "ratio": 0.75, "flag": true}},
		{ID: "r2", Fields: map[string]interface{}{"label": "y", "count": int64(7), "ratio": 1.25, "flag": false}},
	}

	require.NoError(t, target.Write(context.Background(), records))

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()

	var n int
	require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table)).Scan(&n))
	assert.Equal(t, 2, n)
}

func TestPostgresTarget_InvalidDSN(t *testing.T) {
	_, err := NewPostgresTarget("postgres://invalid:5432/nodb?sslmode=disable", "t")
	assert.Error(t, err)
}

func TestPostgresTarget_ContextCancellation(t *testing.T) {
	dsn := postgresDSN(t)
	table := uniqueTable(t)
	defer dropTable(t, dsn, table)

	target, err := NewPostgresTarget(dsn, table)
	require.NoError(t, err)
	defer target.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Write

	records := []*model.Record{
		{ID: "r1", Fields: map[string]interface{}{"name": "test"}},
	}

	err = target.Write(ctx, records)
	assert.Error(t, err)
}
