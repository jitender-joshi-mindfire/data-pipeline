package export

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	_ "github.com/lib/pq"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// Compile-time check that PostgresTarget implements ExportTarget.
var _ ExportTarget = (*PostgresTarget)(nil)

// PostgresTarget implements ExportTarget for writing results to a PostgreSQL database.
type PostgresTarget struct {
	dsn       string
	tableName string
	db        *sql.DB
}

// NewPostgresTarget opens a connection to the PostgreSQL database at dsn and
// verifies connectivity. dsn must be a valid libpq connection string or URL,
// e.g. "postgres://user:pass@localhost:5432/dbname?sslmode=disable".
func NewPostgresTarget(dsn, tableName string) (*PostgresTarget, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to postgres (%s): %w", dsn, err)
	}

	return &PostgresTarget{
		dsn:       dsn,
		tableName: tableName,
		db:        db,
	}, nil
}

// Write creates the table if it does not exist and inserts all results in a
// single transaction using a prepared statement with $N placeholders.
func (p *PostgresTarget) Write(ctx context.Context, results []*model.Record) error {
	if len(results) == 0 {
		return nil
	}

	columns := p.deriveColumns(results)
	if len(columns) == 0 {
		return nil
	}

	if err := p.createTable(ctx, columns, results); err != nil {
		return fmt.Errorf("failed to create table %s: %w", p.tableName, err)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// PostgreSQL uses $1, $2, … placeholders instead of ?.
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		p.quoteIdentifier(p.tableName),
		p.quoteColumns(columns),
		strings.Join(placeholders, ", "),
	)

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, record := range results {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = record.Fields[col]
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("failed to insert record %s: %w", record.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Type returns the export target type identifier.
func (p *PostgresTarget) Type() string {
	return "postgres"
}

// Identifier returns the DSN (connection string).
func (p *PostgresTarget) Identifier() string {
	return p.dsn
}

// Close closes the underlying database connection pool.
func (p *PostgresTarget) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// deriveColumns collects all unique field names across all records, sorted.
func (p *PostgresTarget) deriveColumns(results []*model.Record) []string {
	seen := make(map[string]struct{})
	for _, r := range results {
		for k := range r.Fields {
			seen[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(seen))
	for k := range seen {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

// createTable issues a CREATE TABLE IF NOT EXISTS with types inferred from
// the result values. Existing tables with a matching name are left untouched.
func (p *PostgresTarget) createTable(ctx context.Context, columns []string, results []*model.Record) error {
	colDefs := make([]string, len(columns))
	for i, col := range columns {
		colDefs[i] = fmt.Sprintf("%s %s", p.quoteIdentifier(col), p.inferColumnType(col, results))
	}

	_, err := p.db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		p.quoteIdentifier(p.tableName),
		strings.Join(colDefs, ", "),
	))
	return err
}

// inferColumnType maps Go value types to PostgreSQL column types.
// It scans all records for the first non-nil value of the column.
func (p *PostgresTarget) inferColumnType(column string, results []*model.Record) string {
	for _, r := range results {
		v, ok := r.Fields[column]
		if !ok || v == nil {
			continue
		}
		switch v.(type) {
		case int, int8, int16, int32, int64:
			return "BIGINT"
		case float32, float64:
			return "DOUBLE PRECISION"
		case bool:
			return "BOOLEAN"
		default:
			return "TEXT"
		}
	}
	return "TEXT"
}

// quoteIdentifier wraps a name in double-quotes for safe use in SQL DDL/DML.
func (p *PostgresTarget) quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteColumns returns a comma-separated list of quoted column identifiers.
func (p *PostgresTarget) quoteColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = p.quoteIdentifier(col)
	}
	return strings.Join(quoted, ", ")
}
