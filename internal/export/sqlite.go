package export

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// Compile-time check that SQLiteTarget implements ExportTarget.
var _ ExportTarget = (*SQLiteTarget)(nil)

// SQLiteTarget implements ExportTarget for writing results to a SQLite database.
type SQLiteTarget struct {
	dbPath    string
	tableName string
	db        *sql.DB
	schema    map[string]string // optional explicit column → SQLite type overrides
}

// NewSQLiteTarget creates a new SQLiteTarget with the given database path and table name.
func NewSQLiteTarget(dbPath, tableName string) (*SQLiteTarget, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database %s: %w", dbPath, err)
	}

	// Verify connectivity
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to SQLite database %s: %w", dbPath, err)
	}

	return &SQLiteTarget{
		dbPath:    dbPath,
		tableName: tableName,
		db:        db,
	}, nil
}

// Write creates the table if it does not exist and inserts all results as rows.
func (s *SQLiteTarget) Write(ctx context.Context, results []*model.Record) error {
	if len(results) == 0 {
		return nil
	}

	// Derive columns from all result fields (union of all keys across records).
	columns := s.deriveColumns(results)
	if len(columns) == 0 {
		return nil
	}

	// Create table if not exists.
	if err := s.createTable(ctx, columns, results); err != nil {
		return fmt.Errorf("failed to create table %s: %w", s.tableName, err)
	}

	// Insert all results as rows within a transaction.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		s.quoteIdentifier(s.tableName),
		s.quoteColumns(columns),
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

// Type returns the export target type.
func (s *SQLiteTarget) Type() string {
	return "sqlite"
}

// Identifier returns the database path.
func (s *SQLiteTarget) Identifier() string {
	return s.dbPath
}

// Close closes the database connection.
func (s *SQLiteTarget) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// deriveColumns collects all unique field names across all records in sorted order.
func (s *SQLiteTarget) deriveColumns(results []*model.Record) []string {
	columnSet := make(map[string]struct{})
	for _, record := range results {
		for key := range record.Fields {
			columnSet[key] = struct{}{}
		}
	}

	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)
	return columns
}

// createTable creates the table if it does not exist, deriving column types from the first record's values.
func (s *SQLiteTarget) createTable(ctx context.Context, columns []string, results []*model.Record) error {
	colDefs := make([]string, len(columns))
	for i, col := range columns {
		colType := s.inferColumnType(col, results)
		colDefs[i] = fmt.Sprintf("%s %s", s.quoteIdentifier(col), colType)
	}

	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		s.quoteIdentifier(s.tableName),
		strings.Join(colDefs, ", "),
	)

	_, err := s.db.ExecContext(ctx, createSQL)
	return err
}

// SetSchema sets explicit column type overrides, bypassing inference for named columns.
func (s *SQLiteTarget) SetSchema(schema map[string]string) {
	s.schema = schema
}

// inferColumnType determines the SQLite column type for a column. If an explicit
// type is provided in the schema override, it is used directly; otherwise the
// type is inferred from the first non-nil value across all records.
func (s *SQLiteTarget) inferColumnType(column string, results []*model.Record) string {
	if s.schema != nil {
		if t, ok := s.schema[column]; ok && t != "" {
			return strings.ToUpper(t)
		}
	}
	for _, record := range results {
		val, ok := record.Fields[column]
		if !ok || val == nil {
			continue
		}
		switch val.(type) {
		case int, int8, int16, int32, int64:
			return "INTEGER"
		case float32, float64:
			return "REAL"
		case bool:
			return "INTEGER"
		default:
			return "TEXT"
		}
	}
	return "TEXT"
}

// quoteIdentifier wraps an identifier in double quotes for safe use in SQL.
func (s *SQLiteTarget) quoteIdentifier(name string) string {
	// Escape any double quotes within the identifier
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// quoteColumns builds a comma-separated list of quoted column identifiers.
func (s *SQLiteTarget) quoteColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = s.quoteIdentifier(col)
	}
	return strings.Join(quoted, ", ")
}
