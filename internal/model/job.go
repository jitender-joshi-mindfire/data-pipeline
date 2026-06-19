package model

import "time"

// JobStatus represents the current state of a pipeline job
type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
)

// Job represents a pipeline execution instance
type Job struct {
	ID          string     `json:"id"`
	Config      JobConfig  `json:"config"`
	Status      JobStatus  `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

// JobConfig defines the complete configuration for a pipeline job
type JobConfig struct {
	Sources         []SourceConfig      `json:"sources"`
	Validation      ValidationConfig    `json:"validation"`
	Transformations []TransformConfig   `json:"transformations"`
	Aggregation     AggregationConfig   `json:"aggregation"`
	Exports         []ExportConfig      `json:"exports"`
	WorkerPools     WorkerPoolConfig    `json:"worker_pools"`
	TimeoutSeconds  *int                `json:"timeout_seconds,omitempty"`
}

// SourceConfig defines a data source for ingestion
type SourceConfig struct {
	Type           string `json:"type"`                      // "csv", "json", "http"
	Path           string `json:"path"`                      // File path or URL
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"` // HTTP timeout, default 30
}

// ValidationConfig defines the validation schema for records
type ValidationConfig struct {
	Fields []FieldSchema `json:"fields"`
}

// FieldSchema defines validation rules for a single field
type FieldSchema struct {
	Name     string   `json:"name"`
	Type     string   `json:"type"`              // "string", "number", "date", "boolean"
	Required bool     `json:"required"`
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`
	Pattern  string   `json:"pattern,omitempty"` // Regex pattern for strings
}

// TransformConfig defines a transformation rule for a field
type TransformConfig struct {
	Field      string `json:"field"`
	Operation  string `json:"operation"`             // "type_convert", "trim", "lowercase", "uppercase", "enrich"
	TargetType string `json:"target_type,omitempty"` // For type_convert
	Expression string `json:"expression,omitempty"`  // For enrich
}

// AggregationConfig defines how records should be aggregated
type AggregationConfig struct {
	GroupBy   []string              `json:"group_by,omitempty"`
	Functions []AggregationFunction `json:"functions"`
}

// AggregationFunction defines a single aggregation computation
type AggregationFunction struct {
	Name  string `json:"name"`  // "count", "sum", "average"
	Field string `json:"field"` // Target field
	Alias string `json:"alias"` // Output field name
}

// ExportConfig defines an export target for results
type ExportConfig struct {
	Type      string `json:"type"`                 // "sqlite", "postgres", "csv", "json"
	Path      string `json:"path"`                 // File path, SQLite path, or Postgres DSN
	TableName string `json:"table_name,omitempty"` // For SQLite and Postgres
}

// WorkerPoolConfig defines the number of workers per pipeline stage
type WorkerPoolConfig struct {
	Ingester    int `json:"ingester,omitempty"`
	Validator   int `json:"validator,omitempty"`
	Transformer int `json:"transformer,omitempty"`
	Aggregator  int `json:"aggregator,omitempty"`
	Exporter    int `json:"exporter,omitempty"`
}
