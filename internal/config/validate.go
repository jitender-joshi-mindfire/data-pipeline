package config

import (
	"fmt"
	"strings"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// ValidationError holds details about a single validation failure.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidateJobConfig validates a JobConfig and returns a slice of ValidationErrors.
// An empty slice indicates a valid configuration.
func ValidateJobConfig(cfg model.JobConfig) []ValidationError {
	var errs []ValidationError

	errs = append(errs, validateSources(cfg.Sources)...)
	errs = append(errs, validateExports(cfg.Exports)...)
	errs = append(errs, validateWorkerPools(cfg.WorkerPools)...)
	errs = append(errs, validateTimeout(cfg.TimeoutSeconds)...)

	return errs
}

// validSourceTypes defines the allowed source types.
var validSourceTypes = map[string]bool{
	"csv":  true,
	"json": true,
	"http": true,
}

// validExportTypes defines the allowed export types.
var validExportTypes = map[string]bool{
	"sqlite": true,
	"csv":    true,
	"json":   true,
}

func validateSources(sources []model.SourceConfig) []ValidationError {
	var errs []ValidationError

	if len(sources) == 0 {
		errs = append(errs, ValidationError{
			Field:   "sources",
			Message: "at least one source is required",
		})
		return errs
	}

	for i, src := range sources {
		if !validSourceTypes[src.Type] {
			errs = append(errs, ValidationError{
				Field:   fmt.Sprintf("sources[%d].type", i),
				Message: fmt.Sprintf("invalid source type %q; must be one of: %s", src.Type, joinKeys(validSourceTypes)),
			})
		}
	}

	return errs
}

func validateExports(exports []model.ExportConfig) []ValidationError {
	var errs []ValidationError

	if len(exports) == 0 {
		errs = append(errs, ValidationError{
			Field:   "exports",
			Message: "at least one export target is required",
		})
		return errs
	}

	for i, exp := range exports {
		if !validExportTypes[exp.Type] {
			errs = append(errs, ValidationError{
				Field:   fmt.Sprintf("exports[%d].type", i),
				Message: fmt.Sprintf("invalid export type %q; must be one of: %s", exp.Type, joinKeys(validExportTypes)),
			})
		}
	}

	return errs
}

func validateWorkerPools(pools model.WorkerPoolConfig) []ValidationError {
	var errs []ValidationError

	type poolEntry struct {
		name string
		size int
	}

	entries := []poolEntry{
		{"worker_pools.ingester", pools.Ingester},
		{"worker_pools.validator", pools.Validator},
		{"worker_pools.transformer", pools.Transformer},
		{"worker_pools.aggregator", pools.Aggregator},
		{"worker_pools.exporter", pools.Exporter},
	}

	for _, entry := range entries {
		// A size of 0 means "not specified" and defaults to 1, so skip validation.
		if entry.size == 0 {
			continue
		}
		if entry.size < 1 || entry.size > 32 {
			errs = append(errs, ValidationError{
				Field:   entry.name,
				Message: fmt.Sprintf("worker pool size %d is out of range; must be between 1 and 32 inclusive", entry.size),
			})
		}
	}

	return errs
}

func validateTimeout(timeout *int) []ValidationError {
	var errs []ValidationError

	if timeout == nil {
		return errs
	}

	if *timeout < 1 || *timeout > 86400 {
		errs = append(errs, ValidationError{
			Field:   "timeout_seconds",
			Message: fmt.Sprintf("timeout value %d is out of range; must be between 1 and 86400 inclusive", *timeout),
		})
	}

	return errs
}

// joinKeys returns a comma-separated list of map keys sorted for display.
func joinKeys(m map[string]bool) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, fmt.Sprintf("%q", k))
	}
	return strings.Join(keys, ", ")
}
