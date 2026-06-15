package config

import (
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"github.com/stretchr/testify/assert"
)

func validJobConfig() model.JobConfig {
	timeout := 3600
	return model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/data/input.csv"},
		},
		Exports: []model.ExportConfig{
			{Type: "json", Path: "/data/output.json"},
		},
		WorkerPools: model.WorkerPoolConfig{
			Ingester:  2,
			Validator: 4,
		},
		TimeoutSeconds: &timeout,
	}
}

func TestValidateJobConfig_ValidConfig(t *testing.T) {
	cfg := validJobConfig()
	errs := ValidateJobConfig(cfg)
	assert.Empty(t, errs)
}

func TestValidateJobConfig_NoSources(t *testing.T) {
	cfg := validJobConfig()
	cfg.Sources = nil
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "sources", errs[0].Field)
	assert.Contains(t, errs[0].Message, "at least one source is required")
}

func TestValidateJobConfig_NoExports(t *testing.T) {
	cfg := validJobConfig()
	cfg.Exports = nil
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "exports", errs[0].Field)
	assert.Contains(t, errs[0].Message, "at least one export target is required")
}

func TestValidateJobConfig_InvalidSourceType(t *testing.T) {
	cfg := validJobConfig()
	cfg.Sources = []model.SourceConfig{
		{Type: "xml", Path: "/data/input.xml"},
	}
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "sources[0].type", errs[0].Field)
	assert.Contains(t, errs[0].Message, "invalid source type")
}

func TestValidateJobConfig_InvalidExportType(t *testing.T) {
	cfg := validJobConfig()
	cfg.Exports = []model.ExportConfig{
		{Type: "xml", Path: "/data/output.xml"},
	}
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "exports[0].type", errs[0].Field)
	assert.Contains(t, errs[0].Message, "invalid export type")
}

func TestValidateJobConfig_ValidSourceTypes(t *testing.T) {
	for _, srcType := range []string{"csv", "json", "http"} {
		cfg := validJobConfig()
		cfg.Sources = []model.SourceConfig{
			{Type: srcType, Path: "/data/input"},
		}
		errs := ValidateJobConfig(cfg)
		assert.Empty(t, errs, "source type %q should be valid", srcType)
	}
}

func TestValidateJobConfig_ValidExportTypes(t *testing.T) {
	for _, expType := range []string{"sqlite", "csv", "json"} {
		cfg := validJobConfig()
		cfg.Exports = []model.ExportConfig{
			{Type: expType, Path: "/data/output"},
		}
		errs := ValidateJobConfig(cfg)
		assert.Empty(t, errs, "export type %q should be valid", expType)
	}
}

func TestValidateJobConfig_WorkerPoolTooSmall(t *testing.T) {
	cfg := validJobConfig()
	cfg.WorkerPools = model.WorkerPoolConfig{
		Ingester: -1,
	}
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "worker_pools.ingester", errs[0].Field)
	assert.Contains(t, errs[0].Message, "out of range")
}

func TestValidateJobConfig_WorkerPoolTooLarge(t *testing.T) {
	cfg := validJobConfig()
	cfg.WorkerPools = model.WorkerPoolConfig{
		Validator: 33,
	}
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "worker_pools.validator", errs[0].Field)
	assert.Contains(t, errs[0].Message, "out of range")
}

func TestValidateJobConfig_WorkerPoolZeroIsDefault(t *testing.T) {
	cfg := validJobConfig()
	cfg.WorkerPools = model.WorkerPoolConfig{} // all zeros = defaults
	errs := ValidateJobConfig(cfg)
	assert.Empty(t, errs)
}

func TestValidateJobConfig_WorkerPoolBoundaryValues(t *testing.T) {
	cfg := validJobConfig()
	cfg.WorkerPools = model.WorkerPoolConfig{
		Ingester:    1,
		Validator:   32,
		Transformer: 16,
		Aggregator:  1,
		Exporter:    32,
	}
	errs := ValidateJobConfig(cfg)
	assert.Empty(t, errs)
}

func TestValidateJobConfig_TimeoutTooSmall(t *testing.T) {
	cfg := validJobConfig()
	timeout := 0
	cfg.TimeoutSeconds = &timeout
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "timeout_seconds", errs[0].Field)
	assert.Contains(t, errs[0].Message, "out of range")
}

func TestValidateJobConfig_TimeoutTooLarge(t *testing.T) {
	cfg := validJobConfig()
	timeout := 86401
	cfg.TimeoutSeconds = &timeout
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 1)
	assert.Equal(t, "timeout_seconds", errs[0].Field)
	assert.Contains(t, errs[0].Message, "out of range")
}

func TestValidateJobConfig_TimeoutNilIsValid(t *testing.T) {
	cfg := validJobConfig()
	cfg.TimeoutSeconds = nil
	errs := ValidateJobConfig(cfg)
	assert.Empty(t, errs)
}

func TestValidateJobConfig_TimeoutBoundaryValues(t *testing.T) {
	cfg := validJobConfig()

	timeout := 1
	cfg.TimeoutSeconds = &timeout
	errs := ValidateJobConfig(cfg)
	assert.Empty(t, errs)

	timeout = 86400
	cfg.TimeoutSeconds = &timeout
	errs = ValidateJobConfig(cfg)
	assert.Empty(t, errs)
}

func TestValidateJobConfig_MultipleErrors(t *testing.T) {
	timeout := -5
	cfg := model.JobConfig{
		Sources: nil,
		Exports: nil,
		WorkerPools: model.WorkerPoolConfig{
			Ingester: 100,
		},
		TimeoutSeconds: &timeout,
	}
	errs := ValidateJobConfig(cfg)
	// Expect: missing sources, missing exports, invalid pool size, invalid timeout
	assert.Len(t, errs, 4)

	fields := make(map[string]bool)
	for _, e := range errs {
		fields[e.Field] = true
	}
	assert.True(t, fields["sources"])
	assert.True(t, fields["exports"])
	assert.True(t, fields["worker_pools.ingester"])
	assert.True(t, fields["timeout_seconds"])
}

func TestValidateJobConfig_MultipleInvalidSources(t *testing.T) {
	cfg := validJobConfig()
	cfg.Sources = []model.SourceConfig{
		{Type: "csv", Path: "/ok.csv"},
		{Type: "xml", Path: "/bad.xml"},
		{Type: "ftp", Path: "/bad.ftp"},
	}
	errs := ValidateJobConfig(cfg)
	assert.Len(t, errs, 2)
	assert.Equal(t, "sources[1].type", errs[0].Field)
	assert.Equal(t, "sources[2].type", errs[1].Field)
}

func TestValidationError_ErrorMethod(t *testing.T) {
	e := ValidationError{Field: "sources", Message: "at least one source is required"}
	assert.Equal(t, "sources: at least one source is required", e.Error())
}
