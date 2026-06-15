package config

import (
	"os"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
)

func TestApplyEnvOverrides_OverridesSourcePaths(t *testing.T) {
	// Set environment variables for source paths
	os.Setenv("PIPELINE_SOURCE_0_PATH", "/override/path.csv")
	os.Setenv("PIPELINE_SOURCE_1_PATH", "https://override.example.com/data")
	defer os.Unsetenv("PIPELINE_SOURCE_0_PATH")
	defer os.Unsetenv("PIPELINE_SOURCE_1_PATH")

	cfg := &model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/original/path.csv"},
			{Type: "http", Path: "https://original.example.com/data"},
		},
	}

	ApplyEnvOverrides(cfg)

	if cfg.Sources[0].Path != "/override/path.csv" {
		t.Errorf("expected source 0 path to be overridden, got %s", cfg.Sources[0].Path)
	}
	if cfg.Sources[1].Path != "https://override.example.com/data" {
		t.Errorf("expected source 1 path to be overridden, got %s", cfg.Sources[1].Path)
	}
}

func TestApplyEnvOverrides_NoOverrideWhenEnvNotSet(t *testing.T) {
	// Ensure env vars are not set
	os.Unsetenv("PIPELINE_SOURCE_0_PATH")
	os.Unsetenv("PIPELINE_SOURCE_1_PATH")

	cfg := &model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/original/path.csv"},
			{Type: "json", Path: "/original/data.json"},
		},
	}

	ApplyEnvOverrides(cfg)

	if cfg.Sources[0].Path != "/original/path.csv" {
		t.Errorf("expected source 0 path unchanged, got %s", cfg.Sources[0].Path)
	}
	if cfg.Sources[1].Path != "/original/data.json" {
		t.Errorf("expected source 1 path unchanged, got %s", cfg.Sources[1].Path)
	}
}

func TestApplyEnvOverrides_EmptyEnvVarDoesNotOverride(t *testing.T) {
	os.Setenv("PIPELINE_SOURCE_0_PATH", "")
	defer os.Unsetenv("PIPELINE_SOURCE_0_PATH")

	cfg := &model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/original/path.csv"},
		},
	}

	ApplyEnvOverrides(cfg)

	if cfg.Sources[0].Path != "/original/path.csv" {
		t.Errorf("expected source 0 path unchanged when env is empty, got %s", cfg.Sources[0].Path)
	}
}

func TestApplyEnvOverrides_PartialOverride(t *testing.T) {
	// Only override source 1, not source 0
	os.Unsetenv("PIPELINE_SOURCE_0_PATH")
	os.Setenv("PIPELINE_SOURCE_1_PATH", "/new/api/endpoint")
	defer os.Unsetenv("PIPELINE_SOURCE_1_PATH")

	cfg := &model.JobConfig{
		Sources: []model.SourceConfig{
			{Type: "csv", Path: "/original/path.csv"},
			{Type: "http", Path: "https://original.api.com/data"},
			{Type: "json", Path: "/original/data.json"},
		},
	}

	ApplyEnvOverrides(cfg)

	if cfg.Sources[0].Path != "/original/path.csv" {
		t.Errorf("expected source 0 path unchanged, got %s", cfg.Sources[0].Path)
	}
	if cfg.Sources[1].Path != "/new/api/endpoint" {
		t.Errorf("expected source 1 path to be overridden, got %s", cfg.Sources[1].Path)
	}
	if cfg.Sources[2].Path != "/original/data.json" {
		t.Errorf("expected source 2 path unchanged, got %s", cfg.Sources[2].Path)
	}
}

func TestApplyEnvOverrides_EmptySources(t *testing.T) {
	cfg := &model.JobConfig{
		Sources: []model.SourceConfig{},
	}

	// Should not panic with empty sources
	ApplyEnvOverrides(cfg)

	if len(cfg.Sources) != 0 {
		t.Errorf("expected sources to remain empty")
	}
}
