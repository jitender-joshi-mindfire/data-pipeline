package config

import (
	"fmt"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 14: Job Configuration Validation
// Validates: Requirements 14.1, 14.2, 14.4, 14.5
//
// For any JSON job configuration payload, it shall be accepted if and only if
// it contains at least one valid source, at least one valid export target,
// all source types are in {"csv", "json", "http"}, all export types are in
// {"sqlite", "csv", "json"}, and all required fields are present; otherwise a
// 400 response shall identify each specific validation failure.
func TestProperty14_JobConfigurationValidation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cfg := genJobConfig(t)
		errs := ValidateJobConfig(cfg)

		// Independently compute whether the config should be valid
		shouldBeValid := isConfigValid(cfg)

		if shouldBeValid {
			// Property: valid configs produce zero validation errors
			if len(errs) != 0 {
				t.Fatalf("expected valid config to have no errors, got %d errors: %v", len(errs), errs)
			}
		} else {
			// Property: invalid configs produce at least one validation error
			if len(errs) == 0 {
				t.Fatalf("expected invalid config to have errors, but got none")
			}

			// Property: each error identifies a specific field
			for _, e := range errs {
				if e.Field == "" {
					t.Fatalf("validation error has empty field: %v", e)
				}
				if e.Message == "" {
					t.Fatalf("validation error has empty message: %v", e)
				}
			}

			// Property: specific failure conditions produce specific error fields
			if len(cfg.Sources) == 0 {
				assertHasErrorForField(t, errs, "sources")
			}
			if len(cfg.Exports) == 0 {
				assertHasErrorForField(t, errs, "exports")
			}
			for i, src := range cfg.Sources {
				if !isValidSourceType(src.Type) {
					assertHasErrorForFieldPrefix(t, errs, "sources[", i)
				}
			}
			for i, exp := range cfg.Exports {
				if !isValidExportType(exp.Type) {
					assertHasErrorForFieldPrefix(t, errs, "exports[", i)
				}
			}
		}
	})
}

// genJobConfig generates an arbitrary JobConfig for property testing.
// It may generate both valid and invalid configs to exercise all branches.
func genJobConfig(t *rapid.T) model.JobConfig {
	// Decide how many sources (0..5); 0 triggers "missing sources" error
	numSources := rapid.IntRange(0, 5).Draw(t, "numSources")
	sources := make([]model.SourceConfig, numSources)
	for i := 0; i < numSources; i++ {
		sources[i] = genSourceConfig(t, i)
	}

	// Decide how many exports (0..5); 0 triggers "missing exports" error
	numExports := rapid.IntRange(0, 5).Draw(t, "numExports")
	exports := make([]model.ExportConfig, numExports)
	for i := 0; i < numExports; i++ {
		exports[i] = genExportConfig(t, i)
	}

	// Generate worker pool config
	pools := genWorkerPoolConfig(t)

	// Generate optional timeout
	var timeout *int
	hasTimeout := rapid.Bool().Draw(t, "hasTimeout")
	if hasTimeout {
		tv := rapid.IntRange(-10, 100000).Draw(t, "timeoutValue")
		timeout = &tv
	}

	return model.JobConfig{
		Sources:        sources,
		Exports:        exports,
		WorkerPools:    pools,
		TimeoutSeconds: timeout,
	}
}

// genSourceConfig generates a source config. It may pick invalid types
// to exercise the validation error path.
func genSourceConfig(t *rapid.T, idx int) model.SourceConfig {
	allTypes := []string{"csv", "json", "http", "xml", "ftp", "grpc", ""}
	srcType := rapid.SampledFrom(allTypes).Draw(t, fmt.Sprintf("srcType_%d", idx))
	path := rapid.StringOfN(rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz/._-")),
		1, 50, -1).Draw(t, fmt.Sprintf("srcPath_%d", idx))
	return model.SourceConfig{
		Type: srcType,
		Path: path,
	}
}

// genExportConfig generates an export config. It may pick invalid types
// to exercise the validation error path.
func genExportConfig(t *rapid.T, idx int) model.ExportConfig {
	allTypes := []string{"sqlite", "csv", "json", "xml", "parquet", "mongo", ""}
	expType := rapid.SampledFrom(allTypes).Draw(t, fmt.Sprintf("expType_%d", idx))
	path := rapid.StringOfN(rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz/._-")),
		1, 50, -1).Draw(t, fmt.Sprintf("expPath_%d", idx))
	return model.ExportConfig{
		Type: expType,
		Path: path,
	}
}

// genWorkerPoolConfig generates a worker pool config that may include
// out-of-range values.
func genWorkerPoolConfig(t *rapid.T) model.WorkerPoolConfig {
	// 0 means "not specified" (defaults to 1), so include 0 as valid.
	// Include negative and > 32 for invalid cases.
	genPoolSize := func(name string) int {
		return rapid.IntRange(-5, 50).Draw(t, name)
	}
	return model.WorkerPoolConfig{
		Ingester:    genPoolSize("pool_ingester"),
		Validator:   genPoolSize("pool_validator"),
		Transformer: genPoolSize("pool_transformer"),
		Aggregator:  genPoolSize("pool_aggregator"),
		Exporter:    genPoolSize("pool_exporter"),
	}
}

// isConfigValid independently determines if a config should pass validation.
func isConfigValid(cfg model.JobConfig) bool {
	// Must have at least one source
	if len(cfg.Sources) == 0 {
		return false
	}
	// Must have at least one export
	if len(cfg.Exports) == 0 {
		return false
	}
	// All source types must be valid
	for _, src := range cfg.Sources {
		if !isValidSourceType(src.Type) {
			return false
		}
	}
	// All export types must be valid
	for _, exp := range cfg.Exports {
		if !isValidExportType(exp.Type) {
			return false
		}
	}
	// Worker pool sizes: 0 is "not specified" (valid), otherwise must be in [1, 32]
	pools := []int{
		cfg.WorkerPools.Ingester,
		cfg.WorkerPools.Validator,
		cfg.WorkerPools.Transformer,
		cfg.WorkerPools.Aggregator,
		cfg.WorkerPools.Exporter,
	}
	for _, size := range pools {
		if size != 0 && (size < 1 || size > 32) {
			return false
		}
	}
	// Timeout: nil is valid, otherwise must be in [1, 86400]
	if cfg.TimeoutSeconds != nil {
		if *cfg.TimeoutSeconds < 1 || *cfg.TimeoutSeconds > 86400 {
			return false
		}
	}
	return true
}

func isValidSourceType(t string) bool {
	return t == "csv" || t == "json" || t == "http"
}

func isValidExportType(t string) bool {
	return t == "sqlite" || t == "csv" || t == "json"
}

// Feature: data-processing-pipeline, Property 2: Worker Pool Size Validation
// Validates: Requirements 2.1, 2.4
//
// For any integer value provided as a worker pool size, the pipeline configuration
// shall accept it if and only if it falls within the range [1, 32] inclusive;
// all values outside this range shall be rejected with an appropriate error.
func TestProperty2_WorkerPoolSizeValidation(t *testing.T) {
	// Base valid config used in all sub-tests (has valid sources and exports)
	baseConfig := func() model.JobConfig {
		return model.JobConfig{
			Sources: []model.SourceConfig{
				{Type: "csv", Path: "/data/input.csv"},
			},
			Exports: []model.ExportConfig{
				{Type: "json", Path: "/data/output.json"},
			},
		}
	}

	stages := []string{"ingester", "validator", "transformer", "aggregator", "exporter"}

	// Sub-test: valid pool sizes within [1, 32] produce no worker pool errors
	t.Run("ValidPoolSizes", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			size := rapid.IntRange(1, 32).Draw(t, "validPoolSize")
			stageIdx := rapid.IntRange(0, len(stages)-1).Draw(t, "stageIndex")

			cfg := baseConfig()
			setPoolSize(&cfg.WorkerPools, stages[stageIdx], size)

			errs := ValidateJobConfig(cfg)

			// No errors should reference any worker pool field
			for _, e := range errs {
				if isWorkerPoolError(e.Field) {
					t.Fatalf("expected no worker pool errors for size %d on stage %q, got: %v",
						size, stages[stageIdx], e)
				}
			}
		})
	})

	// Sub-test: pool sizes < 1 (excluding 0 which means "not specified") are rejected
	t.Run("InvalidPoolSizesBelow", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Generate negative values (0 is "not specified" and valid)
			size := rapid.IntRange(-1000, -1).Draw(t, "invalidPoolSizeBelow")
			stageIdx := rapid.IntRange(0, len(stages)-1).Draw(t, "stageIndex")

			cfg := baseConfig()
			setPoolSize(&cfg.WorkerPools, stages[stageIdx], size)

			errs := ValidateJobConfig(cfg)

			expectedField := fmt.Sprintf("worker_pools.%s", stages[stageIdx])
			found := false
			for _, e := range errs {
				if e.Field == expectedField {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected error for field %q with pool size %d, but none found in: %v",
					expectedField, size, errs)
			}
		})
	})

	// Sub-test: pool sizes > 32 are rejected
	t.Run("InvalidPoolSizesAbove", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			size := rapid.IntRange(33, 1000).Draw(t, "invalidPoolSizeAbove")
			stageIdx := rapid.IntRange(0, len(stages)-1).Draw(t, "stageIndex")

			cfg := baseConfig()
			setPoolSize(&cfg.WorkerPools, stages[stageIdx], size)

			errs := ValidateJobConfig(cfg)

			expectedField := fmt.Sprintf("worker_pools.%s", stages[stageIdx])
			found := false
			for _, e := range errs {
				if e.Field == expectedField {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected error for field %q with pool size %d, but none found in: %v",
					expectedField, size, errs)
			}
		})
	})

	// Sub-test: all stages reject invalid sizes simultaneously
	t.Run("AllStagesInvalidSimultaneously", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Generate invalid sizes for all stages
			sizes := make([]int, len(stages))
			for i := range stages {
				// Choose either below 1 (not 0) or above 32
				below := rapid.Bool().Draw(t, fmt.Sprintf("below_%d", i))
				if below {
					sizes[i] = rapid.IntRange(-1000, -1).Draw(t, fmt.Sprintf("size_%d", i))
				} else {
					sizes[i] = rapid.IntRange(33, 1000).Draw(t, fmt.Sprintf("size_%d", i))
				}
			}

			cfg := baseConfig()
			cfg.WorkerPools = model.WorkerPoolConfig{
				Ingester:    sizes[0],
				Validator:   sizes[1],
				Transformer: sizes[2],
				Aggregator:  sizes[3],
				Exporter:    sizes[4],
			}

			errs := ValidateJobConfig(cfg)

			// Each stage should have a corresponding error
			for i, stage := range stages {
				expectedField := fmt.Sprintf("worker_pools.%s", stage)
				found := false
				for _, e := range errs {
					if e.Field == expectedField {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("expected error for field %q with pool size %d, but none found in: %v",
						expectedField, sizes[i], errs)
				}
			}
		})
	})
}

// setPoolSize sets the pool size for a specific stage on the WorkerPoolConfig.
func setPoolSize(pools *model.WorkerPoolConfig, stage string, size int) {
	switch stage {
	case "ingester":
		pools.Ingester = size
	case "validator":
		pools.Validator = size
	case "transformer":
		pools.Transformer = size
	case "aggregator":
		pools.Aggregator = size
	case "exporter":
		pools.Exporter = size
	}
}

// isWorkerPoolError checks if the error field refers to a worker pool setting.
func isWorkerPoolError(field string) bool {
	return len(field) > 13 && field[:13] == "worker_pools."
}

// assertHasErrorForField checks that at least one error references the given field.
func assertHasErrorForField(t *rapid.T, errs []ValidationError, field string) {
	for _, e := range errs {
		if e.Field == field {
			return
		}
	}
	t.Fatalf("expected error for field %q, but none found in: %v", field, errs)
}

// assertHasErrorForFieldPrefix checks that at least one error references the given
// prefix with an index (e.g., "sources[1].type").
func assertHasErrorForFieldPrefix(t *rapid.T, errs []ValidationError, prefix string, idx int) {
	target := fmt.Sprintf("%s%d]", prefix, idx)
	for _, e := range errs {
		if len(e.Field) >= len(target) && e.Field[:len(target)] == target {
			return
		}
	}
	t.Fatalf("expected error for field with prefix %q, but none found in: %v", target, errs)
}
