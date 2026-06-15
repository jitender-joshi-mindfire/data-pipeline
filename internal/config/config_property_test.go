package config

import (
	"fmt"
	"os"
	"testing"

	"github.com/jitendraj/data-pipeline/internal/model"
	"pgregory.net/rapid"
)

// Feature: data-processing-pipeline, Property 20: Environment Variable Override
// Validates: Requirements 14.3
//
// For any job configuration where environment variables are set for source file
// paths or API URLs, the pipeline shall use the environment variable values
// instead of the corresponding values in the JSON configuration.
func TestProperty20_EnvironmentVariableOverride(t *testing.T) {
	// Sub-test: env vars override JSON config source paths
	t.Run("EnvVarOverridesSourcePath", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Generate a random number of sources (1..5)
			numSources := rapid.IntRange(1, 5).Draw(t, "numSources")

			// Generate original paths from JSON config
			sources := make([]model.SourceConfig, numSources)
			sourceTypes := []string{"csv", "json", "http"}
			for i := 0; i < numSources; i++ {
				srcType := rapid.SampledFrom(sourceTypes).Draw(t, fmt.Sprintf("srcType_%d", i))
				originalPath := rapid.StringOfN(
					rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz0123456789/._-")),
					1, 100, -1,
				).Draw(t, fmt.Sprintf("originalPath_%d", i))
				sources[i] = model.SourceConfig{
					Type: srcType,
					Path: originalPath,
				}
			}

			// Generate override paths from env vars (non-empty)
			envPaths := make([]string, numSources)
			for i := 0; i < numSources; i++ {
				envPaths[i] = rapid.StringOfN(
					rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz0123456789/._-")),
					1, 100, -1,
				).Draw(t, fmt.Sprintf("envPath_%d", i))
			}

			// Set environment variables
			for i := 0; i < numSources; i++ {
				envKey := fmt.Sprintf("PIPELINE_SOURCE_%d_PATH", i)
				os.Setenv(envKey, envPaths[i])
				defer os.Unsetenv(envKey)
			}

			cfg := model.JobConfig{
				Sources: sources,
			}

			// Apply env overrides
			ApplyEnvOverrides(&cfg)

			// Property: after applying env overrides, each source path must equal
			// the environment variable value, NOT the original JSON config value
			for i := 0; i < numSources; i++ {
				if cfg.Sources[i].Path != envPaths[i] {
					t.Fatalf("source[%d]: expected path %q (from env var), got %q",
						i, envPaths[i], cfg.Sources[i].Path)
				}
			}
		})
	})

	// Sub-test: unset env vars do NOT override JSON config values
	t.Run("UnsetEnvVarPreservesOriginalPath", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Generate a random number of sources (1..5)
			numSources := rapid.IntRange(1, 5).Draw(t, "numSources")

			// Generate original paths from JSON config
			sources := make([]model.SourceConfig, numSources)
			originalPaths := make([]string, numSources)
			sourceTypes := []string{"csv", "json", "http"}
			for i := 0; i < numSources; i++ {
				srcType := rapid.SampledFrom(sourceTypes).Draw(t, fmt.Sprintf("srcType_%d", i))
				originalPaths[i] = rapid.StringOfN(
					rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz0123456789/._-")),
					1, 100, -1,
				).Draw(t, fmt.Sprintf("originalPath_%d", i))
				sources[i] = model.SourceConfig{
					Type: srcType,
					Path: originalPaths[i],
				}
			}

			// Ensure env vars are NOT set
			for i := 0; i < numSources; i++ {
				envKey := fmt.Sprintf("PIPELINE_SOURCE_%d_PATH", i)
				os.Unsetenv(envKey)
			}

			cfg := model.JobConfig{
				Sources: sources,
			}

			// Apply env overrides
			ApplyEnvOverrides(&cfg)

			// Property: without env vars set, original paths are preserved
			for i := 0; i < numSources; i++ {
				if cfg.Sources[i].Path != originalPaths[i] {
					t.Fatalf("source[%d]: expected original path %q preserved, got %q",
						i, originalPaths[i], cfg.Sources[i].Path)
				}
			}
		})
	})

	// Sub-test: partial env var overrides (some set, some not)
	t.Run("PartialEnvVarOverrides", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Generate a random number of sources (2..5)
			numSources := rapid.IntRange(2, 5).Draw(t, "numSources")

			// Generate original paths from JSON config
			sources := make([]model.SourceConfig, numSources)
			originalPaths := make([]string, numSources)
			sourceTypes := []string{"csv", "json", "http"}
			for i := 0; i < numSources; i++ {
				srcType := rapid.SampledFrom(sourceTypes).Draw(t, fmt.Sprintf("srcType_%d", i))
				originalPaths[i] = rapid.StringOfN(
					rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz0123456789/._-")),
					1, 100, -1,
				).Draw(t, fmt.Sprintf("originalPath_%d", i))
				sources[i] = model.SourceConfig{
					Type: srcType,
					Path: originalPaths[i],
				}
			}

			// Randomly decide which sources get env var overrides
			overrideFlags := make([]bool, numSources)
			envPaths := make([]string, numSources)
			for i := 0; i < numSources; i++ {
				overrideFlags[i] = rapid.Bool().Draw(t, fmt.Sprintf("override_%d", i))
				envKey := fmt.Sprintf("PIPELINE_SOURCE_%d_PATH", i)
				if overrideFlags[i] {
					envPaths[i] = rapid.StringOfN(
						rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz0123456789/._-")),
						1, 100, -1,
					).Draw(t, fmt.Sprintf("envPath_%d", i))
					os.Setenv(envKey, envPaths[i])
					defer os.Unsetenv(envKey)
				} else {
					os.Unsetenv(envKey)
				}
			}

			cfg := model.JobConfig{
				Sources: sources,
			}

			// Apply env overrides
			ApplyEnvOverrides(&cfg)

			// Property: sources with env vars set use env var value;
			// sources without env vars preserve original path
			for i := 0; i < numSources; i++ {
				if overrideFlags[i] {
					if cfg.Sources[i].Path != envPaths[i] {
						t.Fatalf("source[%d]: expected env override path %q, got %q",
							i, envPaths[i], cfg.Sources[i].Path)
					}
				} else {
					if cfg.Sources[i].Path != originalPaths[i] {
						t.Fatalf("source[%d]: expected original path %q preserved, got %q",
							i, originalPaths[i], cfg.Sources[i].Path)
					}
				}
			}
		})
	})
}
