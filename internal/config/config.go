package config

import (
	"fmt"
	"os"

	"github.com/jitendraj/data-pipeline/internal/model"
)

// ApplyEnvOverrides applies environment variable overrides to a JobConfig.
// For each source in the configuration, if an environment variable is set for
// its path (file path or API URL), the environment variable value takes precedence
// over the JSON configuration value.
//
// Environment variable naming convention:
//   - PIPELINE_SOURCE_<index>_PATH overrides the path for the source at that index
//     (e.g., PIPELINE_SOURCE_0_PATH overrides the first source's path)
//
// Only non-empty environment variable values are applied as overrides.
func ApplyEnvOverrides(cfg *model.JobConfig) {
	for i := range cfg.Sources {
		envKey := fmt.Sprintf("PIPELINE_SOURCE_%d_PATH", i)
		if val := os.Getenv(envKey); val != "" {
			cfg.Sources[i].Path = val
		}
	}
}
