package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// envVarPattern matches ${ENV_VAR} or ${ENV_VAR:-default} patterns.
var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// ResolveSecrets performs environment variable substitution on sensitive fields in the config.
// Supported syntax:
//   - ${VAR_NAME}           — replaced with the env var value; error if unset
//   - ${VAR_NAME:-default}  — replaced with env var value, or "default" if unset/empty
//
// Only password fields are resolved by default. Call this after LoadConfig.
func ResolveSecrets(cfg *Config) error {
	for i := range cfg.Databases {
		resolved, err := resolveValue(cfg.Databases[i].Password)
		if err != nil {
			return fmt.Errorf("database %q password: %w", cfg.Databases[i].Name, err)
		}
		cfg.Databases[i].Password = resolved

		// Also resolve user (may come from env in some setups)
		resolved, err = resolveValue(cfg.Databases[i].User)
		if err != nil {
			return fmt.Errorf("database %q user: %w", cfg.Databases[i].Name, err)
		}
		cfg.Databases[i].User = resolved

		// Resolve host (useful for dynamic service discovery)
		resolved, err = resolveValue(cfg.Databases[i].Host)
		if err != nil {
			return fmt.Errorf("database %q host: %w", cfg.Databases[i].Name, err)
		}
		cfg.Databases[i].Host = resolved
	}
	return nil
}

// resolveValue replaces ${...} patterns in a string with environment variable values.
// If the string does not contain any ${...} pattern, it's returned as-is.
func resolveValue(value string) (string, error) {
	if !strings.Contains(value, "${") {
		return value, nil
	}

	var resolveErr error
	result := envVarPattern.ReplaceAllStringFunc(value, func(match string) string {
		// if resolveErr != nil {
		// 	return match
		// }

		// Strip ${ and }
		inner := match[2 : len(match)-1]

		varName := inner
		defaultVal := ""
		hasDefault := false

		// Check for :- syntax
		if idx := strings.Index(inner, ":-"); idx >= 0 {
			varName = inner[:idx]
			defaultVal = inner[idx+2:]
			hasDefault = true
		}

		envVal, exists := os.LookupEnv(varName)
		if !exists || envVal == "" {
			if hasDefault {
				return defaultVal
			}
			resolveErr = fmt.Errorf("environment variable %q is not set", varName)
			return match
		}
		return envVal
	})

	if resolveErr != nil {
		return "", resolveErr
	}
	return result, nil
}
