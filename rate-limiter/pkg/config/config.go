package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all service configuration, loaded from environment variables.
type Config struct {
	// Server ports
	HTTPPort int
	GRPCPort int

	// Default rate limiter settings
	DefaultAlgorithm string
	DefaultRate      float64       // requests per second
	DefaultCapacity  int           // max burst / bucket size
	DefaultWindow    time.Duration // sliding window size
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		HTTPPort: getEnvInt("HTTP_PORT", 8080),
		GRPCPort: getEnvInt("GRPC_PORT", 9090),

		DefaultAlgorithm: getEnvStr("DEFAULT_ALGORITHM", "token_bucket"),
		DefaultRate:      getEnvFloat("DEFAULT_RATE", 100.0),
		DefaultCapacity:  getEnvInt("DEFAULT_CAPACITY", 200),
		DefaultWindow:    getEnvDuration("DEFAULT_WINDOW", time.Second),
	}
}

func getEnvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
