// Package membrane provides the top-level API surface that wires together
// all subsystems of the memory substrate: ingestion, retrieval, decay,
// revision, consolidation, and metrics.
package membrane

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configurable parameters for a Membrane instance.
type Config struct {
	// DBPath is the SQLite database path.
	DBPath string `yaml:"db_path"`

	// ListenAddr is the gRPC listen address (default: ":9090").
	ListenAddr string `yaml:"listen_addr"`

	// DecayInterval is how often the decay scheduler runs (default: 1h).
	DecayInterval time.Duration `yaml:"decay_interval"`

	// ConsolidationInterval is how often the consolidation scheduler runs (default: 6h).
	ConsolidationInterval time.Duration `yaml:"consolidation_interval"`

	// DefaultSensitivity is the ingestion default sensitivity level (default: "low").
	DefaultSensitivity string `yaml:"default_sensitivity"`

	// SelectionConfidenceThreshold is the minimum confidence for the retrieval
	// selector to consider a competence or plan_graph candidate (default: 0.7).
	SelectionConfidenceThreshold float64 `yaml:"selection_confidence_threshold"`

	// EncryptionKey is the SQLCipher encryption key for the database.
	// If empty, the database is not encrypted. Read from MEMBRANE_ENCRYPTION_KEY
	// environment variable if not set in config.
	EncryptionKey string `yaml:"encryption_key"`

	// TLSCertFile is the path to the TLS certificate PEM file.
	// If empty, the server runs without TLS.
	TLSCertFile string `yaml:"tls_cert_file"`

	// TLSKeyFile is the path to the TLS private key PEM file.
	TLSKeyFile string `yaml:"tls_key_file"`

	// APIKey is a shared secret for authenticating gRPC clients.
	// If empty, authentication is disabled. Read from MEMBRANE_API_KEY
	// environment variable if not set in config.
	APIKey string `yaml:"api_key"`

	// RateLimitPerSecond is the maximum requests per second per client.
	// 0 means no rate limiting. Default: 100.
	RateLimitPerSecond int `yaml:"rate_limit_per_second"`
}

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		DBPath:                       "membrane.db",
		ListenAddr:                   ":9090",
		DecayInterval:                1 * time.Hour,
		ConsolidationInterval:        6 * time.Hour,
		DefaultSensitivity:           "low",
		SelectionConfidenceThreshold: 0.7,
		EncryptionKey:                "",
		RateLimitPerSecond:           100,
	}
}

// LoadConfig reads a YAML configuration file from path and returns a Config.
// Fields not present in the file retain their default values.
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	return cfg, nil
}
