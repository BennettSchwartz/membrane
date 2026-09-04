// Package membrane provides the top-level API surface that wires together
// all subsystems of the memory substrate: ingestion, retrieval, decay,
// revision, consolidation, and metrics.
package membrane

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configurable parameters for a Membrane instance.
type Config struct {
	// PostgresDSN is the PostgreSQL connection string.
	PostgresDSN string `yaml:"postgres_dsn"`

	// ListenAddr is the gRPC listen address (default: "127.0.0.1:9090").
	ListenAddr string `yaml:"listen_addr"`

	// DecayInterval is how often the decay scheduler runs (default: 1h).
	DecayInterval time.Duration `yaml:"decay_interval"`

	// ConsolidationInterval is how often the consolidation scheduler runs (default: 6h).
	ConsolidationInterval time.Duration `yaml:"consolidation_interval"`

	// DefaultSensitivity is the ingestion default sensitivity level (default: "low").
	DefaultSensitivity string `yaml:"default_sensitivity"`

	// ReadMaxSensitivity is the highest sensitivity retrievable over gRPC.
	ReadMaxSensitivity string `yaml:"read_max_sensitivity"`

	// ReadScopes are the scopes retrievable over gRPC.
	ReadScopes []string `yaml:"read_scopes"`

	// WriteMaxSensitivity is the highest sensitivity mutable over gRPC.
	WriteMaxSensitivity string `yaml:"write_max_sensitivity"`

	// WriteScopes are the scopes mutable over gRPC.
	WriteScopes []string `yaml:"write_scopes"`

	// SelectionConfidenceThreshold is the minimum confidence for the retrieval
	// selector to consider a competence or plan_graph candidate (default: 0.7).
	SelectionConfidenceThreshold float64 `yaml:"selection_confidence_threshold"`

	// EmbeddingEndpoint is the HTTP endpoint used to generate embeddings.
	EmbeddingEndpoint string `yaml:"embedding_endpoint"`

	// EmbeddingModel is the embedding model name sent to the embedding endpoint.
	EmbeddingModel string `yaml:"embedding_model"`

	// EmbeddingDimensions is the output dimension of the embedding model.
	EmbeddingDimensions int `yaml:"embedding_dimensions"`

	// EmbeddingAPIKey authenticates requests to the embedding endpoint.
	EmbeddingAPIKey string `yaml:"embedding_api_key"`

	// LLMEndpoint is the HTTP endpoint used for semantic fact extraction.
	LLMEndpoint string `yaml:"llm_endpoint"`

	// LLMModel is the chat model name sent to the LLM endpoint.
	LLMModel string `yaml:"llm_model"`

	// LLMAPIKey authenticates requests to the LLM endpoint.
	LLMAPIKey string `yaml:"llm_api_key"`

	// IngestLLMEnabled enables ingest-side interpretation during CaptureMemory.
	IngestLLMEnabled bool `yaml:"ingest_llm_enabled"`

	// IngestLLMEndpoint is the HTTP endpoint used for ingest interpretation.
	IngestLLMEndpoint string `yaml:"ingest_llm_endpoint"`

	// IngestLLMModel is the chat model name sent to the ingest LLM endpoint.
	IngestLLMModel string `yaml:"ingest_llm_model"`

	// IngestLLMAPIKey authenticates requests to the ingest LLM endpoint.
	IngestLLMAPIKey string `yaml:"ingest_llm_api_key"`

	// TLSCertFile is the path to the TLS certificate PEM file.
	// If empty, the server runs without TLS.
	TLSCertFile string `yaml:"tls_cert_file"`

	// TLSKeyFile is the path to the TLS private key PEM file.
	TLSKeyFile string `yaml:"tls_key_file"`

	// APIKey is a shared secret for authenticating gRPC clients.
	// If empty, authentication is disabled. Read from MEMBRANE_API_KEY
	// environment variable if not set in config.
	APIKey string `yaml:"api_key"`

	// AllowInsecureCredentials permits API-key authentication over plaintext
	// gRPC on non-loopback listeners. Use only on trusted development networks.
	AllowInsecureCredentials bool `yaml:"allow_insecure_credentials"`

	// RateLimitPerSecond is the maximum requests per second per authenticated
	// API-key principal, or per source IP when API-key authentication is disabled.
	// 0 means no rate limiting. Default: 100.
	RateLimitPerSecond int `yaml:"rate_limit_per_second"`

	// GraphDefaultRootLimit is the default root count for graph retrieval.
	GraphDefaultRootLimit int `yaml:"graph_default_root_limit"`

	// GraphDefaultNodeLimit is the default total node limit for graph retrieval.
	GraphDefaultNodeLimit int `yaml:"graph_default_node_limit"`

	// GraphDefaultEdgeLimit is the default total edge limit for graph retrieval.
	GraphDefaultEdgeLimit int `yaml:"graph_default_edge_limit"`

	// GraphDefaultMaxHops is the default maximum expansion depth for graph retrieval.
	GraphDefaultMaxHops int `yaml:"graph_default_max_hops"`
}

const defaultEmbeddingDimensions = 1536

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:                   "127.0.0.1:9090",
		DecayInterval:                1 * time.Hour,
		ConsolidationInterval:        6 * time.Hour,
		DefaultSensitivity:           "low",
		ReadMaxSensitivity:           "low",
		ReadScopes:                   []string{"default"},
		WriteMaxSensitivity:          "low",
		WriteScopes:                  []string{"default"},
		SelectionConfidenceThreshold: 0.7,
		EmbeddingDimensions:          defaultEmbeddingDimensions,
		RateLimitPerSecond:           100,
		GraphDefaultRootLimit:        10,
		GraphDefaultNodeLimit:        25,
		GraphDefaultEdgeLimit:        100,
		GraphDefaultMaxHops:          1,
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

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	return cfg, nil
}
