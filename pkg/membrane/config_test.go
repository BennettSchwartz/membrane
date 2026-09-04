package membrane

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/schema"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ListenAddr != "127.0.0.1:9090" {
		t.Fatalf("ListenAddr = %q, want 127.0.0.1:9090", cfg.ListenAddr)
	}
	if cfg.DecayInterval != time.Hour {
		t.Fatalf("DecayInterval = %s, want 1h", cfg.DecayInterval)
	}
	if cfg.ConsolidationInterval != 6*time.Hour {
		t.Fatalf("ConsolidationInterval = %s, want 6h", cfg.ConsolidationInterval)
	}
	if cfg.DefaultSensitivity != string(schema.SensitivityLow) {
		t.Fatalf("DefaultSensitivity = %q, want low", cfg.DefaultSensitivity)
	}
	if cfg.ReadMaxSensitivity != string(schema.SensitivityLow) || strings.Join(cfg.ReadScopes, ",") != "default" {
		t.Fatalf("read policy = %q %v, want low [default]", cfg.ReadMaxSensitivity, cfg.ReadScopes)
	}
	if cfg.WriteMaxSensitivity != string(schema.SensitivityLow) || strings.Join(cfg.WriteScopes, ",") != "default" {
		t.Fatalf("write policy = %q %v, want low [default]", cfg.WriteMaxSensitivity, cfg.WriteScopes)
	}
	if cfg.SelectionConfidenceThreshold != 0.7 {
		t.Fatalf("SelectionConfidenceThreshold = %.2f, want 0.70", cfg.SelectionConfidenceThreshold)
	}
	if cfg.GraphDefaultRootLimit != 10 || cfg.GraphDefaultNodeLimit != 25 || cfg.GraphDefaultEdgeLimit != 100 || cfg.GraphDefaultMaxHops != 1 {
		t.Fatalf("graph defaults = root:%d node:%d edge:%d hops:%d", cfg.GraphDefaultRootLimit, cfg.GraphDefaultNodeLimit, cfg.GraphDefaultEdgeLimit, cfg.GraphDefaultMaxHops)
	}
	if cfg.AllowInsecureCredentials {
		t.Fatalf("AllowInsecureCredentials = true, want false")
	}
}

func TestLoadConfigMergesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(strings.Join([]string{
		`postgres_dsn: postgres://membrane@example/membrane`,
		`listen_addr: ":9191"`,
		`decay_interval: 2h`,
		`default_sensitivity: medium`,
		`selection_confidence_threshold: 0.82`,
		`embedding_model: text-embedding-test`,
		`ingest_llm_enabled: true`,
		`allow_insecure_credentials: true`,
		`graph_default_root_limit: 7`,
		`graph_default_node_limit: 44`,
		`graph_default_edge_limit: 88`,
		`graph_default_max_hops: 3`,
	}, "\n"))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.PostgresDSN != "postgres://membrane@example/membrane" {
		t.Fatalf("PostgresDSN = %q, want configured DSN", cfg.PostgresDSN)
	}
	if cfg.ListenAddr != ":9191" {
		t.Fatalf("ListenAddr = %q, want :9191", cfg.ListenAddr)
	}
	if cfg.DecayInterval != 2*time.Hour {
		t.Fatalf("DecayInterval = %s, want 2h", cfg.DecayInterval)
	}
	if cfg.ConsolidationInterval != 6*time.Hour {
		t.Fatalf("ConsolidationInterval default = %s, want 6h", cfg.ConsolidationInterval)
	}
	if cfg.DefaultSensitivity != string(schema.SensitivityMedium) {
		t.Fatalf("DefaultSensitivity = %q, want medium", cfg.DefaultSensitivity)
	}
	if cfg.SelectionConfidenceThreshold != 0.82 {
		t.Fatalf("SelectionConfidenceThreshold = %.2f, want 0.82", cfg.SelectionConfidenceThreshold)
	}
	if !cfg.IngestLLMEnabled {
		t.Fatalf("IngestLLMEnabled = false, want true")
	}
	if cfg.EmbeddingModel != "text-embedding-test" {
		t.Fatalf("EmbeddingModel = %q, want text-embedding-test", cfg.EmbeddingModel)
	}
	if !cfg.AllowInsecureCredentials {
		t.Fatalf("AllowInsecureCredentials = false, want true")
	}
	if cfg.GraphDefaultRootLimit != 7 || cfg.GraphDefaultNodeLimit != 44 || cfg.GraphDefaultEdgeLimit != 88 || cfg.GraphDefaultMaxHops != 3 {
		t.Fatalf("graph config = root:%d node:%d edge:%d hops:%d", cfg.GraphDefaultRootLimit, cfg.GraphDefaultNodeLimit, cfg.GraphDefaultEdgeLimit, cfg.GraphDefaultMaxHops)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml")); err == nil || !strings.Contains(err.Error(), "read config file") {
		t.Fatalf("missing config error = %v, want read config file", err)
	}

	path := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(path, []byte("postgres_dsn: ["), 0o600); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "parse config file") {
		t.Fatalf("invalid config error = %v, want parse config file", err)
	}

	unknownPath := filepath.Join(t.TempDir(), "unknown.yaml")
	if err := os.WriteFile(unknownPath, []byte("backend: postgres\npostgres_dsn: postgres://membrane@example/membrane\n"), 0o600); err != nil {
		t.Fatalf("write unknown config: %v", err)
	}
	if _, err := LoadConfig(unknownPath); err == nil || !strings.Contains(err.Error(), "field backend not found") {
		t.Fatalf("unknown config field error = %v, want backend not found", err)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultSensitivity = "classified"
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "invalid default sensitivity") {
		t.Fatalf("invalid sensitivity error = %v, want invalid default sensitivity", err)
	}

	cfg = DefaultConfig()
	cfg.ReadMaxSensitivity = "classified"
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "invalid read_max_sensitivity") {
		t.Fatalf("invalid read sensitivity error = %v, want invalid read_max_sensitivity", err)
	}

	cfg = DefaultConfig()
	cfg.WriteMaxSensitivity = "classified"
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "invalid write_max_sensitivity") {
		t.Fatalf("invalid write sensitivity error = %v, want invalid write_max_sensitivity", err)
	}

	cfg = DefaultConfig()
	cfg.ReadScopes = nil
	t.Setenv("MEMBRANE_READ_SCOPES", "")
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "read_scopes must contain at least one scope") {
		t.Fatalf("missing read scopes error = %v, want read_scopes error", err)
	}

	cfg = DefaultConfig()
	cfg.PostgresDSN = ""
	t.Setenv("MEMBRANE_POSTGRES_DSN", "")
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "postgres_dsn is required") {
		t.Fatalf("missing postgres dsn error = %v, want postgres_dsn is required", err)
	}
}
