package tests_test

import (
	"strings"
	"testing"

	"github.com/BennettSchwartz/membrane/pkg/membrane"
)

func TestMembraneNewRejectsInvalidDefaultSensitivity(t *testing.T) {
	cfg := membrane.DefaultConfig()
	cfg.DBPath = t.TempDir() + "/membrane.db"
	cfg.DefaultSensitivity = "invalid"

	_, err := membrane.New(cfg)
	if err == nil {
		t.Fatalf("expected membrane.New to reject invalid default sensitivity")
	}
	if !strings.Contains(err.Error(), "invalid default sensitivity") {
		t.Fatalf("unexpected error: %v", err)
	}
}
