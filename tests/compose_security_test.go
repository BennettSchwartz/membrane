package tests_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestBundledComposeRequiresSecretAndBindsPostgresToLoopback(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("docker is required to render the Compose security contract")
	}
	if err := exec.Command(docker, "compose", "version").Run(); err != nil {
		t.Skip("docker compose is required to render the Compose security contract")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate the test file")
	}
	composeFile := filepath.Join(filepath.Dir(currentFile), "..", "docker-compose.yml")

	withoutSecret := exec.Command(docker, "compose", "-f", composeFile, "config", "--format", "json")
	// Explicitly shadow any value from a developer's project .env file. Merely
	// removing the process variable lets Compose load a local .env value and
	// turns this negative test into a machine-dependent false pass.
	withoutSecret.Env = append(environmentWithout("MEMBRANE_POSTGRES_PASSWORD"), "MEMBRANE_POSTGRES_PASSWORD=")
	if output, err := withoutSecret.CombinedOutput(); err == nil {
		t.Fatalf("docker compose config without MEMBRANE_POSTGRES_PASSWORD succeeded; output=%s", output)
	}

	const secret = "test-only-random-postgres-secret"
	withSecret := exec.Command(docker, "compose", "-f", composeFile, "config", "--format", "json")
	withSecret.Env = append(environmentWithout("MEMBRANE_POSTGRES_PASSWORD"), "MEMBRANE_POSTGRES_PASSWORD="+secret)
	output, err := withSecret.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose config with secret: %v\n%s", err, output)
	}

	var rendered struct {
		Services map[string]struct {
			Environment map[string]string `json:"environment"`
			Ports       []struct {
				HostIP    string `json:"host_ip"`
				Published string `json:"published"`
				Target    int    `json:"target"`
			} `json:"ports"`
		} `json:"services"`
	}
	if err := json.Unmarshal(output, &rendered); err != nil {
		t.Fatalf("decode rendered Compose config: %v\n%s", err, output)
	}
	postgres, ok := rendered.Services["postgres"]
	if !ok {
		t.Fatal("rendered Compose config has no postgres service")
	}
	if got := postgres.Environment["POSTGRES_PASSWORD"]; got != secret {
		t.Fatalf("rendered POSTGRES_PASSWORD = %q, want supplied secret", got)
	}
	if len(postgres.Ports) != 1 || postgres.Ports[0].HostIP != "127.0.0.1" || postgres.Ports[0].Target != 5432 {
		t.Fatalf("rendered Postgres ports = %+v, want one 127.0.0.1:5432 binding", postgres.Ports)
	}
}

func environmentWithout(name string) []string {
	prefix := name + "="
	out := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return out
}
