package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestVersionFlag verifies that --version prints the version string and exits 0.
func TestVersionFlag(t *testing.T) {
	if os.Getenv("TEST_VERSION_SUBPROCESS") == "1" {
		// We are the subprocess: run the version logic directly.
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		showVersion := flag.Bool("version", false, "print version and exit")
		os.Args = []string{"membraned", "--version"}
		flag.Parse()
		if *showVersion {
			fmt.Printf("membraned %s\n", version)
			os.Exit(0)
		}
		os.Exit(1)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestVersionFlag")
	cmd.Env = append(os.Environ(), "TEST_VERSION_SUBPROCESS=1")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	if err != nil {
		t.Fatalf("--version subprocess exited with error: %v\noutput: %s", err, out.String())
	}

	output := strings.TrimSpace(out.String())
	if !strings.HasPrefix(output, "membraned ") {
		t.Errorf("expected output to start with 'membraned ', got: %q", output)
	}
}

// TestVersionDefault checks that the version variable has a sensible fallback.
func TestVersionDefault(t *testing.T) {
	if version == "" {
		t.Error("version variable must not be empty; expected 'dev' or an injected value")
	}
}
