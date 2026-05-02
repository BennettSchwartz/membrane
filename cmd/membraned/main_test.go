package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/BennettSchwartz/membrane/pkg/membrane"
)

type fakeDaemonMembrane struct {
	startErr error
	stopErr  error

	started bool
	stopped bool
	ctx     context.Context
}

func (f *fakeDaemonMembrane) Start(ctx context.Context) error {
	f.started = true
	f.ctx = ctx
	return f.startErr
}

func (f *fakeDaemonMembrane) Stop() error {
	f.stopped = true
	return f.stopErr
}

type fakeDaemonServer struct {
	startErr error
	started  chan struct{}
	stopCh   chan struct{}
	stopOnce sync.Once

	stopped bool
}

func newFakeDaemonServer(startErr error) *fakeDaemonServer {
	return &fakeDaemonServer{
		startErr: startErr,
		started:  make(chan struct{}),
		stopCh:   make(chan struct{}),
	}
}

func (f *fakeDaemonServer) Start() error {
	close(f.started)
	if f.startErr != nil {
		return f.startErr
	}
	<-f.stopCh
	return nil
}

func (f *fakeDaemonServer) Stop() {
	f.stopped = true
	f.stopOnce.Do(func() { close(f.stopCh) })
}

func TestMainReportsCLIError(t *testing.T) {
	oldArgs := os.Args
	oldFatalf := daemonFatalf
	defer func() {
		os.Args = oldArgs
		daemonFatalf = oldFatalf
	}()

	os.Args = []string{"membraned", "-bad-flag"}
	daemonFatalf = func(format string, args ...any) {
		panic(format)
	}

	defer func() {
		got := recover()
		msg, ok := got.(string)
		if !ok || !strings.Contains(msg, "failed to run membraned") {
			t.Fatalf("main fatal panic = %#v, want failed run format", got)
		}
	}()
	main()
	t.Fatalf("main returned without fatal")
}

func TestConfigFromOptionsAppliesDefaultsOverridesAndEnvAPIKey(t *testing.T) {
	cfg, err := configFromOptions(serverOptions{
		dbPath:      "override.db",
		postgresDSN: "postgres://user:pass@example/db",
		addr:        "127.0.0.1:19090",
	}, func(key string) string {
		if key == "MEMBRANE_API_KEY" {
			return "env-key"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("configFromOptions: %v", err)
	}
	if cfg.DBPath != "override.db" {
		t.Fatalf("DBPath = %q, want override.db", cfg.DBPath)
	}
	if cfg.Backend != "postgres" || cfg.PostgresDSN != "postgres://user:pass@example/db" {
		t.Fatalf("backend/dsn = %q/%q, want postgres override", cfg.Backend, cfg.PostgresDSN)
	}
	if cfg.ListenAddr != "127.0.0.1:19090" {
		t.Fatalf("ListenAddr = %q, want override", cfg.ListenAddr)
	}
	if cfg.APIKey != "env-key" {
		t.Fatalf("APIKey = %q, want env-key", cfg.APIKey)
	}
}

func TestConfigFromOptionsLoadsConfigFileAndPreservesConfiguredAPIKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membrane.yaml")
	if err := os.WriteFile(path, []byte(`
db_path: file.db
listen_addr: ":19091"
api_key: file-key
default_sensitivity: medium
`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := configFromOptions(serverOptions{
		configPath: path,
		dbPath:     "override.db",
	}, func(string) string {
		return "env-key"
	})
	if err != nil {
		t.Fatalf("configFromOptions: %v", err)
	}
	if cfg.DBPath != "override.db" {
		t.Fatalf("DBPath = %q, want flag override", cfg.DBPath)
	}
	if cfg.ListenAddr != ":19091" {
		t.Fatalf("ListenAddr = %q, want config value", cfg.ListenAddr)
	}
	if cfg.APIKey != "file-key" {
		t.Fatalf("APIKey = %q, want configured API key", cfg.APIKey)
	}
	if cfg.DefaultSensitivity != "medium" {
		t.Fatalf("DefaultSensitivity = %q, want medium", cfg.DefaultSensitivity)
	}
}

func TestConfigFromOptionsReturnsConfigLoadErrors(t *testing.T) {
	_, err := configFromOptions(serverOptions{configPath: filepath.Join(t.TempDir(), "missing.yaml")}, nil)
	if err == nil {
		t.Fatalf("configFromOptions missing file error = nil")
	}
}

func TestMainVersionFlag(t *testing.T) {
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	oldStdout := os.Stdout
	oldVersion := version
	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
		os.Stdout = oldStdout
		version = oldVersion
	})

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	os.Stdout = write
	os.Args = []string{"membraned", "-version"}
	flag.CommandLine = flag.NewFlagSet("membraned", flag.ContinueOnError)
	version = "test-version"

	main()

	if err := write.Close(); err != nil {
		t.Fatalf("Close pipe writer: %v", err)
	}
	out, err := io.ReadAll(read)
	if err != nil {
		t.Fatalf("ReadAll stdout: %v", err)
	}
	if string(out) != "membraned test-version\n" {
		t.Fatalf("stdout = %q, want version output", out)
	}
}

func TestRunMembranedCLIRunsWithInjectedDependencies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mem := &fakeDaemonMembrane{}
	srv := newFakeDaemonServer(nil)
	sigCh := make(chan os.Signal, 1)
	var signalStopped bool
	var gotConfig *membrane.Config
	result := make(chan error, 1)

	deps := daemonCLIDependencies{
		newMembrane: func(cfg *membrane.Config) (daemonMembrane, error) {
			gotConfig = cfg
			return mem, nil
		},
		newServer: func(m daemonMembrane, cfg *membrane.Config) (daemonServer, error) {
			if m != mem {
				t.Fatalf("newServer membrane = %#v, want injected membrane", m)
			}
			if cfg != gotConfig {
				t.Fatalf("newServer config pointer changed")
			}
			return srv, nil
		},
		signalChannel: func() (<-chan os.Signal, func()) {
			return sigCh, func() { signalStopped = true }
		},
	}

	go func() {
		result <- runMembranedCLI(ctx, []string{
			"-db", "override.db",
			"-addr", "127.0.0.1:19090",
		}, func(key string) string {
			if key == "MEMBRANE_API_KEY" {
				return "env-key"
			}
			return ""
		}, io.Discard, deps)
	}()

	select {
	case <-srv.started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runMembranedCLI: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runMembranedCLI did not return after cancellation")
	}
	if gotConfig == nil || gotConfig.DBPath != "override.db" || gotConfig.ListenAddr != "127.0.0.1:19090" || gotConfig.APIKey != "env-key" {
		t.Fatalf("config = %+v, want CLI overrides and env API key", gotConfig)
	}
	if !signalStopped {
		t.Fatalf("signal cleanup was not called")
	}
}

func TestRunMembranedCLIReturnsSetupErrors(t *testing.T) {
	if err := runMembranedCLI(context.Background(), []string{"-bad-flag"}, nil, io.Discard, daemonCLIDependencies{}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("bad flag error = %v, want flag parse error", err)
	}

	if err := runMembranedCLI(context.Background(), []string{"-config", filepath.Join(t.TempDir(), "missing.yaml")}, nil, io.Discard, daemonCLIDependencies{}); err == nil || !strings.Contains(err.Error(), "configure") {
		t.Fatalf("missing config error = %v, want configure error", err)
	}

	initErr := errors.New("init failed")
	err := runMembranedCLI(context.Background(), nil, nil, io.Discard, daemonCLIDependencies{
		newMembrane: func(*membrane.Config) (daemonMembrane, error) {
			return nil, initErr
		},
	})
	if err == nil || !errors.Is(err, initErr) || !strings.Contains(err.Error(), "initialize membrane") {
		t.Fatalf("initialize error = %v, want wrapped init error", err)
	}

	mem := &fakeDaemonMembrane{}
	serverErr := errors.New("server failed")
	err = runMembranedCLI(context.Background(), nil, nil, io.Discard, daemonCLIDependencies{
		newMembrane: func(*membrane.Config) (daemonMembrane, error) {
			return mem, nil
		},
		newServer: func(daemonMembrane, *membrane.Config) (daemonServer, error) {
			return nil, serverErr
		},
	})
	if err == nil || !errors.Is(err, serverErr) || !strings.Contains(err.Error(), "create grpc server") {
		t.Fatalf("server error = %v, want wrapped server error", err)
	}
	if !mem.stopped {
		t.Fatalf("membrane stopped = false, want cleanup after server creation error")
	}

	mem = &fakeDaemonMembrane{stopErr: errors.New("cleanup failed")}
	err = runMembranedCLI(context.Background(), nil, nil, io.Discard, daemonCLIDependencies{
		newMembrane: func(*membrane.Config) (daemonMembrane, error) {
			return mem, nil
		},
		newServer: func(daemonMembrane, *membrane.Config) (daemonServer, error) {
			return nil, serverErr
		},
	})
	if err == nil || !errors.Is(err, serverErr) {
		t.Fatalf("server error with cleanup failure = %v, want server error", err)
	}
	if !mem.stopped {
		t.Fatalf("membrane stopped = false, want cleanup even when stop returns error")
	}
}

func TestRunMembranedCLIVersionUsesProvidedWriter(t *testing.T) {
	oldVersion := version
	t.Cleanup(func() { version = oldVersion })
	version = "helper-version"

	var out bytes.Buffer
	if err := runMembranedCLI(context.Background(), []string{"-version"}, nil, &out, daemonCLIDependencies{}); err != nil {
		t.Fatalf("runMembranedCLI version: %v", err)
	}
	if out.String() != "membraned helper-version\n" {
		t.Fatalf("version output = %q, want helper version", out.String())
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	oldStdout := os.Stdout
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = oldStdout })
	if err := runMembranedCLI(context.Background(), []string{"-version"}, nil, nil, daemonCLIDependencies{}); err != nil {
		t.Fatalf("runMembranedCLI version with default stdout: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close captured stdout: %v", err)
	}
	captured, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	if string(captured) != "membraned helper-version\n" {
		t.Fatalf("default stdout version output = %q, want helper version", captured)
	}
}

func TestDaemonCLIDependenciesDefaults(t *testing.T) {
	deps := daemonCLIDependencies{}.withDefaults()
	if deps.newMembrane == nil || deps.newServer == nil || deps.signalChannel == nil {
		t.Fatalf("withDefaults returned incomplete dependencies: %+v", deps)
	}

	cfg := membrane.DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	mem, err := deps.newMembrane(cfg)
	if err != nil {
		t.Fatalf("default newMembrane: %v", err)
	}
	t.Cleanup(func() { _ = mem.Stop() })

	if _, err := deps.newServer(&fakeDaemonMembrane{}, cfg); err == nil || !strings.Contains(err.Error(), "unsupported membrane implementation") {
		t.Fatalf("default newServer fake membrane error = %v, want unsupported implementation", err)
	}
	srv, err := deps.newServer(mem, cfg)
	if err != nil {
		t.Fatalf("default newServer real membrane: %v", err)
	}
	srv.Stop()

	sigCh, stopSignals := deps.signalChannel()
	if sigCh == nil || stopSignals == nil {
		t.Fatalf("default signalChannel returned nil channel or cleanup")
	}
	stopSignals()
}

func TestRunDaemonStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mem := &fakeDaemonMembrane{}
	srv := newFakeDaemonServer(nil)
	sigCh := make(chan os.Signal, 1)
	result := make(chan error, 1)

	go func() {
		result <- runDaemon(ctx, mem, srv, "127.0.0.1:0", sigCh)
	}()

	select {
	case <-srv.started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runDaemon: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runDaemon did not return after context cancellation")
	}
	if !mem.started || !mem.stopped || !srv.stopped {
		t.Fatalf("started/stopped = mem:%v/%v server:%v, want all stopped", mem.started, mem.stopped, srv.stopped)
	}
	if mem.ctx == nil || mem.ctx.Err() == nil {
		t.Fatalf("membrane context was not canceled on shutdown")
	}
}

func TestRunDaemonStopsOnSignalAndServerError(t *testing.T) {
	t.Run("signal", func(t *testing.T) {
		mem := &fakeDaemonMembrane{}
		srv := newFakeDaemonServer(nil)
		sigCh := make(chan os.Signal, 1)
		result := make(chan error, 1)

		go func() {
			result <- runDaemon(context.Background(), mem, srv, "127.0.0.1:0", sigCh)
		}()
		select {
		case <-srv.started:
		case <-time.After(time.Second):
			t.Fatal("server did not start")
		}
		sigCh <- syscall.SIGTERM

		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("runDaemon signal: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("runDaemon did not return after signal")
		}
		if !mem.stopped || !srv.stopped {
			t.Fatalf("stopped = mem:%v server:%v, want true/true", mem.stopped, srv.stopped)
		}
	})

	t.Run("server error", func(t *testing.T) {
		mem := &fakeDaemonMembrane{}
		srv := newFakeDaemonServer(errors.New("listen failed"))
		err := runDaemon(context.Background(), mem, srv, "127.0.0.1:0", make(chan os.Signal, 1))
		if err != nil {
			t.Fatalf("runDaemon server error = %v, want logged graceful shutdown", err)
		}
		if !mem.stopped || !srv.stopped {
			t.Fatalf("stopped = mem:%v server:%v, want true/true", mem.stopped, srv.stopped)
		}
	})
}

func TestRunDaemonStartErrorCleansUp(t *testing.T) {
	mem := &fakeDaemonMembrane{startErr: errors.New("start failed")}
	srv := newFakeDaemonServer(nil)
	err := runDaemon(context.Background(), mem, srv, "127.0.0.1:0", make(chan os.Signal, 1))
	if err == nil || !errors.Is(err, mem.startErr) {
		t.Fatalf("runDaemon start error = %v, want wrapped start error", err)
	}
	if !mem.stopped {
		t.Fatalf("membrane stopped = false, want cleanup after start error")
	}
	select {
	case <-srv.started:
		t.Fatal("server started despite membrane start error")
	default:
	}

	mem = &fakeDaemonMembrane{startErr: errors.New("start failed"), stopErr: errors.New("stop failed")}
	err = runDaemon(context.Background(), mem, srv, "127.0.0.1:0", make(chan os.Signal, 1))
	if err == nil || !errors.Is(err, mem.startErr) {
		t.Fatalf("runDaemon start+cleanup error = %v, want start error", err)
	}
	if !mem.stopped {
		t.Fatalf("membrane stopped = false, want cleanup after start error")
	}
}

func TestRunDaemonLogsShutdownStopError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mem := &fakeDaemonMembrane{stopErr: errors.New("stop failed")}
	srv := newFakeDaemonServer(nil)
	result := make(chan error, 1)

	go func() {
		result <- runDaemon(ctx, mem, srv, "127.0.0.1:0", make(chan os.Signal, 1))
	}()
	select {
	case <-srv.started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runDaemon stop error path returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runDaemon did not return after cancellation")
	}
	if !mem.stopped || !srv.stopped {
		t.Fatalf("stopped = mem:%v server:%v, want true/true", mem.stopped, srv.stopped)
	}
}
