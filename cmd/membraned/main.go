package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	grpcapi "github.com/BennettSchwartz/membrane/api/grpc"
	"github.com/BennettSchwartz/membrane/pkg/membrane"
)

var version = "dev"

var daemonFatalf = log.Fatalf

type serverOptions struct {
	configPath  string
	dbPath      string
	postgresDSN string
	addr        string
}

type daemonMembrane interface {
	Start(context.Context) error
	Stop() error
}

type daemonServer interface {
	Start() error
	Stop()
}

type daemonCLIDependencies struct {
	newMembrane   func(*membrane.Config) (daemonMembrane, error)
	newServer     func(daemonMembrane, *membrane.Config) (daemonServer, error)
	signalChannel func() (<-chan os.Signal, func())
}

func (d daemonCLIDependencies) withDefaults() daemonCLIDependencies {
	if d.newMembrane == nil {
		d.newMembrane = func(cfg *membrane.Config) (daemonMembrane, error) {
			return membrane.New(cfg)
		}
	}
	if d.newServer == nil {
		d.newServer = func(m daemonMembrane, cfg *membrane.Config) (daemonServer, error) {
			realMembrane, ok := m.(*membrane.Membrane)
			if !ok {
				return nil, fmt.Errorf("unsupported membrane implementation %T", m)
			}
			return grpcapi.NewServer(realMembrane, cfg)
		}
	}
	if d.signalChannel == nil {
		d.signalChannel = func() (<-chan os.Signal, func()) {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			return sigCh, func() { signal.Stop(sigCh) }
		}
	}
	return d
}

func configFromOptions(opts serverOptions, lookupEnv func(string) string) (*membrane.Config, error) {
	if lookupEnv == nil {
		lookupEnv = os.Getenv
	}

	var cfg *membrane.Config
	if opts.configPath != "" {
		loaded, err := membrane.LoadConfig(opts.configPath)
		if err != nil {
			return nil, fmt.Errorf("load config: %w", err)
		}
		cfg = loaded
	} else {
		cfg = membrane.DefaultConfig()
	}

	if opts.dbPath != "" {
		cfg.DBPath = opts.dbPath
	}
	if opts.postgresDSN != "" {
		cfg.Backend = "postgres"
		cfg.PostgresDSN = opts.postgresDSN
	}
	if opts.addr != "" {
		cfg.ListenAddr = opts.addr
	}

	if cfg.APIKey == "" {
		cfg.APIKey = lookupEnv("MEMBRANE_API_KEY")
	}
	return cfg, nil
}

func main() {
	if err := runMembranedCLI(context.Background(), os.Args[1:], os.Getenv, os.Stdout, daemonCLIDependencies{}); err != nil {
		daemonFatalf("failed to run membraned: %v", err)
	}
}

func runMembranedCLI(ctx context.Context, args []string, lookupEnv func(string) string, stdout io.Writer, deps daemonCLIDependencies) error {
	deps = deps.withDefaults()
	if lookupEnv == nil {
		lookupEnv = os.Getenv
	}
	if stdout == nil {
		stdout = os.Stdout
	}

	fs := flag.NewFlagSet("membraned", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to YAML config file")
	dbPath := fs.String("db", "", "SQLite database path (overrides config)")
	postgresDSN := fs.String("postgres-dsn", "", "PostgreSQL DSN (overrides config and selects the postgres backend)")
	addr := fs.String("addr", "", "gRPC listen address (overrides config)")
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *showVersion {
		_, err := fmt.Fprintf(stdout, "membraned %s\n", version)
		return err
	}

	cfg, err := configFromOptions(serverOptions{
		configPath:  *configPath,
		dbPath:      *dbPath,
		postgresDSN: *postgresDSN,
		addr:        *addr,
	}, lookupEnv)
	if err != nil {
		return fmt.Errorf("configure: %w", err)
	}

	m, err := deps.newMembrane(cfg)
	if err != nil {
		return fmt.Errorf("initialize membrane: %w", err)
	}

	srv, err := deps.newServer(m, cfg)
	if err != nil {
		if stopErr := m.Stop(); stopErr != nil {
			log.Printf("membraned: error during cleanup: %v", stopErr)
		}
		return fmt.Errorf("create grpc server: %w", err)
	}

	sigCh, stopSignals := deps.signalChannel()
	defer stopSignals()

	return runDaemon(ctx, m, srv, cfg.ListenAddr, sigCh)
}

func runDaemon(ctx context.Context, m daemonMembrane, srv daemonServer, listenAddr string, sigCh <-chan os.Signal) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start background schedulers.
	if err := m.Start(runCtx); err != nil {
		if stopErr := m.Stop(); stopErr != nil {
			log.Printf("membraned: error during cleanup: %v", stopErr)
		}
		return fmt.Errorf("start membrane: %w", err)
	}

	// Start gRPC server in a goroutine (Start blocks).
	errCh := make(chan error, 1)
	go func() {
		log.Printf("membraned: listening on %s", listenAddr)
		errCh <- srv.Start()
	}()

	select {
	case <-ctx.Done():
		log.Printf("membraned: context canceled, shutting down")
	case sig := <-sigCh:
		log.Printf("membraned: received signal %v, shutting down", sig)
	case err := <-errCh:
		log.Printf("membraned: grpc server error: %v", err)
	}

	// Graceful shutdown: cancel context first to stop background
	// schedulers, then drain in-flight gRPC requests, then close
	// the database. This ordering prevents panics from gRPC handlers
	// hitting a closed database.
	cancel()
	srv.Stop()
	if err := m.Stop(); err != nil {
		log.Printf("membraned: error during shutdown: %v", err)
	}
	log.Println("membraned: shutdown complete")
	return nil
}
