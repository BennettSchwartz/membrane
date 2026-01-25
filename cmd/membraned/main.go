package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	grpcapi "github.com/GustyCube/membrane/api/grpc"
	"github.com/GustyCube/membrane/pkg/membrane"
)

func main() {
	configPath := flag.String("config", "", "path to YAML config file")
	dbPath := flag.String("db", "", "SQLite database path (overrides config)")
	addr := flag.String("addr", "", "gRPC listen address (overrides config)")
	flag.Parse()

	// Load configuration.
	var cfg *membrane.Config
	if *configPath != "" {
		var err error
		cfg, err = membrane.LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("failed to load config: %v", err)
		}
	} else {
		cfg = membrane.DefaultConfig()
	}

	// Apply flag overrides.
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	if *addr != "" {
		cfg.ListenAddr = *addr
	}

	// Initialize Membrane.
	m, err := membrane.New(cfg)
	if err != nil {
		log.Fatalf("failed to initialize membrane: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background schedulers.
	if err := m.Start(ctx); err != nil {
		log.Fatalf("failed to start membrane: %v", err)
	}

	// Create gRPC server.
	srv, err := grpcapi.NewServer(m, cfg.ListenAddr)
	if err != nil {
		log.Fatalf("failed to create grpc server: %v", err)
	}

	// Start gRPC server in a goroutine (Start blocks).
	errCh := make(chan error, 1)
	go func() {
		log.Printf("membraned: listening on %s", cfg.ListenAddr)
		errCh <- srv.Start()
	}()

	// Wait for shutdown signal or server error.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		log.Printf("membraned: received signal %v, shutting down", sig)
	case err := <-errCh:
		log.Printf("membraned: grpc server error: %v", err)
	}

	// Graceful shutdown.
	srv.Stop()
	cancel()
	if err := m.Stop(); err != nil {
		log.Printf("membraned: error during shutdown: %v", err)
	}
	log.Println("membraned: shutdown complete")
}
