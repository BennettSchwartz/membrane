// Package grpc provides the gRPC transport layer for the Membrane service.
// It registers the protoc-generated service descriptor and delegates every
// RPC to the pkg/membrane API surface.
package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpcHealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	pb "github.com/GustyCube/membrane/api/grpc/gen/membranev1"
	"github.com/GustyCube/membrane/pkg/membrane"
)

// Server wraps a gRPC server wired to a Membrane instance.
type Server struct {
	membrane *membrane.Membrane
	grpc     *grpc.Server
	health   *grpcHealth.Server
	listener net.Listener

	gracefulStopTimeout time.Duration
}

const defaultGracefulStopTimeout = 5 * time.Second

// NewServer creates a Server that will listen on the configured address and
// serve RPCs backed by the given Membrane instance. It configures TLS,
// authentication, and rate limiting based on the provided config.
func NewServer(m *membrane.Membrane, cfg *membrane.Config) (*Server, error) {
	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("grpc: listen %s: %w", cfg.ListenAddr, err)
	}

	var opts []grpc.ServerOption

	// TLS.
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			lis.Close()
			return nil, fmt.Errorf("grpc: load TLS credentials: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// Build interceptor chain.
	apiKey := cfg.APIKey
	rateLimit := cfg.RateLimitPerSecond
	interceptor := chainInterceptors(apiKey, rateLimit)
	opts = append(opts, grpc.UnaryInterceptor(interceptor))

	gs := grpc.NewServer(opts...)
	healthServer := grpcHealth.NewServer()
	healthServer.SetServingStatus(pb.MembraneService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, healthServer)

	handler := &Handler{membrane: m}
	pb.RegisterMembraneServiceServer(gs, handler)

	return &Server{
		membrane: m,
		grpc:     gs,
		health:   healthServer,
		listener: lis,

		gracefulStopTimeout: defaultGracefulStopTimeout,
	}, nil
}

// chainInterceptors builds a unary server interceptor that applies
// authentication and rate limiting.
func chainInterceptors(apiKey string, ratePerSecond int) grpc.UnaryServerInterceptor {
	var limiter *clientRateLimiter
	if ratePerSecond > 0 {
		limiter = newClientRateLimiter(ratePerSecond)
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Authentication.
		if apiKey != "" {
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				return nil, status.Error(codes.Unauthenticated, "missing metadata")
			}
			tokens := md.Get("authorization")
			if len(tokens) == 0 || tokens[0] != "Bearer "+apiKey {
				return nil, status.Error(codes.Unauthenticated, "invalid or missing API key")
			}
		}

		// Rate limiting.
		if limiter != nil {
			if !limiter.allow(clientIdentity(ctx)) {
				return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
			}
		}

		return handler(ctx, req)
	}
}

func clientIdentity(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "anonymous"
	}
	tokens := md.Get("authorization")
	if len(tokens) > 0 {
		return "auth:" + tokens[0]
	}
	return "anonymous"
}

// rateLimiter implements a simple token bucket rate limiter.
type rateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func newRateLimiter(perSecond int) *rateLimiter {
	return newRateLimiterAt(perSecond, time.Now())
}

func newRateLimiterAt(perSecond int, now time.Time) *rateLimiter {
	return &rateLimiter{
		tokens:     float64(perSecond),
		maxTokens:  float64(perSecond),
		refillRate: float64(perSecond),
		lastRefill: now,
	}
}

func (r *rateLimiter) allowAt(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsed := now.Sub(r.lastRefill).Seconds()
	r.tokens += elapsed * r.refillRate
	if r.tokens > r.maxTokens {
		r.tokens = r.maxTokens
	}
	r.lastRefill = now

	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}

const (
	rateLimiterIdleTTL    = 5 * time.Minute
	rateLimiterMaxClients = 4096
)

type clientRateLimiter struct {
	mu          sync.Mutex
	perSecond   int
	idleTTL     time.Duration
	maxClients  int
	lastCleanup time.Time
	buckets     map[string]*clientBucket
}

type clientBucket struct {
	limiter  *rateLimiter
	lastSeen time.Time
}

func newClientRateLimiter(perSecond int) *clientRateLimiter {
	return &clientRateLimiter{
		perSecond:   perSecond,
		idleTTL:     rateLimiterIdleTTL,
		maxClients:  rateLimiterMaxClients,
		lastCleanup: time.Now(),
		buckets:     make(map[string]*clientBucket),
	}
}

func (r *clientRateLimiter) allow(clientID string) bool {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	if now.Sub(r.lastCleanup) >= r.idleTTL/2 {
		r.evictIdleLocked(now)
		r.lastCleanup = now
	}

	bucket, ok := r.buckets[clientID]
	if !ok {
		if len(r.buckets) >= r.maxClients {
			r.evictOldestLocked(now)
		}
		bucket = &clientBucket{
			limiter:  newRateLimiterAt(r.perSecond, now),
			lastSeen: now,
		}
		r.buckets[clientID] = bucket
	}

	bucket.lastSeen = now
	return bucket.limiter.allowAt(now)
}

func (r *clientRateLimiter) evictIdleLocked(now time.Time) {
	for clientID, bucket := range r.buckets {
		if now.Sub(bucket.lastSeen) > r.idleTTL {
			delete(r.buckets, clientID)
		}
	}
}

func (r *clientRateLimiter) evictOldestLocked(now time.Time) {
	r.evictIdleLocked(now)
	if len(r.buckets) < r.maxClients {
		return
	}

	var oldestClient string
	var oldestSeen time.Time
	first := true
	for clientID, bucket := range r.buckets {
		if first || bucket.lastSeen.Before(oldestSeen) {
			first = false
			oldestClient = clientID
			oldestSeen = bucket.lastSeen
		}
	}
	if oldestClient != "" {
		delete(r.buckets, oldestClient)
	}
}

// Start serves gRPC requests. It blocks until Stop is called or an
// unrecoverable error occurs.
func (s *Server) Start() error {
	return s.grpc.Serve(s.listener)
}

// Stop performs a graceful shutdown of the gRPC server, finishing in-flight
// RPCs before returning.
func (s *Server) Stop() {
	if s.health != nil {
		s.health.Shutdown()
	}
	gracefulStopWithTimeout(s.grpc, s.gracefulStopTimeout)
}

type grpcStopper interface {
	GracefulStop()
	Stop()
}

func gracefulStopWithTimeout(server grpcStopper, timeout time.Duration) {
	if timeout <= 0 {
		server.Stop()
		return
	}

	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return
	case <-timer.C:
		server.Stop()
	}
}

// Addr returns the network address the server is listening on. This is
// useful in tests where ":0" is passed to let the OS pick a free port.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}
