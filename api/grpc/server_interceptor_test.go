package grpc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	basegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/BennettSchwartz/membrane/pkg/membrane"
)

func TestChainInterceptorsAuthAndRateLimit(t *testing.T) {
	info := &basegrpc.UnaryServerInfo{FullMethod: "/membrane.Test/Call"}
	calls := 0
	handler := func(context.Context, any) (any, error) {
		calls++
		return "ok", nil
	}

	interceptor := chainInterceptors("secret", 0)
	if _, err := interceptor(context.Background(), nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing metadata code = %v, want Unauthenticated", status.Code(err))
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong"))
	if _, err := interceptor(ctx, nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("bad token code = %v, want Unauthenticated", status.Code(err))
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	resp, err := interceptor(ctx, nil, info, handler)
	if err != nil || resp != "ok" {
		t.Fatalf("valid auth response = %#v, err = %v; want ok nil", resp, err)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}

	rateLimited := chainInterceptors("", 1)
	if _, err := rateLimited(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("first rate-limited call: %v", err)
	}
	if _, err := rateLimited(context.Background(), nil, info, handler); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second rate-limited call code = %v, want ResourceExhausted", status.Code(err))
	}
}

func TestClientIdentity(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}})
	if got := clientIdentity(ctx); got != "127.0.0.1:1234" {
		t.Fatalf("peer identity = %q, want 127.0.0.1:1234", got)
	}

	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token"))
	if got := clientIdentity(ctx); got != "auth:Bearer token" {
		t.Fatalf("auth identity = %q, want auth:Bearer token", got)
	}

	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-client", "tester"))
	if got := clientIdentity(ctx); got != "anonymous" {
		t.Fatalf("metadata without auth identity = %q, want anonymous", got)
	}

	if got := clientIdentity(context.Background()); got != "anonymous" {
		t.Fatalf("anonymous identity = %q, want anonymous", got)
	}
}

func TestRateLimiterAndClientEviction(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	limiter := newRateLimiterAt(2, now)
	if !limiter.allowAt(now) {
		t.Fatalf("expected first initial burst token to allow request")
	}
	if !limiter.allowAt(now) {
		t.Fatalf("expected initial burst tokens to allow two requests")
	}
	if limiter.allowAt(now) {
		t.Fatalf("expected third immediate request to be denied")
	}
	if !limiter.allowAt(now.Add(time.Second)) {
		t.Fatalf("expected refill after one second to allow request")
	}
	if !limiter.allowAt(now.Add(10 * time.Second)) {
		t.Fatalf("expected refill beyond capacity to clamp and allow request")
	}

	clients := newClientRateLimiter(1)
	clients.idleTTL = time.Second
	clients.maxClients = 2
	old := now.Add(-2 * time.Second)
	clients.buckets["old"] = &clientBucket{limiter: newRateLimiterAt(1, old), lastSeen: old}
	clients.buckets["new"] = &clientBucket{limiter: newRateLimiterAt(1, now), lastSeen: now}
	clients.evictIdleLocked(now)
	if _, ok := clients.buckets["old"]; ok {
		t.Fatalf("idle bucket was not evicted")
	}
	if _, ok := clients.buckets["new"]; !ok {
		t.Fatalf("active bucket was evicted")
	}

	clients.buckets["older"] = &clientBucket{limiter: newRateLimiterAt(1, now.Add(-time.Second)), lastSeen: now.Add(-time.Second)}
	clients.buckets["newest"] = &clientBucket{limiter: newRateLimiterAt(1, now), lastSeen: now}
	clients.maxClients = 2
	clients.evictOldestLocked(now)
	if len(clients.buckets) != 2 {
		t.Fatalf("bucket len = %d, want 2", len(clients.buckets))
	}
	if _, ok := clients.buckets["older"]; ok {
		t.Fatalf("oldest bucket was not evicted")
	}
}

func TestClientRateLimiterAllowCleansIdleAndEvictsAtCapacity(t *testing.T) {
	clients := newClientRateLimiter(1)
	clients.idleTTL = time.Second
	clients.maxClients = 2
	now := time.Now()
	clients.lastCleanup = now.Add(-time.Second)
	clients.buckets["idle"] = &clientBucket{
		limiter:  newRateLimiterAt(1, now.Add(-3*time.Second)),
		lastSeen: now.Add(-3 * time.Second),
	}

	if !clients.allow("active") {
		t.Fatalf("first active client should be allowed")
	}
	if _, ok := clients.buckets["idle"]; ok {
		t.Fatalf("idle client was not removed during allow cleanup")
	}

	clients.buckets["oldest"] = &clientBucket{
		limiter:  newRateLimiterAt(1, now.Add(-2*time.Second)),
		lastSeen: now.Add(-2 * time.Second),
	}
	if !clients.allow("newest") {
		t.Fatalf("new client should be allowed after capacity eviction")
	}
	if len(clients.buckets) != 2 {
		t.Fatalf("bucket len = %d, want capped at 2", len(clients.buckets))
	}
	if _, ok := clients.buckets["oldest"]; ok {
		t.Fatalf("oldest client was not evicted at capacity")
	}
}

type recordingStopper struct {
	blockGraceful chan struct{}
	stopped       atomic.Bool
	graceful      atomic.Bool
}

func (s *recordingStopper) GracefulStop() {
	s.graceful.Store(true)
	if s.blockGraceful != nil {
		<-s.blockGraceful
	}
}

func (s *recordingStopper) Stop() {
	s.stopped.Store(true)
}

func TestGracefulStopWithTimeoutBranches(t *testing.T) {
	zero := &recordingStopper{}
	gracefulStopWithTimeout(zero, 0)
	if !zero.stopped.Load() || zero.graceful.Load() {
		t.Fatalf("zero timeout stopper = stopped:%v graceful:%v, want immediate Stop only", zero.stopped.Load(), zero.graceful.Load())
	}

	block := make(chan struct{})
	timeout := &recordingStopper{blockGraceful: block}
	gracefulStopWithTimeout(timeout, time.Millisecond)
	close(block)
	if !timeout.stopped.Load() || !timeout.graceful.Load() {
		t.Fatalf("timeout stopper = stopped:%v graceful:%v, want graceful attempt then Stop", timeout.stopped.Load(), timeout.graceful.Load())
	}
}

func writeTestTLSFiles(t *testing.T) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test TLS key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create test TLS certificate: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write test TLS cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write test TLS key: %v", err)
	}
	return certPath, keyPath
}

func TestNewServerAddrStartStopAndTLSError(t *testing.T) {
	cfg := membrane.DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "membrane.db")
	cfg.ListenAddr = "127.0.0.1:0"

	m, err := membrane.New(cfg)
	if err != nil {
		t.Fatalf("membrane.New: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop() })

	srv, err := NewServer(m, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.Addr() == nil || srv.Addr().String() == "" {
		t.Fatalf("Addr = %v, want assigned listener address", srv.Addr())
	}

	tlsSuccessCfg := *cfg
	tlsSuccessCfg.ListenAddr = "127.0.0.1:0"
	tlsSuccessCfg.TLSCertFile, tlsSuccessCfg.TLSKeyFile = writeTestTLSFiles(t)
	tlsSrv, err := NewServer(m, &tlsSuccessCfg)
	if err != nil {
		t.Fatalf("NewServer with TLS: %v", err)
	}
	tlsSrv.Stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()
	time.Sleep(10 * time.Millisecond)
	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil && err != basegrpc.ErrServerStopped {
			t.Fatalf("Start returned %v, want nil or ErrServerStopped", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop within timeout")
	}

	tlsCfg := *cfg
	tlsCfg.ListenAddr = "127.0.0.1:0"
	tlsCfg.TLSCertFile = filepath.Join(t.TempDir(), "missing.crt")
	tlsCfg.TLSKeyFile = filepath.Join(t.TempDir(), "missing.key")
	if tlsSrv, err := NewServer(m, &tlsCfg); err == nil {
		tlsSrv.Stop()
		t.Fatalf("NewServer with missing TLS files error = nil")
	}

	badListenCfg := *cfg
	badListenCfg.ListenAddr = "127.0.0.1:bad-port"
	if badSrv, err := NewServer(m, &badListenCfg); err == nil {
		badSrv.Stop()
		t.Fatalf("NewServer with bad listen address error = nil")
	}
}
