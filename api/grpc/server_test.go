package grpc

import (
	"sync"
	"testing"
	"time"
)

type fakeGRPCStopper struct {
	mu           sync.Mutex
	stopCalls    int
	gracefulDone chan struct{}
}

func newFakeGRPCStopper() *fakeGRPCStopper {
	return &fakeGRPCStopper{
		gracefulDone: make(chan struct{}),
	}
}

func (f *fakeGRPCStopper) GracefulStop() {
	<-f.gracefulDone
}

func (f *fakeGRPCStopper) Stop() {
	f.mu.Lock()
	f.stopCalls++
	f.mu.Unlock()

	select {
	case <-f.gracefulDone:
	default:
		close(f.gracefulDone)
	}
}

func (f *fakeGRPCStopper) stopCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopCalls
}

func TestGracefulStopWithTimeoutReturnsGracefully(t *testing.T) {
	fake := newFakeGRPCStopper()
	close(fake.gracefulDone)

	start := time.Now()
	gracefulStopWithTimeout(fake, 200*time.Millisecond)

	if fake.stopCount() != 0 {
		t.Fatalf("expected Stop not to be called when GracefulStop completes")
	}
	if elapsed := time.Since(start); elapsed >= 200*time.Millisecond {
		t.Fatalf("expected graceful shutdown before timeout, elapsed=%v", elapsed)
	}
}

func TestGracefulStopWithTimeoutFallsBackToStop(t *testing.T) {
	fake := newFakeGRPCStopper()

	start := time.Now()
	gracefulStopWithTimeout(fake, 20*time.Millisecond)

	if fake.stopCount() != 1 {
		t.Fatalf("expected Stop to be called once after timeout, got %d", fake.stopCount())
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("expected shutdown to wait for timeout before forcing stop, elapsed=%v", elapsed)
	}
}
