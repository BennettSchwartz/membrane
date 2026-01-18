// Package grpc provides the gRPC transport layer for the Membrane service.
// It registers a hand-written service descriptor (no protoc required) and
// delegates every RPC to the pkg/membrane API surface.
package grpc

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/GustyCube/membrane/pkg/membrane"
)

// Server wraps a gRPC server wired to a Membrane instance.
type Server struct {
	membrane *membrane.Membrane
	grpc     *grpc.Server
	listener net.Listener
}

// NewServer creates a Server that will listen on addr and serve RPCs backed
// by the given Membrane instance.
func NewServer(m *membrane.Membrane, addr string) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("grpc: listen %s: %w", addr, err)
	}

	gs := grpc.NewServer()

	handler := &Handler{membrane: m}
	registerMembraneService(gs, handler)

	return &Server{
		membrane: m,
		grpc:     gs,
		listener: lis,
	}, nil
}

// Start serves gRPC requests. It blocks until Stop is called or an
// unrecoverable error occurs.
func (s *Server) Start() error {
	return s.grpc.Serve(s.listener)
}

// Stop performs a graceful shutdown of the gRPC server, finishing in-flight
// RPCs before returning.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
}

// Addr returns the network address the server is listening on. This is
// useful in tests where ":0" is passed to let the OS pick a free port.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}
