package runtimegrpc

import (
	"context"
	"errors"
	"net"
	"sync"

	grpcinterceptors "github.com/nucleuskit/grpc/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	server *grpc.Server
	mu     sync.RWMutex
	addr   net.Addr
}

type Option func(*serverOptions)

type serverOptions struct {
	grpcOptions        []grpc.ServerOption
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
	configure          []func(*grpc.Server)
}

func NewServer(opts ...Option) *Server {
	var options serverOptions
	for _, opt := range opts {
		opt(&options)
	}
	grpcOptions := append([]grpc.ServerOption{}, options.grpcOptions...)
	if len(options.unaryInterceptors) > 0 {
		grpcOptions = append(grpcOptions, grpc.ChainUnaryInterceptor(options.unaryInterceptors...))
	}
	if len(options.streamInterceptors) > 0 {
		grpcOptions = append(grpcOptions, grpc.ChainStreamInterceptor(options.streamInterceptors...))
	}
	server := grpc.NewServer(grpcOptions...)
	for _, configure := range options.configure {
		configure(server)
	}
	return &Server{server: server}
}

func WithServerOptions(options ...grpc.ServerOption) Option {
	return func(config *serverOptions) {
		config.grpcOptions = append(config.grpcOptions, options...)
	}
}

func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	return func(config *serverOptions) {
		config.unaryInterceptors = append(config.unaryInterceptors, interceptors...)
	}
}

func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) Option {
	return func(config *serverOptions) {
		config.streamInterceptors = append(config.streamInterceptors, interceptors...)
	}
}

func WithDefaultInterceptors(config grpcinterceptors.DefaultChainConfig) Option {
	return func(options *serverOptions) {
		options.unaryInterceptors = append(options.unaryInterceptors, grpcinterceptors.DefaultUnaryChain(config))
		options.streamInterceptors = append(options.streamInterceptors, grpcinterceptors.DefaultStreamChain(config))
	}
}

func WithTransportCredentials(credentials credentials.TransportCredentials) Option {
	return WithServerOptions(grpc.Creds(credentials))
}

func WithHealthStatus(service string, status healthpb.HealthCheckResponse_ServingStatus) Option {
	return func(config *serverOptions) {
		config.configure = append(config.configure, func(server *grpc.Server) {
			healthServer := health.NewServer()
			healthpb.RegisterHealthServer(server, healthServer)
			healthServer.SetServingStatus(service, status)
		})
	}
}

func WithReflection() Option {
	return func(config *serverOptions) {
		config.configure = append(config.configure, func(server *grpc.Server) {
			reflection.Register(server)
		})
	}
}

func (s *Server) GRPCServer() *grpc.Server {
	return s.server
}

func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.server.RegisterService(desc, impl)
}

func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.addr = listener.Addr()
	s.mu.Unlock()

	err := s.server.Serve(listener)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func (s *Server) ListenAndServe(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

func (s *Server) GracefulStop() {
	s.server.GracefulStop()
}

func (s *Server) Stop() {
	s.server.Stop()
}

func (s *Server) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		<-done
		return ctx.Err()
	}
}
