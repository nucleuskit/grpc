package runtimegrpc

import (
	"context"
	"net"
	"testing"
	"time"

	grpcinterceptors "github.com/nucleuskit/grpc/interceptors"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerStartsClientChecksHealthAndGracefullyStops(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := NewServer()
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server.GRPCServer(), healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("unexpected health status: %s", resp.GetStatus())
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-serveErr; err != nil {
		t.Fatalf("Serve returned unexpected error: %v", err)
	}
}

func TestServerOptionsRegisterHealthService(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := NewServer(WithHealthStatus("", healthpb.HealthCheckResponse_SERVING))
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("unexpected health status: %s", resp.GetStatus())
	}

	server.Stop()
	if err := <-serveErr; err != nil {
		t.Fatalf("Serve returned unexpected error: %v", err)
	}
}

func TestServerOptionsAcceptDefaultInterceptorChain(t *testing.T) {
	server := NewServer(WithDefaultInterceptors(grpcinterceptors.DefaultChainConfig{Timeout: time.Second}))
	if server.GRPCServer() == nil {
		t.Fatal("expected grpc server")
	}
}
