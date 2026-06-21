package runtimegrpc

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func TestDialWithConfigDefaultsToInsecure(t *testing.T) {
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
	conn, err := DialWithConfig(ctx, listener.Addr().String(), DialConfig{})
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

func TestDialWithConfigChainsUnaryInterceptorAndServiceMetadata(t *testing.T) {
	seenMetadata := make(chan metadata.MD, 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := NewServer(WithUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		seenMetadata <- md.Copy()
		return handler(ctx, req)
	}))
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server.GRPCServer(), healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	called := false
	clientInterceptor := func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		called = true
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := DialWithConfig(ctx, listener.Addr().String(), DialConfig{
		Service:           "orders-api",
		Metadata:          map[string]string{"x-nucleus-client": "checkout"},
		UnaryInterceptors: []grpc.UnaryClientInterceptor{clientInterceptor},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected unary client interceptor to be called")
	}

	select {
	case md := <-seenMetadata:
		if got := md.Get("x-nucleus-service"); len(got) != 1 || got[0] != "orders-api" {
			t.Fatalf("expected service metadata, got %#v", md)
		}
		if got := md.Get("x-nucleus-client"); len(got) != 1 || got[0] != "checkout" {
			t.Fatalf("expected custom metadata, got %#v", md)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for server metadata")
	}

	server.Stop()
	if err := <-serveErr; err != nil {
		t.Fatalf("Serve returned unexpected error: %v", err)
	}
}

func TestDialWithConfigBlockHonorsTimeout(t *testing.T) {
	start := time.Now()
	_, err := DialWithConfig(context.Background(), "127.0.0.1:1", DialConfig{
		Block:   true,
		Timeout: 25 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("dial did not honor timeout quickly: %s", time.Since(start))
	}
}
