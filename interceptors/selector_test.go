package interceptors

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestSelectUnaryMatchesFullMethod(t *testing.T) {
	called := false
	interceptor := SelectUnary(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		called = true
		return handler(ctx, req)
	}, MatchFullMethod("/test.Service/Get"))

	if _, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Get"}, func(context.Context, any) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected matching interceptor to run")
	}

	called = false
	if _, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/List"}, func(context.Context, any) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("non-matching interceptor should not run")
	}
}

func TestSelectUnaryMatchesServiceAndMethodName(t *testing.T) {
	serviceCalled := false
	byService := SelectUnary(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		serviceCalled = true
		return handler(ctx, req)
	}, MatchService("test.Service"))

	if _, err := byService(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Get"}, func(context.Context, any) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !serviceCalled {
		t.Fatal("expected service matcher to run interceptor")
	}

	methodCalled := false
	byMethod := SelectUnary(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		methodCalled = true
		return handler(ctx, req)
	}, MatchMethodName("List"))
	if _, err := byMethod(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/other.Service/List"}, func(context.Context, any) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !methodCalled {
		t.Fatal("expected method-name matcher to run interceptor")
	}
}

func TestSelectStreamAndClientSelectors(t *testing.T) {
	streamCalled := false
	serverStream := SelectStream(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		streamCalled = true
		return handler(srv, stream)
	}, MatchMethodName("Stream"))
	if err := serverStream(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(any, grpc.ServerStream) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !streamCalled {
		t.Fatal("expected stream selector to run interceptor")
	}

	clientUnaryCalled := false
	clientUnary := SelectUnaryClient(func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		clientUnaryCalled = true
		return invoker(ctx, method, req, reply, cc, opts...)
	}, MatchService("test.Service"))
	if err := clientUnary(context.Background(), "/test.Service/Get", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !clientUnaryCalled {
		t.Fatal("expected client unary selector to run interceptor")
	}

	clientStreamCalled := false
	clientStream := SelectStreamClient(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		clientStreamCalled = true
		return streamer(ctx, desc, cc, method, opts...)
	}, MatchMethodName("Stream"))
	stream, err := clientStream(context.Background(), &grpc.StreamDesc{ServerStreams: true}, nil, "/other.Service/Stream", func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return &testClientStream{ctx: ctx}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stream == nil || !clientStreamCalled {
		t.Fatalf("expected client stream selector to run interceptor, stream=%#v called=%v", stream, clientStreamCalled)
	}
}

func TestTimeoutUnaryByMethodUsesSpecificTimeout(t *testing.T) {
	timeout := TimeoutUnaryByMethod(time.Second, map[string]time.Duration{
		"/test.Service/Slow": time.Nanosecond,
	})
	_, err := timeout(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Slow"}, func(ctx context.Context, req any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err == nil {
		t.Fatal("expected method timeout error")
	}
}

func TestTimeoutForMethodSupportsServiceAndDefault(t *testing.T) {
	timeouts := map[string]time.Duration{
		"test.Service/*": 2 * time.Second,
		"default":        3 * time.Second,
	}
	if got := timeoutForMethod("/test.Service/Get", time.Second, timeouts); got != 2*time.Second {
		t.Fatalf("expected service timeout, got %s", got)
	}
	if got := timeoutForMethod("/other.Service/Get", time.Second, timeouts); got != 3*time.Second {
		t.Fatalf("expected default timeout, got %s", got)
	}
}
