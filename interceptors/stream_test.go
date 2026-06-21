package interceptors

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	capauth "github.com/nucleuskit/cap/auth"
	captrace "github.com/nucleuskit/cap/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRecoveryStreamConvertsPanicToInternalError(t *testing.T) {
	err := RecoveryStream()(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(any, grpc.ServerStream) error {
		panic("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal status, got %v (%v)", status.Code(err), err)
	}
}

func TestChainStreamRunsInterceptorsInOrder(t *testing.T) {
	var calls []string
	first := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		calls = append(calls, "first-before")
		err := handler(srv, stream)
		calls = append(calls, "first-after")
		return err
	}
	second := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		calls = append(calls, "second-before")
		err := handler(srv, stream)
		calls = append(calls, "second-after")
		return err
	}

	err := ChainStream(first, second)(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(any, grpc.ServerStream) error {
		calls = append(calls, "handler")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"first-before", "second-before", "handler", "second-after", "first-after"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected calls: %#v", calls)
	}
}

func TestAuthStreamRejectsUnauthenticatedRequest(t *testing.T) {
	authenticator := AuthenticatorFunc(func(ctx context.Context, fullMethod string) (context.Context, error) {
		return ctx, status.Error(codes.Unauthenticated, "missing token")
	})
	err := AuthStream(authenticator)(nil, &testServerStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.MD{})}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(any, grpc.ServerStream) error {
		t.Fatal("handler should not run when auth fails")
		return nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestTraceStreamExtractsMetadataAndWrapsContext(t *testing.T) {
	tracer := &recordingTracer{}
	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	stream := &testServerStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		captrace.HeaderTraceParent, traceparent,
		captrace.HeaderBaggage, "tenant=acme",
	))}

	err := TraceStream(tracer)(nil, stream, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(_ any, stream grpc.ServerStream) error {
		if stream == nil || stream.Context() == nil {
			t.Fatal("expected wrapped stream context")
		}
		if extracted, _ := stream.Context().Value(traceExtractedKey{}).(bool); !extracted {
			t.Fatal("expected stream context to include extracted trace context")
		}
		return status.Error(codes.Aborted, "stop")
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("expected aborted, got %v", err)
	}
	if tracer.extracted.Get(captrace.HeaderTraceParent) != traceparent {
		t.Fatalf("expected traceparent carrier value, got %#v", tracer.extracted)
	}
	if tracer.span.err == nil || !tracer.span.ended {
		t.Fatalf("expected stream span to record error and end: %#v", tracer.span)
	}
}

func TestMeterStreamRecordsRequestAndDuration(t *testing.T) {
	meter := newRecordingMeter()
	err := MeterStream(meter)(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(any, grpc.ServerStream) error {
		return status.Error(codes.Unavailable, "down")
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
	assertMetricCall(t, meter.counters["grpc.server.requests"].calls, "/test.Service/Stream", codes.Unavailable.String(), "true")
	assertMetricCall(t, meter.histograms["grpc.server.duration_ms"].calls, "/test.Service/Stream", codes.Unavailable.String(), "true")
	assertMetricCall(t, meter.gauges["grpc.server.duration_ms.latest"].calls, "/test.Service/Stream", codes.Unavailable.String(), "true")
}

func TestTimeoutStreamAddsDeadline(t *testing.T) {
	err := TimeoutStream(time.Second)(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(_ any, stream grpc.ServerStream) error {
		if _, ok := stream.Context().Deadline(); !ok {
			t.Fatal("expected timeout interceptor to set stream deadline")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSentinelStreamGuardsAndReportsHandlerError(t *testing.T) {
	sentinel := &recordingSentinel{}
	handlerErr := status.Error(codes.Unavailable, "down")

	err := SentinelStream(sentinel, nil)(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(any, grpc.ServerStream) error {
		return handlerErr
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if sentinel.resource.Name != "/test.Service/Stream" {
		t.Fatalf("unexpected sentinel resource: %#v", sentinel.resource)
	}
	if sentinel.guard.doneErr == nil {
		t.Fatal("expected guard to receive handler error")
	}
}

func TestDefaultStreamChainSkipsNilProvidersAndRecoversPanic(t *testing.T) {
	err := DefaultStreamChain(DefaultChainConfig{})(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Panic"}, func(any, grpc.ServerStream) error {
		panic("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal recovery error, got %v (%v)", status.Code(err), err)
	}
}

func TestDefaultStreamChainRunsConfiguredSentinelAndTimeout(t *testing.T) {
	var calls []string
	sentinel := &recordingSentinel{order: &calls}
	authenticator := AuthenticatorFunc(func(ctx context.Context, fullMethod string) (context.Context, error) {
		calls = append(calls, "auth")
		return ctx, nil
	})

	err := DefaultStreamChain(DefaultChainConfig{
		Breaker:       sentinel,
		Timeout:       time.Second,
		Authenticator: authenticator,
	})(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(_ any, stream grpc.ServerStream) error {
		if _, ok := stream.Context().Deadline(); !ok {
			t.Fatal("expected default stream chain timeout to set deadline")
		}
		calls = append(calls, "handler")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"sentinel-allow", "auth", "handler", "sentinel-done"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected default stream chain calls: %#v", calls)
	}
}

func TestDefaultStreamChainRunsRecoveryTraceLogMetricAuth(t *testing.T) {
	var calls []string
	logger := &recordingLogger{calls: &calls}
	tracer := &recordingTracer{calls: &calls}
	meter := newRecordingMeter()
	meter.calls = &calls
	authenticator := AuthenticatorFunc(func(ctx context.Context, fullMethod string) (context.Context, error) {
		calls = append(calls, "auth")
		return ctx, nil
	})

	err := DefaultStreamChain(DefaultChainConfig{
		Logger:        logger,
		Tracer:        tracer,
		Meter:         meter,
		Authenticator: authenticator,
	})(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(any, grpc.ServerStream) error {
		calls = append(calls, "handler")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"trace-start", "auth", "handler", "metric-counter", "metric-histogram", "metric-gauge", "log-info", "trace-end"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected default stream chain calls: %#v", calls)
	}
}

func TestDefaultStreamClientChainRunsTraceLogMetricSentinelTimeoutAuth(t *testing.T) {
	var calls []string
	logger := &recordingLogger{calls: &calls}
	tracer := &recordingTracer{calls: &calls}
	meter := newRecordingMeter()
	meter.calls = &calls
	sentinel := &recordingSentinel{order: &calls}
	authenticator := AuthenticatorFunc(func(ctx context.Context, fullMethod string) (context.Context, error) {
		calls = append(calls, "auth")
		return ctx, nil
	})

	stream, err := DefaultStreamClientChain(ClientChainConfig{
		Logger:        logger,
		Tracer:        tracer,
		Meter:         meter,
		Breaker:       sentinel,
		Timeout:       time.Second,
		Authenticator: authenticator,
	})(context.Background(), &grpc.StreamDesc{ServerStreams: true}, nil, "/test.Service/Stream", func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("expected client stream chain timeout to set deadline")
		}
		calls = append(calls, "handler")
		return &testClientStream{ctx: ctx}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stream == nil {
		t.Fatal("expected client stream")
	}
	want := []string{"trace-start", "sentinel-allow", "auth", "handler", "sentinel-done", "metric-counter", "metric-histogram", "metric-gauge", "log-info", "trace-end"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected default client stream chain calls: %#v", calls)
	}
}

func TestDefaultStreamChainRecordsRecoveredPanicInMetrics(t *testing.T) {
	meter := newRecordingMeter()
	err := DefaultStreamChain(DefaultChainConfig{Meter: meter})(nil, &testServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test.Service/Panic"}, func(any, grpc.ServerStream) error {
		panic(errors.New("boom"))
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal recovery error, got %v (%v)", status.Code(err), err)
	}
	assertMetricCall(t, meter.counters["grpc.server.requests"].calls, "/test.Service/Panic", codes.Internal.String(), "true")
}

func TestAuthStreamCapabilityUsesInjectedPermission(t *testing.T) {
	authenticator := &capAuthenticator{principal: capauth.Principal{Subject: "alice"}}
	authorizer := &capAuthorizer{}
	stream := &testServerStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer stream-token"))}

	err := AuthStreamCapability(authenticator, authorizer,
		WithAuthPermission(func(_ context.Context, fullMethod string, principal capauth.Principal) capauth.Permission {
			return capauth.Permission{Resource: fullMethod, Action: "stream", Scope: principal.Subject}
		}),
	)(nil, stream, &grpc.StreamServerInfo{FullMethod: "/test.Service/Stream"}, func(_ any, stream grpc.ServerStream) error {
		principal, ok := capauth.PrincipalFromContext(stream.Context())
		if !ok || principal.Subject != "alice" {
			t.Fatalf("expected stream principal, got %#v %v", principal, ok)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if authenticator.credentials.Token != "stream-token" {
		t.Fatalf("unexpected stream credentials: %#v", authenticator.credentials)
	}
	if authorizer.permission.Action != "stream" || authorizer.permission.Scope != "alice" {
		t.Fatalf("unexpected stream permission: %#v", authorizer.permission)
	}
}

type testServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *testServerStream) Context() context.Context {
	return s.ctx
}

type testClientStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *testClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *testClientStream) Trailer() metadata.MD {
	return nil
}

func (s *testClientStream) CloseSend() error {
	return nil
}

func (s *testClientStream) Context() context.Context {
	return s.ctx
}

func (s *testClientStream) SendMsg(any) error {
	return nil
}

func (s *testClientStream) RecvMsg(any) error {
	return nil
}
