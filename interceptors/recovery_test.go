package interceptors

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	capauth "github.com/nucleuskit/nucleus/cap/auth"
	caplog "github.com/nucleuskit/nucleus/cap/log"
	capmetric "github.com/nucleuskit/nucleus/cap/metric"
	capsentinel "github.com/nucleuskit/nucleus/cap/sentinel"
	captrace "github.com/nucleuskit/nucleus/cap/trace"
	nucleuscontext "github.com/nucleuskit/nucleus/core/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRecoveryUnaryConvertsPanicToInternalError(t *testing.T) {
	_, err := RecoveryUnary()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Panic"}, func(context.Context, any) (any, error) {
		panic("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal status, got %v (%v)", status.Code(err), err)
	}
}

func TestChainUnaryRunsInterceptorsInOrder(t *testing.T) {
	var calls []string
	first := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		calls = append(calls, "first-before")
		resp, err := handler(ctx, req)
		calls = append(calls, "first-after")
		return resp, err
	}
	second := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		calls = append(calls, "second-before")
		resp, err := handler(ctx, req)
		calls = append(calls, "second-after")
		return resp, err
	}

	resp, err := ChainUnary(first, second)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		calls = append(calls, "handler")
		return "response", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp != "response" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	want := []string{"first-before", "second-before", "handler", "second-after", "first-after"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected calls: %#v", calls)
	}
}

func TestLogUnaryRecordsCompletedCall(t *testing.T) {
	logger := &recordingLogger{}
	_, err := LogUnary(logger)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		return "response", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logger.messages) != 1 || logger.messages[0] != "grpc request completed" {
		t.Fatalf("unexpected log messages: %#v", logger.messages)
	}
}

func TestTraceUnaryStartsSpanAndRecordsHandlerError(t *testing.T) {
	tracer := &recordingTracer{}
	handlerErr := errors.New("handler failed")
	_, err := TraceUnary(tracer)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		return nil, handlerErr
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if tracer.name != "/test.Service/Call" {
		t.Fatalf("unexpected span name: %q", tracer.name)
	}
	if tracer.span.err == nil || !tracer.span.ended {
		t.Fatalf("expected span to record error and end: %#v", tracer.span)
	}
}

func TestTraceUnaryExtractsW3CMetadataBeforeStart(t *testing.T) {
	tracer := &recordingTracer{}
	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		captrace.HeaderTraceParent, traceparent,
		captrace.HeaderBaggage, "tenant=acme,feature=beta",
	))

	_, err := TraceUnary(tracer)(ctx, "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(ctx context.Context, req any) (any, error) {
		if extracted, _ := ctx.Value(traceExtractedKey{}).(bool); !extracted {
			t.Fatal("expected handler context to include extracted trace context")
		}
		return "response", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if tracer.extracted.Get(captrace.HeaderTraceParent) != traceparent {
		t.Fatalf("expected traceparent carrier value, got %#v", tracer.extracted)
	}
	if tracer.extracted.Get(captrace.HeaderBaggage) != "tenant=acme,feature=beta" {
		t.Fatalf("expected baggage carrier value, got %#v", tracer.extracted)
	}
	if extracted, _ := tracer.startContext.Value(traceExtractedKey{}).(bool); !extracted {
		t.Fatal("expected tracer.Start to receive extracted context")
	}
}

func TestMetricUnaryRecordsStatusCode(t *testing.T) {
	recorder := &recordingMetrics{}
	_, err := MetricUnary(recorder)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "missing")
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found, got %v", err)
	}
	if recorder.method != "/test.Service/Call" || recorder.code != codes.NotFound.String() {
		t.Fatalf("unexpected metric record: %#v", recorder)
	}
}

func TestMeterUnaryRecordsRequestAndDuration(t *testing.T) {
	meter := newRecordingMeter()
	_, err := MeterUnary(meter)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "missing")
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found, got %v", err)
	}
	assertMetricCall(t, meter.counters["grpc.server.requests"].calls, "/test.Service/Call", codes.NotFound.String(), "false")
	assertMetricCall(t, meter.histograms["grpc.server.duration_ms"].calls, "/test.Service/Call", codes.NotFound.String(), "false")
	assertMetricCall(t, meter.gauges["grpc.server.duration_ms.latest"].calls, "/test.Service/Call", codes.NotFound.String(), "false")
}

func TestTimeoutUnaryAddsDeadline(t *testing.T) {
	_, err := TimeoutUnary(time.Second)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(ctx context.Context, req any) (any, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("expected timeout interceptor to set deadline")
		}
		return "response", nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSentinelUnaryGuardsAndReportsHandlerError(t *testing.T) {
	sentinel := &recordingSentinel{}
	handlerErr := status.Error(codes.Unavailable, "down")

	_, err := SentinelUnary(sentinel, nil)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		return nil, handlerErr
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if sentinel.resource.Name != "/test.Service/Call" || sentinel.resource.Attributes[0].Key != "component" {
		t.Fatalf("unexpected sentinel resource: %#v", sentinel.resource)
	}
	if sentinel.guard.doneErr == nil {
		t.Fatal("expected guard to receive handler error")
	}
}

func TestAuthUnaryRejectsUnauthenticatedRequest(t *testing.T) {
	authenticator := AuthenticatorFunc(func(ctx context.Context, fullMethod string) (context.Context, error) {
		return ctx, status.Error(codes.Unauthenticated, "missing token")
	})
	_, err := AuthUnary(authenticator)(metadata.NewIncomingContext(context.Background(), metadata.MD{}), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		t.Fatal("handler should not run when auth fails")
		return nil, nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestDefaultUnaryChainSkipsNilProvidersAndRecoversPanic(t *testing.T) {
	_, err := DefaultUnaryChain(DefaultChainConfig{})(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Panic"}, func(context.Context, any) (any, error) {
		panic("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal recovery error, got %v (%v)", status.Code(err), err)
	}
}

func TestDefaultUnaryChainRunsConfiguredSentinelAndTimeout(t *testing.T) {
	var calls []string
	sentinel := &recordingSentinel{order: &calls}
	authenticator := AuthenticatorFunc(func(ctx context.Context, fullMethod string) (context.Context, error) {
		calls = append(calls, "auth")
		return ctx, nil
	})

	_, err := DefaultUnaryChain(DefaultChainConfig{
		Breaker:       sentinel,
		Timeout:       time.Second,
		Authenticator: authenticator,
	})(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(ctx context.Context, req any) (any, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("expected default chain timeout to set deadline")
		}
		calls = append(calls, "handler")
		return "response", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"sentinel-allow", "auth", "handler", "sentinel-done"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected default chain calls: %#v", calls)
	}
}

func TestDefaultUnaryChainRunsRecoveryTraceLogMetricAuth(t *testing.T) {
	var calls []string
	logger := &recordingLogger{calls: &calls}
	tracer := &recordingTracer{calls: &calls}
	meter := newRecordingMeter()
	meter.calls = &calls
	authenticator := AuthenticatorFunc(func(ctx context.Context, fullMethod string) (context.Context, error) {
		calls = append(calls, "auth")
		return ctx, nil
	})

	resp, err := DefaultUnaryChain(DefaultChainConfig{
		Logger:        logger,
		Tracer:        tracer,
		Meter:         meter,
		Authenticator: authenticator,
	})(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		calls = append(calls, "handler")
		return "response", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp != "response" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	want := []string{"trace-start", "auth", "handler", "metric-counter", "metric-histogram", "metric-gauge", "log-info", "trace-end"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected default chain calls: %#v", calls)
	}
}

func TestDefaultUnaryClientChainRunsTraceLogMetricSentinelTimeoutAuth(t *testing.T) {
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

	err := DefaultUnaryClientChain(ClientChainConfig{
		Logger:        logger,
		Tracer:        tracer,
		Meter:         meter,
		Breaker:       sentinel,
		Timeout:       time.Second,
		Authenticator: authenticator,
	})(context.Background(), "/test.Service/Call", "request", nil, nil, func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("expected client chain timeout to set deadline")
		}
		calls = append(calls, "handler")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"trace-start", "sentinel-allow", "auth", "handler", "sentinel-done", "metric-counter", "metric-histogram", "metric-gauge", "log-info", "trace-end"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected default client chain calls: %#v", calls)
	}
}

func TestAuthUnaryCapabilityAuthenticatesAuthorizesAndStoresPrincipal(t *testing.T) {
	authenticator := &capAuthenticator{principal: capauth.Principal{Subject: "alice", Tenant: "tenant-a"}}
	authorizer := &capAuthorizer{}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token-1"))

	resp, err := AuthUnaryCapability(authenticator, authorizer)(ctx, "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(ctx context.Context, req any) (any, error) {
		principal, ok := capauth.PrincipalFromContext(ctx)
		if !ok || principal.Subject != "alice" {
			t.Fatalf("expected principal in context, got %#v %v", principal, ok)
		}
		if tenant := nucleuscontext.Tenant(ctx); tenant != "tenant-a" {
			t.Fatalf("expected tenant tenant-a, got %q", tenant)
		}
		return "response", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp != "response" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if authenticator.credentials.Token != "token-1" || authenticator.credentials.Scheme != capauth.SchemeBearer {
		t.Fatalf("unexpected credentials: %#v", authenticator.credentials)
	}
	if authorizer.permission.Resource != "/test.Service/Call" || authorizer.permission.Action != capauth.ActionInvoke {
		t.Fatalf("unexpected permission: %#v", authorizer.permission)
	}
}

func TestAuthUnaryCapabilitySanitizesAuthErrors(t *testing.T) {
	authenticator := &capAuthenticator{err: errors.New("token backend detail")}
	_, err := AuthUnaryCapability(authenticator, nil)(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		t.Fatal("handler should not run when auth fails")
		return nil, nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
	if err != nil && errors.Is(err, authenticator.err) {
		t.Fatalf("auth error should be sanitized, got %v", err)
	}
}

type recordingLogger struct {
	messages []string
	calls    *[]string
}

func (l *recordingLogger) Debug(context.Context, string, ...caplog.Field) {}
func (l *recordingLogger) Info(_ context.Context, message string, _ ...caplog.Field) {
	l.messages = append(l.messages, message)
	if l.calls != nil {
		*l.calls = append(*l.calls, "log-info")
	}
}
func (l *recordingLogger) Warn(context.Context, string, ...caplog.Field)  {}
func (l *recordingLogger) Error(context.Context, string, ...caplog.Field) {}

type recordingTracer struct {
	name         string
	span         *recordingSpan
	extracted    captrace.Carrier
	startContext context.Context
	calls        *[]string
}

func (t *recordingTracer) Start(ctx context.Context, name string, _ ...captrace.Attribute) (context.Context, captrace.Span) {
	if t.calls != nil {
		*t.calls = append(*t.calls, "trace-start")
	}
	t.name = name
	t.startContext = ctx
	t.span = &recordingSpan{calls: t.calls}
	return ctx, t.span
}

func (t *recordingTracer) Inject(context.Context, captrace.Carrier) {}

func (t *recordingTracer) Extract(ctx context.Context, carrier captrace.Carrier) context.Context {
	t.extracted = cloneTraceCarrier(carrier)
	return context.WithValue(ctx, traceExtractedKey{}, true)
}

type recordingSpan struct {
	err   error
	ended bool
	calls *[]string
}

func (s *recordingSpan) Context() captrace.SpanContext {
	return captrace.SpanContext{}
}

func (s *recordingSpan) SetAttribute(string, any) {}
func (s *recordingSpan) RecordError(err error) {
	s.err = err
}
func (s *recordingSpan) End() {
	if s.calls != nil {
		*s.calls = append(*s.calls, "trace-end")
	}
	s.ended = true
}

type recordingMetrics struct {
	method string
	code   string
}

func (r *recordingMetrics) RecordUnary(_ context.Context, method string, code string) {
	r.method = method
	r.code = code
}

type traceExtractedKey struct{}

func cloneTraceCarrier(carrier captrace.Carrier) captrace.Carrier {
	if len(carrier) == 0 {
		return nil
	}
	values := captrace.Carrier{}
	for key, value := range carrier {
		values[key] = value
	}
	return values
}

type recordingMeter struct {
	counters   map[string]*recordingInstrument
	gauges     map[string]*recordingInstrument
	histograms map[string]*recordingInstrument
	calls      *[]string
}

func newRecordingMeter() *recordingMeter {
	return &recordingMeter{
		counters:   map[string]*recordingInstrument{},
		gauges:     map[string]*recordingInstrument{},
		histograms: map[string]*recordingInstrument{},
	}
}

func (m *recordingMeter) Counter(name string, options ...capmetric.InstrumentOption) capmetric.Counter {
	instrument := newRecordingInstrument(capmetric.KindCounter, name, options...)
	instrument.callsLog = m.calls
	instrument.callsName = "metric-counter"
	m.counters[name] = instrument
	return instrument
}

func (m *recordingMeter) Gauge(name string, options ...capmetric.InstrumentOption) capmetric.Gauge {
	instrument := newRecordingInstrument(capmetric.KindGauge, name, options...)
	instrument.callsLog = m.calls
	instrument.callsName = "metric-gauge"
	m.gauges[name] = instrument
	return instrument
}

func (m *recordingMeter) Histogram(name string, options ...capmetric.InstrumentOption) capmetric.Histogram {
	instrument := newRecordingInstrument(capmetric.KindHistogram, name, options...)
	instrument.callsLog = m.calls
	instrument.callsName = "metric-histogram"
	m.histograms[name] = instrument
	return instrument
}

func (m *recordingMeter) Snapshot() map[string]float64 {
	return map[string]float64{}
}

type recordingInstrument struct {
	descriptor capmetric.Descriptor
	calls      []metricCall
	callsLog   *[]string
	callsName  string
}

type metricCall struct {
	value      float64
	attributes []capmetric.Attribute
}

func newRecordingInstrument(kind capmetric.InstrumentKind, name string, options ...capmetric.InstrumentOption) *recordingInstrument {
	values := capmetric.NewInstrumentOptions(options...)
	return &recordingInstrument{descriptor: capmetric.Descriptor{
		Kind:        kind,
		Name:        name,
		Unit:        values.Unit,
		Description: values.Description,
		Labels:      append([]string(nil), values.Labels...),
		Buckets:     append([]float64(nil), values.Buckets...),
	}}
}

func (i *recordingInstrument) Descriptor() capmetric.Descriptor {
	return i.descriptor.Clone()
}

func (i *recordingInstrument) Record(_ context.Context, value float64, attributes ...capmetric.Attribute) {
	i.record(value, attributes...)
}

func (i *recordingInstrument) Add(_ context.Context, value float64, attributes ...capmetric.Attribute) {
	i.record(value, attributes...)
}

func (i *recordingInstrument) Set(_ context.Context, value float64, attributes ...capmetric.Attribute) {
	i.record(value, attributes...)
}

func (i *recordingInstrument) Observe(_ context.Context, value float64, attributes ...capmetric.Attribute) {
	i.record(value, attributes...)
}

func (i *recordingInstrument) record(value float64, attributes ...capmetric.Attribute) {
	if i.callsLog != nil {
		*i.callsLog = append(*i.callsLog, i.callsName)
	}
	i.calls = append(i.calls, metricCall{value: value, attributes: append([]capmetric.Attribute(nil), attributes...)})
}

func assertMetricCall(t *testing.T, calls []metricCall, method string, code string, stream string) {
	t.Helper()
	if len(calls) != 1 {
		t.Fatalf("expected one metric call, got %#v", calls)
	}
	if calls[0].value < 0 {
		t.Fatalf("expected non-negative metric value, got %f", calls[0].value)
	}
	if got := metricAttribute(calls[0].attributes, "method"); got != method {
		t.Fatalf("expected method label %q, got %q", method, got)
	}
	if got := metricAttribute(calls[0].attributes, "code"); got != code {
		t.Fatalf("expected code label %q, got %q", code, got)
	}
	if got := metricAttribute(calls[0].attributes, "stream"); got != stream {
		t.Fatalf("expected stream label %q, got %q", stream, got)
	}
}

func metricAttribute(attributes []capmetric.Attribute, key string) string {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return fmt.Sprint(attribute.Value)
		}
	}
	return ""
}

type capAuthenticator struct {
	principal   capauth.Principal
	credentials capauth.Credentials
	err         error
}

func (a *capAuthenticator) Authenticate(_ context.Context, credentials capauth.Credentials) (capauth.Principal, error) {
	a.credentials = credentials
	if a.err != nil {
		return capauth.Principal{}, a.err
	}
	return a.principal, nil
}

type capAuthorizer struct {
	principal  capauth.Principal
	permission capauth.Permission
	err        error
}

func (a *capAuthorizer) Authorize(_ context.Context, principal capauth.Principal, permission capauth.Permission) error {
	a.principal = principal
	a.permission = permission
	return a.err
}

type recordingSentinel struct {
	resource capsentinel.Resource
	guard    *recordingSentinelGuard
	permit   *recordingSentinelPermit
	err      error
	order    *[]string
}

func (s *recordingSentinel) Allow(_ context.Context, resource capsentinel.Resource) (capsentinel.Guard, error) {
	s.resource = resource
	if s.order != nil {
		*s.order = append(*s.order, "sentinel-allow")
	}
	if s.err != nil {
		return nil, s.err
	}
	s.guard = &recordingSentinelGuard{order: s.order}
	return s.guard, nil
}

func (s *recordingSentinel) Acquire(_ context.Context, resource capsentinel.Resource) (capsentinel.Permit, error) {
	s.resource = resource
	if s.order != nil {
		*s.order = append(*s.order, "sentinel-acquire")
	}
	if s.err != nil {
		return nil, s.err
	}
	s.permit = &recordingSentinelPermit{order: s.order}
	return s.permit, nil
}

type recordingSentinelGuard struct {
	doneErr error
	order   *[]string
}

func (g *recordingSentinelGuard) Done(err error) {
	g.doneErr = err
	if g.order != nil {
		*g.order = append(*g.order, "sentinel-done")
	}
}

type recordingSentinelPermit struct {
	released bool
	order    *[]string
}

func (p *recordingSentinelPermit) Release() {
	p.released = true
	if p.order != nil {
		*p.order = append(*p.order, "sentinel-release")
	}
}
