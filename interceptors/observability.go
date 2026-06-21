package interceptors

import (
	"context"
	"fmt"
	"strings"
	"time"

	caplog "github.com/nucleuskit/cap/log"
	capmetric "github.com/nucleuskit/cap/metric"
	capsentinel "github.com/nucleuskit/cap/sentinel"
	captrace "github.com/nucleuskit/cap/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	grpcServerRequestsMetric       = "grpc.server.requests"
	grpcServerDurationMetric       = "grpc.server.duration_ms"
	grpcServerDurationLatestMetric = "grpc.server.duration_ms.latest"
	grpcClientRequestsMetric       = "grpc.client.requests"
	grpcClientDurationMetric       = "grpc.client.duration_ms"
	grpcClientDurationLatestMetric = "grpc.client.duration_ms.latest"
)

type DefaultChainConfig struct {
	Logger         caplog.Logger
	Tracer         captrace.Tracer
	Meter          capmetric.Meter
	Breaker        capsentinel.Breaker
	Limiter        capsentinel.Limiter
	Timeout        time.Duration
	MethodTimeouts map[string]time.Duration
	Authenticator  Authenticator
}

type ClientChainConfig struct {
	Logger         caplog.Logger
	Tracer         captrace.Tracer
	Meter          capmetric.Meter
	Breaker        capsentinel.Breaker
	Limiter        capsentinel.Limiter
	Timeout        time.Duration
	MethodTimeouts map[string]time.Duration
	Authenticator  Authenticator
}

func DefaultUnaryChain(config DefaultChainConfig) grpc.UnaryServerInterceptor {
	chain := []grpc.UnaryServerInterceptor{RecoveryUnary()}
	if len(config.MethodTimeouts) > 0 {
		chain = append(chain, TimeoutUnaryByMethod(config.Timeout, config.MethodTimeouts))
	} else if config.Timeout > 0 {
		chain = append(chain, TimeoutUnary(config.Timeout))
	}
	if config.Tracer != nil {
		chain = append(chain, TraceUnary(config.Tracer))
	}
	if config.Logger != nil {
		chain = append(chain, LogUnary(config.Logger))
	}
	if config.Meter != nil {
		chain = append(chain, MeterUnary(config.Meter))
	}
	if config.Breaker != nil || config.Limiter != nil {
		chain = append(chain, SentinelUnary(config.Breaker, config.Limiter))
	}
	if config.Authenticator != nil {
		chain = append(chain, AuthUnary(config.Authenticator))
	}
	return ChainUnary(chain...)
}

func DefaultStreamChain(config DefaultChainConfig) grpc.StreamServerInterceptor {
	chain := []grpc.StreamServerInterceptor{RecoveryStream()}
	if len(config.MethodTimeouts) > 0 {
		chain = append(chain, TimeoutStreamByMethod(config.Timeout, config.MethodTimeouts))
	} else if config.Timeout > 0 {
		chain = append(chain, TimeoutStream(config.Timeout))
	}
	if config.Tracer != nil {
		chain = append(chain, TraceStream(config.Tracer))
	}
	if config.Logger != nil {
		chain = append(chain, LogStream(config.Logger))
	}
	if config.Meter != nil {
		chain = append(chain, MeterStream(config.Meter))
	}
	if config.Breaker != nil || config.Limiter != nil {
		chain = append(chain, SentinelStream(config.Breaker, config.Limiter))
	}
	if config.Authenticator != nil {
		chain = append(chain, AuthStream(config.Authenticator))
	}
	return ChainStream(chain...)
}

func DefaultUnaryClientChain(config ClientChainConfig) grpc.UnaryClientInterceptor {
	chain := []grpc.UnaryClientInterceptor{}
	if len(config.MethodTimeouts) > 0 {
		chain = append(chain, TimeoutUnaryClientByMethod(config.Timeout, config.MethodTimeouts))
	} else if config.Timeout > 0 {
		chain = append(chain, TimeoutUnaryClient(config.Timeout))
	}
	if config.Tracer != nil {
		chain = append(chain, TraceUnaryClient(config.Tracer))
	}
	if config.Logger != nil {
		chain = append(chain, LogUnaryClient(config.Logger))
	}
	if config.Meter != nil {
		chain = append(chain, MeterUnaryClient(config.Meter))
	}
	if config.Breaker != nil || config.Limiter != nil {
		chain = append(chain, SentinelUnaryClient(config.Breaker, config.Limiter))
	}
	if config.Authenticator != nil {
		chain = append(chain, AuthUnaryClient(config.Authenticator))
	}
	return ChainUnaryClient(chain...)
}

func DefaultStreamClientChain(config ClientChainConfig) grpc.StreamClientInterceptor {
	chain := []grpc.StreamClientInterceptor{}
	if len(config.MethodTimeouts) > 0 {
		chain = append(chain, TimeoutStreamClientByMethod(config.Timeout, config.MethodTimeouts))
	} else if config.Timeout > 0 {
		chain = append(chain, TimeoutStreamClient(config.Timeout))
	}
	if config.Tracer != nil {
		chain = append(chain, TraceStreamClient(config.Tracer))
	}
	if config.Logger != nil {
		chain = append(chain, LogStreamClient(config.Logger))
	}
	if config.Meter != nil {
		chain = append(chain, MeterStreamClient(config.Meter))
	}
	if config.Breaker != nil || config.Limiter != nil {
		chain = append(chain, SentinelStreamClient(config.Breaker, config.Limiter))
	}
	if config.Authenticator != nil {
		chain = append(chain, AuthStreamClient(config.Authenticator))
	}
	return ChainStreamClient(chain...)
}

func LogUnary(logger caplog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		if logger != nil {
			fields := []caplog.Field{
				caplog.String("grpc.method", info.FullMethod),
				caplog.String("grpc.code", status.Code(err).String()),
				caplog.Any("duration_ms", time.Since(start).Milliseconds()),
			}
			if err != nil {
				logger.Error(ctx, "grpc request completed", append(fields, caplog.Any("error", err))...)
			} else {
				logger.Info(ctx, "grpc request completed", fields...)
			}
		}
		return resp, err
	}
}

func TraceUnary(tracer captrace.Tracer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if tracer == nil {
			return handler(ctx, req)
		}
		if carrier := traceCarrierFromIncomingContext(ctx); len(carrier) > 0 {
			ctx = tracer.Extract(ctx, carrier)
		}
		ctx, span := tracer.Start(ctx, info.FullMethod, captrace.String("rpc.system", "grpc"), captrace.String("rpc.method", info.FullMethod))
		if span == nil {
			return handler(ctx, req)
		}
		defer span.End()
		resp, err := handler(ctx, req)
		if err != nil {
			span.RecordError(err)
		}
		span.SetAttribute("rpc.grpc.status_code", status.Code(err).String())
		return resp, err
	}
}

type UnaryMetrics interface {
	RecordUnary(ctx context.Context, method string, code string)
}

func MetricUnary(recorder UnaryMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if recorder != nil {
			recorder.RecordUnary(ctx, info.FullMethod, status.Code(err).String())
		}
		return resp, err
	}
}

func MeterUnary(meter capmetric.Meter) grpc.UnaryServerInterceptor {
	if meter == nil {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	metrics := newGRPCServerMetrics(meter)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		defer func() {
			code := status.Code(err).String()
			if recovered := recover(); recovered != nil {
				metrics.record(ctx, info.FullMethod, codes.Internal.String(), false, start)
				panic(recovered)
			}
			metrics.record(ctx, info.FullMethod, code, false, start)
		}()
		resp, err = handler(ctx, req)
		return resp, err
	}
}

func TimeoutUnary(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return timeoutUnary(ctx, req, info.FullMethod, handler, timeout)
	}
}

func TimeoutUnaryByMethod(defaultTimeout time.Duration, timeouts map[string]time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return timeoutUnary(ctx, req, info.FullMethod, handler, timeoutForMethod(info.FullMethod, defaultTimeout, timeouts))
	}
}

func SentinelUnary(breaker capsentinel.Breaker, limiter capsentinel.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if breaker == nil && limiter == nil {
			return handler(ctx, req)
		}
		resource := sentinelResource("grpc.server", info.FullMethod, false)
		guard, permit, err := acquireSentinel(ctx, resource, breaker, limiter)
		if err != nil {
			return nil, status.Error(codes.Unavailable, codes.Unavailable.String())
		}
		if permit != nil {
			defer permit.Release()
		}
		if guard != nil {
			defer func() {
				if recovered := recover(); recovered != nil {
					guard.Done(fmt.Errorf("panic: %v", recovered))
					panic(recovered)
				}
				guard.Done(err)
			}()
		}
		return handler(ctx, req)
	}
}

type Authenticator interface {
	Authenticate(ctx context.Context, fullMethod string) (context.Context, error)
}

type AuthenticatorFunc func(ctx context.Context, fullMethod string) (context.Context, error)

func (fn AuthenticatorFunc) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	return fn(ctx, fullMethod)
}

func AuthUnary(authenticator Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if authenticator == nil {
			return handler(ctx, req)
		}
		nextCtx, err := authenticator.Authenticate(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(nextCtx, req)
	}
}

func LogUnaryClient(logger caplog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		if logger != nil {
			fields := []caplog.Field{
				caplog.String("grpc.method", method),
				caplog.String("grpc.code", status.Code(err).String()),
				caplog.Any("duration_ms", time.Since(start).Milliseconds()),
			}
			if err != nil {
				logger.Error(ctx, "grpc client completed", append(fields, caplog.Any("error", err))...)
			} else {
				logger.Info(ctx, "grpc client completed", fields...)
			}
		}
		return err
	}
}

func TraceUnaryClient(tracer captrace.Tracer) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if tracer == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		ctx, span := tracer.Start(ctx, method, captrace.String("rpc.system", "grpc"), captrace.String("rpc.method", method), captrace.String("rpc.kind", "client"))
		ctx = outgoingTraceContext(ctx, tracer)
		if span == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		defer span.End()
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			span.RecordError(err)
		}
		span.SetAttribute("rpc.grpc.status_code", status.Code(err).String())
		return err
	}
}

func MeterUnaryClient(meter capmetric.Meter) grpc.UnaryClientInterceptor {
	if meter == nil {
		return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}
	metrics := newGRPCClientMetrics(meter)
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) (err error) {
		start := time.Now()
		defer func() {
			code := status.Code(err).String()
			if recovered := recover(); recovered != nil {
				metrics.record(ctx, method, codes.Internal.String(), false, start)
				panic(recovered)
			}
			metrics.record(ctx, method, code, false, start)
		}()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func TimeoutUnaryClient(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if timeout <= 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func TimeoutUnaryClientByMethod(defaultTimeout time.Duration, timeouts map[string]time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		timeout := timeoutForMethod(method, defaultTimeout, timeouts)
		if timeout <= 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func SentinelUnaryClient(breaker capsentinel.Breaker, limiter capsentinel.Limiter) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) (err error) {
		if breaker == nil && limiter == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		resource := sentinelResource("grpc.client", method, false)
		guard, permit, err := acquireSentinel(ctx, resource, breaker, limiter)
		if err != nil {
			return status.Error(codes.Unavailable, codes.Unavailable.String())
		}
		if permit != nil {
			defer permit.Release()
		}
		if guard != nil {
			defer func() {
				if recovered := recover(); recovered != nil {
					guard.Done(fmt.Errorf("panic: %v", recovered))
					panic(recovered)
				}
				guard.Done(err)
			}()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func AuthUnaryClient(authenticator Authenticator) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if authenticator == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		nextCtx, err := authenticator.Authenticate(ctx, method)
		if err != nil {
			return err
		}
		return invoker(nextCtx, method, req, reply, cc, opts...)
	}
}

func LogStream(logger caplog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		if logger != nil {
			ctx := stream.Context()
			fields := []caplog.Field{
				caplog.String("grpc.method", info.FullMethod),
				caplog.String("grpc.code", status.Code(err).String()),
				caplog.Any("duration_ms", time.Since(start).Milliseconds()),
			}
			if err != nil {
				logger.Error(ctx, "grpc stream completed", append(fields, caplog.Any("error", err))...)
			} else {
				logger.Info(ctx, "grpc stream completed", fields...)
			}
		}
		return err
	}
}

func TraceStream(tracer captrace.Tracer) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if tracer == nil {
			return handler(srv, stream)
		}
		ctx := stream.Context()
		if carrier := traceCarrierFromIncomingContext(ctx); len(carrier) > 0 {
			ctx = tracer.Extract(ctx, carrier)
		}
		ctx, span := tracer.Start(ctx, info.FullMethod, captrace.String("rpc.system", "grpc"), captrace.String("rpc.method", info.FullMethod))
		if span == nil {
			return handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
		}
		defer span.End()
		err := handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
		if err != nil {
			span.RecordError(err)
		}
		span.SetAttribute("rpc.grpc.status_code", status.Code(err).String())
		return err
	}
}

func MeterStream(meter capmetric.Meter) grpc.StreamServerInterceptor {
	if meter == nil {
		return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, stream)
		}
	}
	metrics := newGRPCServerMetrics(meter)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		start := time.Now()
		ctx := stream.Context()
		defer func() {
			code := status.Code(err).String()
			if recovered := recover(); recovered != nil {
				metrics.record(ctx, info.FullMethod, codes.Internal.String(), true, start)
				panic(recovered)
			}
			metrics.record(ctx, info.FullMethod, code, true, start)
		}()
		err = handler(srv, stream)
		return err
	}
}

func TimeoutStream(timeout time.Duration) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if timeout <= 0 {
			return handler(srv, stream)
		}
		ctx, cancel := context.WithTimeout(stream.Context(), timeout)
		defer cancel()
		return handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
	}
}

func TimeoutStreamByMethod(defaultTimeout time.Duration, timeouts map[string]time.Duration) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		timeout := timeoutForMethod(info.FullMethod, defaultTimeout, timeouts)
		if timeout <= 0 {
			return handler(srv, stream)
		}
		ctx, cancel := context.WithTimeout(stream.Context(), timeout)
		defer cancel()
		return handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
	}
}

func SentinelStream(breaker capsentinel.Breaker, limiter capsentinel.Limiter) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		if breaker == nil && limiter == nil {
			return handler(srv, stream)
		}
		resource := sentinelResource("grpc.server", info.FullMethod, true)
		guard, permit, err := acquireSentinel(stream.Context(), resource, breaker, limiter)
		if err != nil {
			return status.Error(codes.Unavailable, codes.Unavailable.String())
		}
		if permit != nil {
			defer permit.Release()
		}
		if guard != nil {
			defer func() {
				if recovered := recover(); recovered != nil {
					guard.Done(fmt.Errorf("panic: %v", recovered))
					panic(recovered)
				}
				guard.Done(err)
			}()
		}
		return handler(srv, stream)
	}
}

func LogStreamClient(logger caplog.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if logger != nil {
			fields := []caplog.Field{
				caplog.String("grpc.method", method),
				caplog.String("grpc.code", status.Code(err).String()),
				caplog.Any("duration_ms", time.Since(start).Milliseconds()),
			}
			if err != nil {
				logger.Error(ctx, "grpc client stream completed", append(fields, caplog.Any("error", err))...)
			} else {
				logger.Info(ctx, "grpc client stream completed", fields...)
			}
		}
		return stream, err
	}
}

func TraceStreamClient(tracer captrace.Tracer) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if tracer == nil {
			return streamer(ctx, desc, cc, method, opts...)
		}
		ctx, span := tracer.Start(ctx, method, captrace.String("rpc.system", "grpc"), captrace.String("rpc.method", method), captrace.String("rpc.kind", "client"))
		ctx = outgoingTraceContext(ctx, tracer)
		if span == nil {
			return streamer(ctx, desc, cc, method, opts...)
		}
		defer span.End()
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			span.RecordError(err)
		}
		span.SetAttribute("rpc.grpc.status_code", status.Code(err).String())
		return stream, err
	}
}

func MeterStreamClient(meter capmetric.Meter) grpc.StreamClientInterceptor {
	if meter == nil {
		return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(ctx, desc, cc, method, opts...)
		}
	}
	metrics := newGRPCClientMetrics(meter)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (stream grpc.ClientStream, err error) {
		start := time.Now()
		defer func() {
			code := status.Code(err).String()
			if recovered := recover(); recovered != nil {
				metrics.record(ctx, method, codes.Internal.String(), true, start)
				panic(recovered)
			}
			metrics.record(ctx, method, code, true, start)
		}()
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func TimeoutStreamClient(timeout time.Duration) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if timeout <= 0 {
			return streamer(ctx, desc, cc, method, opts...)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			cancel()
			return nil, err
		}
		return &cancelClientStream{ClientStream: stream, cancel: cancel}, nil
	}
}

func TimeoutStreamClientByMethod(defaultTimeout time.Duration, timeouts map[string]time.Duration) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		timeout := timeoutForMethod(method, defaultTimeout, timeouts)
		if timeout <= 0 {
			return streamer(ctx, desc, cc, method, opts...)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			cancel()
			return nil, err
		}
		return &cancelClientStream{ClientStream: stream, cancel: cancel}, nil
	}
}

func SentinelStreamClient(breaker capsentinel.Breaker, limiter capsentinel.Limiter) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (stream grpc.ClientStream, err error) {
		if breaker == nil && limiter == nil {
			return streamer(ctx, desc, cc, method, opts...)
		}
		resource := sentinelResource("grpc.client", method, true)
		guard, permit, err := acquireSentinel(ctx, resource, breaker, limiter)
		if err != nil {
			return nil, status.Error(codes.Unavailable, codes.Unavailable.String())
		}
		if permit != nil {
			defer permit.Release()
		}
		if guard != nil {
			defer func() {
				if recovered := recover(); recovered != nil {
					guard.Done(fmt.Errorf("panic: %v", recovered))
					panic(recovered)
				}
				guard.Done(err)
			}()
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func AuthStreamClient(authenticator Authenticator) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if authenticator == nil {
			return streamer(ctx, desc, cc, method, opts...)
		}
		nextCtx, err := authenticator.Authenticate(ctx, method)
		if err != nil {
			return nil, err
		}
		return streamer(nextCtx, desc, cc, method, opts...)
	}
}

type StreamMetrics interface {
	RecordStream(ctx context.Context, method string, code string)
}

func MetricStream(recorder StreamMetrics) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		if recorder != nil {
			recorder.RecordStream(stream.Context(), info.FullMethod, status.Code(err).String())
		}
		return err
	}
}

func AuthStream(authenticator Authenticator) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if authenticator == nil {
			return handler(srv, stream)
		}
		ctx, err := authenticator.Authenticate(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
	}
}

func timeoutUnary(ctx context.Context, req any, _ string, handler grpc.UnaryHandler, timeout time.Duration) (any, error) {
	if timeout <= 0 {
		return handler(ctx, req)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return handler(ctx, req)
}

func timeoutForMethod(fullMethod string, fallback time.Duration, timeouts map[string]time.Duration) time.Duration {
	if len(timeouts) == 0 {
		return fallback
	}
	candidates := []string{fullMethod}
	service, method := splitFullMethod(fullMethod)
	if method != "" {
		candidates = append(candidates, method)
	}
	if service != "" {
		candidates = append(candidates, service+"/*")
	}
	candidates = append(candidates, "*", "default")
	for _, candidate := range candidates {
		if timeout, ok := timeouts[candidate]; ok {
			return timeout
		}
	}
	return fallback
}

func splitFullMethod(fullMethod string) (service string, method string) {
	fullMethod = strings.Trim(fullMethod, "/")
	if fullMethod == "" {
		return "", ""
	}
	before, after, ok := strings.Cut(fullMethod, "/")
	if !ok {
		return "", before
	}
	return before, after
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextServerStream) Context() context.Context {
	return s.ctx
}

type grpcServerMetrics struct {
	requests       capmetric.Counter
	duration       capmetric.Histogram
	durationLatest capmetric.Gauge
}

type grpcClientMetrics struct {
	requests       capmetric.Counter
	duration       capmetric.Histogram
	durationLatest capmetric.Gauge
}

func newGRPCServerMetrics(meter capmetric.Meter) grpcServerMetrics {
	labels := []string{"method", "code", "stream"}
	return grpcServerMetrics{
		requests: meter.Counter(
			grpcServerRequestsMetric,
			capmetric.WithDescription("Total number of completed gRPC server calls."),
			capmetric.WithLabels(labels...),
		),
		duration: meter.Histogram(
			grpcServerDurationMetric,
			capmetric.WithUnit("ms"),
			capmetric.WithDescription("Duration of completed gRPC server calls."),
			capmetric.WithLabels(labels...),
			capmetric.WithBuckets(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
		),
		durationLatest: meter.Gauge(
			grpcServerDurationLatestMetric,
			capmetric.WithUnit("ms"),
			capmetric.WithDescription("Latest duration of completed gRPC server calls."),
			capmetric.WithLabels(labels...),
		),
	}
}

func (m grpcServerMetrics) record(ctx context.Context, method string, code string, stream bool, start time.Time) {
	attrs := []capmetric.Attribute{
		capmetric.String("method", method),
		capmetric.String("code", code),
		capmetric.Bool("stream", stream),
	}
	durationMS := float64(time.Since(start).Microseconds()) / 1000
	if m.requests != nil {
		m.requests.Add(ctx, 1, attrs...)
	}
	if m.duration != nil {
		m.duration.Observe(ctx, durationMS, attrs...)
	}
	if m.durationLatest != nil {
		m.durationLatest.Set(ctx, durationMS, attrs...)
	}
}

func newGRPCClientMetrics(meter capmetric.Meter) grpcClientMetrics {
	labels := []string{"method", "code", "stream"}
	return grpcClientMetrics{
		requests: meter.Counter(
			grpcClientRequestsMetric,
			capmetric.WithDescription("Total number of completed gRPC client calls."),
			capmetric.WithLabels(labels...),
		),
		duration: meter.Histogram(
			grpcClientDurationMetric,
			capmetric.WithUnit("ms"),
			capmetric.WithDescription("Duration of completed gRPC client calls."),
			capmetric.WithLabels(labels...),
			capmetric.WithBuckets(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
		),
		durationLatest: meter.Gauge(
			grpcClientDurationLatestMetric,
			capmetric.WithUnit("ms"),
			capmetric.WithDescription("Latest duration of completed gRPC client calls."),
			capmetric.WithLabels(labels...),
		),
	}
}

func (m grpcClientMetrics) record(ctx context.Context, method string, code string, stream bool, start time.Time) {
	attrs := []capmetric.Attribute{
		capmetric.String("method", method),
		capmetric.String("code", code),
		capmetric.Bool("stream", stream),
	}
	durationMS := float64(time.Since(start).Microseconds()) / 1000
	if m.requests != nil {
		m.requests.Add(ctx, 1, attrs...)
	}
	if m.duration != nil {
		m.duration.Observe(ctx, durationMS, attrs...)
	}
	if m.durationLatest != nil {
		m.durationLatest.Set(ctx, durationMS, attrs...)
	}
}

func ChainUnaryClient(interceptors ...grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if len(interceptors) == 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		chained := invoker
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(current context.Context, currentMethod string, currentReq any, currentReply any, currentConn *grpc.ClientConn, currentOpts ...grpc.CallOption) error {
				return interceptor(current, currentMethod, currentReq, currentReply, currentConn, next, currentOpts...)
			}
		}
		return chained(ctx, method, req, reply, cc, opts...)
	}
}

func ChainStreamClient(interceptors ...grpc.StreamClientInterceptor) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if len(interceptors) == 0 {
			return streamer(ctx, desc, cc, method, opts...)
		}
		chained := streamer
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(current context.Context, currentDesc *grpc.StreamDesc, currentConn *grpc.ClientConn, currentMethod string, currentOpts ...grpc.CallOption) (grpc.ClientStream, error) {
				return interceptor(current, currentDesc, currentConn, currentMethod, next, currentOpts...)
			}
		}
		return chained(ctx, desc, cc, method, opts...)
	}
}

func outgoingTraceContext(ctx context.Context, tracer captrace.Tracer) context.Context {
	carrier := captrace.Carrier{}
	tracer.Inject(ctx, carrier)
	pairs := make([]string, 0, len(carrier)*2)
	for key, value := range carrier {
		if strings.TrimSpace(key) == "" || value == "" {
			continue
		}
		pairs = append(pairs, key, value)
	}
	if len(pairs) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

func sentinelResource(component string, method string, stream bool) capsentinel.Resource {
	return capsentinel.Resource{
		Name: method,
		Attributes: []capsentinel.Attribute{
			capsentinel.String("component", component),
			capsentinel.String("method", method),
			capsentinel.Any("stream", stream),
		},
	}
}

func acquireSentinel(ctx context.Context, resource capsentinel.Resource, breaker capsentinel.Breaker, limiter capsentinel.Limiter) (capsentinel.Guard, capsentinel.Permit, error) {
	var guard capsentinel.Guard
	var permit capsentinel.Permit
	var err error
	if breaker != nil {
		guard, err = breaker.Allow(ctx, resource)
		if err != nil {
			return nil, nil, err
		}
	}
	if limiter != nil {
		permit, err = limiter.Acquire(ctx, resource)
		if err != nil {
			if guard != nil {
				guard.Done(err)
			}
			return nil, nil, err
		}
	}
	return guard, permit, nil
}

type cancelClientStream struct {
	grpc.ClientStream
	cancel context.CancelFunc
}

func (s *cancelClientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	s.cancel()
	return err
}

func (s *cancelClientStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	if err != nil {
		s.cancel()
	}
	return err
}

func traceCarrierFromIncomingContext(ctx context.Context) captrace.Carrier {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	carrier := captrace.Carrier{}
	setTraceCarrierHeader(carrier, incoming, captrace.HeaderTraceParent, false)
	setTraceCarrierHeader(carrier, incoming, captrace.HeaderBaggage, true)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

func setTraceCarrierHeader(carrier captrace.Carrier, incoming metadata.MD, key string, join bool) {
	values := incoming.Get(key)
	if len(values) == 0 {
		return
	}
	if join {
		carrier.Set(key, strings.Join(values, ","))
		return
	}
	carrier.Set(key, values[0])
}
