package interceptors

import (
	"context"
	"strings"

	"google.golang.org/grpc"
)

type MethodMatcher func(fullMethod string) bool

func SelectUnary(interceptor grpc.UnaryServerInterceptor, matchers ...MethodMatcher) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if interceptor == nil || !methodMatches(info.FullMethod, matchers...) {
			return handler(ctx, req)
		}
		return interceptor(ctx, req, info, handler)
	}
}

func SelectStream(interceptor grpc.StreamServerInterceptor, matchers ...MethodMatcher) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if interceptor == nil || !methodMatches(info.FullMethod, matchers...) {
			return handler(srv, stream)
		}
		return interceptor(srv, stream, info, handler)
	}
}

func SelectUnaryClient(interceptor grpc.UnaryClientInterceptor, matchers ...MethodMatcher) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if interceptor == nil || !methodMatches(method, matchers...) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		return interceptor(ctx, method, req, reply, cc, invoker, opts...)
	}
}

func SelectStreamClient(interceptor grpc.StreamClientInterceptor, matchers ...MethodMatcher) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if interceptor == nil || !methodMatches(method, matchers...) {
			return streamer(ctx, desc, cc, method, opts...)
		}
		return interceptor(ctx, desc, cc, method, streamer, opts...)
	}
}

func MatchFullMethod(methods ...string) MethodMatcher {
	allowed := methodSet(methods...)
	return func(fullMethod string) bool {
		return allowed[strings.TrimSpace(fullMethod)]
	}
}

func MatchService(services ...string) MethodMatcher {
	allowed := methodSet(services...)
	return func(fullMethod string) bool {
		service, _ := splitFullMethod(fullMethod)
		return allowed[service]
	}
}

func MatchMethodName(names ...string) MethodMatcher {
	allowed := methodSet(names...)
	return func(fullMethod string) bool {
		_, method := splitFullMethod(fullMethod)
		return allowed[method]
	}
}

func methodMatches(fullMethod string, matchers ...MethodMatcher) bool {
	if len(matchers) == 0 {
		return true
	}
	for _, matcher := range matchers {
		if matcher != nil && matcher(fullMethod) {
			return true
		}
	}
	return false
}

func methodSet(values ...string) map[string]bool {
	result := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result[value] = true
		}
	}
	return result
}
