package interceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func RecoveryUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				resp = nil
				err = status.Errorf(codes.Internal, "grpc panic recovered")
			}
		}()
		return handler(ctx, req)
	}
}

func RecoveryStream() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = status.Errorf(codes.Internal, "grpc stream panic recovered")
			}
		}()
		return handler(srv, stream)
	}
}

func ChainUnary(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if len(interceptors) == 0 {
			return handler(ctx, req)
		}
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(current context.Context, request any) (any, error) {
				return interceptor(current, request, info, next)
			}
		}
		return chained(ctx, req)
	}
}

func ChainStream(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if len(interceptors) == 0 {
			return handler(srv, stream)
		}
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(current any, currentStream grpc.ServerStream) error {
				return interceptor(current, currentStream, info, next)
			}
		}
		return chained(srv, stream)
	}
}
