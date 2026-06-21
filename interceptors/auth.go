package interceptors

import (
	"context"

	capauth "github.com/nucleuskit/nucleus/cap/auth"
	nucleuscontext "github.com/nucleuskit/nucleus/core/context"
	coreerrors "github.com/nucleuskit/nucleus/core/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthOption func(*authOptions)

type authOptions struct {
	credentials func(ctx context.Context, fullMethod string) capauth.Credentials
	permission  func(ctx context.Context, fullMethod string, principal capauth.Principal) capauth.Permission
}

func AuthUnaryCapability(authenticator capauth.Authenticator, authorizer capauth.Authorizer, options ...AuthOption) grpc.UnaryServerInterceptor {
	config := newAuthOptions(options...)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if authenticator == nil {
			return handler(ctx, req)
		}
		principal, err := authenticator.Authenticate(ctx, config.credentials(ctx, info.FullMethod))
		if err != nil {
			return nil, authStatusError(coreerrors.CodeUnauthenticated)
		}
		if authorizer != nil {
			permission := config.permission(ctx, info.FullMethod, principal)
			if err := authorizer.Authorize(ctx, principal, permission); err != nil {
				return nil, authStatusError(coreerrors.CodePermissionDenied)
			}
		}
		return handler(contextWithPrincipal(ctx, principal), req)
	}
}

func AuthStreamCapability(authenticator capauth.Authenticator, authorizer capauth.Authorizer, options ...AuthOption) grpc.StreamServerInterceptor {
	config := newAuthOptions(options...)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if authenticator == nil {
			return handler(srv, stream)
		}
		ctx := stream.Context()
		principal, err := authenticator.Authenticate(ctx, config.credentials(ctx, info.FullMethod))
		if err != nil {
			return authStatusError(coreerrors.CodeUnauthenticated)
		}
		if authorizer != nil {
			permission := config.permission(ctx, info.FullMethod, principal)
			if err := authorizer.Authorize(ctx, principal, permission); err != nil {
				return authStatusError(coreerrors.CodePermissionDenied)
			}
		}
		nextStream := &contextServerStream{ServerStream: stream, ctx: contextWithPrincipal(ctx, principal)}
		return handler(srv, nextStream)
	}
}

func WithAuthCredentials(fn func(ctx context.Context, fullMethod string) capauth.Credentials) AuthOption {
	return func(options *authOptions) {
		if fn != nil {
			options.credentials = fn
		}
	}
}

func WithAuthPermission(fn func(ctx context.Context, fullMethod string, principal capauth.Principal) capauth.Permission) AuthOption {
	return func(options *authOptions) {
		if fn != nil {
			options.permission = fn
		}
	}
}

func newAuthOptions(options ...AuthOption) authOptions {
	config := authOptions{
		credentials: defaultAuthCredentials,
		permission:  defaultAuthPermission,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	return config
}

func defaultAuthCredentials(ctx context.Context, _ string) capauth.Credentials {
	headers, _ := metadata.FromIncomingContext(ctx)
	return capauth.CredentialsFromHeaders(headers)
}

func defaultAuthPermission(_ context.Context, fullMethod string, _ capauth.Principal) capauth.Permission {
	return capauth.InvokePermission(fullMethod)
}

func contextWithPrincipal(ctx context.Context, principal capauth.Principal) context.Context {
	ctx = capauth.ContextWithPrincipal(ctx, principal)
	if principal.Tenant != "" {
		ctx = nucleuscontext.WithTenant(ctx, principal.Tenant)
	}
	return ctx
}

func authStatusError(code coreerrors.Code) error {
	return status.Error(grpcCode(code), code.DefaultMessage())
}

func grpcCode(code coreerrors.Code) codes.Code {
	switch code {
	case coreerrors.CodeUnauthenticated:
		return codes.Unauthenticated
	case coreerrors.CodePermissionDenied:
		return codes.PermissionDenied
	case coreerrors.CodeInvalidArgument, coreerrors.CodeFailedPrecondition:
		return codes.InvalidArgument
	case coreerrors.CodeNotFound:
		return codes.NotFound
	case coreerrors.CodeDeadlineExceeded:
		return codes.DeadlineExceeded
	case coreerrors.CodeUnavailable:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}
