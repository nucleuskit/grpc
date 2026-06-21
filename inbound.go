package runtimegrpc

import (
	"context"

	nucleuscontext "github.com/nucleuskit/nucleus/core/context"
	"github.com/nucleuskit/nucleus/core/inbound"
	"google.golang.org/grpc/metadata"
)

func NewInboundRequest(ctx context.Context, fullMethod string, message any) inbound.Request {
	metadata := metadataFromGRPCContext(ctx)
	return inbound.Request{
		Kind: inbound.KindGRPC,
		Route: inbound.Route{
			Method: fullMethod,
			Path:   fullMethod,
		},
		Body: inbound.Body{
			Value: message,
		},
		Metadata: metadata,
	}
}

func metadataFromGRPCContext(ctx context.Context) inbound.Metadata {
	result := inbound.Metadata{}
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		for key, values := range incoming {
			for _, value := range values {
				result.Add(key, value)
			}
		}
	}
	setIfPresent(result, inbound.KeyTraceID, result.Get(inbound.HeaderTraceParent))
	setIfPresent(result, inbound.KeyRequestID, result.Get(inbound.HeaderRequestID))
	setIfPresent(result, inbound.KeyTenant, result.Get(inbound.HeaderTenant))
	if result.Get(inbound.KeyTraceID) == "" {
		setIfPresent(result, inbound.KeyTraceID, nucleuscontext.TraceID(ctx))
	}
	if result.Get(inbound.KeyRequestID) == "" {
		setIfPresent(result, inbound.KeyRequestID, nucleuscontext.RequestID(ctx))
	}
	if result.Get(inbound.KeyTenant) == "" {
		setIfPresent(result, inbound.KeyTenant, nucleuscontext.Tenant(ctx))
	}
	return result
}

func setIfPresent(metadata inbound.Metadata, key string, value string) {
	if value != "" {
		metadata.Set(key, value)
	}
}
