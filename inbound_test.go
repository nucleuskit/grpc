package runtimegrpc

import (
	"context"
	"reflect"
	"testing"

	"github.com/nucleuskit/nucleus/core/inbound"
	"google.golang.org/grpc/metadata"
)

func TestNewInboundRequestFromGRPCPreservesMethodMessageAndMetadata(t *testing.T) {
	message := map[string]string{"name": "annie"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"traceparent", "trace-grpc",
		"x-request-id", "request-grpc",
		"x-tenant-id", "tenant-grpc",
	))

	got := NewInboundRequest(ctx, "/nucleus.UserService/GetUser", message)

	if got.Kind != inbound.KindGRPC {
		t.Fatalf("expected gRPC kind, got %q", got.Kind)
	}
	if got.Route.Method != "/nucleus.UserService/GetUser" || got.Route.Path != "/nucleus.UserService/GetUser" {
		t.Fatalf("unexpected route: %#v", got.Route)
	}
	if !reflect.DeepEqual(got.Body.Value, message) {
		t.Fatalf("unexpected body value: %#v", got.Body.Value)
	}
	if got.TraceID() != "trace-grpc" || got.RequestID() != "request-grpc" || got.Tenant() != "tenant-grpc" {
		t.Fatalf("metadata did not propagate: %#v", got.Metadata)
	}
}
