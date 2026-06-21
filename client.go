package runtimegrpc

import (
	"context"
	"fmt"
	"time"

	grpcinterceptors "github.com/nucleuskit/nucleus/runtime/grpc/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

type DialConfig struct {
	Insecure             bool
	TransportCredentials credentials.TransportCredentials
	Keepalive            *keepalive.ClientParameters
	UnaryInterceptors    []grpc.UnaryClientInterceptor
	StreamInterceptors   []grpc.StreamClientInterceptor
	Block                bool
	Timeout              time.Duration
	Authority            string
	Service              string
	Metadata             map[string]string
	Resolver             Resolver
	ResolvePolicy        ResolvePolicy
	DialOptions          []grpc.DialOption
}

type Client struct {
	target string
	config DialConfig
}

type ClientOption func(*Client)

func NewClient(options ...ClientOption) *Client {
	client := &Client{}
	for _, option := range options {
		if option != nil {
			option(client)
		}
	}
	client.config = cloneDialConfig(client.config)
	return client
}

func WithClientTarget(target string) ClientOption {
	return func(client *Client) {
		client.target = target
	}
}

func WithClientDialConfig(config DialConfig) ClientOption {
	return func(client *Client) {
		previous := client.config
		client.config = cloneDialConfig(config)
		if client.config.Resolver == nil {
			client.config.Resolver = previous.Resolver
			client.config.ResolvePolicy = previous.ResolvePolicy
		}
		if len(client.config.UnaryInterceptors) == 0 {
			client.config.UnaryInterceptors = append([]grpc.UnaryClientInterceptor(nil), previous.UnaryInterceptors...)
		}
		if len(client.config.StreamInterceptors) == 0 {
			client.config.StreamInterceptors = append([]grpc.StreamClientInterceptor(nil), previous.StreamInterceptors...)
		}
	}
}

func WithClientResolver(resolver Resolver, policy ResolvePolicy) ClientOption {
	return func(client *Client) {
		client.config.Resolver = resolver
		client.config.ResolvePolicy = policy
	}
}

func WithClientUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) ClientOption {
	return func(client *Client) {
		client.config.UnaryInterceptors = append(client.config.UnaryInterceptors, interceptors...)
	}
}

func WithClientStreamInterceptors(interceptors ...grpc.StreamClientInterceptor) ClientOption {
	return func(client *Client) {
		client.config.StreamInterceptors = append(client.config.StreamInterceptors, interceptors...)
	}
}

func WithClientDefaultInterceptors(config grpcinterceptors.ClientChainConfig) ClientOption {
	return func(client *Client) {
		client.config.UnaryInterceptors = append(client.config.UnaryInterceptors, grpcinterceptors.DefaultUnaryClientChain(config))
		client.config.StreamInterceptors = append(client.config.StreamInterceptors, grpcinterceptors.DefaultStreamClientChain(config))
	}
}

func (c *Client) Dial(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return c.DialTarget(ctx, c.target, opts...)
}

func (c *Client) DialTarget(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return DialWithConfig(ctx, target, c.config, opts...)
}

func Dial(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return DialWithConfig(ctx, target, DialConfig{}, opts...)
}

func DialWithConfig(ctx context.Context, target string, config DialConfig, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}

	target, err := resolveDialTarget(ctx, target, config)
	if err != nil {
		return nil, err
	}

	dialOptions := []grpc.DialOption{
		transportCredentialsOption(config),
	}
	if config.Keepalive != nil {
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(*config.Keepalive))
	}
	if config.Block {
		dialOptions = append(dialOptions, grpc.WithBlock())
	}
	if config.Authority != "" {
		dialOptions = append(dialOptions, grpc.WithAuthority(config.Authority))
	}

	unaryInterceptors := append([]grpc.UnaryClientInterceptor{}, config.UnaryInterceptors...)
	streamInterceptors := append([]grpc.StreamClientInterceptor{}, config.StreamInterceptors...)
	if len(config.Metadata) > 0 || config.Service != "" {
		unaryInterceptors = append([]grpc.UnaryClientInterceptor{metadataUnaryClientInterceptor(config)}, unaryInterceptors...)
		streamInterceptors = append([]grpc.StreamClientInterceptor{metadataStreamClientInterceptor(config)}, streamInterceptors...)
	}
	if len(unaryInterceptors) > 0 {
		dialOptions = append(dialOptions, grpc.WithChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		dialOptions = append(dialOptions, grpc.WithChainStreamInterceptor(streamInterceptors...))
	}
	dialOptions = append(dialOptions, config.DialOptions...)
	dialOptions = append(dialOptions, opts...)
	return grpc.DialContext(ctx, target, dialOptions...)
}

func resolveDialTarget(ctx context.Context, raw string, config DialConfig) (string, error) {
	target, err := ParseTarget(raw)
	if err != nil {
		return "", err
	}
	if target.Kind != TargetKindDiscovery {
		return target.Endpoint.Addr, nil
	}
	if config.Resolver == nil {
		return raw, nil
	}
	endpoints, err := ResolveWithPolicy(ctx, config.Resolver, target.Service, config.ResolvePolicy)
	if err != nil {
		return "", err
	}
	endpoint, ok := SelectEndpoint(endpoints, config.ResolvePolicy.Balancer)
	if !ok {
		return "", fmt.Errorf("grpc discovery target has no endpoint selected: %s", target.Service)
	}
	return endpoint.Addr, nil
}

func transportCredentialsOption(config DialConfig) grpc.DialOption {
	if config.TransportCredentials != nil && !config.Insecure {
		return grpc.WithTransportCredentials(config.TransportCredentials)
	}
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

func metadataUnaryClientInterceptor(config DialConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(outgoingMetadataContext(ctx, config), method, req, reply, cc, opts...)
	}
}

func metadataStreamClientInterceptor(config DialConfig) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(outgoingMetadataContext(ctx, config), desc, cc, method, opts...)
	}
}

func outgoingMetadataContext(ctx context.Context, config DialConfig) context.Context {
	pairs := make([]string, 0, (len(config.Metadata)+1)*2)
	if config.Service != "" {
		pairs = append(pairs, "x-nucleus-service", config.Service)
	}
	for key, value := range config.Metadata {
		if key == "" {
			continue
		}
		pairs = append(pairs, key, value)
	}
	if len(pairs) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

func cloneDialConfig(config DialConfig) DialConfig {
	config.UnaryInterceptors = append([]grpc.UnaryClientInterceptor(nil), config.UnaryInterceptors...)
	config.StreamInterceptors = append([]grpc.StreamClientInterceptor(nil), config.StreamInterceptors...)
	config.Metadata = cloneStringMap(config.Metadata)
	config.DialOptions = append([]grpc.DialOption(nil), config.DialOptions...)
	return config
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
