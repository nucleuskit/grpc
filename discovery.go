package runtimegrpc

import (
	"context"
	"fmt"
	"strings"

	capdiscovery "github.com/nucleuskit/nucleus/cap/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	grpcresolver "google.golang.org/grpc/resolver"
)

type Service = capdiscovery.Service
type EndpointHealth = capdiscovery.Health
type Topology = capdiscovery.Topology
type Endpoint = capdiscovery.Endpoint
type Resolver = capdiscovery.Resolver
type ProviderHealth = capdiscovery.ProviderHealth
type ProviderSnapshot = capdiscovery.Snapshot
type EndpointFilter = capdiscovery.EndpointFilter
type EndpointOrder = capdiscovery.EndpointOrder
type BalanceStrategy = capdiscovery.BalanceStrategy
type ResolvePolicy = capdiscovery.ResolvePolicy
type RetryPolicy = capdiscovery.RetryPolicy
type Selector = capdiscovery.Selector
type SelectorOption = capdiscovery.SelectorOption
type StaticProviderOption = capdiscovery.StaticProviderOption
type StaticProvider = capdiscovery.StaticProvider
type StaticResolver = capdiscovery.StaticResolver
type TargetKind = capdiscovery.TargetKind
type Target = capdiscovery.Target
type ServiceInstance = capdiscovery.ServiceInstance

const (
	HealthUnknown   = capdiscovery.HealthUnknown
	HealthUnhealthy = capdiscovery.HealthUnhealthy
	HealthDraining  = capdiscovery.HealthDraining
	HealthServing   = capdiscovery.HealthServing
)

const (
	TargetKindDirect    = capdiscovery.TargetKindDirect
	TargetKindDiscovery = capdiscovery.TargetKindDiscovery
)

const (
	BalanceFirst              = capdiscovery.BalanceFirst
	BalanceWeighted           = capdiscovery.BalanceWeighted
	BalanceRandom             = capdiscovery.BalanceRandom
	BalanceRoundRobin         = capdiscovery.BalanceRoundRobin
	BalanceWeightedRoundRobin = capdiscovery.BalanceWeightedRoundRobin
	BalanceP2C                = capdiscovery.BalanceP2C
	BalanceEWMA               = capdiscovery.BalanceEWMA
	BalanceConsistentHash     = capdiscovery.BalanceConsistentHash
)

const DiscoveryResolverScheme = capdiscovery.TargetSchemeDiscovery

type Provider interface {
	Resolver
	Check(ctx context.Context) (ProviderHealth, error)
	Metadata() map[string]string
}

type SnapshotProvider interface {
	Provider
	Snapshot(ctx context.Context, service string) (ProviderSnapshot, error)
	Watch(ctx context.Context, service string) (<-chan ProviderSnapshot, error)
}

var (
	NewStaticProvider     = capdiscovery.NewStaticProvider
	WithProviderServices  = capdiscovery.WithProviderServices
	WithProviderInstances = capdiscovery.WithProviderInstances
	WithProviderMetadata  = capdiscovery.WithProviderMetadata
	WithProviderHealth    = capdiscovery.WithProviderHealth
	WithProviderTopology  = capdiscovery.WithProviderTopology

	DirectEndpoint  = capdiscovery.DirectEndpoint
	DirectTarget    = capdiscovery.DirectTarget
	DiscoveryTarget = capdiscovery.DiscoveryTarget
	ParseTarget     = capdiscovery.ParseTarget

	ResolveWithPolicy  = capdiscovery.ResolveWithPolicy
	SnapshotWithPolicy = capdiscovery.SnapshotWithPolicy
	WatchWithPolicy    = capdiscovery.WatchWithPolicy
	ApplyResolvePolicy = capdiscovery.ApplyResolvePolicy
	FilterEndpoints    = capdiscovery.FilterEndpoints
	Retry              = capdiscovery.Retry

	EndpointMetadataEquals = capdiscovery.EndpointMetadataEquals
	EndpointWeightAtLeast  = capdiscovery.EndpointWeightAtLeast
	EndpointHealthAtLeast  = capdiscovery.EndpointHealthAtLeast
	EndpointInTopology     = capdiscovery.EndpointInTopology
	EndpointVersion        = capdiscovery.EndpointVersion
	OrderByWeightDesc      = capdiscovery.OrderByWeightDesc
	OrderByWeightAsc       = capdiscovery.OrderByWeightAsc
	SelectEndpoint         = capdiscovery.SelectEndpoint
	NewSelector            = capdiscovery.NewSelector
	WithSelectorRand       = capdiscovery.WithSelectorRand
)

type DiscoveryResolverOption func(*discoveryResolverOptions)

type discoveryResolverOptions struct {
	scheme string
	policy ResolvePolicy
}

func WithDiscoveryResolverScheme(scheme string) DiscoveryResolverOption {
	return func(options *discoveryResolverOptions) {
		if strings.TrimSpace(scheme) != "" {
			options.scheme = strings.TrimSpace(scheme)
		}
	}
}

func WithDiscoveryResolverPolicy(policy ResolvePolicy) DiscoveryResolverOption {
	return func(options *discoveryResolverOptions) {
		options.policy = policy
	}
}

func RegisterDiscoveryResolver(source Resolver, options ...DiscoveryResolverOption) string {
	config := discoveryResolverOptions{scheme: DiscoveryResolverScheme}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	grpcresolver.Register(discoveryResolverBuilder{
		source: source,
		scheme: config.scheme,
		policy: config.policy,
	})
	return config.scheme
}

func DialService(ctx context.Context, resolver Resolver, service string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	endpoints, err := resolver.Resolve(ctx, service)
	if err != nil {
		return nil, err
	}
	return Dial(ctx, endpoints[0].Addr, opts...)
}

func DialServiceWithPolicy(ctx context.Context, resolver Resolver, service string, policy ResolvePolicy, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	endpoints, err := ResolveWithPolicy(ctx, resolver, service, policy)
	if err != nil {
		return nil, err
	}
	endpoint, ok := NewSelector(policy).Select(endpoints)
	if !ok {
		return nil, fmt.Errorf("grpc service has no endpoint selected: %s", service)
	}
	return Dial(ctx, endpoint.Addr, opts...)
}

type discoveryResolverBuilder struct {
	source Resolver
	scheme string
	policy ResolvePolicy
}

func (b discoveryResolverBuilder) Scheme() string {
	return b.scheme
}

func (b discoveryResolverBuilder) Build(target grpcresolver.Target, cc grpcresolver.ClientConn, _ grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	if b.source == nil {
		return nil, fmt.Errorf("grpc discovery resolver is nil")
	}
	service := discoveryResolverService(target)
	if service == "" {
		return nil, fmt.Errorf("grpc discovery target service is empty")
	}
	ctx, cancel := context.WithCancel(context.Background())
	resolved := &discoveryResolver{
		source:  b.source,
		service: service,
		policy:  b.policy,
		cc:      cc,
		cancel:  cancel,
	}
	if err := resolved.update(ctx); err != nil {
		cancel()
		return nil, err
	}
	if watcher, ok := b.source.(interface {
		Watch(context.Context, string) (<-chan ProviderSnapshot, error)
	}); ok {
		var updates <-chan ProviderSnapshot
		err := Retry(ctx, b.policy.Retry, func(ctx context.Context) error {
			next, err := watcher.Watch(ctx, service)
			if err != nil {
				return err
			}
			updates = next
			return nil
		})
		if err == nil {
			go resolved.watch(ctx, updates)
		} else {
			cc.ReportError(err)
		}
	}
	return resolved, nil
}

type discoveryResolver struct {
	source  Resolver
	service string
	policy  ResolvePolicy
	cc      grpcresolver.ClientConn
	cancel  context.CancelFunc
}

func (r *discoveryResolver) ResolveNow(_ grpcresolver.ResolveNowOptions) {
	if err := r.update(context.Background()); err != nil {
		r.cc.ReportError(err)
	}
}

func (r *discoveryResolver) Close() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *discoveryResolver) update(ctx context.Context) error {
	endpoints, err := ResolveWithPolicy(ctx, r.source, r.service, r.policy)
	if err != nil {
		return err
	}
	return r.cc.UpdateState(grpcresolver.State{Addresses: endpointAddresses(endpoints)})
}

func (r *discoveryResolver) watch(ctx context.Context, updates <-chan ProviderSnapshot) {
	for {
		select {
		case <-ctx.Done():
			return
		case snapshot, ok := <-updates:
			if !ok {
				return
			}
			endpoints := ApplyResolvePolicy(snapshot.Endpoints, r.policy)
			if len(endpoints) == 0 {
				continue
			}
			if err := r.cc.UpdateState(grpcresolver.State{Addresses: endpointAddresses(endpoints)}); err != nil {
				r.cc.ReportError(err)
			}
		}
	}
}

func discoveryResolverService(target grpcresolver.Target) string {
	if endpoint := strings.Trim(target.Endpoint(), "/"); endpoint != "" {
		return endpoint
	}
	if target.URL.Host != "" {
		return strings.Trim(target.URL.Host, "/")
	}
	return strings.Trim(target.URL.Path, "/")
}

func endpointAddresses(endpoints []Endpoint) []grpcresolver.Address {
	addresses := make([]grpcresolver.Address, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint.Addr) == "" {
			continue
		}
		addresses = append(addresses, grpcresolver.Address{
			Addr:       endpoint.Addr,
			ServerName: endpoint.Metadata["server_name"],
			Attributes: attributes.New("nucleus.discovery.addr", endpoint.Addr),
		})
	}
	return addresses
}
