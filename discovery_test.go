package runtimegrpc

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	capdiscovery "github.com/nucleuskit/nucleus/cap/discovery"
	"google.golang.org/grpc/connectivity"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestStaticResolverAndDialService(t *testing.T) {
	resolver := StaticResolver{
		"hello-grpc": {{Addr: "127.0.0.1:1"}},
	}
	endpoints, err := resolver.Resolve(context.Background(), "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 1 || endpoints[0].Addr != "127.0.0.1:1" {
		t.Fatalf("unexpected endpoints: %#v", endpoints)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	conn, err := DialService(ctx, resolver, "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

func TestNewClientDialsDirectTarget(t *testing.T) {
	addr, stop := startGRPCHealthServer(t)
	defer stop()

	client := NewClient(
		WithClientTarget(DirectTarget(addr)),
		WithClientDialConfig(DialConfig{Timeout: 2 * time.Second}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := client.Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("unexpected health status: %s", resp.GetStatus())
	}
}

func TestNewClientResolvesDiscoveryTargetWithoutRegistry(t *testing.T) {
	addr, stop := startGRPCHealthServer(t)
	defer stop()

	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {{Addr: addr, Weight: 10, Health: HealthServing}},
	})
	client := NewClient(
		WithClientTarget(DiscoveryTarget("hello-grpc")),
		WithClientResolver(provider, ResolvePolicy{
			Filters:  []EndpointFilter{EndpointHealthAtLeast(HealthServing)},
			Balancer: BalanceWeighted,
		}),
		WithClientDialConfig(DialConfig{Timeout: 2 * time.Second}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := client.Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if conn.Target() != addr {
		t.Fatalf("expected resolved direct target %q, got %q", addr, conn.Target())
	}
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestRegisteredDiscoveryResolverDialsProviderTarget(t *testing.T) {
	addr, stop := startGRPCHealthServer(t)
	defer stop()

	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {{Addr: addr, Health: HealthServing}},
	})
	scheme := RegisterDiscoveryResolver(provider,
		WithDiscoveryResolverScheme(uniqueDiscoveryScheme(t)),
		WithDiscoveryResolverPolicy(ResolvePolicy{Filters: []EndpointFilter{EndpointHealthAtLeast(HealthServing)}}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := DialWithConfig(ctx, fmt.Sprintf("%s:///hello-grpc", scheme), DialConfig{
		Block:   true,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestDialServiceAcceptsCapDiscoveryResolver(t *testing.T) {
	resolver := capdiscovery.NewStaticProvider(map[string][]capdiscovery.Endpoint{
		"hello-grpc": {{Addr: "127.0.0.1:1"}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	conn, err := DialService(ctx, resolver, "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

func TestDialServiceRejectsMissingEndpoint(t *testing.T) {
	_, err := DialService(context.Background(), StaticResolver{}, "missing")
	if err == nil {
		t.Fatal("expected missing endpoint error")
	}
}

func TestStaticProviderResolvesMetadataAndReportsHealth(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {{
			Addr:     "127.0.0.1:50051",
			Metadata: map[string]string{"zone": "dev-a"},
		}},
	}, WithProviderMetadata(map[string]string{
		"provider": "static",
		"cluster":  "local",
	}))

	health, err := provider.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !health.Serving {
		t.Fatalf("expected serving health: %#v", health)
	}
	if health.Metadata["provider"] != "static" || health.Metadata["cluster"] != "local" {
		t.Fatalf("unexpected health metadata: %#v", health.Metadata)
	}

	endpoints, err := provider.Resolve(context.Background(), "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 1 || endpoints[0].Addr != "127.0.0.1:50051" {
		t.Fatalf("unexpected endpoints: %#v", endpoints)
	}
	if endpoints[0].Metadata["zone"] != "dev-a" {
		t.Fatalf("unexpected endpoint metadata: %#v", endpoints[0].Metadata)
	}

	endpoints[0].Metadata["zone"] = "mutated"
	metadata := provider.Metadata()
	metadata["provider"] = "mutated"

	endpoints, err = provider.Resolve(context.Background(), "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}
	if endpoints[0].Metadata["zone"] != "dev-a" {
		t.Fatalf("provider endpoint metadata was mutated: %#v", endpoints[0].Metadata)
	}
	if provider.Metadata()["provider"] != "static" {
		t.Fatalf("provider metadata was mutated: %#v", provider.Metadata())
	}
}

func TestStaticProviderCanReportUnhealthy(t *testing.T) {
	provider := NewStaticProvider(nil, WithProviderHealth(ProviderHealth{
		Serving: false,
		Message: "registry warming",
	}))

	health, err := provider.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Serving || health.Message != "registry warming" {
		t.Fatalf("unexpected health: %#v", health)
	}
}

func TestResolveWithPolicyFiltersMetadataAndWeight(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {
			{Addr: "127.0.0.1:50051", Weight: 1, Metadata: map[string]string{"zone": "dev-a", "version": "v1"}},
			{Addr: "127.0.0.1:50052", Weight: 10, Metadata: map[string]string{"zone": "dev-b", "version": "v2"}},
			{Addr: "127.0.0.1:50053", Weight: 5, Metadata: map[string]string{"zone": "dev-b", "version": "v1"}},
		},
	})

	endpoints, err := ResolveWithPolicy(context.Background(), provider, "hello-grpc", ResolvePolicy{
		Filters: []EndpointFilter{
			EndpointMetadataEquals("zone", "dev-b"),
			EndpointWeightAtLeast(6),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 1 || endpoints[0].Addr != "127.0.0.1:50052" {
		t.Fatalf("unexpected filtered endpoints: %#v", endpoints)
	}
	if endpoints[0].Weight != 10 || endpoints[0].Metadata["version"] != "v2" {
		t.Fatalf("unexpected endpoint fields: %#v", endpoints[0])
	}
}

func TestResolveWithPolicyFiltersHealthAndOrdersByWeight(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {
			{Addr: "127.0.0.1:50051", Weight: 1, Health: HealthServing},
			{Addr: "127.0.0.1:50052", Weight: 10, Health: HealthServing},
			{Addr: "127.0.0.1:50053", Weight: 100, Health: HealthUnhealthy},
			{Addr: "127.0.0.1:50054", Weight: 5},
		},
	})

	endpoints, err := ResolveWithPolicy(context.Background(), provider, "hello-grpc", ResolvePolicy{
		Filters: []EndpointFilter{EndpointHealthAtLeast(HealthServing)},
		Order:   OrderByWeightDesc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 2 {
		t.Fatalf("expected two healthy endpoints, got %#v", endpoints)
	}
	if endpoints[0].Addr != "127.0.0.1:50052" || endpoints[1].Addr != "127.0.0.1:50051" {
		t.Fatalf("expected weight-desc order, got %#v", endpoints)
	}
}

func TestSnapshotWithPolicyFiltersTopology(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {
			{Addr: "127.0.0.1:50051", Topology: Topology{Region: "cn", Zone: "a", Cell: "blue"}},
			{Addr: "127.0.0.1:50052", Topology: Topology{Region: "cn", Zone: "b", Cell: "blue"}},
			{Addr: "127.0.0.1:50053", Topology: Topology{Region: "us", Zone: "a", Cell: "green"}},
		},
	}, WithProviderTopology(Topology{Region: "cn", Zone: "a"}))

	snapshot, err := SnapshotWithPolicy(context.Background(), provider, "hello-grpc", ResolvePolicy{
		Filters: []EndpointFilter{EndpointInTopology(Topology{Region: "cn", Cell: "blue"})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Topology.Region != "cn" || snapshot.Topology.Zone != "a" {
		t.Fatalf("unexpected provider topology: %#v", snapshot.Topology)
	}
	if len(snapshot.Endpoints) != 2 {
		t.Fatalf("expected topology-filtered endpoints, got %#v", snapshot.Endpoints)
	}
}

func TestWatchWithPolicyPublishesFilteredSnapshot(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {
			{Addr: "127.0.0.1:50051", Weight: 1, Health: HealthServing},
			{Addr: "127.0.0.1:50052", Weight: 10, Health: HealthDraining},
		},
	})

	updates, err := WatchWithPolicy(context.Background(), provider, "hello-grpc", ResolvePolicy{
		Filters: []EndpointFilter{EndpointHealthAtLeast(HealthServing)},
		Order:   OrderByWeightAsc,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := <-updates
	if len(snapshot.Endpoints) != 1 || snapshot.Endpoints[0].Addr != "127.0.0.1:50051" {
		t.Fatalf("unexpected filtered snapshot: %#v", snapshot)
	}
}

func TestDialServiceWithPolicyUsesWeightedBalancer(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {
			{Addr: "127.0.0.1:50051", Weight: 1, Health: HealthServing},
			{Addr: "127.0.0.1:50052", Weight: 10, Health: HealthServing},
			{Addr: "127.0.0.1:50053", Weight: 100, Health: HealthUnhealthy},
		},
	})

	conn, err := DialServiceWithPolicy(context.Background(), provider, "hello-grpc", ResolvePolicy{
		Filters:  []EndpointFilter{EndpointHealthAtLeast(HealthServing)},
		Balancer: BalanceWeighted,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if conn.Target() != "127.0.0.1:50052" {
		t.Fatalf("expected weighted target, got %q", conn.Target())
	}
}

func TestStaticProviderSnapshotReturnsClonedWeightedEndpoints(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {{
			Addr:     "127.0.0.1:50051",
			Weight:   3,
			Metadata: map[string]string{"zone": "dev-a"},
			Health:   HealthServing,
			Topology: Topology{Region: "cn", Zone: "a"},
		}},
	}, WithProviderMetadata(map[string]string{"provider": "static"}), WithProviderTopology(Topology{Region: "cn"}))

	snapshot, err := provider.Snapshot(context.Background(), "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Service != "hello-grpc" {
		t.Fatalf("unexpected service: %q", snapshot.Service)
	}
	if snapshot.Metadata["provider"] != "static" {
		t.Fatalf("unexpected snapshot metadata: %#v", snapshot.Metadata)
	}
	if len(snapshot.Endpoints) != 1 || snapshot.Endpoints[0].Weight != 3 {
		t.Fatalf("unexpected snapshot endpoints: %#v", snapshot.Endpoints)
	}
	if snapshot.Endpoints[0].Health != HealthServing || snapshot.Endpoints[0].Topology.Zone != "a" || snapshot.Topology.Region != "cn" {
		t.Fatalf("unexpected snapshot health/topology: %#v", snapshot)
	}

	snapshot.Endpoints[0].Metadata["zone"] = "mutated"
	snapshot.Metadata["provider"] = "mutated"

	next, err := provider.Snapshot(context.Background(), "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}
	if next.Endpoints[0].Metadata["zone"] != "dev-a" || next.Metadata["provider"] != "static" {
		t.Fatalf("snapshot leaked mutable state: %#v", next)
	}
}

func TestStaticProviderWatchPublishesCurrentSnapshot(t *testing.T) {
	provider := NewStaticProvider(map[string][]Endpoint{
		"hello-grpc": {{Addr: "127.0.0.1:50051", Weight: 2}},
	})

	updates, err := provider.Watch(context.Background(), "hello-grpc")
	if err != nil {
		t.Fatal(err)
	}

	snapshot, ok := <-updates
	if !ok {
		t.Fatal("expected initial snapshot")
	}
	if len(snapshot.Endpoints) != 1 || snapshot.Endpoints[0].Weight != 2 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if _, ok := <-updates; ok {
		t.Fatal("static watch should close after current snapshot")
	}
}

func TestDiscoveryResolverRetriesInitialWatchFailureAndUpdatesState(t *testing.T) {
	source := &retryWatchSource{
		endpoints: []Endpoint{{Addr: "127.0.0.1:50051"}},
		updates:   make(chan ProviderSnapshot, 1),
	}
	cc := &recordingResolverClientConn{updated: make(chan []resolver.Address, 2)}
	builder := discoveryResolverBuilder{
		source: source,
		scheme: uniqueDiscoveryScheme(t),
		policy: ResolvePolicy{Retry: capdiscovery.RetryPolicy{
			MaxAttempts:    2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
		}},
	}

	built, err := builder.Build(mustResolverTarget(t, "discovery:///hello-grpc"), cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer built.Close()
	waitForResolverAddresses(t, cc.updated)

	source.updates <- ProviderSnapshot{
		Service:   "hello-grpc",
		Endpoints: []Endpoint{{Addr: "127.0.0.2:50051"}},
	}
	next := waitForResolverAddresses(t, cc.updated)
	if len(next) != 1 || next[0].Addr != "127.0.0.2:50051" {
		t.Fatalf("expected retried watch update, got %#v", next)
	}
	if source.watchAttempts() != 2 {
		t.Fatalf("watch attempts = %d, want 2", source.watchAttempts())
	}
}

func startGRPCHealthServer(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(WithHealthStatus("", healthpb.HealthCheckResponse_SERVING))
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	stop := func() {
		server.Stop()
		if err := <-serveErr; err != nil {
			t.Fatalf("Serve returned unexpected error: %v", err)
		}
	}
	return listener.Addr().String(), stop
}

func uniqueDiscoveryScheme(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(t.Name())
	replacer := strings.NewReplacer("/", "-", "_", "-", " ", "-")
	return "nucleus-test-" + replacer.Replace(name)
}

type retryWatchSource struct {
	mu        sync.Mutex
	attempts  int
	endpoints []Endpoint
	updates   chan ProviderSnapshot
}

func (s *retryWatchSource) Resolve(ctx context.Context, service string) ([]Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return capdiscovery.FilterEndpoints(s.endpoints), nil
}

func (s *retryWatchSource) Watch(ctx context.Context, service string) (<-chan ProviderSnapshot, error) {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()
	if attempt == 1 {
		return nil, fmt.Errorf("temporary watch failure")
	}
	return s.updates, nil
}

func (s *retryWatchSource) watchAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

type recordingResolverClientConn struct {
	updated chan []resolver.Address
}

func (c *recordingResolverClientConn) UpdateState(state resolver.State) error {
	c.updated <- state.Addresses
	return nil
}

func (c *recordingResolverClientConn) ReportError(error) {}

func (c *recordingResolverClientConn) NewAddress([]resolver.Address) {}

func (c *recordingResolverClientConn) NewServiceConfig(string) {}

func (c *recordingResolverClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return nil
}

func (c *recordingResolverClientConn) UpdateAddresses(string, []resolver.Address) {}

func (c *recordingResolverClientConn) UpdateStateCallback(resolver.State, func(error)) {}

func (c *recordingResolverClientConn) GetState() connectivity.State {
	return connectivity.Idle
}

func mustResolverTarget(t *testing.T, raw string) resolver.Target {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return resolver.Target{URL: *parsed}
}

func waitForResolverAddresses(t *testing.T, updates <-chan []resolver.Address) []resolver.Address {
	t.Helper()
	select {
	case addresses := <-updates:
		return addresses
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resolver update")
		return nil
	}
}
