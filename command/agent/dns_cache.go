package agent

import (
	"fmt"
	"github.com/hashicorp/consul/consul/structs"
	"sync"
	"time"
)

const (
	// dnsReapInterval controls how often we reap old DNS entries
	// This is the default if a value is not provided.
	dnsReapInterval = 5 * time.Second

	// ServiceWildcard is used to set a TTL for all services
	ServiceWildcard = "*"
)

// DNSCache is used for controling caching of DNS results
type DNSCache struct {
	nodeTTL    time.Duration
	serviceTTL map[string]time.Duration

	nodeCache   map[string]*structs.NodeServices
	nodeExpires map[string]time.Time

	serviceCache   map[string]structs.CheckServiceNodes
	serviceExpires map[string]time.Time

	l sync.RWMutex

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewDNSCache is used to construct a new DNSCache
func NewDNSCache(nodeTTL time.Duration, serviceTTL map[string]time.Duration, reapInterval time.Duration) *DNSCache {
	// Ensure serviceTTL is non-nil
	if serviceTTL == nil {
		serviceTTL = make(map[string]time.Duration)
	}
	// Ensure we have a reap interval
	if reapInterval == 0 {
		reapInterval = dnsReapInterval
	}
	d := &DNSCache{
		nodeTTL:        nodeTTL,
		serviceTTL:     serviceTTL,
		nodeCache:      make(map[string]*structs.NodeServices),
		nodeExpires:    make(map[string]time.Time),
		serviceCache:   make(map[string]structs.CheckServiceNodes),
		serviceExpires: make(map[string]time.Time),
		shutdownCh:     make(chan struct{}),
	}
	go d.reap(reapInterval)
	return d
}

// Close is used to shutdown the DNSCache and stop the background tasks
func (d *DNSCache) Close() {
	d.shutdownLock.Lock()
	defer d.shutdownLock.Unlock()

	if !d.shutdown {
		d.shutdown = true
		close(d.shutdownCh)
	}
}

// NodeTTL returns the maximum TTL for a node
func (d *DNSCache) NodeTTL() time.Duration {
	return d.nodeTTL
}

// ServiceTTL returns the maximum TTL for a given service
func (d *DNSCache) ServiceTTL(service string) time.Duration {
	// Lookup the service
	ttl, ok := d.serviceTTL[service]
	if ok {
		return ttl
	}

	// Check for a wildcard
	ttl, ok = d.serviceTTL[ServiceWildcard]
	if ok {
		return ttl
	}
	return 0
}

// SetNodeResult sets the results for a node lookup to be cached
func (d *DNSCache) SetNodeResult(node string, services *structs.NodeServices) {
	// Check if caching is enabled
	ttl := d.NodeTTL()
	if ttl == 0 {
		return
	}

	// Update the cache entries
	d.l.Lock()
	d.nodeCache[node] = services
	d.nodeExpires[node] = time.Now().Add(ttl)
	d.l.Unlock()
}

// GetNodeResult is used to lookup a cache result for a node
func (d *DNSCache) GetNodeResult(node string) (*structs.NodeServices, time.Duration) {
	// Check if caching is enabled
	ttl := d.NodeTTL()
	if ttl == 0 {
		return nil, 0
	}

	// Read from the cache
	d.l.RLock()
	services, ok := d.nodeCache[node]
	expires := d.nodeExpires[node]
	d.l.RUnlock()

	// Check for no result or expired
	now := time.Now()
	if !ok || now.After(expires) {
		return nil, 0
	}
	return services, expires.Sub(now)
}

// SetServiceResult sets the results for a node lookup to be cached
func (d *DNSCache) SetServiceResult(service, tag string, nodes structs.CheckServiceNodes) {
	// Check if we can cache this
	ttl := d.ServiceTTL(service)
	if ttl == 0 {
		return
	}

	// Create the cache key
	key := fmt.Sprintf("%s||%s||", service, tag)

	// Update the cache entries
	d.l.Lock()
	d.serviceCache[key] = nodes
	d.serviceExpires[key] = time.Now().Add(ttl)
	d.l.Unlock()
}

// GetServiceResult is used to lookup a cache result for a node
func (d *DNSCache) GetServiceResult(service, tag string) (structs.CheckServiceNodes, time.Duration) {
	// Check if we can cache this
	ttl := d.ServiceTTL(service)
	if ttl == 0 {
		return nil, 0
	}

	// Create the cache key
	key := fmt.Sprintf("%s||%s||", service, tag)

	// Update the cache entries
	d.l.RLock()
	nodes, ok := d.serviceCache[key]
	expires := d.serviceExpires[key]
	d.l.RUnlock()

	// Check for no result or expired
	now := time.Now()
	if !ok || now.After(expires) {
		return nil, 0
	}
	return nodes, expires.Sub(now)
}

// reap is used to cleanup old expired DNS entries
func (d *DNSCache) reap(reapInterval time.Duration) {
	for {
		select {
		case <-time.After(reapInterval):
			d.reapOnce()
		case <-d.shutdownCh:
			return
		}
	}
}

// reapOnce is used to reap the expired DNS entries immediately
func (d *DNSCache) reapOnce() {
	d.l.Lock()
	defer d.l.Unlock()

	// Gather the expired node entries
	now := time.Now()
	var keys []string
	for key, expires := range d.nodeExpires {
		if now.After(expires) {
			keys = append(keys, key)
		}
	}

	// Clear the expired node entries
	for _, key := range keys {
		delete(d.nodeCache, key)
		delete(d.nodeExpires, key)
	}

	// Gather the expired service entries
	keys = keys[:0]
	for key, expires := range d.serviceExpires {
		if now.After(expires) {
			keys = append(keys, key)
		}
	}

	// Clear the expired service entries
	for _, key := range keys {
		delete(d.serviceCache, key)
		delete(d.serviceExpires, key)
	}
}
