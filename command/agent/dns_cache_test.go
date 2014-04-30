package agent

import (
	"github.com/hashicorp/consul/consul/structs"
	"testing"
	"time"
)

func TestDNSCache_TTL(t *testing.T) {
	serviceTTL := map[string]time.Duration{
		ServiceWildcard: 2 * time.Second,
		"api":           5 * time.Second,
		"sensitive":     0,
	}
	d := NewDNSCache(time.Second, serviceTTL, 0)
	defer d.Close()

	if d.NodeTTL() != time.Second {
		t.Fatalf("Bad node TTL")
	}

	if ttl := d.ServiceTTL("api"); ttl != 5*time.Second {
		t.Fatalf("bad: %v", ttl)
	}

	if ttl := d.ServiceTTL("sensitive"); ttl != 0 {
		t.Fatalf("bad: %v", ttl)
	}

	if ttl := d.ServiceTTL("other"); ttl != 2*time.Second {
		t.Fatalf("bad: %v", ttl)
	}
}

func TestDNSCache_NodeCache(t *testing.T) {
	d := NewDNSCache(50*time.Millisecond, nil, 0)
	defer d.Close()

	// Read before cache
	result, expires := d.GetNodeResult("foo")
	if result != nil || expires != 0 {
		t.Fatalf("Bad: %v %v", result, expires)
	}

	// Update cache
	val := &structs.NodeServices{Node: structs.Node{Node: "foo"}}
	d.SetNodeResult("foo", val)

	// Should get cached result
	result, expires = d.GetNodeResult("foo")
	if result == nil || expires < 40*time.Millisecond {
		t.Fatalf("Bad: %v %v", result, expires)
	}

	// Wait for expiration
	time.Sleep(50 * time.Millisecond)
	result, expires = d.GetNodeResult("foo")
	if result != nil || expires != 0 {
		t.Fatalf("Bad: %v %v", result, expires)
	}
}

func TestDNSCache_ServiceCache(t *testing.T) {
	serviceCache := map[string]time.Duration{
		"api": 50 * time.Millisecond,
	}
	d := NewDNSCache(0, serviceCache, 0)
	defer d.Close()

	// Read before cache
	result, expires := d.GetServiceResult("api", "cool")
	if result != nil || expires != 0 {
		t.Fatalf("Bad: %v %v", result, expires)
	}

	// Update cache
	val := structs.CheckServiceNodes{
		structs.CheckServiceNode{
			Node: structs.Node{Node: "foo"},
		},
	}
	d.SetServiceResult("api", "cool", val)

	// Should get cached result
	result, expires = d.GetServiceResult("api", "cool")
	if result == nil || expires < 40*time.Millisecond {
		t.Fatalf("Bad: %v %v", result, expires)
	}

	// Tags should be independent
	result, expires = d.GetServiceResult("api", "other")
	if result != nil || expires != 0 {
		t.Fatalf("Bad: %v %v", result, expires)
	}

	result, expires = d.GetServiceResult("api", "")
	if result != nil || expires != 0 {
		t.Fatalf("Bad: %v %v", result, expires)
	}

	// Wait for expiration
	time.Sleep(50 * time.Millisecond)
	result, expires = d.GetServiceResult("api", "cool")
	if result != nil || expires != 0 {
		t.Fatalf("Bad: %v %v", result, expires)
	}
}

func TestDNSCache_Reap(t *testing.T) {
	serviceCache := map[string]time.Duration{
		"api": 30 * time.Millisecond,
	}
	d := NewDNSCache(30*time.Millisecond, serviceCache, 10*time.Millisecond)
	defer d.Close()

	// Update cache
	nval := &structs.NodeServices{Node: structs.Node{Node: "foo"}}
	d.SetNodeResult("foo", nval)

	// Update cache
	val := structs.CheckServiceNodes{
		structs.CheckServiceNode{
			Node: structs.Node{Node: "foo"},
		},
	}
	d.SetServiceResult("api", "cool", val)

	// Wait for reap
	time.Sleep(50 * time.Millisecond)

	// Verify reap
	if len(d.nodeCache) != 0 {
		t.Fatalf("reap failed")
	}
	if len(d.nodeExpires) != 0 {
		t.Fatalf("reap failed")
	}
	if len(d.serviceCache) != 0 {
		t.Fatalf("reap failed")
	}
	if len(d.serviceExpires) != 0 {
		t.Fatalf("reap failed")
	}
}
