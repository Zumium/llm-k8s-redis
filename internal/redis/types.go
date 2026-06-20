// Package redis defines the Client interface the controller's action
// executors use to talk to individual Redis Cluster nodes, plus a
// go-redis-backed default implementation.
//
// The interface intentionally exposes only the handful of commands the
// executors need, so unit tests can substitute a fake without pulling in a
// running Redis instance.
package redis

import (
	"context"
	"time"
)

// Client is the subset of Redis operations the action executors rely on.
type Client interface {
	// Ping checks that the node responds to commands.
	Ping(ctx context.Context) error
	// ConfigGet returns the value of a CONFIG GET key as a string.
	ConfigGet(ctx context.Context, key string) (string, error)
	// ConfigSet sets a CONFIG key to a string value.
	ConfigSet(ctx context.Context, key, value string) error
	// ClusterMyID returns this node's Redis Cluster node id via
	// `CLUSTER MYID`. It fails on non-cluster nodes.
	ClusterMyID(ctx context.Context) (string, error)
	// ClusterNodes returns the raw `CLUSTER NODES` output from this node.
	// Each line describes one cluster member, including its node id,
	// flags, host/port and slot ownership.
	ClusterNodes(ctx context.Context) (string, error)
	// ClusterMeet instructs this node to introduce host:port into the
	// cluster gossip network via `CLUSTER MEET`.
	ClusterMeet(ctx context.Context, host string, port int) error
	// ClusterReplicate turns this node into a replica of masterNodeID via
	// `CLUSTER REPLICATE`. It must be issued against the future replica,
	// not the master.
	ClusterReplicate(ctx context.Context, masterNodeID string) error
	// ClusterAddSlots assigns the given slot numbers to this node via
	// `CLUSTER ADDSLOTS`. The slice may be empty (the call is a no-op in
	// that case). Slots must be in [0, 16383].
	ClusterAddSlots(ctx context.Context, slots []int) error
	ClusterSetSlotImporting(ctx context.Context, slot int, sourceNodeID string) error
	ClusterSetSlotMigrating(ctx context.Context, slot int, targetNodeID string) error
	ClusterSetSlotNode(ctx context.Context, slot int, nodeID string) error
	ClusterGetKeysInSlot(ctx context.Context, slot, count int) ([]string, error)
	MigrateKeys(ctx context.Context, host string, port int, keys []string, timeout time.Duration) error
	// ClusterInfo returns the raw `CLUSTER INFO` output from this node.
	// The output is a newline-separated `key:value` report including
	// `cluster_state`, slot coverage and failover counters.
	ClusterInfo(ctx context.Context) (string, error)
	// Close releases any underlying connection resources.
	Close() error
}

// Factory builds a Client for a Redis node reachable at addr (host:port).
// It is injected into executors so tests can substitute a fake.
type Factory func(addr string) (Client, error)

// DefaultFactory is the production Factory backed by go-redis.
var DefaultFactory Factory = func(addr string) (Client, error) {
	return NewClient(addr), nil
}
