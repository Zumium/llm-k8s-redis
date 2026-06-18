package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// goRedisClient adapts github.com/redis/go-redis/v9 to the redis.Client
// interface. It holds a single *redis.Client per node address.
type goRedisClient struct {
	c *goredis.Client
}

// NewClient returns a Client backed by github.com/redis/go-redis/v9 that
// connects to addr (host:port). Commands use a 3s timeout by default.
func NewClient(addr string) Client {
	host, port := splitAddr(addr)
	return &goRedisClient{
		c: goredis.NewClient(&goredis.Options{
			Addr:        netAddr(host, port),
			DialTimeout: 3 * time.Second,
			ReadTimeout: 3 * time.Second,
			PoolSize:    2,
		}),
	}
}

func (g *goRedisClient) Ping(ctx context.Context) error {
	if err := g.c.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis PING: %w", err)
	}
	return nil
}

func (g *goRedisClient) ConfigGet(ctx context.Context, key string) (string, error) {
	res := g.c.ConfigGet(ctx, key)
	if err := res.Err(); err != nil {
		return "", fmt.Errorf("redis CONFIG GET %q: %w", key, err)
	}
	m, err := res.Result()
	if err != nil {
		return "", fmt.Errorf("redis CONFIG GET %q result: %w", key, err)
	}
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, nil
		}
	}
	return "", fmt.Errorf("redis CONFIG GET %q returned no value", key)
}

func (g *goRedisClient) ConfigSet(ctx context.Context, key, value string) error {
	if err := g.c.ConfigSet(ctx, key, value).Err(); err != nil {
		return fmt.Errorf("redis CONFIG SET %q=%q: %w", key, value, err)
	}
	return nil
}

func (g *goRedisClient) ClusterMyID(ctx context.Context) (string, error) {
	res := g.c.Do(ctx, "CLUSTER", "MYID")
	if err := res.Err(); err != nil {
		return "", fmt.Errorf("redis CLUSTER MYID: %w", err)
	}
	id, err := res.Text()
	if err != nil {
		return "", fmt.Errorf("redis CLUSTER MYID result: %w", err)
	}
	return strings.TrimSpace(id), nil
}

func (g *goRedisClient) ClusterNodes(ctx context.Context) (string, error) {
	res := g.c.ClusterNodes(ctx)
	if err := res.Err(); err != nil {
		return "", fmt.Errorf("redis CLUSTER NODES: %w", err)
	}
	return res.Val(), nil
}

func (g *goRedisClient) ClusterMeet(ctx context.Context, host string, port int) error {
	res := g.c.Do(ctx, "CLUSTER", "MEET", host, port)
	if err := res.Err(); err != nil {
		return fmt.Errorf("redis CLUSTER MEET %s:%d: %w", host, port, err)
	}
	return nil
}

func (g *goRedisClient) ClusterReplicate(ctx context.Context, masterNodeID string) error {
	res := g.c.Do(ctx, "CLUSTER", "REPLICATE", masterNodeID)
	if err := res.Err(); err != nil {
		return fmt.Errorf("redis CLUSTER REPLICATE %s: %w", masterNodeID, err)
	}
	return nil
}

func (g *goRedisClient) ClusterAddSlots(ctx context.Context, slots []int) error {
	if len(slots) == 0 {
		return nil
	}
	args := make([]any, 0, 2+len(slots))
	args = append(args, "CLUSTER", "ADDSLOTS")
	for _, s := range slots {
		args = append(args, s)
	}
	res := g.c.Do(ctx, args...)
	if err := res.Err(); err != nil {
		return fmt.Errorf("redis CLUSTER ADDSLOTS %v: %w", slots, err)
	}
	return nil
}

func (g *goRedisClient) ClusterInfo(ctx context.Context) (string, error) {
	res := g.c.Do(ctx, "CLUSTER", "INFO")
	if err := res.Err(); err != nil {
		return "", fmt.Errorf("redis CLUSTER INFO: %w", err)
	}
	s, err := res.Text()
	if err != nil {
		return "", fmt.Errorf("redis CLUSTER INFO result: %w", err)
	}
	return s, nil
}

func (g *goRedisClient) Close() error { return g.c.Close() }

// splitAddr splits host:port; if port is missing it defaults to 6379.
func splitAddr(addr string) (host, port string) {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[:i], addr[i+1:]
	}
	return addr, "6379"
}

func netAddr(host, port string) string {
	if port == "" {
		port = "6379"
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host + ":" + port
}
