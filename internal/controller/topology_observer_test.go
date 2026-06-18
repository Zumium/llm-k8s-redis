package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/redis"
)

func TestObserveTopology_HappyPath(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) { return clusterOK(), nil },
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	obs, err := exec.observeTopology(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.healthy {
		t.Fatalf("expected healthy observation, got message: %s", obs.message)
	}
	if obs.topology == nil || len(obs.topology.Shards) != 2 {
		t.Fatalf("expected 2 shards, got %+v", obs.topology)
	}
	if obs.topology.Shards[0].Master.Pod != "redis-0" {
		t.Errorf("unexpected master pod: %s", obs.topology.Shards[0].Master.Pod)
	}
}

func TestObserveTopology_NoPods(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := vcExec(t, cluster, nil, nil)

	obs, err := exec.observeTopology(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.healthy {
		t.Fatal("expected unhealthy when no pods")
	}
	if obs.message == "" {
		t.Fatal("expected a reason message")
	}
}

func TestObserveTopology_NoSeed(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := []*corev1.Pod{vcPod("redis-0", "", false)}
	exec := vcExec(t, cluster, pods, nil)

	obs, err := exec.observeTopology(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.healthy {
		t.Fatal("expected unhealthy when no ready seed")
	}
}

func TestObserveTopology_PingFail(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{pingErr: errors.New("no route to host")}
	exec := vcExec(t, cluster, pods, fc)

	obs, err := exec.observeTopology(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.healthy {
		t.Fatal("expected unhealthy when ping fails")
	}
}

func TestObserveTopology_ClusterStateNotOk(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterInfo: func() (string, error) { return "cluster_state:fail\r\n", nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	obs, err := exec.observeTopology(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.healthy {
		t.Fatal("expected unhealthy when cluster_state not ok")
	}
}

func TestObserveTopology_ClusterNodesError(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return "", errors.New("nodes err") },
	}
	exec := vcExec(t, cluster, pods, fc)

	obs, err := exec.observeTopology(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obs.healthy {
		t.Fatal("expected unhealthy when CLUSTER NODES fails")
	}
}

func TestObserveTopology_FactoryError(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	exec := vcExec(t, cluster, pods, nil)
	exec.RedisFactory = func(string) (redis.Client, error) { return nil, errors.New("factory err") }

	_, err := exec.observeTopology(ctx, cluster)
	if err == nil {
		t.Fatal("expected error from factory failure")
	}
}

func TestObserveTopology_Interface_HealthySetsCondition(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) { return clusterOK(), nil },
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	if err := exec.ObserveTopology(ctx, cluster); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cluster.Status.Topology == nil {
		t.Fatal("expected topology set")
	}
	if cluster.Status.TopologyObservedAt.IsZero() {
		t.Fatal("expected TopologyObservedAt set")
	}
	healthy := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == ConditionHealthy && c.Status == metav1.ConditionTrue {
			healthy = true
		}
	}
	if !healthy {
		t.Fatalf("expected Healthy=True, got conditions %+v", cluster.Status.Conditions)
	}
}

func TestObserveTopology_Interface_UnhealthyKeepsOldTopology(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	old := &api.ClusterTopology{Shards: []api.ShardTopology{{ID: "shard-0"}}}
	cluster.Status.Topology = old
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{pingErr: errors.New("down")}
	exec := vcExec(t, cluster, pods, fc)

	if err := exec.ObserveTopology(ctx, cluster); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cluster.Status.Topology != old {
		t.Fatal("expected old topology preserved on transient failure")
	}
	if cluster.Status.TopologyObservedAt.IsZero() {
		t.Fatal("expected TopologyObservedAt still updated")
	}
	healthy := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == ConditionHealthy && c.Status == metav1.ConditionFalse {
			healthy = true
		}
	}
	if !healthy {
		t.Fatalf("expected Healthy=False, got conditions %+v", cluster.Status.Conditions)
	}
}

func TestObserveTopology_PodMissingFromK8s(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := []*corev1.Pod{
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
		vcPod("redis-3", "10.0.0.4", true),
	}
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) { return clusterOK(), nil },
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	obs, err := exec.observeTopology(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !obs.healthy {
		t.Fatalf("expected healthy observation, got message: %s", obs.message)
	}
	if obs.topology == nil || len(obs.topology.Shards) != 2 {
		t.Fatalf("expected 2 shards, got %+v", obs.topology)
	}
	m0 := obs.topology.Shards[0].Master
	if m0.Pod != "" {
		t.Errorf("expected empty Pod for missing redis-0, got %q", m0.Pod)
	}
	if m0.Ready {
		t.Error("expected Ready=false for missing redis-0")
	}
	if m0.NodeID != vcMaster0ID || m0.Slots != "0-8191" {
		t.Errorf("expected NodeID/Slots preserved from Redis, got %+v", m0)
	}
	m1 := obs.topology.Shards[1].Master
	if m1.Pod != "redis-1" || !m1.Ready {
		t.Errorf("expected redis-1 master intact, got %+v", m1)
	}
}
