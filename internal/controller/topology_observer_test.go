package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/redis"
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

func TestPickObservationSeedPod_PrefersReadyMaster(t *testing.T) {
	cluster := testCluster()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{Master: api.NodeTopology{Pod: "redis-2"}},
	}}
	pods := []corev1.Pod{
		*vcPod("redis-1", "10.0.0.2", true),
		*vcPod("redis-2", "10.0.0.3", true),
	}

	seed, ok := pickObservationSeedPod(cluster, pods)
	if !ok {
		t.Fatal("expected seed")
	}
	if seed.Name != "redis-2" {
		t.Fatalf("expected master seed redis-2, got %s", seed.Name)
	}
}

func TestPickObservationSeedPod_FallsBackWithoutTopology(t *testing.T) {
	cluster := testCluster()
	pods := []corev1.Pod{
		*vcPod("redis-1", "10.0.0.2", true),
		*vcPod("redis-2", "10.0.0.3", true),
	}

	seed, ok := pickObservationSeedPod(cluster, pods)
	if !ok {
		t.Fatal("expected seed")
	}
	if seed.Name != "redis-1" {
		t.Fatalf("expected fallback seed redis-1, got %s", seed.Name)
	}
}

func TestPickObservationSeedPod_SkipsUnusableMaster(t *testing.T) {
	cluster := testCluster()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{Master: api.NodeTopology{Pod: "redis-0"}},
		{Master: api.NodeTopology{Pod: "redis-2"}},
	}}
	pods := []corev1.Pod{
		*vcPod("redis-1", "10.0.0.2", true),
		*vcPod("redis-0", "", true),
		*vcPod("redis-2", "10.0.0.3", true),
	}

	seed, ok := pickObservationSeedPod(cluster, pods)
	if !ok {
		t.Fatal("expected seed")
	}
	if seed.Name != "redis-2" {
		t.Fatalf("expected second usable master redis-2, got %s", seed.Name)
	}
}

func TestCollectObservedNodes_NoSeedKeepsPodFacts(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := []*corev1.Pod{vcPod("redis-0", "", false)}
	exec := vcExec(t, cluster, pods, nil)

	nodes, err := exec.CollectObservedNodes(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected one observed node, got %#v", nodes)
	}
	if nodes[0].Pod != "redis-0" || !nodes[0].PodExists || nodes[0].RedisSeen || nodes[0].Ready {
		t.Fatalf("unexpected pod fact: %#v", nodes[0])
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

func TestCollectObservedNodes_ClusterNodesErrorFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) { return "", errors.New("nodes err") },
	}
	exec := vcExec(t, cluster, pods, fc)

	nodes, err := exec.CollectObservedNodes(ctx, cluster)
	if err == nil {
		t.Fatalf("expected error, got nodes %#v", nodes)
	}
}

func TestCollectObservedNodes_JoinsPodsRedisAndLastKnownTopology(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{{
		Master:   api.NodeTopology{Pod: "redis-0", NodeID: vcMaster0ID},
		Replicas: []api.NodeTopology{{Pod: "redis-2", NodeID: vcReplica0ID}},
	}}}
	pods := []*corev1.Pod{
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
	}
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) { return clusterOK(), nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	nodes, err := exec.CollectObservedNodes(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("expected 4 observed nodes, got %#v", nodes)
	}
	if nodes[0].Pod != "redis-0" || !nodes[0].PodExists || !nodes[0].RedisSeen || nodes[0].NodeID != vcMaster0ID || nodes[0].Slots != "0-8191" {
		t.Fatalf("unexpected redis-0 fact: %#v", nodes[0])
	}
	if nodes[1].Pod != "redis-1" || nodes[1].NodeID != vcMaster1ID || nodes[1].Role != "master" {
		t.Fatalf("unexpected redis-1 fact: %#v", nodes[1])
	}
	if nodes[2].Pod != "redis-2" || nodes[2].PodExists || !nodes[2].RedisSeen || nodes[2].NodeID != vcReplica0ID || nodes[2].MasterPod != "redis-0" {
		t.Fatalf("unexpected redis-only fact with last known pod: %#v", nodes[2])
	}
	if nodes[3].MasterPod != "redis-1" {
		t.Fatalf("expected live join to set replica master pod, got %#v", nodes[3])
	}
}

func TestCollectObservedNodes_UsesMostCompleteSeedSnapshot(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{Master: api.NodeTopology{Pod: "redis-14"}},
	}}
	pods := []*corev1.Pod{
		vcPod("redis-14", "10.0.0.14", true),
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
		vcPod("redis-3", "10.0.0.4", true),
	}
	exec := vcExec(t, cluster, pods, nil)
	exec.RedisFactory = func(addr string) (redis.Client, error) {
		if addr == "10.0.0.14:6379" {
			return &fakeRedisClient{clusterNodes: func() (string, error) {
				return "new 10.0.0.14:6379@16379 master - 0 0 1 connected\n" +
					vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 2 connected 0-8191\n", nil
			}}, nil
		}
		return &fakeRedisClient{clusterNodes: func() (string, error) { return clusterOK(), nil }}, nil
	}

	nodes, err := exec.CollectObservedNodes(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 5 {
		t.Fatalf("expected complete Redis snapshot plus replacement pod, got %#v", nodes)
	}
	if nodes[0].Pod != "redis-0" || nodes[0].NodeID != vcMaster0ID || nodes[0].Slots != "0-8191" {
		t.Fatalf("expected complete seed facts for redis-0, got %#v", nodes[0])
	}
	if nodes[4].Pod != "redis-14" || nodes[4].RedisSeen {
		t.Fatalf("expected redis-14 to remain pod-only, got %#v", nodes[4])
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
