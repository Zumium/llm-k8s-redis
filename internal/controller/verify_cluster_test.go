package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

const (
	vcMaster0ID  = "aaa000"
	vcMaster1ID  = "bbb111"
	vcReplica0ID = "ccc222"
	vcReplica1ID = "ddd333"
)

func verifyParams() map[string]any {
	return map[string]any{
		"expectedShards":                   2,
		"expectedReplicasPerShard":         1,
		"requireClusterStateOk":            true,
		"requireFullSlotCoverage":          true,
		"requireAllSlotOwnersHaveReplicas": true,
	}
}

func verifyStep() plan.Step {
	return plan.Step{ID: "verify", Action: plan.ActionVerifyCluster, Params: verifyParams()}
}

// clusterOK returns CLUSTER NODES for a healthy 2-shard, 1-replica cluster
// with full slot coverage and every node mapped to a managed pod IP.
func clusterOK() string {
	return vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 3 connected\n" +
		vcReplica1ID + " 10.0.0.4:6379@16379 slave " + vcMaster1ID + " 0 0 4 connected\n"
}

func vcPod(name, ip string, ready bool) *corev1.Pod {
	pod := desiredPod(testCluster(), "example", name, "redis:7.2", "2Gi", twoGiBytes)
	pod.Status.PodIP = ip
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	} else {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	}
	return pod
}

func vcExec(t *testing.T, cluster *api.RedisCluster, pods []*corev1.Pod, fc *fakeRedisClient) *ActionExecutor {
	t.Helper()
	objs := []client.Object{cluster}
	for _, p := range pods {
		if err := ctrl.SetControllerReference(cluster, p, newExecutorScheme(t)); err != nil {
			t.Fatalf("set owner ref: %v", err)
		}
		objs = append(objs, p)
	}
	return newExecutor(t, objs, fc)
}

func vcFourReadyPods() []*corev1.Pod {
	return []*corev1.Pod{
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
		vcPod("redis-3", "10.0.0.4", true),
	}
}

func TestVerifyCluster_HappyPathReturnsCompletedAndRebuildsTopology(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) { return clusterOK(), nil },
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if cluster.Status.Topology == nil || len(cluster.Status.Topology.Shards) != 2 {
		t.Fatalf("expected 2 shards in rebuilt topology, got %+v", cluster.Status.Topology)
	}
	shard0 := cluster.Status.Topology.Shards[0]
	if shard0.ID != "shard-0" {
		t.Errorf("expected shard-0 first, got %q", shard0.ID)
	}
	if shard0.Master.NodeID != vcMaster0ID || shard0.Master.Pod != "redis-0" || shard0.Master.Slots != "0-8191" || !shard0.Master.Ready {
		t.Errorf("unexpected shard-0 master: %+v", shard0.Master)
	}
	if len(shard0.Replicas) != 1 || shard0.Replicas[0].NodeID != vcReplica0ID || shard0.Replicas[0].Pod != "redis-2" || !shard0.Replicas[0].Ready {
		t.Errorf("unexpected shard-0 replicas: %+v", shard0.Replicas)
	}
	shard1 := cluster.Status.Topology.Shards[1]
	if shard1.Master.NodeID != vcMaster1ID || shard1.Master.Pod != "redis-1" || shard1.Master.Slots != "8192-16383" {
		t.Errorf("unexpected shard-1 master: %+v", shard1.Master)
	}
	if len(shard1.Replicas) != 1 || shard1.Replicas[0].Pod != "redis-3" {
		t.Errorf("unexpected shard-1 replicas: %+v", shard1.Replicas)
	}
	healthy := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == ConditionHealthy && c.Status == metav1.ConditionTrue {
			healthy = true
		}
	}
	if !healthy {
		t.Error("expected Healthy=True condition after successful verify")
	}
}

func TestVerifyCluster_ExpectedShardsMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, pods, fc)

	step := verifyStep()
	step.Params["expectedShards"] = 3
	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
	if err == nil {
		t.Fatal("expected error on expectedShards mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_ExpectedReplicasMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, pods, fc)

	step := verifyStep()
	step.Params["expectedReplicasPerShard"] = 2
	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
	if err == nil {
		t.Fatal("expected error on expectedReplicas mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_RequireFlagFalseFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, pods, fc)

	for _, key := range []string{"requireClusterStateOk", "requireFullSlotCoverage", "requireAllSlotOwnersHaveReplicas"} {
		step := verifyStep()
		step.Params[key] = false
		outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
		if err == nil {
			t.Fatalf("expected error when %s=false", key)
		}
		if outcome.Status != plan.StepStateFailed {
			t.Fatalf("expected Failed when %s=false, got %q: %s", key, outcome.Status, outcome.Message)
		}
	}
}

func TestVerifyCluster_MissingExpectedShardsFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, pods, fc)

	step := verifyStep()
	delete(step.Params, "expectedShards")
	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
	if err == nil {
		t.Fatal("expected error on missing expectedShards")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_NonIntExpectedShardsFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, pods, fc)

	step := verifyStep()
	step.Params["expectedShards"] = "two"
	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
	if err == nil {
		t.Fatal("expected error on non-int expectedShards")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_FloatNonIntegerShardsFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, pods, fc)

	step := verifyStep()
	step.Params["expectedShards"] = 2.5
	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
	if err == nil {
		t.Fatal("expected error on non-integer float expectedShards")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_NoReadySeedReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := []*corev1.Pod{
		vcPod("redis-0", "10.0.0.1", false),
		vcPod("redis-1", "10.0.0.2", false),
		vcPod("redis-2", "10.0.0.3", false),
		vcPod("redis-3", "10.0.0.4", false),
	}
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when no ready seed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_NoManagedPodsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	fc := &fakeRedisClient{}
	exec := vcExec(t, cluster, nil, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when no managed pods, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_PingFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{pingErr: errPing}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on ping fail, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_ClusterInfoFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterInfo: func() (string, error) { return "", errors.New("info err") },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on CLUSTER INFO fail, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_ClusterStateNotOkReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterInfo: func() (string, error) { return "cluster_state:fail\r\n", nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_ClusterNodesFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return "", errors.New("nodes err") },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on CLUSTER NODES fail, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_MasterCountMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 2 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error on master count mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_ReplicaCountMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 3 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error on replica count mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_SlotCoverageGapFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8190\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 3 connected\n" +
		vcReplica1ID + " 10.0.0.4:6379@16379 slave " + vcMaster1ID + " 0 0 4 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error on slot coverage gap")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_MigratingSlotFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191 [100->-" + vcMaster1ID + "]\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 3 connected\n" +
		vcReplica1ID + " 10.0.0.4:6379@16379 slave " + vcMaster1ID + " 0 0 4 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error on migrating slot")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_SlotOwnerNotMasterFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16382\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 3 connected 16383\n" +
		vcReplica1ID + " 10.0.0.4:6379@16379 slave " + vcMaster1ID + " 0 0 4 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error when a replica owns a slot")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_FailedNodeFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master,fail - 0 0 1 connected 0-8191\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 3 connected\n" +
		vcReplica1ID + " 10.0.0.4:6379@16379 slave " + vcMaster1ID + " 0 0 4 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error when a node is failed")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_ReplicaFailedFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave,fail " + vcMaster0ID + " 0 0 3 connected\n" +
		vcReplica1ID + " 10.0.0.4:6379@16379 slave " + vcMaster1ID + " 0 0 4 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error when a replica is failed (replica count drops)")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_UnmappedRedisNodeFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := []*corev1.Pod{
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
	}
	nodes := vcMaster0ID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
		vcMaster1ID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n" +
		vcReplica0ID + " 10.0.0.3:6379@16379 slave " + vcMaster0ID + " 0 0 3 connected\n" +
		vcReplica1ID + " 10.0.0.9:6379@16379 slave " + vcMaster1ID + " 0 0 4 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err == nil {
		t.Fatal("expected error when a redis node is not a managed pod")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_HandshakeNodeIgnored(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	nodes := clusterOK() + "eee999 10.0.0.99:6379@16379 handshake - 0 0 0 connected\n"
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return nodes, nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{verifyStep()}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed with handshake node ignored, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestVerifyCluster_NonIntegerFloatShardsAccepted(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pods := vcFourReadyPods()
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\n", nil },
		clusterNodes: func() (string, error) { return clusterOK(), nil },
	}
	exec := vcExec(t, cluster, pods, fc)

	step := verifyStep()
	step.Params["expectedShards"] = float64(2)
	step.Params["expectedReplicasPerShard"] = float64(1)
	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed with integer-valued float params, got %q: %s", outcome.Status, outcome.Message)
	}
}

var _ client.Object = &corev1.Pod{}
