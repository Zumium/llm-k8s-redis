package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func waitReadyPod(ip string, ready bool) *corev1.Pod {
	pod := desiredPod(testCluster(), "example", "redis-0", "redis:7.2", "2Gi", twoGiBytes)
	pod.Status.PodIP = ip
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}
	} else {
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionFalse,
		}}
	}
	return pod
}

func withOwnerRef(t *testing.T, cluster *api.RedisCluster, pod *corev1.Pod) {
	t.Helper()
	if err := ctrl.SetControllerReference(cluster, pod, newExecutorScheme(t)); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
}

// buildCreatePlan returns a plan with one EnsureNode + one WaitNodeReady step.
func buildCreatePlan() *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "p1",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
		},
	}
}

func TestWaitNodeReady_ReadyAndNodeIdReturnsCompleted(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.10", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{clusterMyID: func() (string, error) { return "abc123", nil }}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_RefreshesExistingTopologyNode(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{{
		ID:     "shard-0",
		Master: api.NodeTopology{Pod: "redis-0"},
	}}}
	pod := waitReadyPod("10.0.0.11", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{clusterMyID: func() (string, error) { return "deadbeef", nil }}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	if _, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := cluster.Status.Topology.Shards[0].Master
	if got.NodeID != "deadbeef" || !got.Ready {
		t.Errorf("expected master node refreshed to nodeId=deadbeef ready=true, got %+v", got)
	}
}

func TestWaitNodeReady_NoTopologyEntryStillCompleted(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.12", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{clusterMyID: func() (string, error) { return "ff", nil }}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed even without topology entry, got %q: %s", outcome.Status, outcome.Message)
	}
	if cluster.Status.Topology != nil {
		t.Errorf("expected topology untouched, got %+v", cluster.Status.Topology)
	}
}

func TestWaitNodeReady_PodNotReadyReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.13", false)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_NoIPReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when no IP, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_RedisPingFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.14", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{pingErr: errPing}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on ping fail, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_ClusterMyIDErrorReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.15", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{clusterMyID: func() (string, error) { return "", errors.New("cluster disabled") }}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when CLUSTER MYID fails, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_EmptyNodeIDReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.16", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{clusterMyID: func() (string, error) { return "  ", nil }}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when node id empty, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_PodNotFoundFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	fc := &fakeRedisClient{}
	exec := newExecutor(t, []client.Object{cluster}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, buildCreatePlan(), 1)
	if err == nil {
		t.Fatal("expected error when pod missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed when pod missing, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_NoPrecedingEnsureNodeFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.17", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{clusterMyID: func() (string, error) { return "id", nil }}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "p2",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
		},
	}
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 0)
	if err == nil {
		t.Fatal("expected error when no preceding EnsureNode")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed without EnsureNode, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_NamespaceMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	pod := waitReadyPod("10.0.0.18", true)
	withOwnerRef(t, cluster, pod)
	fc := &fakeRedisClient{}
	exec := newExecutor(t, []client.Object{cluster, pod}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "p3",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-wrong", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "wrong", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-wrong", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "wrong", "pod": "redis-0"}},
		},
	}
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 1)
	if err == nil {
		t.Fatal("expected error on namespace mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on namespace mismatch, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestWaitNodeReady_MissingPodParamFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	fc := &fakeRedisClient{}
	exec := newExecutor(t, []client.Object{cluster}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "p4",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example"}},
		},
	}
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 1)
	if err == nil {
		t.Fatal("expected error on missing pod param")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on missing pod param, got %q: %s", outcome.Status, outcome.Message)
	}
}

// Ensure _ keeps client.Object imported for the test helpers above.
var _ client.Object = &corev1.Pod{}
var _ = metav1.Now
