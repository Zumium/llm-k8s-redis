package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/redis"
)

func deleteStep(pod string) plan.Step {
	return plan.Step{ID: "delete", Action: plan.ActionDeleteNode, Params: map[string]any{"namespace": "example", "pod": pod}}
}

func deletePlan(pod string) *plan.Plan {
	return &plan.Plan{Steps: []plan.Step{deleteStep(pod)}}
}

func deleteNodesOK() string {
	return "master 10.0.0.1:6379@16379 master - 0 0 1 connected 0-16383\n" +
		"replica 10.0.0.2:6379@16379 slave master 0 0 2 connected\n"
}

func TestDeleteNode_PodAlreadyMissingCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := vcExec(t, cluster, nil, &fakeRedisClient{})

	outcome, err := exec.ExecuteStep(ctx, cluster, deletePlan("redis-9"), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestDeleteNode_NeverJoinedPodDeletes(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	seed := vcPod("redis-0", "10.0.0.1", true)
	replica := vcPod("redis-1", "10.0.0.2", true)
	target := vcPod("redis-4", "10.0.0.4", true)
	fc := &fakeRedisClient{clusterNodes: func() (string, error) { return deleteNodesOK(), nil }}
	exec := vcExec(t, cluster, []*corev1.Pod{seed, replica, target}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, deletePlan("redis-4"), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
	var got corev1.Pod
	if err := exec.Get(ctx, client.ObjectKey{Namespace: "example", Name: "redis-4"}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod deleted, got err=%v pod=%+v", err, got)
	}
	outcome, err = exec.ExecuteStep(ctx, cluster, deletePlan("redis-4"), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestDeleteNode_ForgottenPodDeletes(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{{
		ID:     "shard-0",
		Master: api.NodeTopology{Pod: "redis-0", NodeID: "master", Slots: "0-16383", Ready: true},
		Replicas: []api.NodeTopology{
			{Pod: "redis-1", NodeID: "replica", Ready: true},
			{Pod: "redis-4", NodeID: "old-replica", Ready: false},
		},
	}}}
	seed := vcPod("redis-0", "10.0.0.1", true)
	replica := vcPod("redis-1", "10.0.0.2", true)
	target := vcPod("redis-4", "10.0.0.4", true)
	fc := &fakeRedisClient{clusterNodes: func() (string, error) { return deleteNodesOK(), nil }}
	exec := vcExec(t, cluster, []*corev1.Pod{seed, replica, target}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, deletePlan("redis-4"), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
	var got corev1.Pod
	if err := exec.Get(ctx, client.ObjectKey{Namespace: "example", Name: "redis-4"}, &got); !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod deleted, got err=%v pod=%+v", err, got)
	}
	outcome, err = exec.ExecuteStep(ctx, cluster, deletePlan("redis-4"), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestDeleteNode_ActiveMemberFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	seed := vcPod("redis-0", "10.0.0.1", true)
	replica := vcPod("redis-1", "10.0.0.2", true)
	fc := &fakeRedisClient{clusterNodes: func() (string, error) { return deleteNodesOK(), nil }}
	exec := vcExec(t, cluster, []*corev1.Pod{seed, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, deletePlan("redis-1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
	var got corev1.Pod
	if err := exec.Get(ctx, client.ObjectKey{Namespace: "example", Name: "redis-1"}, &got); err != nil {
		t.Fatalf("expected pod to remain: %v", err)
	}
}

func TestDeleteNode_RejectsMasterWithoutReplica(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	seed := vcPod("redis-0", "10.0.0.1", true)
	target := vcPod("redis-4", "10.0.0.4", true)
	fc := &fakeRedisClient{clusterNodes: func() (string, error) {
		return "master 10.0.0.1:6379@16379 master - 0 0 1 connected 0-16383\n", nil
	}}
	exec := vcExec(t, cluster, []*corev1.Pod{seed, target}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, deletePlan("redis-4"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestDeleteNode_DoesNotUseTargetAsSeed(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	other := vcPod("redis-0", "10.0.0.1", true)
	target := vcPod("redis-4", "10.0.0.4", true)
	fc := &fakeRedisClient{clusterNodes: func() (string, error) { return deleteNodesOK(), nil }}
	exec := vcExec(t, cluster, []*corev1.Pod{target, other}, fc)
	var addr string
	exec.RedisFactory = func(a string) (redis.Client, error) {
		addr = a
		return fc, nil
	}

	outcome, err := exec.ExecuteStep(ctx, cluster, deletePlan("redis-4"), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
	if addr != "10.0.0.1:6379" {
		t.Fatalf("expected non-target seed, got %q", addr)
	}
}

func TestDeleteNode_RejectsUnmanagedPod(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	seed := vcPod("redis-0", "10.0.0.1", true)
	target := vcPod("redis-4", "10.0.0.4", true)
	target.Labels[labelManagedBy] = "someone-else"
	exec := vcExec(t, cluster, []*corev1.Pod{seed, target}, &fakeRedisClient{})

	outcome, err := exec.ExecuteStep(ctx, cluster, deletePlan("redis-4"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
	var got corev1.Pod
	if err := exec.Get(ctx, client.ObjectKey{Namespace: "example", Name: "redis-4"}, &got); err != nil {
		t.Fatalf("expected pod to remain: %v", err)
	}
}

func TestDeleteNode_ParamValidationFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := vcExec(t, cluster, nil, &fakeRedisClient{})
	tests := []plan.Step{
		{ID: "delete", Action: plan.ActionDeleteNode, Params: map[string]any{"pod": "redis-4"}},
		{ID: "delete", Action: plan.ActionDeleteNode, Params: map[string]any{"namespace": "example"}},
		{ID: "delete", Action: plan.ActionDeleteNode, Params: map[string]any{"namespace": "other", "pod": "redis-4"}},
	}
	for _, step := range tests {
		outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
		if err == nil {
			t.Fatalf("expected error for params %#v", step.Params)
		}
		if outcome.Status != plan.StepStateFailed {
			t.Fatalf("expected Failed for params %#v, got %q: %s", step.Params, outcome.Status, outcome.Message)
		}
	}
}
