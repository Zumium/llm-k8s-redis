package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func TestForgetNode_LastKnownNodeIDCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{{
		ID:     "shard-0",
		Master: api.NodeTopology{Pod: "redis-0", NodeID: "master", Slots: "0-16383", Ready: true},
		Replicas: []api.NodeTopology{
			{Pod: "redis-1", NodeID: "old-replica", Ready: false},
		},
	}}}
	pod := vcPod("redis-0", "10.0.0.1", true)
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}, pod).Build()
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls == 1 {
				return "master 10.0.0.1:6379@16379 master - 0 0 1 connected 0-16383\nold-replica 10.0.0.9:6379@16379 slave master 0 0 1 connected\n", nil
			}
			return "master 10.0.0.1:6379@16379 master - 0 0 1 connected 0-16383\n", nil
		},
	}
	exec := &ActionExecutor{Client: cl, Scheme: newScheme(t), RedisFactory: fakeFactory(fc)}
	step := plan.Step{ID: "forget", Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "lastKnownNodeId": "old-replica"}}
	outcome, err := exec.forgetNode(ctx, cluster, step)
	if err != nil {
		t.Fatalf("forgetNode: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected completed, got %s: %s", outcome.Status, outcome.Message)
	}
	if len(fc.forgetCalls) != 1 || fc.forgetCalls[0] != "old-replica" {
		t.Fatalf("unexpected forget calls: %#v", fc.forgetCalls)
	}
	var got corev1.Pod
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "example", Name: "redis-0"}, &got); err != nil {
		t.Fatalf("expected peer pod to remain: %v", err)
	}
}

func TestForgetNode_PodAbsentFromClusterNodesCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := clusterWithTopology()
	seed := vcPod("redis-0", "10.0.0.1", true)
	target := vcPod("redis-8", "10.0.0.8", true)
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}, seed, target).Build()
	fc := &fakeRedisClient{clusterNodes: func() (string, error) {
		return "master 10.0.0.1:6379@16379 master - 0 0 1 connected 0-16383\n" +
			"replica 10.0.0.2:6379@16379 slave master 0 0 1 connected\n", nil
	}}
	exec := &ActionExecutor{Client: cl, Scheme: newScheme(t), RedisFactory: fakeFactory(fc)}
	step := plan.Step{ID: "forget", Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-8"}}

	outcome, err := exec.forgetNode(ctx, cluster, step)
	if err != nil {
		t.Fatalf("forgetNode: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected completed, got %s: %s", outcome.Status, outcome.Message)
	}
	if len(fc.forgetCalls) != 0 {
		t.Fatalf("unexpected forget calls: %#v", fc.forgetCalls)
	}
}

func TestForgetNode_RejectsSlotOwner(t *testing.T) {
	ctx := context.Background()
	cluster := clusterWithTopology()
	pod := vcPod("redis-0", "10.0.0.1", true)
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}, pod).Build()
	fc := &fakeRedisClient{clusterNodes: func() (string, error) {
		return "master 10.0.0.1:6379@16379 master - 0 0 1 connected\nold-master 10.0.0.9:6379@16379 master - 0 0 1 connected 0-10\n", nil
	}}
	exec := &ActionExecutor{Client: cl, Scheme: newScheme(t), RedisFactory: fakeFactory(fc)}
	step := plan.Step{ID: "forget", Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "lastKnownNodeId": "old-master"}}
	outcome, err := exec.forgetNode(ctx, cluster, step)
	if err == nil {
		t.Fatal("expected error")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected failed, got %s", outcome.Status)
	}
}
