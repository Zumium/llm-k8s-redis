package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

const (
	migrateSourceID = "src111"
	migrateTargetID = "tgt222"
	migrateReplica1 = "rep111"
	migrateReplica2 = "rep222"
)

func migratePlan() *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "migrate",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "migrate", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2", "slots": "0"}},
		},
	}
}

func migrateExec(t *testing.T, cluster *api.RedisCluster, fc *fakeRedisClient) *ActionExecutor {
	t.Helper()
	pods := []*corev1.Pod{
		readyPodWithIP(cluster, "redis-0", "10.0.0.1"),
		readyPodWithIP(cluster, "redis-1", "10.0.0.2"),
		readyPodWithIP(cluster, "redis-2", "10.0.0.3"),
		readyPodWithIP(cluster, "redis-3", "10.0.0.4"),
	}
	objs := []client.Object{cluster}
	for _, p := range pods {
		if err := ctrl.SetControllerReference(cluster, p, newExecutorScheme(t)); err != nil {
			t.Fatalf("set owner ref: %v", err)
		}
		objs = append(objs, p)
	}
	return newExecutor(t, objs, fc)
}

func migrateTopology(cluster *api.RedisCluster) {
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", Slots: "0-8191", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", Ready: true}}},
		{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-2", Slots: "8192-16383", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-3", Ready: true}}},
	}}
}

func migrateClusterNodes(sourceSlots, targetSlots string) string {
	return migrateSourceID + " 10.0.0.1:6379@16379 master - 0 0 1 connected " + sourceSlots + "\n" +
		migrateReplica1 + " 10.0.0.2:6379@16379 slave " + migrateSourceID + " 0 0 2 connected\n" +
		migrateTargetID + " 10.0.0.3:6379@16379 master - 0 0 3 connected " + targetSlots + "\n" +
		migrateReplica2 + " 10.0.0.4:6379@16379 slave " + migrateTargetID + " 0 0 4 connected\n"
}

func TestMigrateSlots_AlreadyOwnedByTargetCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return migrateClusterNodes("1-8191", "0 8192-16383"), nil
		},
	}
	exec := migrateExec(t, cluster, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, migratePlan(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.setSlotCalls) != 0 || len(fc.migrateCalls) != 0 {
		t.Fatalf("expected no redis mutations, got setslot=%v migrate=%v", fc.setSlotCalls, fc.migrateCalls)
	}
}

func TestMigrateSlots_EmptySlotSwitchesOwner(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls == 1 {
				return migrateClusterNodes("0-8191", "8192-16383"), nil
			}
			return migrateClusterNodes("1-8191", "0 8192-16383"), nil
		},
	}
	exec := migrateExec(t, cluster, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, migratePlan(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	want := []setSlotCall{
		{mode: "IMPORTING", slot: 0, nodeID: migrateSourceID},
		{mode: "MIGRATING", slot: 0, nodeID: migrateTargetID},
		{mode: "NODE", slot: 0, nodeID: migrateTargetID},
		{mode: "NODE", slot: 0, nodeID: migrateTargetID},
	}
	if len(fc.setSlotCalls) != len(want) {
		t.Fatalf("expected %d setslot calls, got %d: %v", len(want), len(fc.setSlotCalls), fc.setSlotCalls)
	}
	for i := range want {
		if fc.setSlotCalls[i] != want[i] {
			t.Fatalf("setslot call %d = %+v, want %+v", i, fc.setSlotCalls[i], want[i])
		}
	}
}

func TestMigrateSlots_MigratesKeysAndReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return migrateClusterNodes("0-8191", "8192-16383"), nil
		},
		getKeysInSlot: func(slot, count int) ([]string, error) {
			return []string{"key-a"}, nil
		},
	}
	exec := migrateExec(t, cluster, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, migratePlan(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.migrateCalls) != 1 {
		t.Fatalf("expected one migrate call, got %v", fc.migrateCalls)
	}
	call := fc.migrateCalls[0]
	if call.host != "10.0.0.3" || call.port != 6379 || len(call.keys) != 1 || call.keys[0] != "key-a" {
		t.Fatalf("unexpected migrate call: %+v", call)
	}
}

func TestMigrateSlots_ThirdPartyOwnerFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return "other 10.0.0.9:6379@16379 master - 0 0 9 connected 0\n" +
				migrateClusterNodes("1-8191", "8192-16383"), nil
		},
	}
	exec := migrateExec(t, cluster, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, migratePlan(), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}
