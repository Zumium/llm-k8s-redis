package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/redis"
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

func TestMigrateSlots_WaitsForTargetMasterView(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	fc := &fakeRedisClient{
		clusterNodesAddr: func(addr string) (string, error) {
			if addr == "10.0.0.3:6379" {
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
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.setSlotCalls) != 0 || len(fc.migrateCalls) != 0 {
		t.Fatalf("expected no redis mutations, got setslot=%v migrate=%v", fc.setSlotCalls, fc.migrateCalls)
	}
}

func TestMigrateSlots_WaitsForThirdMasterView(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	fc := &fakeRedisClient{
		clusterNodesAddr: func(addr string) (string, error) {
			base := migrateClusterNodes("1-8191", "0 8192-16383")
			third := "third333 10.0.0.5:6379@16379 master - 0 0 5 connected\n"
			if addr == "10.0.0.5:6379" {
				return migrateClusterNodes("0-8191", "8192-16383") + third, nil
			}
			return base + third, nil
		},
	}
	exec := migrateExec(t, cluster, fc)
	thirdPod := readyPodWithIP(cluster, "redis-4", "10.0.0.5")
	if err := ctrl.SetControllerReference(cluster, thirdPod, exec.Scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	if err := exec.Create(ctx, thirdPod); err != nil {
		t.Fatalf("create third pod: %v", err)
	}

	outcome, err := exec.ExecuteStep(ctx, cluster, migratePlan(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMigrateSlots_UsesLiveManagedOwnerWhenPlanSourceIsStale(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	thirdID := "third333"
	before := thirdID + " 10.0.0.5:6379@16379 master - 0 0 5 connected 0\n" +
		migrateClusterNodes("1-8191", "8192-16383")
	after := thirdID + " 10.0.0.5:6379@16379 master - 0 0 5 connected\n" +
		migrateClusterNodes("1-8191", "0 8192-16383")
	fc := &fakeRedisClient{}
	exec := migrateExec(t, cluster, fc)
	exec.RedisFactory = func(addr string) (redis.Client, error) {
		return &addrFakeRedisClient{fakeRedisClient: fc, addr: addr}, nil
	}
	thirdPod := readyPodWithIP(cluster, "redis-4", "10.0.0.5")
	if err := ctrl.SetControllerReference(cluster, thirdPod, exec.Scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	if err := exec.Create(ctx, thirdPod); err != nil {
		t.Fatalf("create third pod: %v", err)
	}
	fc.clusterNodesAddr = func(string) (string, error) {
		fc.mu.Lock()
		calls := len(fc.setSlotCalls)
		fc.mu.Unlock()
		if calls >= 4 {
			return after, nil
		}
		return before, nil
	}

	outcome, err := exec.ExecuteStep(ctx, cluster, migratePlan(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	want := []setSlotCall{
		{mode: "IMPORTING", slot: 0, nodeID: thirdID},
		{mode: "MIGRATING", slot: 0, nodeID: migrateTargetID},
		{addr: "10.0.0.5:6379", mode: "NODE", slot: 0, nodeID: migrateTargetID},
		{addr: "10.0.0.3:6379", mode: "NODE", slot: 0, nodeID: migrateTargetID},
		{addr: "10.0.0.1:6379", mode: "NODE", slot: 0, nodeID: migrateTargetID},
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
		{addr: "10.0.0.1:6379", mode: "NODE", slot: 0, nodeID: migrateTargetID},
		{addr: "10.0.0.3:6379", mode: "NODE", slot: 0, nodeID: migrateTargetID},
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

func TestMigrateSlots_UnownedSlotAddsToTarget(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls <= 2 {
				return migrateClusterNodes("1-8191", "8192-16383"), nil
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
	if len(fc.addSlotsCalls) != 1 || len(fc.addSlotsCalls[0]) != 1 || fc.addSlotsCalls[0][0] != 0 {
		t.Fatalf("expected one ADDSLOTS call for slot 0, got %v", fc.addSlotsCalls)
	}
	if len(fc.setSlotCalls) != 0 || len(fc.migrateCalls) != 0 {
		t.Fatalf("expected no migration commands, got setslot=%v migrate=%v", fc.setSlotCalls, fc.migrateCalls)
	}
}

func TestMigrateSlots_DoesNotSetSlotNodeOnStaleMaster(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			staleMaster := "stale 10.0.0.9:6379@16379 master - 0 0 9 connected\n"
			if calls == 1 {
				return migrateClusterNodes("0-8191", "8192-16383") + staleMaster, nil
			}
			return migrateClusterNodes("1-8191", "0 8192-16383") + staleMaster, nil
		},
	}
	exec := migrateExec(t, cluster, fc)
	exec.RedisFactory = func(addr string) (redis.Client, error) {
		if addr == "10.0.0.9:6379" {
			return nil, errors.New("stale master should not be contacted")
		}
		return &addrFakeRedisClient{fakeRedisClient: fc, addr: addr}, nil
	}

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
		{addr: "10.0.0.1:6379", mode: "NODE", slot: 0, nodeID: migrateTargetID},
		{addr: "10.0.0.3:6379", mode: "NODE", slot: 0, nodeID: migrateTargetID},
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

func TestMigrateSlots_ProcessesMultipleEmptySlotsInOneReconcile(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	p := migratePlan()
	p.Steps[0].Params["slots"] = "0-2"
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls == 1 {
				return migrateClusterNodes("0-8191", "8192-16383"), nil
			}
			return migrateClusterNodes("3-8191", "0-2 8192-16383"), nil
		},
	}
	exec := migrateExec(t, cluster, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, p, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.setSlotCalls) != 12 {
		t.Fatalf("expected 12 setslot calls, got %d: %v", len(fc.setSlotCalls), fc.setSlotCalls)
	}
	seen := map[int]map[string]int{}
	for _, call := range fc.setSlotCalls {
		if seen[call.slot] == nil {
			seen[call.slot] = map[string]int{}
		}
		seen[call.slot][call.mode]++
	}
	for slot := 0; slot <= 2; slot++ {
		if seen[slot]["IMPORTING"] != 1 || seen[slot]["MIGRATING"] != 1 || seen[slot]["NODE"] != 2 {
			t.Fatalf("unexpected setslot calls for slot %d: %#v", slot, seen[slot])
		}
	}
}

func TestMigrateSlots_MigratesKeysForMultipleSlotsInOneReconcile(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	migrateTopology(cluster)
	p := migratePlan()
	p.Steps[0].Params["slots"] = "0-2"
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return migrateClusterNodes("0-8191", "8192-16383"), nil
		},
		getKeysInSlot: func(slot, count int) ([]string, error) {
			return []string{fmt.Sprintf("key-%d", slot)}, nil
		},
	}
	exec := migrateExec(t, cluster, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, p, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.migrateCalls) != 3 {
		t.Fatalf("expected 3 migrate calls, got %v", fc.migrateCalls)
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
