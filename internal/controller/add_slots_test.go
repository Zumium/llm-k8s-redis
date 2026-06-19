package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

const (
	asMasterID  = "aaa111"
	asReplicaID = "bbb222"
)

// addSlotsPlan builds a Create plan whose final AddSlots step is at index 6.
//
//	0: EnsureNode redis-0
//	1: EnsureNode redis-1
//	2: WaitNodeReady redis-0
//	3: WaitNodeReady redis-1
//	4: MeetNode(redis-0 -> redis-1)
//	5: ReplicateNode(redis-0 master, redis-1 replica)
//	6: AddSlots(redis-0, "0-100")
func addSlotsPlan() *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "addslots",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "replicate-redis-1", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
			{ID: "add-slots-redis-0", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-100"}},
		},
	}
}

func addSlotsExec(t *testing.T, cluster *api.RedisCluster, pods []*corev1.Pod, fc *fakeRedisClient) *ActionExecutor {
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

// clusterNodesWithSlots builds CLUSTER NODES output for a master (owning the
// provided slot tokens) plus one healthy replica.
func clusterNodesWithSlots(masterID, masterIP string, slotTokens []string, replicaID, replicaIP string) string {
	masterLine := masterID + " " + masterIP + ":6379@16379 master - 0 0 1 connected"
	for _, tok := range slotTokens {
		masterLine += " " + tok
	}
	masterLine += "\n"
	replicaLine := replicaID + " " + replicaIP + ":6379@16379 slave " + masterID + " 0 0 2 connected\n"
	return masterLine + replicaLine
}

func TestAddSlots_AllSlotsAlreadyOwnedReturnsCompleted(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodesWithSlots(asMasterID, "10.0.0.1", []string{"0-100"}, asReplicaID, "10.0.0.2"), nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.addSlotsCalls) != 0 {
		t.Errorf("expected no CLUSTER ADDSLOTS when already owned, got %v", fc.addSlotsCalls)
	}
}

func TestAddSlots_IssuesAddSlotsThenCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls == 1 {
				return clusterNodesWithSlots(asMasterID, "10.0.0.1", nil, asReplicaID, "10.0.0.2"), nil
			}
			return clusterNodesWithSlots(asMasterID, "10.0.0.1", []string{"0-100"}, asReplicaID, "10.0.0.2"), nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.addSlotsCalls) != 1 {
		t.Fatalf("expected one ADDSLOTS batch, got %v", fc.addSlotsCalls)
	}
	if len(fc.addSlotsCalls[0]) != 101 {
		t.Errorf("expected 101 slots in batch, got %d", len(fc.addSlotsCalls[0]))
	}
	if fc.addSlotsCalls[0][0] != 0 || fc.addSlotsCalls[0][100] != 100 {
		t.Errorf("expected batch 0..100, got %v", fc.addSlotsCalls[0])
	}
}

func TestAddSlots_NotYetPropagatedReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodesWithSlots(asMasterID, "10.0.0.1", nil, asReplicaID, "10.0.0.2"), nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when slots not yet propagated, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.addSlotsCalls) != 1 {
		t.Errorf("expected addslots issued, got %v", fc.addSlotsCalls)
	}
}

func TestAddSlots_SlotOwnedByOtherMasterFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	otherMasterID := "ccc333"
	otherReplicaID := "ddd444"
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodesWithSlots(asMasterID, "10.0.0.1", nil, asReplicaID, "10.0.0.2") +
				clusterNodesWithSlots(otherMasterID, "10.0.0.9", []string{"0-100"}, otherReplicaID, "10.0.0.8"), nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err == nil {
		t.Fatal("expected error when slot owned by another master")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.addSlotsCalls) != 0 {
		t.Errorf("expected no ADDSLOTS on conflict, got %v", fc.addSlotsCalls)
	}
}

func TestAddSlots_MigratingSlotFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return asMasterID + " 10.0.0.1:6379@16379 master - 0 0 1 connected [50->-" + asReplicaID + "]\n" +
				asReplicaID + " 10.0.0.2:6379@16379 slave " + asMasterID + " 0 0 2 connected\n", nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err == nil {
		t.Fatal("expected error when slot in migrating state")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_TargetIsReplicaFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return asReplicaID + " 10.0.0.9:6379@16379 master - 0 0 1 connected\n" +
				asMasterID + " 10.0.0.1:6379@16379 slave " + asReplicaID + " 0 0 2 connected\n", nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err == nil {
		t.Fatal("expected error when target is a replica")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_MasterWithoutHealthyReplicaReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return asMasterID + " 10.0.0.1:6379@16379 master - 0 0 1 connected\n", nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_MasterReplicaFailedFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return asMasterID + " 10.0.0.1:6379@16379 master - 0 0 1 connected\n" +
				asReplicaID + " 10.0.0.2:6379@16379 slave,fail " + asMasterID + " 0 0 2 connected\n", nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_TargetNotVisibleReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return asReplicaID + " 10.0.0.2:6379@16379 master - 0 0 1 connected\n", nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when target not visible, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_PingFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{pingErr: errPing}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on ping fail, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_NoIPReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when no IP, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_PodNotFoundFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err == nil {
		t.Fatal("expected error when pod missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_NamespaceMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := addSlotsPlan()
	p.Steps[6].Params["namespace"] = "wrong"
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 6)
	if err == nil {
		t.Fatal("expected error on namespace mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_MissingSlotsParamFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := addSlotsPlan()
	delete(p.Steps[6].Params, "slots")
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 6)
	if err == nil {
		t.Fatal("expected error on missing slots param")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_InvalidSlotsFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := addSlotsPlan()
	p.Steps[6].Params["slots"] = "0-99999"
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 6)
	if err == nil {
		t.Fatal("expected error on invalid slots")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_NoPrecedingWaitNodeReadyFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "addslots-nowait",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "replicate-redis-1", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
			{ID: "add-slots-redis-0", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-100"}},
		},
	}
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 4)
	if err == nil {
		t.Fatal("expected error when WaitNodeReady missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_OverlapWithPreviousAddSlotsFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "addslots-overlap",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "replicate-redis-1", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
			{ID: "add-slots-redis-0-a", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-200"}},
			{ID: "add-slots-redis-0-b", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "100-300"}},
		},
	}
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 7)
	if err == nil {
		t.Fatal("expected error on overlapping AddSlots")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_AddSlotsErrorFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodesWithSlots(asMasterID, "10.0.0.1", nil, asReplicaID, "10.0.0.2"), nil
		},
		clusterAddSlots: func([]int) error { return errors.New("addslots refused") },
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6)
	if err == nil {
		t.Fatal("expected error on CLUSTER ADDSLOTS fail")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestAddSlots_RefreshesExistingTopology(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{{
		ID:       "shard-0",
		Master:   api.NodeTopology{Pod: "redis-0"},
		Replicas: []api.NodeTopology{{Pod: "redis-1"}},
	}}}
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodesWithSlots(asMasterID, "10.0.0.1", []string{"0-100"}, asReplicaID, "10.0.0.2"), nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	if _, err := exec.ExecuteStep(ctx, cluster, addSlotsPlan(), 6); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	shard := cluster.Status.Topology.Shards[0]
	if shard.Master.NodeID != asMasterID || !shard.Master.Ready || shard.Master.Slots != "0-100" {
		t.Errorf("expected master refreshed with slots, got %+v", shard.Master)
	}
}

func TestAddSlots_LargeRangeBatches(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls == 1 {
				return clusterNodesWithSlots(asMasterID, "10.0.0.1", nil, asReplicaID, "10.0.0.2"), nil
			}
			tokens := []string{"0-16383"}
			return clusterNodesWithSlots(asMasterID, "10.0.0.1", tokens, asReplicaID, "10.0.0.2"), nil
		},
	}
	exec := addSlotsExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := addSlotsPlan()
	p.Steps[6].Params["slots"] = "0-16383"
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	totalSlots := 0
	for _, batch := range fc.addSlotsCalls {
		if len(batch) > addSlotsBatchSize {
			t.Errorf("batch exceeded size %d: %d", addSlotsBatchSize, len(batch))
		}
		totalSlots += len(batch)
	}
	if totalSlots != 16384 {
		t.Errorf("expected total 16384 slots across batches, got %d", totalSlots)
	}
}

var _ client.Object = &corev1.Pod{}
