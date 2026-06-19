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
	masterID  = "aaa111"
	replicaID = "bbb222"
)

// replicatePlan builds a Create plan that ends with a ReplicateNode step at
// index 5.
func replicatePlan() *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "replicate",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "replicate-redis-1", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
		},
	}
}

func replicateExec(t *testing.T, cluster *api.RedisCluster, pods []*corev1.Pod, fc *fakeRedisClient) *ActionExecutor {
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

// clusterNodes returns a CLUSTER NODES payload string.
func clusterNodes(masters, replicas [][2]string) string {
	var b []byte
	for _, m := range masters {
		// <id> <ip:port@cport> master - <epoch> <link> <slots...>
		b = append(b, []byte(m[0]+" "+m[1]+":6379@16379 master - 0 0 1 connected 0-100\n")...)
	}
	for _, r := range replicas {
		// r[0]=replica id, r[1]="ip masterID"
		parts := splitSpace(r[1])
		ip := parts[0]
		mid := parts[1]
		b = append(b, []byte(r[0]+" "+ip+":6379@16379 slave "+mid+" 0 0 2 connected\n")...)
	}
	return string(b)
}

func splitSpace(s string) []string {
	out := []string{}
	cur := ""
	for _, c := range s {
		if c == ' ' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func TestReplicateNode_AlreadyReplicatesReturnsCompleted(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodes(
				[][2]string{{masterID, "10.0.0.1"}},
				[][2]string{{replicaID, "10.0.0.2 " + masterID}},
			), nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.replicateCalls) != 0 {
		t.Errorf("expected no CLUSTER REPLICATE when already replica, got %v", fc.replicateCalls)
	}
}

func TestReplicateNode_IssuesReplicateThenCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls == 1 {
				return clusterNodes(
					[][2]string{{masterID, "10.0.0.1"}},
					[][2]string{{replicaID, "10.0.0.2 -"}},
				), nil
			}
			return clusterNodes(
				[][2]string{{masterID, "10.0.0.1"}},
				[][2]string{{replicaID, "10.0.0.2 " + masterID}},
			), nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.replicateCalls) != 1 || fc.replicateCalls[0] != masterID {
		t.Errorf("expected one CLUSTER REPLICATE %s, got %v", masterID, fc.replicateCalls)
	}
}

func TestReplicateNode_NotYetPropagatedReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodes(
				[][2]string{{masterID, "10.0.0.1"}},
				[][2]string{{replicaID, "10.0.0.2 -"}},
			), nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when replication not yet propagated, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.replicateCalls) != 1 {
		t.Errorf("expected replicate issued, got %v", fc.replicateCalls)
	}
}

func TestReplicateNode_ReplicaOwnsSlotsFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return masterID + " 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
				replicaID + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383\n", nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err == nil {
		t.Fatal("expected error when replica owns slots")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed when replica owns slots, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.replicateCalls) != 0 {
		t.Errorf("expected no CLUSTER REPLICATE, got %v", fc.replicateCalls)
	}
}

func TestReplicateNode_MasterNotVisibleReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return replicaID + " 10.0.0.2:6379@16379 master - 0 0 1 connected\n", nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when master not visible, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_ReplicaNotVisibleReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return masterID + " 10.0.0.1:6379@16379 master - 0 0 1 connected\n", nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when replica not visible, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_ReplicaPingFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{pingErr: errPing}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on replica ping fail, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_MasterNoIPReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when master has no IP, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_ReplicaNoIPReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when replica has no IP, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_MasterPodNotFoundFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err == nil {
		t.Fatal("expected error when master pod missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_ReplicaPodNotFoundFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{master}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err == nil {
		t.Fatal("expected error when replica pod missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_SamePodFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{master}, fc)

	p := replicatePlan()
	p.Steps[5].Params["replicaPod"] = "redis-0"
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 5)
	if err == nil {
		t.Fatal("expected error when master==replica")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on same pod, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_NamespaceMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := replicatePlan()
	p.Steps[5].Params["namespace"] = "wrong"
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 5)
	if err == nil {
		t.Fatal("expected error on namespace mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on namespace mismatch, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_MissingReplicaPodParamFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := replicatePlan()
	delete(p.Steps[5].Params, "replicaPod")
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 5)
	if err == nil {
		t.Fatal("expected error on missing replicaPod")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on missing replicaPod, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_NoPrecedingWaitNodeReadyFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "replicate-nowait",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "replicate-redis-1", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
		},
	}
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 2)
	if err == nil {
		t.Fatal("expected error when WaitNodeReady missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_MasterNotActuallyMasterFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	otherID := "ccc333"
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return otherID + " 10.0.0.9:6379@16379 master - 0 0 1 connected\n" +
				masterID + " 10.0.0.1:6379@16379 slave " + otherID + " 0 0 2 connected\n" +
				replicaID + " 10.0.0.2:6379@16379 master - 0 0 3 connected\n", nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err == nil {
		t.Fatal("expected error when master pod is actually a replica")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed when master is a replica, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_ClusterReplicateErrorFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	master := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return clusterNodes(
				[][2]string{{masterID, "10.0.0.1"}},
				[][2]string{{replicaID, "10.0.0.2 -"}},
			), nil
		},
		clusterReplicate: func(string) error { return errors.New("replicate refused") },
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5)
	if err == nil {
		t.Fatal("expected error on CLUSTER REPLICATE fail")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on CLUSTER REPLICATE error, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestReplicateNode_RefreshesExistingTopology(t *testing.T) {
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
			return clusterNodes(
				[][2]string{{masterID, "10.0.0.1"}},
				[][2]string{{replicaID, "10.0.0.2 " + masterID}},
			), nil
		},
	}
	exec := replicateExec(t, cluster, []*corev1.Pod{master, replica}, fc)

	if _, err := exec.ExecuteStep(ctx, cluster, replicatePlan(), 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	shard := cluster.Status.Topology.Shards[0]
	if shard.Master.NodeID != masterID || !shard.Master.Ready {
		t.Errorf("expected master node refreshed, got %+v", shard.Master)
	}
	if len(shard.Replicas) != 1 || shard.Replicas[0].NodeID != replicaID || !shard.Replicas[0].Ready {
		t.Errorf("expected replica node refreshed, got %+v", shard.Replicas)
	}
}

// Keep client.Object referenced for the helper signatures above.
var _ client.Object = &corev1.Pod{}
