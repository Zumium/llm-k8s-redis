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

// meetPlan builds a Create plan: EnsureNode + WaitNodeReady for redis-0 and
// redis-1, then a MeetNode(redis-0 -> redis-1) step at index 4.
func meetPlan() *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "meet",
		Operation:        plan.OpCreate,
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
		},
	}
}

func readyPodWithIP(cluster *api.RedisCluster, name, ip string) *corev1.Pod {
	pod := desiredPod(cluster, "example", name, "redis:7.2", "2Gi", twoGiBytes)
	pod.Status.PodIP = ip
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return pod
}

func meetExec(t *testing.T, cluster *api.RedisCluster, pods []*corev1.Pod, fc *fakeRedisClient) *ActionExecutor {
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

func TestMeetNode_TargetAlreadyInClusterReturnsCompleted(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return "abc 10.0.0.1:6379@16379 master - 0\n" +
				"def 10.0.0.2:6379@16379 master - 0\n", nil
		},
	}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.meetCalls) != 0 {
		t.Errorf("expected no CLUSTER MEET when target already present, got %v", fc.meetCalls)
	}
}

func TestMeetNode_IssuesMeetThenCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	calls := 0
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			calls++
			if calls == 1 {
				return "abc 10.0.0.1:6379@16379 master - 0\n", nil
			}
			return "abc 10.0.0.1:6379@16379 master - 0\n" +
				"def 10.0.0.2:6379@16379 master - 0\n", nil
		},
	}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed after meet, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.meetCalls) != 1 {
		t.Fatalf("expected one CLUSTER MEET, got %v", fc.meetCalls)
	}
	if fc.meetCalls[0].host != "10.0.0.2" || fc.meetCalls[0].port != int(redisClientPort) {
		t.Errorf("unexpected meet call %+v", fc.meetCalls[0])
	}
}

func TestMeetNode_MeetIssuedButNotYetPropagatedReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return "abc 10.0.0.1:6379@16379 master - 0\n", nil
		},
	}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when target not yet visible, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.meetCalls) != 1 {
		t.Errorf("expected meet issued, got %v", fc.meetCalls)
	}
}

func TestMeetNode_SourcePingFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{pingErr: errPing}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on source ping fail, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_SourceNoIPReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when source has no IP, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_TargetNoIPReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when target has no IP, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_SourcePodNotFoundFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err == nil {
		t.Fatal("expected error when source pod missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed when source missing, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_TargetPodNotFoundFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err == nil {
		t.Fatal("expected error when target pod missing")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed when target missing, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_SamePodFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src}, fc)

	p := meetPlan()
	p.Steps[4].Params["targetPod"] = "redis-0"
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 4)
	if err == nil {
		t.Fatal("expected error when source==target")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on same pod, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_NamespaceMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	p := meetPlan()
	p.Steps[4].Params["namespace"] = "wrong"
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 4)
	if err == nil {
		t.Fatal("expected error on namespace mismatch")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on namespace mismatch, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_NoPrecedingEnsureNodeForTargetFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "meet-bad",
		Operation:        plan.OpCreate,
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
		},
	}
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 2)
	if err == nil {
		t.Fatal("expected error when target not declared by EnsureNode")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_NoPrecedingWaitNodeReadyFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "meet-nowait",
		Operation:        plan.OpCreate,
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
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

func TestMeetNode_MissingSourcePodParamFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	p := meetPlan()
	delete(p.Steps[4].Params, "sourcePod")
	outcome, err := exec.ExecuteStep(ctx, cluster, p, 4)
	if err == nil {
		t.Fatal("expected error on missing sourcePod")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on missing sourcePod, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_ClusterNodesErrorReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) { return "", errors.New("cluster nodes err") },
	}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running on CLUSTER NODES error, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestMeetNode_ClusterMeetErrorFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	src := readyPodWithIP(cluster, "redis-0", "10.0.0.1")
	tgt := readyPodWithIP(cluster, "redis-1", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterNodes: func() (string, error) {
			return "abc 10.0.0.1:6379@16379 master - 0\n", nil
		},
		clusterMeet: func(string, int) error { return errors.New("meet refused") },
	}
	exec := meetExec(t, cluster, []*corev1.Pod{src, tgt}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, meetPlan(), 4)
	if err == nil {
		t.Fatal("expected error on CLUSTER MEET fail")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on CLUSTER MEET error, got %q: %s", outcome.Status, outcome.Message)
	}
}
