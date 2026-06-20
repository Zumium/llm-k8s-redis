package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/redis"
)

const twoGiBytes = int64(2147483648)

func newExecutorScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add clientgo scheme: %v", err)
	}
	if err := api.AddToScheme(s); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}
	return s
}

func testCluster() *api.RedisCluster {
	return &api.RedisCluster{
		TypeMeta:   metav1.TypeMeta{Kind: "RedisCluster", APIVersion: api.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: "example", UID: "cluster-uid-123", Generation: 1},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
	}
}

func ensureStep(params map[string]any) plan.Step {
	return plan.Step{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: params}
}

func baseParams() map[string]any {
	return map[string]any{
		"namespace":  "example",
		"pod":        "redis-0",
		"image":      "redis:7.2",
		"memorySize": "2Gi",
	}
}

// fakeRedisClient is a test double for redis.Client.
type fakeRedisClient struct {
	pingErr          error
	configGet        func(key string) (string, error)
	configSet        func(key, value string) error
	clusterMyID      func() (string, error)
	clusterNodes     func() (string, error)
	clusterInfo      func() (string, error)
	clusterMeet      func(host string, port int) error
	clusterReplicate func(masterNodeID string) error
	clusterAddSlots  func(slots []int) error
	setSlotImporting func(slot int, sourceNodeID string) error
	setSlotMigrating func(slot int, targetNodeID string) error
	setSlotNode      func(slot int, nodeID string) error
	getKeysInSlot    func(slot, count int) ([]string, error)
	migrateKeys      func(host string, port int, keys []string, timeout time.Duration) error
	addSlotsCalls    [][]int
	replicateCalls   []string
	meetCalls        []meetCall
	setSlotCalls     []setSlotCall
	getKeysCalls     []getKeysCall
	migrateCalls     []migrateCall
	sets             []string
}

type setSlotCall struct {
	mode   string
	slot   int
	nodeID string
}

type getKeysCall struct {
	slot  int
	count int
}

type migrateCall struct {
	host    string
	port    int
	keys    []string
	timeout time.Duration
}

type meetCall struct {
	host string
	port int
}

func (f *fakeRedisClient) Ping(context.Context) error { return f.pingErr }
func (f *fakeRedisClient) ConfigGet(_ context.Context, key string) (string, error) {
	if f.configGet == nil {
		return "", nil
	}
	return f.configGet(key)
}
func (f *fakeRedisClient) ConfigSet(_ context.Context, key, value string) error {
	f.sets = append(f.sets, key+"="+value)
	if f.configSet == nil {
		return nil
	}
	return f.configSet(key, value)
}
func (f *fakeRedisClient) ClusterMyID(_ context.Context) (string, error) {
	if f.clusterMyID == nil {
		return "node-id-fixed", nil
	}
	return f.clusterMyID()
}
func (f *fakeRedisClient) ClusterNodes(_ context.Context) (string, error) {
	if f.clusterNodes == nil {
		return "", nil
	}
	return f.clusterNodes()
}
func (f *fakeRedisClient) ClusterInfo(_ context.Context) (string, error) {
	if f.clusterInfo == nil {
		return "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n", nil
	}
	return f.clusterInfo()
}
func (f *fakeRedisClient) ClusterMeet(_ context.Context, host string, port int) error {
	f.meetCalls = append(f.meetCalls, meetCall{host: host, port: port})
	if f.clusterMeet == nil {
		return nil
	}
	return f.clusterMeet(host, port)
}
func (f *fakeRedisClient) ClusterReplicate(_ context.Context, masterNodeID string) error {
	f.replicateCalls = append(f.replicateCalls, masterNodeID)
	if f.clusterReplicate == nil {
		return nil
	}
	return f.clusterReplicate(masterNodeID)
}
func (f *fakeRedisClient) ClusterAddSlots(_ context.Context, slots []int) error {
	cp := make([]int, len(slots))
	copy(cp, slots)
	f.addSlotsCalls = append(f.addSlotsCalls, cp)
	if f.clusterAddSlots == nil {
		return nil
	}
	return f.clusterAddSlots(slots)
}
func (f *fakeRedisClient) ClusterSetSlotImporting(_ context.Context, slot int, sourceNodeID string) error {
	f.setSlotCalls = append(f.setSlotCalls, setSlotCall{mode: "IMPORTING", slot: slot, nodeID: sourceNodeID})
	if f.setSlotImporting == nil {
		return nil
	}
	return f.setSlotImporting(slot, sourceNodeID)
}
func (f *fakeRedisClient) ClusterSetSlotMigrating(_ context.Context, slot int, targetNodeID string) error {
	f.setSlotCalls = append(f.setSlotCalls, setSlotCall{mode: "MIGRATING", slot: slot, nodeID: targetNodeID})
	if f.setSlotMigrating == nil {
		return nil
	}
	return f.setSlotMigrating(slot, targetNodeID)
}
func (f *fakeRedisClient) ClusterSetSlotNode(_ context.Context, slot int, nodeID string) error {
	f.setSlotCalls = append(f.setSlotCalls, setSlotCall{mode: "NODE", slot: slot, nodeID: nodeID})
	if f.setSlotNode == nil {
		return nil
	}
	return f.setSlotNode(slot, nodeID)
}
func (f *fakeRedisClient) ClusterGetKeysInSlot(_ context.Context, slot, count int) ([]string, error) {
	f.getKeysCalls = append(f.getKeysCalls, getKeysCall{slot: slot, count: count})
	if f.getKeysInSlot == nil {
		return nil, nil
	}
	return f.getKeysInSlot(slot, count)
}
func (f *fakeRedisClient) MigrateKeys(_ context.Context, host string, port int, keys []string, timeout time.Duration) error {
	cp := append([]string{}, keys...)
	f.migrateCalls = append(f.migrateCalls, migrateCall{host: host, port: port, keys: cp, timeout: timeout})
	if f.migrateKeys == nil {
		return nil
	}
	return f.migrateKeys(host, port, keys, timeout)
}
func (f *fakeRedisClient) Close() error { return nil }

func fakeFactory(fc *fakeRedisClient) redis.Factory {
	return func(string) (redis.Client, error) { return fc, nil }
}

func newExecutor(t *testing.T, objs []client.Object, fc *fakeRedisClient) *ActionExecutor {
	t.Helper()
	scheme := newExecutorScheme(t)
	if fc == nil {
		fc = &fakeRedisClient{}
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	return &ActionExecutor{Client: cl, Scheme: scheme, RedisFactory: fakeFactory(fc)}
}

func TestEnsureNode_CreatesPodWithDesiredSpec(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := newExecutor(t, []client.Object{cluster}, nil)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(baseParams())}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running after create, got %q: %s", outcome.Status, outcome.Message)
	}

	var pod corev1.Pod
	if err := exec.Get(ctx, client.ObjectKey{Namespace: "example", Name: "redis-0"}, &pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if pod.Labels[labelCluster] != "example" || pod.Labels[labelPod] != "redis-0" || pod.Labels[labelManagedBy] != "redis-cluster-controller" {
		t.Errorf("unexpected labels: %v", pod.Labels)
	}
	if pod.Annotations[annImage] != "redis:7.2" || pod.Annotations[annMemorySize] != "2Gi" || pod.Annotations[annMaxmemory] != "2147483648" {
		t.Errorf("unexpected annotations: %v", pod.Annotations)
	}
	if !metav1.IsControlledBy(&pod, cluster) {
		t.Error("expected owner reference to RedisCluster")
	}
	c := containerOf(&pod)
	if c == nil || c.Image != "redis:7.2" {
		t.Fatalf("expected redis container with image redis:7.2, got %+v", c)
	}
	if c.Resources.Limits[corev1.ResourceMemory] != resource.MustParse("2Gi") {
		t.Errorf("expected memory limit 2Gi, got %v", c.Resources.Limits)
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Ports[0].ContainerPort != redisClientPort {
		t.Errorf("expected client port %d, got %+v", redisClientPort, pod.Spec.Containers)
	}
}

func TestEnsureNode_IdempotentMaxmemoryMatch(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	scheme := newExecutorScheme(t)
	wantPod := desiredPod(cluster, "example", "redis-0", "redis:7.2", "2Gi", twoGiBytes)
	wantPod.Status.PodIP = "10.0.0.1"
	if err := ctrl.SetControllerReference(cluster, wantPod, scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	fc := &fakeRedisClient{configGet: func(string) (string, error) { return "2147483648", nil }}
	exec := newExecutor(t, []client.Object{cluster, wantPod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(baseParams())}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.sets) != 0 {
		t.Errorf("expected no CONFIG SET on match, got %v", fc.sets)
	}
}

func TestEnsureNode_SetsMaxmemoryOnMismatch(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	scheme := newExecutorScheme(t)
	wantPod := desiredPod(cluster, "example", "redis-0", "redis:7.2", "2Gi", twoGiBytes)
	wantPod.Status.PodIP = "10.0.0.2"
	if err := ctrl.SetControllerReference(cluster, wantPod, scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	calls := 0
	fc := &fakeRedisClient{
		configGet: func(string) (string, error) {
			calls++
			if calls == 1 {
				return "0", nil
			}
			return "2147483648", nil
		},
	}
	exec := newExecutor(t, []client.Object{cluster, wantPod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(baseParams())}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed, got %q: %s", outcome.Status, outcome.Message)
	}
	if len(fc.sets) != 1 || fc.sets[0] != "maxmemory=2147483648" {
		t.Errorf("expected one CONFIG SET maxmemory=2147483648, got %v", fc.sets)
	}
}

func TestEnsureNode_NoPodIPReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	scheme := newExecutorScheme(t)
	wantPod := desiredPod(cluster, "example", "redis-0", "redis:7.2", "2Gi", twoGiBytes)
	if err := ctrl.SetControllerReference(cluster, wantPod, scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	fc := &fakeRedisClient{}
	exec := newExecutor(t, []client.Object{cluster, wantPod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(baseParams())}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestEnsureNode_RedisPingFailsReturnsRunning(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	scheme := newExecutorScheme(t)
	wantPod := desiredPod(cluster, "example", "redis-0", "redis:7.2", "2Gi", twoGiBytes)
	wantPod.Status.PodIP = "10.0.0.3"
	if err := ctrl.SetControllerReference(cluster, wantPod, scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	fc := &fakeRedisClient{pingErr: errPing}
	exec := newExecutor(t, []client.Object{cluster, wantPod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(baseParams())}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateRunning {
		t.Fatalf("expected Running when redis not ready, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestEnsureNode_ImageDriftFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	scheme := newExecutorScheme(t)
	driftPod := desiredPod(cluster, "example", "redis-0", "redis:7.2", "2Gi", twoGiBytes)
	driftPod.Spec.Containers[0].Image = "redis:6.2"
	if err := ctrl.SetControllerReference(cluster, driftPod, scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	fc := &fakeRedisClient{}
	exec := newExecutor(t, []client.Object{cluster, driftPod}, fc)

	outcome, _ := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(baseParams())}}, 0)
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on image drift, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestEnsureNode_MissingPodParamFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := newExecutor(t, []client.Object{cluster}, nil)
	params := baseParams()
	delete(params, "pod")

	outcome, _ := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(params)}}, 0)
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on missing pod, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestEnsureNode_NamespaceMismatchFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := newExecutor(t, []client.Object{cluster}, nil)
	params := baseParams()
	params["namespace"] = "wrong"

	outcome, _ := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(params)}}, 0)
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on namespace mismatch, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestEnsureNode_ImageMismatchWithSpecFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := newExecutor(t, []client.Object{cluster}, nil)
	params := baseParams()
	params["image"] = "redis:6.2"

	outcome, _ := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(params)}}, 0)
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed on image mismatch with spec, got %q: %s", outcome.Status, outcome.Message)
	}
}

func TestEnsureNode_PatchesMissingLabels(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	scheme := newExecutorScheme(t)
	wantPod := desiredPod(cluster, "example", "redis-0", "redis:7.2", "2Gi", twoGiBytes)
	wantPod.Status.PodIP = "10.0.0.4"
	wantPod.Labels = map[string]string{labelCluster: "example"}
	if err := ctrl.SetControllerReference(cluster, wantPod, scheme); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	fc := &fakeRedisClient{configGet: func(string) (string, error) { return "2147483648", nil }}
	exec := newExecutor(t, []client.Object{cluster, wantPod}, fc)

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{ensureStep(baseParams())}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.Status != plan.StepStateCompleted {
		t.Fatalf("expected Completed after metadata patch, got %q: %s", outcome.Status, outcome.Message)
	}
	var pod corev1.Pod
	if err := exec.Get(ctx, client.ObjectKey{Namespace: "example", Name: "redis-0"}, &pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if pod.Labels[labelPod] != "redis-0" || pod.Labels[labelManagedBy] != "redis-cluster-controller" {
		t.Errorf("expected labels patched in, got %v", pod.Labels)
	}
	if pod.Annotations[annMaxmemory] != "2147483648" {
		t.Errorf("expected annotations patched in, got %v", pod.Annotations)
	}
}

func TestEnsureNode_UnimplementedActionFails(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster()
	exec := newExecutor(t, []client.Object{cluster}, nil)
	step := plan.Step{ID: "migrate-0", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2", "slots": "0-1023"}}

	outcome, err := exec.ExecuteStep(ctx, cluster, &plan.Plan{Steps: []plan.Step{step}}, 0)
	if err == nil {
		t.Fatal("expected error for unimplemented action")
	}
	if outcome.Status != plan.StepStateFailed {
		t.Fatalf("expected Failed, got %q", outcome.Status)
	}
}

var errPing = errPingSentinel{}

type errPingSentinel struct{}

func (errPingSentinel) Error() string { return "redis ping failed" }
