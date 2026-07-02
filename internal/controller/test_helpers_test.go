package controller

import (
	"context"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
)

const testLabelCluster = "redis.example.com/cluster"

func newScheme(t *testing.T) *runtime.Scheme {
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

func vcPod(name, ip string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "example",
			Name:      name,
			Labels:    map[string]string{testLabelCluster: "example"},
		},
		Status: corev1.PodStatus{
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

type statusUpdateErrorClient struct {
	client.Client
	err error
}

type staleFirstGetClient struct {
	client.Client
	stale *api.RedisCluster
	gets  int
}

func (c *staleFirstGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if got, ok := obj.(*api.RedisCluster); ok && key.Name == c.stale.Name && key.Namespace == c.stale.Namespace {
		c.gets++
		if c.gets == 1 {
			c.stale.DeepCopyInto(got)
			return nil
		}
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c statusUpdateErrorClient) Status() client.SubResourceWriter {
	return statusUpdateErrorWriter{err: c.err}
}

type statusUpdateErrorWriter struct {
	err error
}

func (w statusUpdateErrorWriter) Create(_ context.Context, _ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
	return w.err
}

func (w statusUpdateErrorWriter) Update(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
	return w.err
}

func (w statusUpdateErrorWriter) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return w.err
}

func (w statusUpdateErrorWriter) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
	return w.err
}

func hasCondition(conditions []metav1.Condition, typ string, status metav1.ConditionStatus, reason string) bool {
	for _, c := range conditions {
		if c.Type == typ && c.Status == status && c.Reason == reason {
			return true
		}
	}
	return false
}

func managedTestNamespace() *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "example",
		Labels: map[string]string{
			managedByLabel: "redis-cluster-controller",
			clusterLabel:   "example",
		},
	}}
}

type recordingObserver struct {
	nodes    []plan.ObservedNode
	nodeSets [][]plan.ObservedNode
}

func (o *recordingObserver) CollectObservedNodes(_ context.Context, _ *api.RedisCluster) ([]plan.ObservedNode, error) {
	if len(o.nodeSets) > 0 {
		nodes := o.nodeSets[0]
		o.nodeSets = o.nodeSets[1:]
		return nodes, nil
	}
	return o.nodes, nil
}

func (o *recordingObserver) ExecuteStep(_ context.Context, _ *api.RedisCluster, _ *plan.Plan, _ int) (StepOutcome, error) {
	return StepOutcome{Status: plan.StepStateCompleted, Message: "done"}, nil
}

func observedFromAPITopology(t *api.ClusterTopology) []plan.ObservedNode {
	var out []plan.ObservedNode
	if t == nil {
		return out
	}
	for _, sh := range t.Shards {
		out = append(out, plan.ObservedNode{Pod: sh.Master.Pod, PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: sh.Master.NodeID, Role: "master", Slots: sh.Master.Slots, Ready: sh.Master.Ready})
		for _, r := range sh.Replicas {
			out = append(out, plan.ObservedNode{Pod: r.Pod, PodExists: true, Image: "redis:7.2", RedisSeen: true, NodeID: r.NodeID, Role: "replica", MasterPod: sh.Master.Pod, Ready: r.Ready})
		}
	}
	return out
}

type recordingPlanner struct {
	called  int
	plan    *plan.Plan
	plans   []*plan.Plan
	err     error
	lastReq planner.Request
	reqs    []planner.Request
}

func (p *recordingPlanner) Plan(_ context.Context, req planner.Request) (*plan.Plan, error) {
	p.called++
	p.lastReq = req
	p.reqs = append(p.reqs, req)
	if len(p.plans) > 0 {
		next := p.plans[0]
		p.plans = p.plans[1:]
		return next, p.err
	}
	return p.plan, p.err
}

type recordingExecutor struct {
	mu       sync.Mutex
	called   int
	index    int
	indices  []int
	outcome  StepOutcome
	outcomes map[int]StepOutcome
	params   map[string]any
}

func (e *recordingExecutor) ExecuteStep(_ context.Context, _ *api.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	e.mu.Lock()
	e.called++
	e.index = stepIndex
	e.indices = append(e.indices, stepIndex)
	outcome, ok := e.outcomes[stepIndex]
	params := e.params
	defaultOutcome := e.outcome
	e.mu.Unlock()
	if params != nil {
		p.Steps[stepIndex].Params = params
	}
	if ok {
		return outcome, nil
	}
	if defaultOutcome.Status != "" {
		return defaultOutcome, nil
	}
	return StepOutcome{Status: plan.StepStateCompleted, Message: "done"}, nil
}

func (e *recordingExecutor) CollectObservedNodes(_ context.Context, _ *api.RedisCluster) ([]plan.ObservedNode, error) {
	return nil, nil
}

type recordingValidator struct {
	called int
	err    error
	errors []error
}

func (v *recordingValidator) Validate(_ plan.ClusterSpec, _ []plan.ObservedNode, _ *plan.Plan) error {
	v.called++
	if len(v.errors) > 0 {
		err := v.errors[0]
		v.errors = v.errors[1:]
		return err
	}
	return v.err
}

func clusterWithTopology() *api.RedisCluster {
	return &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
		Status: api.RedisClusterStatus{
			Topology: &api.ClusterTopology{Shards: []api.ShardTopology{{ID: "shard-0"}}},
		},
	}
}

func missingReplicaCluster(next int32) *api.RedisCluster {
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.NextPodOrdinal = next
	cluster.Status.ObservedGeneration = 1
	cluster.Status.TopologyObservedAt = metav1.Now()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", NodeID: "master-0", Slots: "0-8191", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", NodeID: "replica-1", Ready: true}}},
		{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-2", NodeID: "master-2", Slots: "8192-16383", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-3", NodeID: "replica-3", Ready: true}}},
	}}
	return cluster
}

func missingReplicaDriftPlan(replacement string) *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "drift-1",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": replacement, "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": replacement}},
			{ID: "meet", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": replacement}},
			{ID: "replicate", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": replacement}},
			{ID: "forget", Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "lastKnownNodeId": "replica-1"}},
			{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
}

func validCreate2x1Plan() *plan.Plan {
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "create-1",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-1", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-2", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-2", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-3", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-3", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "wait-redis-1", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-1"}},
			{ID: "wait-redis-2", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-2"}},
			{ID: "wait-redis-3", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-3"}},
			{ID: "meet-redis-1", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-1"}},
			{ID: "meet-redis-2", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-2"}},
			{ID: "meet-redis-3", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-3"}},
			{ID: "replicate-redis-1", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-1"}},
			{ID: "replicate-redis-3", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-2", "replicaPod": "redis-3"}},
			{ID: "add-slots-redis-0", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-0", "slots": "0-8191"}},
			{ID: "add-slots-redis-2", Action: plan.ActionAddSlots, Params: map[string]any{"namespace": "example", "pod": "redis-2", "slots": "8192-16383"}},
			{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
}

func runningPlan() *api.PlanStatus {
	return &api.PlanStatus{
		ID:               "plan",
		TargetGeneration: 1,
		Steps:            []api.StepStatus{{ID: "s1", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateRunning)}},
	}
}

func completedPlan() *api.PlanStatus {
	return &api.PlanStatus{
		ID:               "plan",
		TargetGeneration: 1,
		Steps:            []api.StepStatus{{ID: "s1", Action: string(plan.ActionVerifyCluster), Status: string(plan.StepStateCompleted)}},
	}
}

func failedPlan() *api.PlanStatus {
	return &api.PlanStatus{
		ID:               "plan",
		TargetGeneration: 1,
		Steps:            []api.StepStatus{{ID: "s1", Action: string(plan.ActionVerifyCluster), Status: string(plan.StepStateFailed)}},
	}
}
