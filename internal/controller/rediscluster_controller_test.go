package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
	"github.com/Zumium/llm-k8s-redis/internal/validator"
)

func TestReconcile_EnsuresFinalizerNamespaceAndPlans(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()

	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme,
		Planner: planner.NoopPlanner{}, ValidatePlan: validator.Validate,
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}

	// 1st reconcile: adds finalizer and requeues.
	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue after adding finalizer")
	}

	// 2nd reconcile: creates namespace, records the first observed sample.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	// 3rd reconcile: same observed sample, namespace becomes visible.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	// 4th reconcile: same observed sample, then planner fails.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 4: %v", err)
	}
	// 5th reconcile: fake client has persisted the waiting condition.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 5: %v", err)
	}

	var ns corev1.Namespace
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &ns); err != nil {
		t.Fatalf("expected namespace to exist: %v", err)
	}

	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, finalizer) {
		t.Error("expected finalizer to be present")
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionFalse, "PlannerFailed") {
		t.Errorf("expected Planned=False/PlannerFailed condition, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_PassesObservedNodesToPlanner(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
		Status: api.RedisClusterStatus{
			TopologyObservedAt: metav1.Now(),
			Topology: &api.ClusterTopology{Shards: []api.ShardTopology{
				{Master: api.NodeTopology{Pod: "redis-2", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", Ready: true}}},
			}},
		},
	}
	cluster.Finalizers = []string{finalizer}
	fp := &recordingPlanner{plan: validCreate2x1Plan()}
	observed := []plan.ObservedNode{
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true, Flags: []string{"master"}, LinkState: "connected"},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-1", Role: "master", Slots: "8192-16383", Ready: true, Flags: []string{"master"}, LinkState: "connected"},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-0", Role: "replica", MasterID: "master-0", MasterPod: "redis-1", Ready: true, Flags: []string{"slave"}, LinkState: "connected"},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(observed)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme,
		Planner: fp, Driver: &recordingObserver{nodes: observed}, ValidatePlan: validator.Validate,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	nodes := fp.lastReq.ObservedState.Nodes
	if len(nodes) != 3 {
		t.Fatalf("expected 3 observed nodes, got %#v", nodes)
	}
	if nodes[0].Pod != "redis-1" || !nodes[0].PodExists || !nodes[0].RedisSeen || nodes[0].NodeID != "master-0" || nodes[0].Role != "master" {
		t.Fatalf("unexpected joined node: %#v", nodes[0])
	}
	if nodes[1].Pod != "redis-2" || !nodes[1].PodExists || !nodes[1].RedisSeen || nodes[1].NodeID != "master-1" || !nodes[1].Ready {
		t.Fatalf("unexpected joined node: %#v", nodes[1])
	}
	if nodes[2].Pod != "redis-3" || !nodes[2].PodExists || !nodes[2].RedisSeen || nodes[2].NodeID != "replica-0" || nodes[2].Role != "replica" || nodes[2].MasterID != "master-0" {
		t.Fatalf("unexpected replica node: %#v", nodes[2])
	}
}

func TestReconcile_WaitsForStableObservedNodesBeforePlanning(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	previous := []plan.ObservedNode{{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "old", Role: "master", Slots: "0-8191", Ready: true}}
	nodes := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(previous)
	fp := &recordingPlanner{err: errors.New("unexpected planner call")}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{nodes: nodes},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Fatalf("expected 10s requeue, got %#v", res)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, types.NamespacedName{Name: "example"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !observedStatusNodesEqual(got.Status.ObservedNodes, apiObservedNodes(nodes)) {
		t.Fatalf("observedNodes = %#v", got.Status.ObservedNodes)
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionFalse, "WaitingForClusterStable") {
		t.Fatalf("expected Planned=False/WaitingForClusterStable, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_PlansAfterStableObservedNodes(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	nodes := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(nodes)
	fp := &recordingPlanner{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{nodes: nodes},
		ValidatePlan: validator.Validate,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner call, got %d", fp.called)
	}
}

func TestReconcile_PlanRejectedOnce(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ObservedNodes = apiObservedNodes(nil)
	fp := &recordingPlanner{plan: &plan.Plan{DSLVersion: plan.DSLVersion, PlanID: "bad", TargetGeneration: 1, Steps: []plan.Step{{ID: "bad", Action: plan.ActionVerifyCluster}}}}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme,
		Planner: fp, Driver: &recordingObserver{}, ValidatePlan: (&recordingValidator{err: errors.New("no")}).Validate,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("planner calls = %d", fp.called)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, types.NamespacedName{Name: "example"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.ActivePlan != nil {
		t.Fatalf("activePlan = %#v", got.Status.ActivePlan)
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionFalse, "PlanRejected") {
		t.Fatalf("expected PlanRejected, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_CompletedPlanClearsAndRequeues(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
		Status: api.RedisClusterStatus{
			ObservedGeneration: 1,
			TopologyObservedAt: metav1.Now(),
			Topology: &api.ClusterTopology{Shards: []api.ShardTopology{
				{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", NodeID: "master-0", Slots: "0-8191", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", NodeID: "replica-1", Ready: true}}},
				{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-2", NodeID: "master-2", Slots: "8192-16383", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-3", NodeID: "replica-3", Ready: true}}},
			}},
			ActivePlan: completedPlan(),
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: &recordingObserver{},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Requeue {
		t.Fatalf("expected immediate requeue, got %#v", res)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan != nil {
		t.Fatalf("expected activePlan cleared, got %#v", got.Status.ActivePlan)
	}
}

func TestReconcile_FailedPlanRetainedWithoutRequeue(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
		Status: api.RedisClusterStatus{
			ObservedGeneration: 1,
			TopologyObservedAt: metav1.Now(),
			Topology: &api.ClusterTopology{Shards: []api.ShardTopology{
				{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", NodeID: "master-0", Slots: "0-8191", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", NodeID: "replica-1", Ready: true}}},
				{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-2", NodeID: "master-2", Slots: "8192-16383", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-3", NodeID: "replica-3", Ready: true}}},
			}},
			ActivePlan: failedPlan(),
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: &recordingObserver{},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %#v", res)
	}
}

func TestReconcile_FailedPlanRetainedWhenTopologyMatchesSpec(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
		Status: api.RedisClusterStatus{
			ObservedGeneration: 1,
			TopologyObservedAt: metav1.Now(),
			Topology: &api.ClusterTopology{Shards: []api.ShardTopology{
				{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", NodeID: "master-0", Slots: "0-8191", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", NodeID: "replica-1", Ready: true}}},
				{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-2", NodeID: "master-2", Slots: "8192-16383", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-3", NodeID: "replica-3", Ready: true}}},
			}},
			NextPodOrdinal: 10,
			ActivePlan: &api.PlanStatus{
				ID:               "plan",
				TargetGeneration: 1,
				Steps: []api.StepStatus{
					{ID: "ensure", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateCompleted)},
					{ID: "verify", Action: string(plan.ActionVerifyCluster), Status: string(plan.StepStateFailed)},
				},
			},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
		vcPod("redis-3", "10.0.0.4", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: &recordingObserver{},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %#v", res)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan == nil || got.Status.ActivePlan.Status != string(plan.PlanStateFailed) {
		t.Fatalf("expected failed activePlan retained, got %#v", got.Status.ActivePlan)
	}
	if got.Status.ObservedGeneration != 1 {
		t.Fatalf("expected observedGeneration 1, got %d", got.Status.ObservedGeneration)
	}
	if got.Status.NextPodOrdinal != 10 {
		t.Fatalf("expected nextPodOrdinal 10, got %d", got.Status.NextPodOrdinal)
	}
	if !hasCondition(got.Status.Conditions, ConditionReady, metav1.ConditionFalse, "PlanFailed") {
		t.Fatalf("expected Ready=False/PlanFailed, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_StaleCompletedPlanIsSuperseded(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Generation = 2
	cluster.Spec.ReplicasPerShard = 2
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ObservedGeneration = 1
	cluster.Status.ActivePlan = completedPlan()
	cluster.Status.TopologyObservedAt = metav1.Now()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: &recordingObserver{},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected immediate requeue after superseding stale plan")
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan != nil {
		t.Fatalf("expected activePlan cleared, got %#v", got.Status.ActivePlan)
	}
	if got.Status.ObservedGeneration != 1 {
		t.Fatalf("expected observedGeneration to remain 1, got %d", got.Status.ObservedGeneration)
	}
	if !hasCondition(got.Status.Conditions, ConditionReady, metav1.ConditionFalse, "Replanning") {
		t.Fatalf("expected Ready=False/Replanning, got %#v", got.Status.Conditions)
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded") {
		t.Fatalf("expected Planned=False/PlanSuperseded, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_StaleRunningPlanIsSupersededBeforeExecute(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Generation = 2
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = runningPlan()
	cluster.Status.TopologyObservedAt = metav1.Now()
	exec := &recordingExecutor{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: exec,
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected immediate requeue after superseding stale plan")
	}
	if exec.called != 0 {
		t.Fatalf("expected no step execution, got %d", exec.called)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan != nil {
		t.Fatalf("expected activePlan cleared, got %#v", got.Status.ActivePlan)
	}
}

func TestReconcile_ReplansAfterStalePlanCleared(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Generation = 2
	cluster.Spec.ReplicasPerShard = 2
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ObservedGeneration = 1
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", Slots: "0-8191", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", Ready: true}}},
		{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-2", Slots: "8192-16383", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-3", Ready: true}}},
	}}
	cluster.Status.ObservedNodes = apiObservedNodes(observedFromAPITopology(cluster.Status.Topology))
	cluster.Status.TopologyObservedAt = metav1.Now()
	p := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "scaleout-2",
		TargetGeneration: 2,
		Steps: []plan.Step{
			{ID: "ensure-redis-0", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-0", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-0", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-0"}},
			{ID: "ensure-redis-2", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-2", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-2", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-2"}},
			{ID: "ensure-redis-4", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-4", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "ensure-redis-5", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-5", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait-redis-4", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-4"}},
			{ID: "wait-redis-5", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-5"}},
			{ID: "meet-redis-4", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-4"}},
			{ID: "meet-redis-5", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-2", "targetPod": "redis-5"}},
			{ID: "replicate-redis-4", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-4"}},
			{ID: "replicate-redis-5", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-2", "replicaPod": "redis-5"}},
			{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 2, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	fp := &recordingPlanner{plan: p}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{nodes: observedFromAPITopology(cluster.Status.Topology)},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected no explicit requeue after accepting new plan, got %#v", res)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner to be called once, got %d", fp.called)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan == nil || got.Status.ActivePlan.TargetGeneration != 2 {
		t.Fatalf("expected generation 2 activePlan, got %#v", got.Status.ActivePlan)
	}
	if got.Status.ObservedGeneration != 2 {
		t.Fatalf("expected observedGeneration 2, got %d", got.Status.ObservedGeneration)
	}
}

func TestReconcile_LiveObservedTopologyWithExtraNoSlotNodeCallsPlanner(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
		Status: api.RedisClusterStatus{
			ObservedGeneration: 1,
			Topology: &api.ClusterTopology{Shards: []api.ShardTopology{
				{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", Slots: "0-8191", Ready: true}},
			}},
		},
	}
	nodes := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		{Pod: "redis-4", PodExists: true, RedisSeen: true, NodeID: "no-slot", Role: "master", Ready: true},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(nodes)
	fp := &recordingPlanner{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{nodes: nodes},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner for extra no-slot node, got %d calls", fp.called)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Fatalf("expected RequeueAfter=10s, got %v", res.RequeueAfter)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionFalse, "PlannerEmpty") {
		t.Fatalf("expected Planned=False/PlannerEmpty, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_LiveObservedTopologyMismatchCallsPlanner(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
	}
	nodes := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(nodes)
	fp := &recordingPlanner{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{nodes: nodes},
		ValidatePlan: validator.Validate,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner to be called once, got %d", fp.called)
	}
}

func TestReconcile_LiveObservedTopologySamePodCountButWrongShapeCallsPlanner(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 3, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
	}
	nodes := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-5460", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "5461-10922", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		{Pod: "redis-11", PodExists: true, RedisSeen: true, NodeID: "master-11", Role: "master", Slots: "10923-16383", Ready: true},
		{Pod: "redis-13", PodExists: true, RedisSeen: true, NodeID: "master-13", Role: "master", Ready: true},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(nodes)
	fp := &recordingPlanner{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{nodes: nodes},
		ValidatePlan: validator.Validate,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner despite matching pod count, got %d calls", fp.called)
	}
}

func TestReconcile_MissingReplicaWaitsBeforeDriftPlan(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := missingReplicaCluster(4)
	nodes := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		{Pod: "redis-1", PodExists: false, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Flags: []string{"fail"}},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(nodes)
	driftPlan := &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           "drift-1",
		TargetGeneration: 1,
		Steps: []plan.Step{
			{ID: "ensure", Action: plan.ActionEnsureNode, Params: map[string]any{"namespace": "example", "pod": "redis-4", "image": "redis:7.2", "memorySize": "2Gi"}},
			{ID: "wait", Action: plan.ActionWaitNodeReady, Params: map[string]any{"namespace": "example", "pod": "redis-4"}},
			{ID: "meet", Action: plan.ActionMeetNode, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-4"}},
			{ID: "replicate", Action: plan.ActionReplicateNode, Params: map[string]any{"namespace": "example", "masterPod": "redis-0", "replicaPod": "redis-4"}},
			{ID: "forget", Action: plan.ActionForgetNode, Params: map[string]any{"namespace": "example", "pod": "redis-1", "lastKnownNodeId": "replica-1"}},
			{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{"expectedShards": 2, "expectedReplicasPerShard": 1, "requireClusterStateOk": true, "requireFullSlotCoverage": true, "requireAllSlotOwnersHaveReplicas": true}},
		},
	}
	fp := &recordingPlanner{plan: driftPlan}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-2", "10.0.0.2", true),
		vcPod("redis-3", "10.0.0.3", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: validator.Validate,
		Driver: &recordingObserver{nodes: nodes},
	}
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 10*time.Second {
		t.Fatalf("expected stable wait requeue, got %#v", res)
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.NextPodOrdinal != 4 {
		t.Fatalf("expected nextPodOrdinal 4, got %d", got.Status.NextPodOrdinal)
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionFalse, "WaitingForClusterStable") {
		t.Fatalf("expected Planned=False/WaitingForClusterStable, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_MissingMasterWaitsForRedisFailover(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := missingReplicaCluster(4)
	fp := &recordingPlanner{err: errors.New("unexpected planner call")}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-3", "10.0.0.3", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: validator.Validate,
		Driver: &recordingObserver{nodes: []plan.ObservedNode{
			{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
			{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-2", Ready: true},
			{Pod: "redis-2", PodExists: false, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Flags: []string{"fail"}},
			{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		}},
	}
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Fatalf("expected 10s requeue, got %#v", res)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan != nil {
		t.Fatalf("expected no active plan, got %#v", got.Status.ActivePlan)
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionFalse, "WaitingForClusterStable") {
		t.Fatalf("expected Planned=False/WaitingForClusterStable, got %#v", got.Status.Conditions)
	}
}

func TestReconcile_MissingMasterPlansAfterStableWaitTimeout(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := missingReplicaCluster(4)
	cluster.Status.Conditions = []metav1.Condition{{
		Type:               ConditionPlanned,
		Status:             metav1.ConditionFalse,
		Reason:             "WaitingForClusterStable",
		LastTransitionTime: metav1.NewTime(time.Now().Add(-verifyStableTimeout - time.Second)),
	}}
	fp := &recordingPlanner{err: errors.New("planner called")}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-3", "10.0.0.3", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: validator.Validate,
		Driver: &recordingObserver{nodes: []plan.ObservedNode{
			{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
			{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-2", Ready: true},
			{Pod: "redis-2", PodExists: false, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Flags: []string{"fail"}},
			{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		}},
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner call after timeout, got %d", fp.called)
	}
}

func TestReconcile_MigratingSlotsWaitsForClusterStable(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.TopologyObservedAt = metav1.Now()
	fp := &recordingPlanner{err: errors.New("unexpected planner call")}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
		vcPod("redis-3", "10.0.0.4", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: validator.Validate,
		Driver: &recordingObserver{nodes: []plan.ObservedNode{
			{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191,[8192->-master-2]", Ready: true},
			{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
			{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8193-16383", Ready: true},
			{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		}},
	}
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Fatalf("expected 10s requeue, got %#v", res)
	}
}

func TestReconcile_ActiveDriftPlanExecutesWhenDriftStillObserved(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := missingReplicaCluster(5)
	active, err := planToStatus(missingReplicaDriftPlan("redis-4"))
	if err != nil {
		t.Fatalf("planToStatus: %v", err)
	}
	cluster.Status.ActivePlan = active
	fp := &recordingPlanner{err: errors.New("unexpected planner call")}
	exec := &recordingExecutor{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-2", "10.0.0.2", true),
		vcPod("redis-3", "10.0.0.3", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: exec,
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue after completing first step")
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
	if exec.called != 1 || exec.index != 0 {
		t.Fatalf("expected executor step 0 once, got calls=%d index=%d", exec.called, exec.index)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan == nil {
		t.Fatal("expected active plan to remain")
	}
	if got.Status.ActivePlan.Steps[0].Status != string(plan.StepStateCompleted) {
		t.Fatalf("expected first step completed, got %#v", got.Status.ActivePlan.Steps[0])
	}
	if got.Status.NextPodOrdinal != 5 {
		t.Fatalf("expected nextPodOrdinal to stay 5, got %d", got.Status.NextPodOrdinal)
	}
}

func TestReconcile_InvalidActiveDriftPlanStillExecutesWhenRunning(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := missingReplicaCluster(5)
	bad := missingReplicaDriftPlan("redis-4")
	bad.Steps[3].Params["masterPod"] = "redis-2"
	active, err := planToStatus(bad)
	if err != nil {
		t.Fatalf("planToStatus: %v", err)
	}
	cluster.Status.ActivePlan = active
	newPlan := missingReplicaDriftPlan("redis-5")
	fp := &recordingPlanner{plan: newPlan}
	exec := &recordingExecutor{}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-2", "10.0.0.2", true),
		vcPod("redis-3", "10.0.0.3", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: exec,
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue after completing first step")
	}
	if exec.called != 1 || exec.index != 0 {
		t.Fatalf("expected executor step 0 once, got calls=%d index=%d", exec.called, exec.index)
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan == nil {
		t.Fatal("expected active plan to remain")
	}
	if got.Status.ActivePlan.Steps[0].Status != string(plan.StepStateCompleted) {
		t.Fatalf("expected first step completed, got %#v", got.Status.ActivePlan.Steps[0])
	}
}

func TestReconcile_CompletedPlan_TopologyMismatchTriggersReplan(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ObservedGeneration = 1
	cluster.Status.TopologyObservedAt = metav1.Now()
	cluster.Status.ObservedNodes = apiObservedNodes(nil)
	cluster.Status.ActivePlan = completedPlan()
	fp := &recordingPlanner{err: errors.New("expected planner call")}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue after clearing drifted completed plan")
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call yet, got %d", fp.called)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan != nil {
		t.Fatalf("expected activePlan cleared, got %#v", got.Status.ActivePlan)
	}
	if !hasCondition(got.Status.Conditions, ConditionPlanned, metav1.ConditionTrue, "PlanCompleted") {
		t.Fatalf("expected Planned=True/PlanCompleted, got %#v", got.Status.Conditions)
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner call, got %d", fp.called)
	}
}

func TestReconcile_FailedPlan_TopologyMismatchTriggersReplan(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 2, Finalizers: []string{finalizer}},
		Spec:       api.RedisClusterSpec{Shards: 5, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
		Status: api.RedisClusterStatus{
			ObservedGeneration: 2,
			TopologyObservedAt: metav1.Now(),
			NextPodOrdinal:     10,
			Topology: &api.ClusterTopology{Shards: []api.ShardTopology{
				{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", Slots: "0-5460", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-1", Ready: true}}},
				{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-2", Slots: "5461-10922", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-3", Ready: true}}},
				{ID: "shard-2", Master: api.NodeTopology{Pod: "redis-4", Slots: "10923-16383", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-5", Ready: true}}},
			}},
			ActivePlan: &api.PlanStatus{
				ID:               "go-shard-scaleout-2",
				TargetGeneration: 2,
				Steps: []api.StepStatus{
					{ID: "ensure-redis-6", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateCompleted)},
					{ID: "ensure-redis-7", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateCompleted)},
					{ID: "ensure-redis-8", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateCompleted)},
					{ID: "ensure-redis-9", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateCompleted)},
					{ID: "verify", Action: string(plan.ActionVerifyCluster), Status: string(plan.StepStateFailed)},
				},
			},
		},
	}
	fp := &recordingPlanner{err: errors.New("expected planner call")}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{},
		ValidatePlan: validator.Validate,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue after retaining failed plan, got %#v", res)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan == nil || got.Status.ActivePlan.Status != string(plan.PlanStateFailed) {
		t.Fatalf("expected failed activePlan retained, got %#v", got.Status.ActivePlan)
	}
	if got.Status.NextPodOrdinal != 10 {
		t.Fatalf("expected nextPodOrdinal 10, got %d", got.Status.NextPodOrdinal)
	}
	if !hasCondition(got.Status.Conditions, ConditionReady, metav1.ConditionFalse, "PlanFailed") {
		t.Fatalf("expected Ready=False/PlanFailed, got %#v", got.Status.Conditions)
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
}

func TestReconcile_RetainedFailedPlanDoesNotRepeatPlanFailedEvent(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = failedPlan()
	rec := events.NewFakeRecorder(10)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: &recordingObserver{},
		Recorder:     rec,
		ValidatePlan: validator.Validate,
	}

	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	got := 0
	for {
		select {
		case ev := <-rec.Events:
			if strings.Contains(ev, "PlanFailed") {
				got++
			}
		default:
			if got != 0 {
				t.Fatalf("expected no retained PlanFailed events, got %d", got)
			}
			return
		}
	}
}
