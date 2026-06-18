package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/planner"
)

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
		Planner: planner.NoopPlanner{}, Executor: NoopExecutor{}, Validator: plan.NewValidator(),
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

	// 2nd reconcile: creates namespace, then planner fails -> phase Failed.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
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
	if got.Status.Phase != api.PhaseFailed {
		t.Errorf("expected phase Failed, got %q", got.Status.Phase)
	}
}

func TestReconcile_DeletesNamespaceOnDeletion(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	now := metav1.Now()
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "example",
			Generation:        1,
			DeletionTimestamp: &now,
			Finalizers:        []string{finalizer},
		},
		Spec: api.RedisClusterSpec{Shards: 1, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "1Gi"},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Validator: plan.NewValidator()}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}

	// 1st delete reconcile: deletes namespace, requeues.
	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("delete reconcile 1: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatal("expected requeueAfter while namespace terminates")
	}
	err = cl.Get(ctx, client.ObjectKey{Name: "example"}, &corev1.Namespace{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected namespace gone, got err=%v", err)
	}

	// 2nd reconcile: finalizer removed, cluster is deleted by the API server.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("delete reconcile 2: %v", err)
	}
	err = cl.Get(ctx, client.ObjectKey{Name: "example"}, &api.RedisCluster{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected cluster to be deleted after finalizer removal, got err=%v", err)
	}
}

type recordingObserver struct {
	called int
}

func (o *recordingObserver) ObserveTopology(_ context.Context, _ *api.RedisCluster) error {
	o.called++
	return nil
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

func runningPlan() *api.PlanStatus {
	return &api.PlanStatus{
		ID:    "plan",
		Steps: []api.StepStatus{{ID: "s1", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateRunning)}},
	}
}

func completedPlan() *api.PlanStatus {
	return &api.PlanStatus{
		ID:    "plan",
		Steps: []api.StepStatus{{ID: "s1", Action: string(plan.ActionVerifyCluster), Status: string(plan.StepStateCompleted)}},
	}
}

func failedPlan() *api.PlanStatus {
	return &api.PlanStatus{
		ID:    "plan",
		Steps: []api.StepStatus{{ID: "s1", Action: string(plan.ActionVerifyCluster), Status: string(plan.StepStateFailed)}},
	}
}

func TestReconcile_LazyRefreshSkipsWhenTopologyNil(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := &api.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1},
		Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: obs,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Second,
	}

	// First reconcile adds finalizer; second creates namespace and plans. Neither should observe.
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if obs.called != 0 {
		t.Fatalf("expected no lazy refresh before topology exists, got %d", obs.called)
	}
}

func TestReconcile_LazyRefreshSkipsWhenPlanRunning(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = runningPlan()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: obs,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Nanosecond,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if obs.called != 0 {
		t.Fatalf("expected no lazy refresh while plan is running, got %d", obs.called)
	}
}

func TestReconcile_LazyRefreshRunsWhenStale(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = completedPlan()
	cluster.Status.TopologyObservedAt = metav1.NewTime(time.Now().Add(-time.Hour))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: obs,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Second,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if obs.called != 1 {
		t.Fatalf("expected lazy refresh when stale, got %d", obs.called)
	}
}

func TestReconcile_LazyRefreshSkipsWhenFresh(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = completedPlan()
	cluster.Status.TopologyObservedAt = metav1.Now()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: obs,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if obs.called != 0 {
		t.Fatalf("expected no lazy refresh when fresh, got %d", obs.called)
	}
}

func TestReconcile_CompletedPlanRequeuesForRefresh(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = completedPlan()
	cluster.Status.TopologyObservedAt = metav1.Now()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: &recordingObserver{},
		TopologyRefreshInterval: 77 * time.Second, TopologyStaleThreshold: time.Hour,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 77*time.Second {
		t.Fatalf("expected RequeueAfter=77s, got %v", res.RequeueAfter)
	}
}

func TestReconcile_FailedPlanRequeuesForRefresh(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = failedPlan()
	cluster.Status.TopologyObservedAt = metav1.Now()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).WithStatusSubresource(&api.RedisCluster{}).Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: &recordingObserver{},
		TopologyRefreshInterval: 88 * time.Second, TopologyStaleThreshold: time.Hour,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 88*time.Second {
		t.Fatalf("expected RequeueAfter=88s, got %v", res.RequeueAfter)
	}
}

func TestReconcile_LazyRefreshForcedOnPodDelete(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", Ready: true}},
		{ID: "shard-1", Master: api.NodeTopology{Pod: "redis-1", Ready: true}},
	}}
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = completedPlan()
	cluster.Status.TopologyObservedAt = metav1.Now()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-1", "10.0.0.2", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: obs,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if obs.called != 1 {
		t.Fatalf("expected lazy refresh forced by pod-set drift, got %d", obs.called)
	}
}

func TestReconcile_LazyRefreshSkipsWhenPodSetMatches(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Status.Topology = &api.ClusterTopology{Shards: []api.ShardTopology{
		{ID: "shard-0", Master: api.NodeTopology{Pod: "redis-0", Ready: true}},
	}}
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ActivePlan = completedPlan()
	cluster.Status.TopologyObservedAt = metav1.Now()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Executor: NoopExecutor{},
		Validator: plan.NewValidator(), Observer: obs,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if obs.called != 0 {
		t.Fatalf("expected no refresh when pod set matches and topology fresh, got %d", obs.called)
	}
}

func TestPodSetDrifted(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)

	tests := []struct {
		name      string
		topo      *api.ClusterTopology
		pods      []client.Object
		wantDrift bool
	}{
		{
			name: "pod deleted from k8s",
			topo: &api.ClusterTopology{Shards: []api.ShardTopology{
				{Master: api.NodeTopology{Pod: "redis-0", Ready: true}},
				{Master: api.NodeTopology{Pod: "redis-1", Ready: true}},
			}},
			pods:      []client.Object{vcPod("redis-1", "10.0.0.2", true)},
			wantDrift: true,
		},
		{
			name: "pod ready status flipped",
			topo: &api.ClusterTopology{Shards: []api.ShardTopology{
				{Master: api.NodeTopology{Pod: "redis-0", Ready: true}},
			}},
			pods:      []client.Object{vcPod("redis-0", "10.0.0.1", false)},
			wantDrift: true,
		},
		{
			name: "new pod appeared in k8s",
			topo: &api.ClusterTopology{Shards: []api.ShardTopology{
				{Master: api.NodeTopology{Pod: "redis-0", Ready: true}},
			}},
			pods:      []client.Object{vcPod("redis-0", "10.0.0.1", true), vcPod("redis-1", "10.0.0.2", true)},
			wantDrift: true,
		},
		{
			name: "identical sets no drift",
			topo: &api.ClusterTopology{Shards: []api.ShardTopology{
				{Master: api.NodeTopology{Pod: "redis-0", Ready: true}},
				{Master: api.NodeTopology{Pod: "redis-1", Ready: true}},
			}},
			pods:      []client.Object{vcPod("redis-0", "10.0.0.1", true), vcPod("redis-1", "10.0.0.2", true)},
			wantDrift: false,
		},
		{
			name:      "nil topology no drift",
			topo:      nil,
			pods:      []client.Object{vcPod("redis-0", "10.0.0.1", true)},
			wantDrift: false,
		},
		{
			name:      "empty topology no pods no drift",
			topo:      &api.ClusterTopology{},
			pods:      nil,
			wantDrift: false,
		},
		{
			name: "replica pod missing from k8s",
			topo: &api.ClusterTopology{Shards: []api.ShardTopology{
				{Master: api.NodeTopology{Pod: "redis-0", Ready: true}, Replicas: []api.NodeTopology{{Pod: "redis-2", Ready: true}}},
			}},
			pods:      []client.Object{vcPod("redis-0", "10.0.0.1", true)},
			wantDrift: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &api.RedisCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "example"},
				Spec:       api.RedisClusterSpec{Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"},
				Status:     api.RedisClusterStatus{Topology: tc.topo},
			}
			objs := []client.Object{cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}}
			objs = append(objs, tc.pods...)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(&api.RedisCluster{}).Build()
			r := &RedisClusterReconciler{Client: cl, Scheme: scheme}

			got := r.podSetDrifted(ctx, cluster)
			if got != tc.wantDrift {
				t.Fatalf("podSetDrifted: got %v want %v", got, tc.wantDrift)
			}
		})
	}
}
