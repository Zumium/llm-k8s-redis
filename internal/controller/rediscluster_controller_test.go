package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
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
		Planner: planner.NoopPlanner{}, ValidatePlan: plan.NewValidator().Validate,
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
	replica := readyPodWithIP(cluster, "redis-1", "10.0.0.1")
	master := readyPodWithIP(cluster, "redis-2", "10.0.0.2")
	fc := &fakeRedisClient{
		clusterInfo:  func() (string, error) { return "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n", nil },
		clusterNodes: func() (string, error) { return clusterOK(), nil },
	}
	fp := &recordingPlanner{plan: validCreate2x1Plan()}
	cluster.Status.ObservedNodes = apiObservedNodes([]plan.ObservedNode{
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: vcMaster0ID, Role: "master", Slots: "0-8191", Ready: true, Flags: []string{"master"}, LinkState: "connected"},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: vcMaster1ID, Role: "master", Slots: "8192-16383", Ready: true, Flags: []string{"master"}, LinkState: "connected"},
		{RedisSeen: true, NodeID: vcReplica0ID, Role: "replica", MasterID: vcMaster0ID, MasterPod: "redis-1", Flags: []string{"slave"}, LinkState: "connected"},
		{RedisSeen: true, NodeID: vcReplica1ID, Role: "replica", MasterID: vcMaster1ID, MasterPod: "redis-2", Flags: []string{"slave"}, LinkState: "connected"},
	})
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}, replica, master).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	exec := &ActionExecutor{Client: cl, Scheme: scheme, RedisFactory: fakeFactory(fc)}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme,
		Planner: fp, Driver: exec, ValidatePlan: plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	nodes := fp.lastReq.ObservedState.Nodes
	if len(nodes) != 4 {
		t.Fatalf("expected 4 observed nodes, got %#v", nodes)
	}
	if nodes[0].Pod != "redis-1" || !nodes[0].PodExists || !nodes[0].RedisSeen || nodes[0].NodeID != vcMaster0ID || nodes[0].Role != "master" {
		t.Fatalf("unexpected joined node: %#v", nodes[0])
	}
	if nodes[1].Pod != "redis-2" || !nodes[1].PodExists || !nodes[1].RedisSeen || nodes[1].NodeID != vcMaster1ID || !nodes[1].Ready {
		t.Fatalf("unexpected joined node: %#v", nodes[1])
	}
	if nodes[2].PodExists || !nodes[2].RedisSeen || nodes[2].NodeID != vcReplica0ID || nodes[2].Role != "replica" || nodes[2].MasterID != vcMaster0ID {
		t.Fatalf("unexpected redis-only node: %#v", nodes[2])
	}
}

func TestReconcile_RetriesRejectedPlanWithValidatorFeedback(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	cluster.Status.ObservedNodes = apiObservedNodes(nil)
	badPlan := &plan.Plan{DSLVersion: plan.DSLVersion, PlanID: "bad", TargetGeneration: 1, Steps: []plan.Step{{ID: "bad", Action: plan.ActionVerifyCluster}}}
	goodPlan := validCreate2x1Plan()
	fp := &recordingPlanner{plans: []*plan.Plan{badPlan, goodPlan}}
	validator := &recordingValidator{errors: []error{errors.New("validator says no")}}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme,
		Planner: fp, Driver: &recordingObserver{}, ValidatePlan: validator.Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
		PlanValidationRetries: 1,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if fp.called != 2 {
		t.Fatalf("planner calls = %d", fp.called)
	}
	if len(fp.reqs[1].ValidationFeedback) != 1 || fp.reqs[1].ValidationFeedback[0].RejectedPlan.PlanID != "bad" {
		t.Fatalf("feedback = %#v", fp.reqs[1].ValidationFeedback)
	}
	if fp.reqs[1].ValidationFeedback[0].Error != "validator says no" {
		t.Fatalf("feedback error = %#v", fp.reqs[1].ValidationFeedback)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, types.NamespacedName{Name: "example"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.ActivePlan == nil || got.Status.ActivePlan.ID != "create-1" {
		t.Fatalf("activePlan = %#v", got.Status.ActivePlan)
	}
}

func TestReconcile_RechecksObservedNodesBeforeValidationRetry(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := clusterWithTopology()
	cluster.Finalizers = []string{finalizer}
	mismatch := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
	}
	match := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
	}
	cluster.Status.ObservedNodes = apiObservedNodes(mismatch)
	fp := &recordingPlanner{plan: &plan.Plan{DSLVersion: plan.DSLVersion, PlanID: "bad", TargetGeneration: 1, Steps: []plan.Step{{ID: "bad", Action: plan.ActionVerifyCluster}}}}
	validator := &recordingValidator{err: errors.New("validator says no")}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme,
		Planner: fp, Driver: &recordingObserver{nodeSets: [][]plan.ObservedNode{mismatch, match}}, ValidatePlan: validator.Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
		PlanValidationRetries: 1,
	}

	r.Driver = &recordingObserver{nodeSets: [][]plan.ObservedNode{mismatch, match}}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
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
	if !hasCondition(got.Status.Conditions, ConditionReady, metav1.ConditionTrue, "ClusterReady") {
		t.Fatalf("expected Ready=True/ClusterReady, got %#v", got.Status.Conditions)
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner call, got %d", fp.called)
	}
}

func TestObservedNodesMatchSpecRejectsUnhealthyNodes(t *testing.T) {
	base := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
		{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
	}
	spec := toClusterSpec(testCluster())
	if !observedNodesMatchSpec(base, spec) {
		t.Fatal("healthy observed nodes should match spec")
	}
	cases := []struct {
		name string
		pod  string
		edit func(*plan.ObservedNode)
	}{
		{name: "fail", pod: "redis-0", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail"} }},
		{name: "fail?", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail?"} }},
		{name: "handshake", pod: "redis-0", edit: func(n *plan.ObservedNode) { n.Flags = []string{"handshake"} }},
		{name: "noaddr", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.Flags = []string{"noaddr"} }},
		{name: "disconnected master", pod: "redis-0", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }},
		{name: "disconnected replica", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }},
		{name: "not ready", pod: "redis-1", edit: func(n *plan.ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := append([]plan.ObservedNode(nil), base...)
			for i := range nodes {
				if nodes[i].Pod == tc.pod {
					tc.edit(&nodes[i])
				}
			}
			if observedNodesMatchSpec(nodes, spec) {
				t.Fatal("unhealthy observed nodes should not match spec")
			}
		})
	}
}

func TestWaitingForRedisFailover(t *testing.T) {
	base := []plan.ObservedNode{
		{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
		{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Ready: true},
	}
	spec := toClusterSpec(testCluster())
	cases := []struct {
		name string
		edit func(*plan.ObservedNode)
		want bool
	}{
		{name: "pod gone", edit: func(n *plan.ObservedNode) { n.PodExists = false }, want: true},
		{name: "deleting", edit: func(n *plan.ObservedNode) { n.Deleting = true }, want: true},
		{name: "fail", edit: func(n *plan.ObservedNode) { n.Flags = []string{"fail"} }, want: true},
		{name: "disconnected", edit: func(n *plan.ObservedNode) { n.LinkState = "disconnected" }},
		{name: "not ready", edit: func(n *plan.ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := append([]plan.ObservedNode(nil), base...)
			tc.edit(&nodes[0])
			if got := waitingForRedisFailover(nodes, spec); got != tc.want {
				t.Fatalf("waitingForRedisFailover() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcile_PlanValidationRetriesZeroRejectsOnce(t *testing.T) {
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
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
		PlanValidationRetries: 0,
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

func hasCondition(conditions []metav1.Condition, typ string, status metav1.ConditionStatus, reason string) bool {
	for _, c := range conditions {
		if c.Type == typ && c.Status == status && c.Reason == reason {
			return true
		}
	}
	return false
}

func TestFinish_StatusConflictRequeuesWithoutError(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RedisCluster{}).Build()
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "redis.example.com", Resource: "redisclusters"}, "example", errors.New("stale resourceVersion"))
	r := &RedisClusterReconciler{Client: statusUpdateErrorClient{Client: base, err: conflict}, APIReader: base}

	res, err := r.finish(ctx, &api.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "example"}}, ctrl.Result{Requeue: true}, nil)
	if err != nil {
		t.Fatalf("expected no error for status conflict, got %v", err)
	}
	if res.Requeue {
		t.Fatal("expected conflict path to use RequeueAfter instead of immediate requeue")
	}
	if res.RequeueAfter != statusConflictRequeueAfter {
		t.Fatalf("expected RequeueAfter=%v, got %v", statusConflictRequeueAfter, res.RequeueAfter)
	}
}

func TestFinish_StatusNonConflictErrorIsReturned(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RedisCluster{}).Build()
	updateErr := errors.New("status store unavailable")
	r := &RedisClusterReconciler{Client: statusUpdateErrorClient{Client: base, err: updateErr}}

	_, err := r.finish(ctx, &api.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "example"}}, ctrl.Result{}, nil)
	if !errors.Is(err, updateErr) {
		t.Fatalf("expected status update error, got %v", err)
	}
}

func TestFinish_PreservesOriginalErrorWhenStatusConflicts(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RedisCluster{}).Build()
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "redis.example.com", Resource: "redisclusters"}, "example", errors.New("stale resourceVersion"))
	originalErr := errors.New("executor failed")
	r := &RedisClusterReconciler{Client: statusUpdateErrorClient{Client: base, err: conflict}}

	want := ctrl.Result{RequeueAfter: 7 * time.Second}
	res, err := r.finish(ctx, &api.RedisCluster{ObjectMeta: metav1.ObjectMeta{Name: "example"}}, want, originalErr)
	if !errors.Is(err, originalErr) {
		t.Fatalf("expected original error, got %v", err)
	}
	if res != want {
		t.Fatalf("expected result %+v, got %+v", want, res)
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
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, ValidatePlan: plan.NewValidator().Validate}
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
	called   int
	nodes    []plan.ObservedNode
	nodeSets [][]plan.ObservedNode
}

func (o *recordingObserver) ObserveTopology(_ context.Context, _ *api.RedisCluster) error {
	o.called++
	return nil
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
		out = append(out, plan.ObservedNode{Pod: sh.Master.Pod, PodExists: true, RedisSeen: true, NodeID: sh.Master.NodeID, Role: "master", Slots: sh.Master.Slots, Ready: sh.Master.Ready})
		for _, r := range sh.Replicas {
			out = append(out, plan.ObservedNode{Pod: r.Pod, PodExists: true, RedisSeen: true, NodeID: r.NodeID, Role: "replica", MasterPod: sh.Master.Pod, Ready: r.Ready})
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

func (e *recordingExecutor) ObserveTopology(_ context.Context, _ *api.RedisCluster) error {
	return nil
}

func (e *recordingExecutor) CollectObservedNodes(_ context.Context, _ *api.RedisCluster) ([]plan.ObservedNode, error) {
	return nil, nil
}

type recordingValidator struct {
	called int
	err    error
	errors []error
}

func (v *recordingValidator) Validate(_ *plan.Plan, _ any) error {
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

func TestExecuteNextStep_PersistsUpdatedParams(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	active := runningPlan()
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{outcome: StepOutcome{
		Status:  plan.StepStateRunning,
		Message: "waiting",
	}, params: map[string]any{"foo": "bar", "stable": 1}}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	if _, err := r.executeNextStep(ctx, cluster, active); err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(active.Steps[0].Params.Raw, &got); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if got["foo"] != "bar" || got["stable"].(float64) != 1 {
		t.Fatalf("unexpected persisted params: %#v", got)
	}
}

func TestExecuteNextStep_NeedsRepairSupersedesPlan(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	active := runningPlan()
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{outcome: StepOutcome{
		Status:        plan.StepStateFailed,
		Message:       "repair needed",
		SupersedePlan: true,
	}}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	res, err := r.executeNextStep(ctx, cluster, active)
	if err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue for replanning")
	}
	if active.Status != string(plan.PlanStateSuperseded) {
		t.Fatalf("expected superseded plan, got %#v", active)
	}
	if active.Steps[0].Status != string(plan.StepStateRunning) {
		t.Fatalf("expected running step while replanning, got %#v", active.Steps[0])
	}
}

func TestExecuteNextStep_RunsOnlyCurrentMigrateStep(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	p := &plan.Plan{PlanID: "migrate", TargetGeneration: 1, Steps: []plan.Step{
		{ID: "m0", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-6", "slots": "0-1"}},
		{ID: "m1", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-2", "targetPod": "redis-8", "slots": "2-3"}},
		{ID: "verify", Action: plan.ActionVerifyCluster, Params: map[string]any{}},
	}}
	active, err := planToStatus(p)
	if err != nil {
		t.Fatalf("planToStatus: %v", err)
	}
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	res, err := r.executeNextStep(ctx, cluster, active)
	if err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected requeue after completed migrate batch")
	}
	exec.mu.Lock()
	got := append([]int{}, exec.indices...)
	exec.mu.Unlock()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected only step 0 to run, got %v", got)
	}
	if active.Steps[0].Status != string(plan.StepStateCompleted) || active.Steps[1].Status != string(plan.StepStatePending) || active.Steps[2].Status != string(plan.StepStatePending) {
		t.Fatalf("unexpected statuses: %#v", active.Steps)
	}
}

func TestExecuteNextStep_DoesNotBatchConflictingMigrateSteps(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	p := &plan.Plan{PlanID: "migrate", TargetGeneration: 1, Steps: []plan.Step{
		{ID: "m0", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-6", "slots": "0-1"}},
		{ID: "m1", Action: plan.ActionMigrateSlots, Params: map[string]any{"namespace": "example", "sourcePod": "redis-0", "targetPod": "redis-8", "slots": "2-3"}},
	}}
	active, err := planToStatus(p)
	if err != nil {
		t.Fatalf("planToStatus: %v", err)
	}
	cluster.Status.ActivePlan = active
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&api.RedisCluster{}).Build()
	exec := &recordingExecutor{}
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Driver: exec}

	if _, err := r.executeNextStep(ctx, cluster, active); err != nil {
		t.Fatalf("executeNextStep: %v", err)
	}
	exec.mu.Lock()
	got := append([]int{}, exec.indices...)
	exec.mu.Unlock()
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected only step 0 to run, got %v", got)
	}
	if active.Steps[0].Status != string(plan.StepStateCompleted) || active.Steps[1].Status != string(plan.StepStatePending) {
		t.Fatalf("unexpected statuses: %#v", active.Steps)
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
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: obs,
		ValidatePlan:            plan.NewValidator().Validate,
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
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
		vcPod("redis-3", "10.0.0.4", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: obs,
		ValidatePlan:            plan.NewValidator().Validate,
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
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
		vcPod("redis-0", "10.0.0.1", true),
		vcPod("redis-1", "10.0.0.2", true),
		vcPod("redis-2", "10.0.0.3", true),
		vcPod("redis-3", "10.0.0.4", true),
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	obs := &recordingObserver{}
	r := &RedisClusterReconciler{
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: obs,
		ValidatePlan:            plan.NewValidator().Validate,
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
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: obs,
		ValidatePlan:            plan.NewValidator().Validate,
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
		ValidatePlan:            plan.NewValidator().Validate,
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
		ValidatePlan:            plan.NewValidator().Validate,
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

func TestReconcile_FailedPlanClearsWhenTopologyMatchesSpec(t *testing.T) {
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: 99 * time.Second, TopologyStaleThreshold: time.Hour,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 99*time.Second {
		t.Fatalf("expected RequeueAfter=99s, got %v", res.RequeueAfter)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan != nil {
		t.Fatalf("expected activePlan to be cleared, got %#v", got.Status.ActivePlan)
	}
	if got.Status.ObservedGeneration != 1 {
		t.Fatalf("expected observedGeneration 1, got %d", got.Status.ObservedGeneration)
	}
	if got.Status.NextPodOrdinal != 10 {
		t.Fatalf("expected nextPodOrdinal 10, got %d", got.Status.NextPodOrdinal)
	}
	if !hasCondition(got.Status.Conditions, ConditionReady, metav1.ConditionTrue, "ClusterReady") {
		t.Fatalf("expected Ready=True/ClusterReady, got %#v", got.Status.Conditions)
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
	if got.Status.ActivePlan == nil || got.Status.ActivePlan.Status != string(plan.PlanStateSuperseded) {
		t.Fatalf("expected activePlan to be superseded, got %#v", got.Status.ActivePlan)
	}
	if got.Status.ObservedGeneration != 1 {
		t.Fatalf("expected observedGeneration to remain 1, got %d", got.Status.ObservedGeneration)
	}
	if !hasCondition(got.Status.Conditions, ConditionReady, metav1.ConditionFalse, "PlanSuperseded") {
		t.Fatalf("expected Ready=False/PlanSuperseded, got %#v", got.Status.Conditions)
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
	if got.Status.ActivePlan == nil || got.Status.ActivePlan.Status != string(plan.PlanStateSuperseded) {
		t.Fatalf("expected superseded activePlan, got %#v", got.Status.ActivePlan)
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: 42 * time.Second, TopologyStaleThreshold: time.Hour,
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner to be called once, got %d", fp.called)
	}
}

func TestFailedPlanMessage(t *testing.T) {
	active := &api.PlanStatus{
		ID:               "repair-missing-replica-and-cleanup-001",
		Status:           string(plan.PlanStateRunning),
		TargetGeneration: 1,
		CurrentStep:      "replicate",
		Steps: []api.StepStatus{
			{ID: "ensure", Action: string(plan.ActionEnsureNode), Status: string(plan.StepStateCompleted)},
			{ID: "replicate", Action: string(plan.ActionReplicateNode), Status: string(plan.StepStateFailed), Message: "replica rejected master"},
		},
	}
	if got := failedPlanMessage(active); got != "replica rejected master" {
		t.Fatalf("message = %q", got)
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner despite matching pod count, got %d calls", fp.called)
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
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: obs,
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if obs.called != 1 {
		t.Fatalf("expected lazy refresh forced by pod-set drift, got %d", obs.called)
	}
}

func TestReconcile_MissingReplicaRequestsDriftPlan(t *testing.T) {
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
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: plan.NewValidator().Validate,
		Driver:                  &recordingObserver{nodes: nodes},
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected no explicit requeue after accepting drift plan, got %#v", res)
	}
	if fp.called != 1 {
		t.Fatalf("expected planner call, got %d", fp.called)
	}
	if len(fp.lastReq.ObservedState.Nodes) != 4 {
		t.Fatalf("observed nodes = %#v", fp.lastReq.ObservedState.Nodes)
	}
	var got api.RedisCluster
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.ActivePlan == nil {
		t.Fatalf("expected active plan, got %#v", got.Status.ActivePlan)
	}
	if got.Status.NextPodOrdinal != 5 {
		t.Fatalf("expected nextPodOrdinal 5, got %d", got.Status.NextPodOrdinal)
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
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: plan.NewValidator().Validate,
		Driver: &recordingObserver{nodes: []plan.ObservedNode{
			{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
			{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-2", Ready: true},
			{Pod: "redis-2", PodExists: false, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Flags: []string{"fail"}},
			{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		}},
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: plan.NewValidator().Validate,
		Driver: &recordingObserver{nodes: []plan.ObservedNode{
			{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191", Ready: true},
			{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-2", Ready: true},
			{Pod: "redis-2", PodExists: false, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8192-16383", Flags: []string{"fail"}},
			{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		}},
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
		Client: cl, Scheme: scheme, Planner: fp, ValidatePlan: plan.NewValidator().Validate,
		Driver: &recordingObserver{nodes: []plan.ObservedNode{
			{Pod: "redis-0", PodExists: true, RedisSeen: true, NodeID: "master-0", Role: "master", Slots: "0-8191,[8192->-master-2]", Ready: true},
			{Pod: "redis-1", PodExists: true, RedisSeen: true, NodeID: "replica-1", Role: "replica", MasterPod: "redis-0", Ready: true},
			{Pod: "redis-2", PodExists: true, RedisSeen: true, NodeID: "master-2", Role: "master", Slots: "8193-16383", Ready: true},
			{Pod: "redis-3", PodExists: true, RedisSeen: true, NodeID: "replica-3", Role: "replica", MasterPod: "redis-2", Ready: true},
		}},
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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

func TestReconcile_FreshActivePlanSkipsStalePlanning(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	stale := missingReplicaCluster(5)
	fresh := stale.DeepCopy()
	fresh.Status.ActivePlan = runningPlan()
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		fresh,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}},
	).WithStatusSubresource(&api.RedisCluster{}).Build()
	cl := &staleFirstGetClient{Client: base, stale: stale}
	fp := &recordingPlanner{err: errors.New("unexpected planner call")}
	exec := &recordingExecutor{}
	r := &RedisClusterReconciler{
		Client: cl, APIReader: base, Scheme: scheme, Planner: fp, Driver: exec,
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
		Client: cl, Scheme: scheme, Planner: planner.NoopPlanner{}, Driver: obs,
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if obs.called != 0 {
		t.Fatalf("expected no refresh when pod set matches and topology fresh, got %d", obs.called)
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
	if !hasCondition(got.Status.Conditions, ConditionReady, metav1.ConditionFalse, "Replanning") {
		t.Fatalf("expected Ready=False/Replanning, got %#v", got.Status.Conditions)
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
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}})
	if err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if res.Requeue || res.RequeueAfter != time.Minute {
		t.Fatalf("expected refresh requeue after retaining failed plan, got %#v", res)
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
		Recorder:                rec,
		ValidatePlan:            plan.NewValidator().Validate,
		TopologyRefreshInterval: time.Minute, TopologyStaleThreshold: time.Hour,
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
