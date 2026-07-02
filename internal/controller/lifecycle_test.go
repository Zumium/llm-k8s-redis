package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/validator"
)

func TestFinish_StatusConflictRequeuesWithoutError(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RedisCluster{}).Build()
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "redis.example.com", Resource: "redisclusters"}, "example", errors.New("stale resourceVersion"))
	r := &RedisClusterReconciler{Client: statusUpdateErrorClient{Client: base, err: conflict}}

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

func TestEvent_PreservesPercentMessage(t *testing.T) {
	recorder := events.NewFakeRecorder(1)
	r := &RedisClusterReconciler{Recorder: recorder}

	r.event(testCluster(), "Reason", "contains %s")

	got := <-recorder.Events
	if !strings.Contains(got, "contains %s") {
		t.Fatalf("event = %q", got)
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
		WithObjects(cluster, managedTestNamespace()).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, ValidatePlan: validator.Validate}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}

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

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("delete reconcile 2: %v", err)
	}
	err = cl.Get(ctx, client.ObjectKey{Name: "example"}, &api.RedisCluster{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected cluster to be deleted after finalizer removal, got err=%v", err)
	}
}

func TestReconcile_RejectsUnmanagedExistingNamespace(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	cluster := testCluster()
	cluster.Finalizers = []string{finalizer}
	fp := &recordingPlanner{}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "example"}}).
		WithStatusSubresource(&api.RedisCluster{}).
		Build()
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, Planner: fp, Driver: &recordingObserver{}, ValidatePlan: validator.Validate}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err == nil {
		t.Fatal("expected unmanaged namespace collision error")
	}
	if fp.called != 0 {
		t.Fatalf("expected no planner call, got %d", fp.called)
	}
}

func TestReconcile_DeleteLeavesUnmanagedNamespace(t *testing.T) {
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
	r := &RedisClusterReconciler{Client: cl, Scheme: scheme, ValidatePlan: validator.Validate}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "example"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKey{Name: "example"}, &corev1.Namespace{}); err != nil {
		t.Fatalf("expected unmanaged namespace to remain: %v", err)
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
