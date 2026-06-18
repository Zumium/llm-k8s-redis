package controller

import (
	"context"
	"testing"

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
		Planner: NoopPlanner{}, Executor: NoopExecutor{}, Validator: plan.NewValidator(),
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
