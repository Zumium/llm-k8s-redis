package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
)

func (r *RedisClusterReconciler) ensureNamespace(ctx context.Context, cluster *v1alpha1.RedisCluster) error {
	logger := log.FromContext(ctx).WithValues("namespace", cluster.Name)
	var ns corev1.Namespace
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name}, &ns)
	if err == nil {
		logger.Info("namespace already exists")
		return nil
	}
	if !apierrors.IsNotFound(err) {
		logger.Error(err, "get namespace failed")
		return err
	}

	logger.Info("creating namespace")
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: cluster.Name,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "redis-cluster-controller",
				"redis.example.com/cluster":    cluster.Name,
			},
		},
	}
	if err := ctrl.SetControllerReference(cluster, nsObj, r.Scheme); err != nil {
		return fmt.Errorf("set namespace owner reference: %w", err)
	}
	if err := r.Create(ctx, nsObj); err != nil {
		logger.Error(err, "create namespace failed")
		return err
	}
	logger.Info("namespace created")
	return nil
}

func (r *RedisClusterReconciler) reconcileDelete(ctx context.Context, cluster *v1alpha1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	logger.Info("delete reconcile started")
	if !controllerutil.ContainsFinalizer(cluster, finalizer) {
		logger.Info("finalizer already absent")
		return ctrl.Result{}, nil
	}

	var ns corev1.Namespace
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name}, &ns)
	if err == nil {
		if ns.DeletionTimestamp.IsZero() {
			logger.Info("deleting owned namespace", "namespace", cluster.Name)
			if derr := r.Delete(ctx, &ns); derr != nil && !apierrors.IsNotFound(derr) {
				logger.Error(derr, "delete namespace failed", "namespace", cluster.Name)
				return ctrl.Result{}, derr
			}
		}
		logger.Info("waiting for namespace deletion", "namespace", cluster.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if !apierrors.IsNotFound(err) {
		logger.Error(err, "get namespace during delete failed", "namespace", cluster.Name)
		return ctrl.Result{}, err
	}

	logger.Info("owned namespace gone, removing finalizer", "namespace", cluster.Name)
	controllerutil.RemoveFinalizer(cluster, finalizer)
	if err := r.Update(ctx, cluster); err != nil {
		logger.Error(err, "remove finalizer failed")
		return ctrl.Result{}, err
	}
	logger.Info("finalizer removed")
	return ctrl.Result{}, nil
}

func (r *RedisClusterReconciler) finish(ctx context.Context, cluster *v1alpha1.RedisCluster, res ctrl.Result, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	start := time.Now()
	logger.Info("status update started", "requeue", res.Requeue, "requeueAfter", res.RequeueAfter, "incomingError", err)
	if uerr := r.Status().Update(ctx, cluster); uerr != nil && err == nil {
		if apierrors.IsConflict(uerr) {
			logger.Info("status update conflict", "duration", time.Since(start), "requeueAfter", statusConflictRequeueAfter)
			return ctrl.Result{RequeueAfter: statusConflictRequeueAfter}, nil
		}
		logger.Error(uerr, "status update failed", "duration", time.Since(start))
		return res, uerr
	}
	logger.Info("status update finished", "duration", time.Since(start), "skippedError", err != nil)
	return res, err
}

func (r *RedisClusterReconciler) event(cluster *v1alpha1.RedisCluster, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(cluster, nil, "Warning", reason, "Reconcile", "%s", msg)
	}
}
